"""
Feishu / Lark platform adapter.

Uses the official ``lark-oapi`` SDK in WebSocket long-connection mode
(``lark_oapi.ws.Client``), so no public IP or reverse proxy is required.

Supports:
  - DM and group chats (group @-mention of the bot)
  - Thread / topic replies (``root_id`` is mapped to ``thread_id``)
  - Streaming edit (``edit_message`` via ``im/v1/messages/:id`` PATCH)
  - Replying to an existing message (``reply_to``)

Requirements::

    pip install lark-oapi>=1.3

Configuration (YAML or env vars)::

    FEISHU_APP_ID=cli_xxxxxxxxxxxxxxxx
    FEISHU_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    FEISHU_DOMAIN=https://open.feishu.cn          # or https://open.larksuite.com
    FEISHU_ALLOWED_USERS=ou_xxx,ou_yyy            # optional allowlist
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from agent_gateway.adapters.feishu_cards import (
    ToolCardBuilder,
    ThrottledCardPatcher,
)
from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.message import (
    ChatType,
    MessageEvent,
    MessageSource,
    MessageType,
    SendResult,
)
from agent_gateway.core.registry import PlatformEntry, registry

logger = logging.getLogger(__name__)


def _check_feishu_deps() -> bool:
    try:
        import lark_oapi  # noqa: F401

        return True
    except ImportError:
        return False


@dataclass
class _FeishuToolRound:
    """Per-round tool-card state, returned by :meth:`begin_tool_round` as the
    opaque handle the runner threads through the ``tool_round_*`` hooks."""

    chat_id: str
    reply_to: Optional[str]
    metadata: Optional[dict[str, Any]]
    builder: ToolCardBuilder
    patcher: ThrottledCardPatcher
    round_start: float
    start_times: dict[str, float] = field(default_factory=dict)
    any_failed: bool = False
    finished: bool = False


class FeishuAdapter(BasePlatformAdapter):
    """Feishu / Lark adapter built on top of ``lark-oapi``.

    The WebSocket client runs in its own daemon thread; inbound events are
    marshalled back onto the asyncio event loop via
    :func:`asyncio.run_coroutine_threadsafe`.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        extra = config.get("extra", {}) if isinstance(config.get("extra"), dict) else {}

        self._app_id: str = (
            config.get("token")
            or extra.get("app_id")
            or os.getenv("FEISHU_APP_ID", "")
        ).strip()
        self._app_secret: str = (
            extra.get("app_secret") or os.getenv("FEISHU_APP_SECRET", "")
        ).strip()
        self._domain: str = (
            extra.get("domain") or os.getenv("FEISHU_DOMAIN", "https://open.feishu.cn")
        ).strip()

        self._client: Any = None  # lark_oapi.Client (HTTP API)
        self._ws: Any = None  # lark_oapi.ws.Client
        self._ws_thread: Optional[threading.Thread] = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_connected: bool = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._ws_fatal_error: Optional[str] = None

        # CardKit streaming state
        self._card_ids: dict[str, str] = {}   # message_id -> card_id
        self._card_seq: dict[str, int] = {}    # card_id -> sequence counter
        self._cardkit_available: Optional[bool] = None  # None = untested

        self._name = "Feishu"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        if not self._app_id or not self._app_secret:
            self._set_fatal_error(
                "no_credentials",
                "FEISHU_APP_ID / FEISHU_APP_SECRET not set",
                retryable=False,
            )
            return False

        try:
            import lark_oapi as lark
            from lark_oapi.ws import Client as WsClient
        except ImportError as exc:
            self._set_fatal_error("missing_dep", str(exc), retryable=False)
            return False

        # Capture the running event loop so the ws-thread callback can
        # schedule coroutines back onto it.
        self._main_loop = asyncio.get_running_loop()

        try:
            dispatcher = (
                lark.EventDispatcherHandler.builder("", "")
                .register_p2_im_message_receive_v1(self._on_message_event)
                .build()
            )

            self._client = (
                lark.Client.builder()
                .app_id(self._app_id)
                .app_secret(self._app_secret)
                .domain(self._domain)
                .log_level(lark.LogLevel.WARNING)
                .build()
            )

            self._ws = WsClient(
                app_id=self._app_id,
                app_secret=self._app_secret,
                event_handler=dispatcher,
                domain=self._domain,
                auto_reconnect=True,
                log_level=lark.LogLevel.WARNING,
            )

            self._ws.on_reconnecting = self._on_ws_reconnecting
            self._ws.on_reconnected = self._on_ws_reconnected

            self._ws_thread = threading.Thread(
                target=self._run_ws_client,
                name="feishu-ws",
                daemon=True,
            )
            self._ws_thread.start()
            self._ws_connected = True

            self._watchdog_task = asyncio.create_task(self._health_watchdog())

            self._mark_connected()
            logger.info("[Feishu] WebSocket client started for app %s", self._app_id)
            return True

        except Exception as exc:
            logger.exception("[Feishu] connect() failed")
            self._set_fatal_error("connect_failed", str(exc))
            return False

    async def disconnect(self) -> None:
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            self._watchdog_task = None
        self._ws_connected = False
        # lark_ws_client does not expose a clean stop(); the daemon thread
        # exits when the process shuts down.
        self._ws = None
        self._client = None
        self._main_loop = None
        self._mark_disconnected()
        logger.info("[Feishu] Disconnected.")

    def _on_ws_reconnecting(self) -> None:
        logger.warning("[Feishu] WebSocket disconnected, attempting reconnect...")
        self._ws_connected = False

    def _on_ws_reconnected(self) -> None:
        logger.info("[Feishu] WebSocket reconnected successfully")
        self._ws_connected = True

    def _run_ws_client(self) -> None:
        """Run ``ws.Client.start()`` with exception capture.

        The SDK caches ``asyncio.get_event_loop()`` at import time as a
        module-level ``loop`` variable.  When running under uvicorn, that
        captures the main thread's already-running loop.  The daemon thread
        then fails with ``RuntimeError: This event loop is already running``
        when ``start()`` calls ``loop.run_until_complete()``.

        Fix: create a dedicated event loop for this thread and patch the
        SDK module before calling ``start()``.
        """
        try:
            import lark_oapi.ws.client as ws_mod

            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            ws_mod.loop = new_loop

            logger.info("[Feishu] WS thread starting (domain=%s)", self._domain)
            self._ws.start()
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.error("[Feishu] WS thread crashed: %s", error_msg)
            self._ws_connected = False
            self._ws_fatal_error = error_msg
        else:
            logger.warning("[Feishu] WS thread exited normally (unexpected)")
            self._ws_connected = False
            self._ws_fatal_error = "WS thread exited without error"

    async def _health_watchdog(self) -> None:
        """Periodically check if the WS thread is alive.

        The ``lark_oapi.ws.Client`` runs in a daemon thread with
        ``auto_reconnect=True``.  If reconnection fails permanently (e.g.
        revoked credentials), the thread exits silently.  This watchdog
        detects that and marks the adapter as fatally errored so the UI
        reflects the true state.
        """
        try:
            first_check = True
            while self._running:
                await asyncio.sleep(5 if first_check else 30)
                first_check = False
                if not self._running:
                    break
                if self._ws_thread is not None and not self._ws_thread.is_alive():
                    detail = self._ws_fatal_error or "unknown"
                    logger.error("[Feishu] WebSocket thread died: %s", detail)
                    self._ws_connected = False
                    self._set_fatal_error(
                        "ws_thread_dead",
                        f"WebSocket connection failed: {detail}",
                        retryable=True,
                    )
                    break
                if not self._ws_connected:
                    logger.debug("[Feishu] WS not yet connected (reconnecting...)")
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Outbound: send / edit
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="Not connected")

        # Try CardKit streaming card for new messages (not replies)
        if not reply_to and self._cardkit_available is not False:
            result = await self._send_cardkit_card(chat_id, content, metadata)
            if result.success:
                return result
            if self._cardkit_available is None:
                logger.warning(
                    "[Feishu] CardKit unavailable (%s) — falling back to plain text. "
                    "Add cardkit:card:write and cardkit:card permissions for streaming cards.",
                    result.error,
                )
                self._cardkit_available = False

        return await self._send_plain_text(chat_id, content, reply_to, metadata)

    async def _send_cardkit_card(
        self,
        chat_id: str,
        content: str,
        metadata: Optional[dict[str, Any]],
    ) -> SendResult:
        """Create a streaming card and send it as a message."""
        try:
            from lark_oapi.api.cardkit.v1 import (
                CreateCardRequest,
                CreateCardRequestBody,
            )
            from lark_oapi.api.im.v1 import (
                CreateMessageRequest,
                CreateMessageRequestBody,
            )

            card_data = json.dumps({
                "schema": "2.0",
                "config": {
                    "streaming_mode": True,
                    "summary": {"content": content[:80] if content else "..."},
                },
                "body": {
                    "elements": [{
                        "tag": "markdown",
                        "element_id": "main",
                        "content": content or " ",
                    }],
                },
            })

            card_req = (
                CreateCardRequest.builder()
                .request_body(
                    CreateCardRequestBody.builder()
                    .type("card")
                    .data(card_data)
                    .build()
                )
                .build()
            )
            card_resp = await asyncio.to_thread(
                self._client.cardkit.v1.card.create, card_req
            )
            if not card_resp.success():
                return SendResult(
                    success=False,
                    error=f"CardKit create: [{card_resp.code}] {card_resp.msg}",
                )

            card_id = card_resp.data.card_id if card_resp.data else None
            if not card_id:
                return SendResult(success=False, error="CardKit: no card_id in response")

            msg_content = json.dumps({
                "type": "card",
                "data": json.dumps({"card_id": card_id}),
            })
            receive_id_type = (metadata or {}).get("receive_id_type", "chat_id")
            msg_req = (
                CreateMessageRequest.builder()
                .receive_id_type(receive_id_type)
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("interactive")
                    .content(msg_content)
                    .build()
                )
                .build()
            )
            msg_resp = await asyncio.to_thread(
                self._client.im.v1.message.create, msg_req
            )
            if not msg_resp.success():
                return SendResult(
                    success=False,
                    error=f"Send card msg: [{msg_resp.code}] {msg_resp.msg}",
                )

            msg_id = getattr(msg_resp.data, "message_id", None) if msg_resp.data else None
            if msg_id:
                self._card_ids[msg_id] = card_id
                self._card_seq[card_id] = 0
                self._cardkit_available = True

            return SendResult(success=True, message_id=msg_id)

        except Exception as exc:
            logger.debug("[Feishu] _send_cardkit_card error: %s", exc)
            return SendResult(success=False, error=str(exc))

    async def _send_plain_text(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str],
        metadata: Optional[dict[str, Any]],
    ) -> SendResult:
        """Send a plain text message (fallback path)."""
        try:
            from lark_oapi.api.im.v1 import (
                CreateMessageRequest,
                CreateMessageRequestBody,
                ReplyMessageRequest,
                ReplyMessageRequestBody,
            )

            body_content = json.dumps({"text": content})

            if reply_to:
                req = (
                    ReplyMessageRequest.builder()
                    .message_id(reply_to)
                    .request_body(
                        ReplyMessageRequestBody.builder()
                        .msg_type("text")
                        .content(body_content)
                        .build()
                    )
                    .build()
                )
                resp = await asyncio.to_thread(self._client.im.v1.message.reply, req)
            else:
                receive_id_type = (metadata or {}).get("receive_id_type", "chat_id")
                req = (
                    CreateMessageRequest.builder()
                    .receive_id_type(receive_id_type)
                    .request_body(
                        CreateMessageRequestBody.builder()
                        .receive_id(chat_id)
                        .msg_type("text")
                        .content(body_content)
                        .build()
                    )
                    .build()
                )
                resp = await asyncio.to_thread(self._client.im.v1.message.create, req)

            if not resp.success():
                return SendResult(
                    success=False,
                    error=f"[{resp.code}] {resp.msg}",
                    retryable=True,
                )

            msg_id = getattr(resp.data, "message_id", None) if resp.data else None
            return SendResult(success=True, message_id=msg_id)

        except Exception as exc:
            logger.exception("[Feishu] _send_plain_text() failed")
            return SendResult(success=False, error=str(exc), retryable=True)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="Not connected")

        card_id = self._card_ids.get(message_id)
        if card_id:
            return await self._edit_cardkit(card_id, message_id, content, finalize)

        # Plain text message — try legacy PATCH API
        try:
            from lark_oapi.api.im.v1 import (
                PatchMessageRequest,
                PatchMessageRequestBody,
            )

            req = (
                PatchMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    PatchMessageRequestBody.builder()
                    .content(json.dumps({"text": content}))
                    .build()
                )
                .build()
            )
            resp = await asyncio.to_thread(self._client.im.v1.message.patch, req)
            if not resp.success():
                return SendResult(
                    success=False,
                    error=f"[{resp.code}] {resp.msg}",
                    retryable=True,
                )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            logger.exception("[Feishu] edit_message() PATCH failed")
            return SendResult(success=False, error=str(exc))

    async def _edit_cardkit(
        self,
        card_id: str,
        message_id: str,
        content: str,
        finalize: bool,
    ) -> SendResult:
        """Update a CardKit streaming card element."""
        try:
            from lark_oapi.api.cardkit.v1 import (
                ContentCardElementRequest,
                ContentCardElementRequestBody,
                SettingsCardRequest,
                SettingsCardRequestBody,
            )

            seq = self._card_seq.get(card_id, 0) + 1
            self._card_seq[card_id] = seq

            elem_req = (
                ContentCardElementRequest.builder()
                .card_id(card_id)
                .element_id("main")
                .request_body(
                    ContentCardElementRequestBody.builder()
                    .content(content)
                    .sequence(seq)
                    .build()
                )
                .build()
            )
            elem_resp = await asyncio.to_thread(
                self._client.cardkit.v1.card_element.content, elem_req
            )
            if not elem_resp.success():
                return SendResult(
                    success=False,
                    error=f"CardKit content: [{elem_resp.code}] {elem_resp.msg}",
                )

            if finalize:
                seq += 1
                self._card_seq[card_id] = seq
                settings_req = (
                    SettingsCardRequest.builder()
                    .card_id(card_id)
                    .request_body(
                        SettingsCardRequestBody.builder()
                        .settings(json.dumps({"streaming_mode": False}))
                        .sequence(seq)
                        .build()
                    )
                    .build()
                )
                settings_resp = await asyncio.to_thread(
                    self._client.cardkit.v1.card.settings, settings_req
                )
                if not settings_resp.success():
                    logger.warning(
                        "[Feishu] CardKit finish streaming failed: [%s] %s",
                        settings_resp.code, settings_resp.msg,
                    )
                self._card_ids.pop(message_id, None)
                self._card_seq.pop(card_id, None)

            return SendResult(success=True, message_id=message_id)

        except Exception as exc:
            logger.debug("[Feishu] _edit_cardkit error: %s", exc)
            return SendResult(success=False, error=str(exc))

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        if not self._client:
            return False
        try:
            from lark_oapi.api.im.v1 import DeleteMessageRequest

            req = (
                DeleteMessageRequest.builder()
                .message_id(message_id)
                .build()
            )
            resp = await asyncio.to_thread(self._client.im.v1.message.delete, req)
            if not resp.success():
                logger.debug("[Feishu] delete_message failed: [%s] %s", resp.code, resp.msg)
                return False
            return True
        except Exception as exc:
            logger.debug("[Feishu] delete_message() error: %s", exc)
            return False

    def supports_edit(self) -> bool:
        return True

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        # Feishu has no native typing indicator API.
        return None

    # ------------------------------------------------------------------
    # Tool-call card streaming (round lifecycle)
    # ------------------------------------------------------------------

    def supports_tool_card(self) -> bool:
        return True

    async def begin_tool_round(
        self,
        chat_id: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Optional[_FeishuToolRound]:
        if not self._client:
            return None
        builder = ToolCardBuilder()
        patcher = ThrottledCardPatcher(
            self, builder, chat_id, reply_to=reply_to, metadata=metadata,
        )
        return _FeishuToolRound(
            chat_id=chat_id,
            reply_to=reply_to,
            metadata=metadata,
            builder=builder,
            patcher=patcher,
            round_start=time.monotonic(),
        )

    async def tool_round_start(self, handle: Optional[_FeishuToolRound], tool: dict[str, Any]) -> None:
        if handle is None:
            return
        tool_id = tool.get("tool_id") or ""
        handle.start_times[tool_id] = time.monotonic()
        handle.builder.add_start(tool_id, tool.get("name", "tool"), tool.get("input"))
        handle.patcher.mark_pending()
        # The first tool forces card creation, so the "💬 处理中" card appears
        # immediately rather than waiting on the coalesce timer.
        await handle.patcher.flush_if_due()

    async def tool_round_complete(self, handle: Optional[_FeishuToolRound], tool: dict[str, Any]) -> None:
        if handle is None:
            return
        tool_id = tool.get("tool_id") or ""
        started = handle.start_times.pop(tool_id, None)
        elapsed = (time.monotonic() - started) if started is not None else None
        is_error = bool(tool.get("is_error"))
        handle.any_failed = handle.any_failed or is_error
        handle.builder.add_complete(
            tool_id, elapsed=elapsed, is_error=is_error,
            error=str(tool.get("error_message") or ""),
        )
        handle.patcher.mark_pending()
        # Failures surface immediately; successes respect the coalesce timer.
        await handle.patcher.flush_if_due(is_error=is_error)

    async def end_tool_round(self, handle: Optional[_FeishuToolRound], *, success: bool = True) -> None:
        if handle is None or handle.finished:
            return
        handle.finished = True
        total = time.monotonic() - handle.round_start
        if not success:
            outcome = "interrupted"
        elif handle.any_failed:
            outcome = "failed"
        else:
            outcome = "done"
        await handle.patcher.finalize(outcome, total)

    # -- Card delivery (CardSender protocol) -------------------------------

    async def create_tool_card(
        self,
        chat_id: str,
        card_json: str,
        *,
        reply_to: Optional[str],
        metadata: Optional[dict[str, Any]],
    ) -> Optional[str]:
        """Create an interactive card message. Returns its message_id, or None."""
        if not self._client:
            return None
        try:
            from lark_oapi.api.im.v1 import (
                CreateMessageRequest,
                CreateMessageRequestBody,
                ReplyMessageRequest,
                ReplyMessageRequestBody,
            )

            if reply_to:
                req = (
                    ReplyMessageRequest.builder()
                    .message_id(reply_to)
                    .request_body(
                        ReplyMessageRequestBody.builder()
                        .msg_type("interactive")
                        .content(card_json)
                        .build()
                    )
                    .build()
                )
                resp = await asyncio.to_thread(self._client.im.v1.message.reply, req)
            else:
                receive_id_type = (metadata or {}).get("receive_id_type", "chat_id")
                req = (
                    CreateMessageRequest.builder()
                    .receive_id_type(receive_id_type)
                    .request_body(
                        CreateMessageRequestBody.builder()
                        .receive_id(chat_id)
                        .msg_type("interactive")
                        .content(card_json)
                        .build()
                    )
                    .build()
                )
                resp = await asyncio.to_thread(self._client.im.v1.message.create, req)

            if not resp.success():
                logger.warning(
                    "[Feishu] tool card create failed: [%s] %s", resp.code, resp.msg,
                )
                return None
            return getattr(resp.data, "message_id", None) if resp.data else None
        except Exception as exc:
            logger.warning("[Feishu] create_tool_card error: %s", exc)
            return None

    async def patch_tool_card(self, message_id: str, card_json: str) -> bool:
        """Update an interactive card in place (streaming). Returns success."""
        if not self._client:
            return False
        try:
            from lark_oapi.api.im.v1 import (
                PatchMessageRequest,
                PatchMessageRequestBody,
            )

            # Use the PATCH endpoint (im.v1.message.patch) to update an
            # interactive card. The PUT endpoint (message.update) rejects
            # msg_type="interactive" with [230001] invalid msg_type — it only
            # accepts text/post. PATCH takes a card-JSON body with no msg_type.
            req = (
                PatchMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    PatchMessageRequestBody.builder()
                    .content(card_json)
                    .build()
                )
                .build()
            )
            resp = await asyncio.to_thread(self._client.im.v1.message.patch, req)
            if not resp.success():
                logger.debug(
                    "[Feishu] tool card patch failed: [%s] %s", resp.code, resp.msg,
                )
                return False
            return True
        except Exception as exc:
            logger.warning("[Feishu] patch_tool_card error: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Inbound: event handler (runs in ws thread)
    # ------------------------------------------------------------------

    def _on_message_event(self, data: Any) -> None:
        """Handle ``im.message.receive_v1``.

        Runs synchronously inside the ws-client thread; we marshal the
        resulting ``handle_message`` coroutine onto the main event loop.
        """
        try:
            event = getattr(data, "event", None)
            if event is None:
                logger.debug("[Feishu] Event handler called with no event data")
                return

            msg = getattr(event, "message", None)
            sender = getattr(event, "sender", None)
            if msg is None or sender is None:
                logger.debug("[Feishu] Event missing message or sender — dropping")
                return

            # Skip bot self-messages (sender_type == "app")
            if getattr(sender, "sender_type", "") == "app":
                return

            message_type = getattr(msg, "message_type", "")
            message_id = getattr(msg, "message_id", "?")
            chat_id_raw = getattr(msg, "chat_id", "") or ""

            logger.info(
                "[Feishu] Received message: id=%s type=%s chat=%s",
                message_id, message_type, chat_id_raw,
            )

            # First version: only handle text messages
            if message_type != "text":
                logger.debug(
                    "[Feishu] Ignoring non-text message type=%s message_id=%s",
                    message_type,
                    message_id,
                )
                return

            text = self._extract_text(msg)
            user_id = self._extract_user_id(sender)
            chat_id = getattr(msg, "chat_id", "") or ""

            if not chat_id or user_id is None:
                logger.warning("[Feishu] Incomplete event (no chat_id or user_id) — dropping")
                return

            # chat_type: "p2p" (DM) or "group"
            chat_type_raw = getattr(msg, "chat_type", "") or ""
            chat_type = ChatType.DM if chat_type_raw == "p2p" else ChatType.GROUP

            # thread_id: Feishu uses root_id as the thread root message_id
            thread_id = getattr(msg, "root_id", None) or None

            source = MessageSource(
                platform="feishu",
                user_id=user_id,
                chat_id=chat_id,
                thread_id=thread_id,
                chat_type=chat_type,
            )

            evt = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                message_id=getattr(msg, "message_id", None),
                reply_to_message_id=getattr(msg, "message_id", None) or None,
                raw_message={"event": event},
            )

            if self._main_loop is not None and not self._main_loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self.handle_message(evt), self._main_loop
                )
                logger.debug(
                    "[Feishu] Dispatched message %s to main loop (user=%s chat=%s)",
                    message_id, user_id, chat_id,
                )
            else:
                logger.warning("[Feishu] No running event loop; dropping message %s", message_id)

        except Exception as exc:
            logger.exception("[Feishu] Event handler error: %s", exc)

    # -- Inbound helpers --------------------------------------------------

    @staticmethod
    def _extract_text(msg: Any) -> str:
        """Parse the ``{"text": "..."}`` content JSON into a plain string,
        substituting @-mention placeholders with display names."""
        raw = getattr(msg, "content", "") or ""
        try:
            parsed = json.loads(raw)
            text = parsed.get("text", "") if isinstance(parsed, dict) else ""
        except Exception:
            text = raw

        mentions = getattr(msg, "mentions", None) or []
        for mention in mentions:
            key = getattr(mention, "key", None)
            name = getattr(mention, "name", None)
            if key:
                # Strip the leading '@' from the placeholder so "@BotName"
                # becomes just "BotName".
                replacement = (name or "").lstrip("@") if name else ""
                text = text.replace(key, replacement)
        return text.strip()

    @staticmethod
    def _extract_user_id(sender: Any) -> Optional[str]:
        sid = getattr(sender, "sender_id", None)
        if sid is None:
            return None
        # Prefer open_id (stable per-app), fall back to user_id, then union_id
        return (
            getattr(sid, "open_id", None)
            or getattr(sid, "user_id", None)
            or getattr(sid, "union_id", None)
        )


# ----------------------------------------------------------------------
# Registry hook
# ----------------------------------------------------------------------


def register_feishu() -> None:
    """Register the Feishu / Lark adapter with the global registry."""
    from agent_gateway.core.registry import EnvVarDef

    registry.register(
        PlatformEntry(
            name="feishu",
            label="Feishu / Lark",
            adapter_factory=lambda cfg: FeishuAdapter(cfg),
            check_fn=_check_feishu_deps,
            install_hint="pip install agent-gateway[feishu]",
            required_env=["FEISHU_APP_ID", "FEISHU_APP_SECRET"],
            max_message_length=4000,
            emoji="\U0001FAB6",
            platform_hint=(
                "You are on Feishu / Lark. Use plain text or simple markdown. "
                "Reply threads (topics) are supported."
            ),
            source="builtin",
            env_var_defs=[
                EnvVarDef(
                    key="FEISHU_APP_ID",
                    description="Feishu / Lark app App ID",
                    prompt="cli_xxxxxxxxxxxxxxxx",
                    required=True,
                    url="https://open.feishu.cn/app",
                ),
                EnvVarDef(
                    key="FEISHU_APP_SECRET",
                    description="Feishu / Lark app App Secret",
                    prompt="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                    is_password=True,
                    required=True,
                    url="https://open.feishu.cn/app",
                ),
                EnvVarDef(
                    key="FEISHU_ALLOWED_USERS",
                    description="Comma-separated Feishu open_id / user_id allowlist",
                    prompt="ou_xxx,ou_yyy",
                    required=False,
                ),
                EnvVarDef(
                    key="FEISHU_DOMAIN",
                    description=(
                        "API domain. Use https://open.larksuite.com for Lark "
                        "(international) or leave default for Feishu (China)"
                    ),
                    prompt="https://open.feishu.cn",
                    required=False,
                    advanced=True,
                    sensitive=False,
                ),
            ],
        )
    )

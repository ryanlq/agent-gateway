"""
Slack platform adapter (skeleton).

Uses ``slack-bolt`` for event-driven message reception.  This is a
functional skeleton that can be extended with Slack-specific features
(rich messages, blocks, modals, etc.).

Requirements::

    pip install agent-gateway[slack]
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

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


def _check_slack_deps() -> bool:
    try:
        import slack_bolt  # noqa: F401
        return True
    except ImportError:
        return False


class SlackAdapter(BasePlatformAdapter):
    """Slack Bolt adapter."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._bot_token = config.get("token", "")
        self._app_token = config.get("extra", {}).get("app_token", "")
        self._app: Any = None
        self._client: Any = None
        self._name = "Slack"

    async def connect(self) -> bool:
        if not self._bot_token:
            self._set_fatal_error("no_token", "SLACK_TOKEN not set", retryable=False)
            return False

        try:
            from slack_bolt.async_app import AsyncApp
            from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

            app = AsyncApp(token=self._bot_token)

            @app.event("message")
            async def handle_message(event: dict, say: Any) -> None:
                await self._on_slack_message(event, say)

            self._app = app
            self._client = app.client

            # Start Socket Mode if app_token is provided
            if self._app_token:
                handler = AsyncSocketModeHandler(app, self._app_token)
                asyncio.create_task(handler.start_async())
            else:
                # Without Socket Mode, use the built-in OAuth server
                logger.warning("No SLACK_APP_TOKEN — Socket Mode disabled. "
                               "Set up a Events API URL for production use.")

            self._mark_connected()
            return True

        except Exception as exc:
            self._set_fatal_error("connect_failed", str(exc))
            return False

    async def disconnect(self) -> None:
        self._mark_disconnected()

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

        try:
            kwargs: dict[str, Any] = {
                "channel": chat_id,
                "text": content[:40000] if len(content) > 40000 else content,
            }
            if metadata and "thread_id" in metadata:
                kwargs["thread_ts"] = metadata["thread_id"]
            elif reply_to:
                kwargs["thread_ts"] = reply_to

            result = await self._client.chat_postMessage(**kwargs)
            return SendResult(
                success=True,
                message_id=result.get("ts"),
            )
        except Exception as exc:
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
        try:
            await self._client.chat_update(
                channel=chat_id,
                ts=message_id,
                text=content[:40000] if len(content) > 40000 else content,
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        # Slack doesn't have a direct typing indicator API
        pass

    # -- Internal ------------------------------------------------------------

    async def _on_slack_message(self, event: dict, say: Any) -> None:
        """Handle an incoming Slack message event."""
        # Skip bot messages and channel join/leave
        if event.get("subtype") in ("bot_message", "channel_join", "channel_leave"):
            return

        text = event.get("text", "")
        user_id = event.get("user", "unknown")
        channel = event.get("channel", "unknown")
        thread_ts = event.get("thread_ts")

        source = MessageSource(
            platform="slack",
            user_id=user_id,
            chat_id=channel,
            thread_id=thread_ts,
            chat_type=ChatType.GROUP if channel.startswith("C") else ChatType.DM,
        )

        event_obj = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=event.get("ts"),
            raw_message=event,
        )

        await self.handle_message(event_obj)


def register_slack() -> None:
    """Register the Slack adapter with the global registry."""
    registry.register(PlatformEntry(
        name="slack",
        label="Slack",
        adapter_factory=lambda cfg: SlackAdapter(cfg),
        check_fn=_check_slack_deps,
        install_hint="pip install agent-gateway[slack]",
        required_env=["SLACK_TOKEN"],
        max_message_length=40000,
        emoji="💼",
        platform_hint="You are on Slack. Use Slack markdown. Threaded replies are supported.",
        source="builtin",
    ))

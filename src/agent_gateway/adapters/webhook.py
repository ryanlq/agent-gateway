"""
Webhook platform adapter.

Provides an HTTP server (FastAPI) that receives messages as webhook
callbacks.  This is the simplest adapter and works with any platform
that can send HTTP POST requests (Slack, Microsoft Teams, custom
integrations, etc.).

Requirements::

    pip install agent-gateway[webhook]
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


def _check_webhook_deps() -> bool:
    try:
        import fastapi  # noqa: F401
        return True
    except ImportError:
        return False


class WebhookAdapter(BasePlatformAdapter):
    """
    HTTP webhook adapter.

    Starts a FastAPI server and exposes a ``POST /webhook`` endpoint.
    Incoming messages are expected as JSON with the following fields:

        ``text``, ``user_id``, ``chat_id``, ``user_name`` (optional),
        ``thread_id`` (optional)

    This adapter is useful for:
      - Custom integrations
      - Slack slash commands / event subscriptions
      - Microsoft Teams bots
      - Testing and development
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._host = config.get("host", "127.0.0.1")
        self._port = int(config.get("port", 8080))
        self._path = config.get("path", "/webhook")
        self._secret = config.get("secret", "")
        self._server: Any = None
        self._app: Any = None
        self._name = "Webhook"

    async def connect(self) -> bool:
        """Start the HTTP webhook server."""
        try:
            from fastapi import FastAPI, Request, HTTPException
            import uvicorn

            app = FastAPI(title="Agent Gateway Webhook")

            @app.post(self._path)
            async def handle_webhook(request: Request) -> dict:
                # Optional secret verification
                if self._secret:
                    auth = request.headers.get("X-Webhook-Secret", "")
                    if auth != self._secret:
                        raise HTTPException(status_code=401, detail="Invalid secret")

                try:
                    data = await request.json()
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid JSON")

                # Build and dispatch event
                event = self._parse_webhook(data)
                if event:
                    await self.handle_message(event)

                return {"ok": True}

            @app.get("/health")
            async def health() -> dict:
                return {"status": "ok", "adapter": self._name}

            self._app = app
            config = uvicorn.Config(app, host=self._host, port=self._port, log_level="warning")
            self._server = uvicorn.Server(config)

            # Start server in background
            asyncio.create_task(self._server.serve())
            self._mark_connected()
            logger.info("Webhook server listening on %s:%s%s", self._host, self._port, self._path)
            return True

        except Exception as exc:
            self._set_fatal_error("start_failed", str(exc))
            logger.error("Webhook server start failed: %s", exc)
            return False

    async def disconnect(self) -> None:
        """Stop the webhook server."""
        if self._server:
            self._server.should_exit = True
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """
        Webhook adapter doesn't push messages outbound.

        Instead, responses are returned synchronously in the webhook
        handler's HTTP response.  This method logs the message for
        debugging but doesn't deliver it.
        """
        logger.info("Webhook send to %s: %s", chat_id, content[:100])
        return SendResult(success=True, message_id=f"webhook-{chat_id}")

    def _parse_webhook(self, data: dict[str, Any]) -> Optional[MessageEvent]:
        """Parse a webhook payload into a MessageEvent."""
        text = data.get("text", "")
        user_id = str(data.get("user_id", "unknown"))
        chat_id = str(data.get("chat_id", "default"))
        user_name = data.get("user_name", "")
        thread_id = data.get("thread_id")

        if not text:
            return None

        source = MessageSource(
            platform="webhook",
            user_id=user_id,
            chat_id=chat_id,
            thread_id=str(thread_id) if thread_id else None,
            chat_type=ChatType.DM,
            display_name=user_name,
        )

        return MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=data,
        )


def register_webhook() -> None:
    """Register the Webhook adapter with the global registry."""
    from agent_gateway.core.registry import EnvVarDef

    registry.register(PlatformEntry(
        name="webhook",
        label="Webhook",
        adapter_factory=lambda cfg: WebhookAdapter(cfg),
        check_fn=_check_webhook_deps,
        install_hint="pip install agent-gateway[webhook]",
        max_message_length=0,
        emoji="🔗",
        platform_hint="You are responding via a webhook. The response will be returned as JSON.",
        source="builtin",
        env_var_defs=[
            EnvVarDef(
                key="WEBHOOK_SECRET",
                description="Secret key for verifying incoming webhook requests",
                prompt="Enter secret",
                is_password=True,
                required=False,
            ),
            EnvVarDef(
                key="WEBHOOK_PORT",
                description="Port for the webhook HTTP server",
                prompt="9120",
                required=False,
                advanced=True,
            ),
        ],
    ))

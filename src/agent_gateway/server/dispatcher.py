"""
JSON-RPC 2.0 method dispatcher.

Routes incoming JSON-RPC frames to registered handler functions.
Handlers receive ``(params, emit, sessions)`` and return a result dict
(or raise to produce an error response).

Server-pushed events are sent via the ``emit`` callback:
``emit(event_type, payload, session_id)``.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from agent_gateway.server.session_manager import SessionManager

logger = logging.getLogger(__name__)

# Handler signature: (params, emit, sessions) -> result dict
Handler = Callable[
    [dict[str, Any], Callable[..., Awaitable[None]], SessionManager],
    Awaitable[Any],
]


class Dispatcher:
    """JSON-RPC method dispatcher for the agent-gateway server."""

    def __init__(self, sessions: SessionManager) -> None:
        self._sessions = sessions
        self._methods: dict[str, Handler] = {}

    def register(self, method: str, handler: Handler) -> None:
        """Register a handler for a JSON-RPC method."""
        self._methods[method] = handler

    async def handle_frame(
        self,
        frame: dict[str, Any],
        emit: Callable[..., Awaitable[None]],
    ) -> dict[str, Any] | None:
        """Process a single JSON-RPC frame and return the response.

        Returns ``None`` for notifications (frames without an ``id``).
        """
        method = frame.get("method", "")
        params = frame.get("params") or {}
        req_id = frame.get("id")

        handler = self._methods.get(method)
        if handler is None:
            if req_id is not None:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                }
            return None

        try:
            result = await handler(params, emit, self._sessions)
        except Exception as exc:
            logger.exception("Handler error for %s", method)
            if req_id is not None:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32603, "message": str(exc)},
                }
            return None

        if req_id is not None:
            return {"jsonrpc": "2.0", "id": req_id, "result": result}

        # Notification — no response needed
        return None

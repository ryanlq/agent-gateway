"""
FastAPI application for hermes-desktop integration.

Exposes:
  - ``GET /api/status``  — readiness probe used by Electron main process
  - ``GET /health``      — basic health check
  - ``WebSocket /api/ws`` — JSON-RPC 2.0 transport for real-time chat

The WebSocket speaks the same protocol as the hermes-agent TUI gateway:
server-pushed events wrapped as ``{"jsonrpc":"2.0","method":"event","params":{...}}``,
and request/response via standard JSON-RPC frames.
"""

from __future__ import annotations

import hmac
import json
import logging
import uuid
from typing import Any, Callable

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from agent_gateway.server.dispatcher import Dispatcher
from agent_gateway.server.session_manager import SessionManager

logger = logging.getLogger(__name__)


def create_app(token: str) -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(title="Agent Gateway", version="0.1.0")
    sessions = SessionManager()
    dispatcher = Dispatcher(sessions)

    # Register RPC method handlers
    from agent_gateway.server import methods as m
    dispatcher.register("session.create", m.handle_session_create)
    dispatcher.register("session.resume", m.handle_session_resume)
    dispatcher.register("session.close", m.handle_session_close)
    dispatcher.register("session.list", m.handle_session_list)
    dispatcher.register("prompt.submit", m.handle_prompt_submit)
    dispatcher.register("model.options", m.handle_model_options)
    dispatcher.register("commands.catalog", m.handle_commands_catalog)
    dispatcher.register("config.get", m.handle_config_get)
    dispatcher.register("config.set", m.handle_config_set)
    dispatcher.register("tools.list", m.handle_tools_list)

    # ------------------------------------------------------------------
    # HTTP endpoints
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/status")
    async def api_status(request: Request) -> dict[str, Any]:
        """Readiness probe — Electron polls this until it returns 200."""
        return {
            "status": "ok",
            "version": "0.1.0",
            "active_sessions": len(sessions.list_sessions()),
            "auth_required": bool(token),
        }

    # ------------------------------------------------------------------
    # HTTP REST stubs — hermes-desktop expects these endpoints via
    # window.hermesDesktop.api().  Return minimal valid responses so the
    # renderer doesn't crash on 404s.
    # ------------------------------------------------------------------

    @app.get("/api/sessions")
    async def rest_sessions(request: Request) -> dict[str, Any]:
        return {"sessions": [], "total": 0}

    @app.get("/api/sessions/{session_id}")
    async def rest_session_detail(session_id: str) -> dict[str, Any]:
        s = sessions.get_session(session_id)
        if s:
            return s.to_dict()
        return {"error": "not found"}

    @app.get("/api/sessions/{session_id}/messages")
    async def rest_session_messages(session_id: str) -> dict[str, Any]:
        s = sessions.get_session(session_id)
        return {"messages": s.history if s else []}

    @app.get("/api/config")
    async def rest_config(request: Request) -> dict[str, Any]:
        return {"config": {"default_agent": sessions.default_agent_type}}

    @app.get("/api/config/defaults")
    async def rest_config_defaults(request: Request) -> dict[str, Any]:
        return {"defaults": {}}

    @app.get("/api/config/schema")
    async def rest_config_schema(request: Request) -> dict[str, Any]:
        return {"schema": {}}

    @app.patch("/api/config")
    async def rest_config_set(request: Request) -> dict[str, Any]:
        return {"updated": True}

    @app.get("/api/model/info")
    async def rest_model_info(request: Request) -> dict[str, Any]:
        return {
            "model": sessions.default_agent_type,
            "provider": sessions.default_agent_type,
        }

    @app.get("/api/env")
    async def rest_env(request: Request) -> dict[str, Any]:
        return {"env": {}}

    @app.patch("/api/env")
    async def rest_env_set(request: Request) -> dict[str, Any]:
        return {"updated": True}

    @app.get("/api/env/reveal")
    async def rest_env_reveal(request: Request) -> dict[str, Any]:
        return {"values": {}}

    @app.get("/api/skills")
    async def rest_skills(request: Request) -> dict[str, Any]:
        return {"skills": []}

    @app.post("/api/skills/toggle")
    async def rest_skills_toggle(request: Request) -> dict[str, Any]:
        return {"toggled": True}

    @app.get("/api/tools/toolsets")
    async def rest_toolsets(request: Request) -> dict[str, Any]:
        return {"toolsets": []}

    @app.get("/api/tools/toolsets/{name}")
    async def rest_toolset_detail(name: str) -> dict[str, Any]:
        return {"name": name, "enabled": True}

    @app.get("/api/tools/toolsets/{name}/config")
    async def rest_toolset_config(name: str) -> dict[str, Any]:
        return {"config": {}}

    @app.get("/api/tools/toolsets/{name}/provider")
    async def rest_toolset_provider(name: str) -> dict[str, Any]:
        return {"provider": None}

    @app.patch("/api/tools/toolsets/{name}/provider")
    async def rest_toolset_provider_set(name: str) -> dict[str, Any]:
        return {"updated": True}

    @app.get("/api/logs")
    async def rest_logs(request: Request) -> dict[str, Any]:
        return {"logs": []}

    @app.get("/api/providers/validate")
    async def rest_providers_validate(request: Request) -> dict[str, Any]:
        return {"valid": True}

    @app.get("/api/providers/oauth")
    async def rest_providers_oauth(request: Request) -> dict[str, Any]:
        return {"providers": []}

    @app.get("/api/profiles/active")
    async def rest_profiles_active(request: Request) -> dict[str, Any]:
        return {"profile": None}

    @app.get("/api/profiles/sessions")
    async def rest_profiles_sessions(request: Request) -> dict[str, Any]:
        return {"sessions": []}

    # ------------------------------------------------------------------
    # WebSocket JSON-RPC endpoint
    # ------------------------------------------------------------------

    @app.websocket("/api/ws")
    async def ws_endpoint(
        ws: WebSocket,
        token_query: str | None = Query(None, alias="token"),
    ) -> None:
        # Auth check
        if token and not _ws_auth_ok(token_query, token):
            await ws.close(code=4401, reason="unauthorized")
            return

        await ws.accept()

        # Create an emit function bound to this WebSocket
        async def emit(
            event_type: str,
            payload: Any = None,
            session_id: str | None = None,
        ) -> None:
            frame = {
                "jsonrpc": "2.0",
                "method": "event",
                "params": {
                    "type": event_type,
                    "payload": payload or {},
                    "session_id": session_id,
                },
            }
            try:
                await ws.send_text(json.dumps(frame))
            except Exception:
                logger.debug("Failed to send event (client disconnected?)")

        # Send gateway.ready immediately
        await emit("gateway.ready", {"server": "agent-gateway", "version": "0.1.0"})

        # Message loop
        try:
            while True:
                raw = await ws.receive_text()
                if not raw or not raw.strip():
                    continue

                try:
                    frame = json.loads(raw)
                except json.JSONDecodeError:
                    await ws.send_text(json.dumps({
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32700, "message": "Parse error"},
                    }))
                    continue

                response = await dispatcher.handle_frame(frame, emit)
                if response is not None:
                    await ws.send_text(json.dumps(response))

        except WebSocketDisconnect:
            logger.debug("WebSocket client disconnected")
        except Exception as exc:
            logger.error("WebSocket error: %s", exc)
        finally:
            # Cleanup sessions on disconnect
            await sessions.close_all()

    return app


def _ws_auth_ok(provided: str | None, expected: str) -> bool:
    """Constant-time token comparison."""
    if not provided:
        return False
    return hmac.compare_digest(provided.encode(), expected.encode())

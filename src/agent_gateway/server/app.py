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

import asyncio
import hmac
import json
import logging
import uuid
from typing import Any, Callable

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from agent_gateway.server.agent_status import detect_agents, get_installed_agent_types
from agent_gateway.server.dispatcher import Dispatcher
from agent_gateway.server.session_manager import SessionManager

logger = logging.getLogger(__name__)


def create_app(token: str) -> FastAPI:
    """Create and configure the FastAPI application."""

    sessions = SessionManager()
    dispatcher = Dispatcher(sessions)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Graceful startup/shutdown: close all sessions on exit."""
        logger.info("Agent Gateway starting up")
        yield
        # Cleanup: close all sessions and their bridge subprocesses
        logger.info("Agent Gateway shutting down, closing %d sessions", len(sessions.list_sessions()))
        try:
            closed = await asyncio.wait_for(sessions.close_all(), timeout=15.0)
            logger.info("Closed %d sessions", closed)
        except asyncio.TimeoutError:
            logger.warning("Timed out closing sessions during shutdown (15s)")
        except Exception as exc:
            logger.error("Error during shutdown: %s", exc)

    app = FastAPI(title="Agent Gateway", version="0.1.0", lifespan=lifespan)

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
    # Agent detection
    # ------------------------------------------------------------------

    @app.get("/api/agents/status")
    async def rest_agents_status(request: Request) -> dict[str, Any]:
        """Detect installed agent CLIs and return their status."""
        agents = detect_agents()
        return {
            "agents": agents,
            "current": sessions.default_agent_type,
        }

    @app.post("/api/agents/switch")
    async def rest_agents_switch(request: Request) -> dict[str, Any]:
        """Switch the default agent type."""
        body = await request.json() if await request.body() else {}
        agent_type = body.get("agent", "")
        session_id = body.get("session_id")

        if session_id:
            await sessions.set_agent(session_id, agent_type)
        else:
            sessions.default_agent_type = agent_type

        return {"ok": True, "agent": agent_type}

    # ------------------------------------------------------------------
    # HTTP REST stubs — hermes-desktop expects these endpoints via
    # window.hermesDesktop.api().  Return minimal valid responses so the
    # renderer doesn't crash on 404s.
    # ------------------------------------------------------------------

    # -- Sessions ----------------------------------------------------------

    @app.get("/api/sessions")
    async def rest_sessions(request: Request) -> dict[str, Any]:
        return {"sessions": [], "total": 0, "offset": 0}

    @app.get("/api/sessions/search")
    async def rest_sessions_search(request: Request) -> dict[str, Any]:
        return {"sessions": [], "total": 0}

    @app.post("/api/sessions")
    async def rest_sessions_create(request: Request) -> dict[str, Any]:
        body = await request.json() if await request.body() else {}
        s = await sessions.create_session(cwd=body.get("cwd"))
        return {"session_id": s.session_id, "stored_session_id": None}

    @app.get("/api/sessions/{session_id}")
    async def rest_session_detail(session_id: str) -> dict[str, Any]:
        s = sessions.get_session(session_id)
        if s:
            return s.to_dict()
        return {"id": session_id, "title": "Chat", "message_count": 0,
                "created_at": 0, "archived": False}

    @app.patch("/api/sessions/{session_id}")
    async def rest_session_update(session_id: str) -> dict[str, Any]:
        return {"ok": True}

    @app.delete("/api/sessions/{session_id}")
    async def rest_session_delete(session_id: str) -> dict[str, Any]:
        return {"ok": True}

    @app.get("/api/sessions/{session_id}/messages")
    async def rest_session_messages(session_id: str) -> dict[str, Any]:
        s = sessions.get_session(session_id)
        return {"messages": s.history if s else []}

    @app.post("/api/sessions/{session_id}/resume")
    async def rest_session_resume(session_id: str) -> dict[str, Any]:
        s = sessions.get_session(session_id)
        if s:
            return {"session_id": s.session_id, "resumed": True,
                    "messages": s.history}
        return {"error": "not found"}

    @app.post("/api/sessions/{session_id}/branch")
    async def rest_session_branch(session_id: str) -> dict[str, Any]:
        s = await sessions.create_session()
        return {"session_id": s.session_id, "branched": True}

    # -- Profiles ----------------------------------------------------------

    @app.get("/api/profiles")
    async def rest_profiles_list(request: Request) -> dict[str, Any]:
        return {"profiles": [{"name": "default", "active": True}],
                "active": "default"}

    @app.get("/api/profiles/active")
    async def rest_profiles_active(request: Request) -> dict[str, Any]:
        return {"profile": "default", "current": "default"}

    @app.get("/api/profiles/sessions")
    async def rest_profiles_sessions(request: Request) -> dict[str, Any]:
        return {"sessions": [], "total": 0, "profile_totals": {}}

    @app.post("/api/profiles")
    async def rest_profiles_create(request: Request) -> dict[str, Any]:
        body = await request.json() if await request.body() else {}
        return {"name": body.get("name", "new"), "ok": True, "path": ""}

    @app.patch("/api/profiles/{name}")
    async def rest_profiles_update(name: str) -> dict[str, Any]:
        return {"ok": True}

    @app.delete("/api/profiles/{name}")
    async def rest_profiles_delete(name: str) -> dict[str, Any]:
        return {"ok": True, "path": ""}

    @app.get("/api/profiles/{name}/soul")
    async def rest_profiles_soul(name: str) -> dict[str, Any]:
        return {"content": ""}

    @app.put("/api/profiles/{name}/soul")
    async def rest_profiles_soul_update(name: str) -> dict[str, Any]:
        return {"ok": True}

    @app.get("/api/profiles/{name}/setup-command")
    async def rest_profiles_setup_command(name: str) -> dict[str, Any]:
        return {"command": ""}

    # -- Config ------------------------------------------------------------

    @app.get("/api/config")
    async def rest_config(request: Request) -> dict[str, Any]:
        agents = detect_agents()
        return {
            "config": {
                "default_agent": sessions.default_agent_type,
            },
            "agents": agents,
        }

    @app.get("/api/config/defaults")
    async def rest_config_defaults(request: Request) -> dict[str, Any]:
        return {"defaults": {}}

    @app.get("/api/config/schema")
    async def rest_config_schema(request: Request) -> dict[str, Any]:
        return {"fields": {}}

    @app.patch("/api/config")
    async def rest_config_set(request: Request) -> dict[str, Any]:
        return {"updated": True}

    @app.put("/api/config")
    async def rest_config_put(request: Request) -> dict[str, Any]:
        return {"ok": True}

    # -- Model -------------------------------------------------------------

    @app.get("/api/model/info")
    async def rest_model_info(request: Request) -> dict[str, Any]:
        """Return current agent as model info."""
        return {
            "model": "default",
            "provider": sessions.default_agent_type,
        }

    @app.get("/api/model/options")
    async def rest_model_options(request: Request) -> dict[str, Any]:
        """Return available agents as model providers.

        Each agent is presented as a "provider" with a single "default" model
        so the existing ModelPickerDialog works as an agent picker.
        """
        agents = detect_agents()
        providers = []
        for agent in agents:
            providers.append({
                "slug": agent["slug"],
                "name": agent["name"],
                "description": agent.get("description", ""),
                "models": ["default"],
                "is_current": agent["slug"] == sessions.default_agent_type,
                "total_models": 1,
                "installed": agent["installed"],
            })
        return {
            "providers": providers,
            "model": "default",
            "provider": sessions.default_agent_type,
        }

    @app.post("/api/model/set")
    async def rest_model_set(request: Request) -> dict[str, Any]:
        """Switch the active agent. Provider field = agent slug."""
        body = await request.json() if await request.body() else {}
        provider = body.get("provider", "")
        if provider:
            sessions.default_agent_type = provider
        return {"ok": True, "provider": provider, "model": body.get("model", "default")}

    @app.get("/api/model/auxiliary")
    async def rest_model_auxiliary(request: Request) -> dict[str, Any]:
        return {"models": {}}

    @app.get("/api/model/recommended-default")
    async def rest_model_recommended_default(request: Request) -> dict[str, Any]:
        return {"provider": "claude-code", "model": "claude-sonnet-4-6",
                "free_tier": None}

    # -- Env ---------------------------------------------------------------

    @app.get("/api/env")
    async def rest_env(request: Request) -> dict[str, Any]:
        return {"env": {}}

    @app.patch("/api/env")
    async def rest_env_set(request: Request) -> dict[str, Any]:
        return {"ok": True}

    @app.put("/api/env")
    async def rest_env_put(request: Request) -> dict[str, Any]:
        return {"ok": True}

    @app.delete("/api/env")
    async def rest_env_delete(request: Request) -> dict[str, Any]:
        return {"ok": True}

    @app.get("/api/env/reveal")
    async def rest_env_reveal(request: Request) -> dict[str, Any]:
        return {"values": {}}

    @app.post("/api/env/reveal")
    async def rest_env_reveal_post(request: Request) -> dict[str, Any]:
        return {"key": "", "value": ""}

    # -- Providers ---------------------------------------------------------

    @app.get("/api/providers/validate")
    async def rest_providers_validate(request: Request) -> dict[str, Any]:
        return {"ok": True, "reachable": True, "message": "ok"}

    @app.post("/api/providers/validate")
    async def rest_providers_validate_post(request: Request) -> dict[str, Any]:
        return {"ok": True, "reachable": True, "message": "ok"}

    @app.get("/api/providers/oauth")
    async def rest_providers_oauth(request: Request) -> dict[str, Any]:
        return {"providers": []}

    @app.post("/api/providers/oauth/{provider_id}/start")
    async def rest_oauth_start(provider_id: str) -> dict[str, Any]:
        return {"session_id": "", "auth_url": ""}

    @app.post("/api/providers/oauth/{provider_id}/submit")
    async def rest_oauth_submit(provider_id: str) -> dict[str, Any]:
        return {"ok": True}

    @app.get("/api/providers/oauth/{provider_id}/poll/{session_id}")
    async def rest_oauth_poll(provider_id: str, session_id: str) -> dict[str, Any]:
        return {"status": "complete"}

    @app.delete("/api/providers/oauth/sessions/{session_id}")
    async def rest_oauth_cancel_session(session_id: str) -> dict[str, Any]:
        return {"ok": True}

    # -- Skills & Tools ----------------------------------------------------

    @app.get("/api/skills")
    async def rest_skills(request: Request) -> list:
        return []

    @app.put("/api/skills/toggle")
    async def rest_skills_toggle(request: Request) -> dict[str, Any]:
        return {"ok": True, "name": "", "enabled": True}

    @app.get("/api/tools/toolsets")
    async def rest_toolsets(request: Request) -> list:
        return []

    @app.get("/api/tools/toolsets/{name}")
    async def rest_toolset_detail(name: str) -> dict[str, Any]:
        return {"name": name, "enabled": True, "tools": []}

    @app.put("/api/tools/toolsets/{name}")
    async def rest_toolset_toggle(name: str) -> dict[str, Any]:
        return {"ok": True, "name": name, "enabled": True}

    @app.get("/api/tools/toolsets/{name}/config")
    async def rest_toolset_config(name: str) -> dict[str, Any]:
        return {"config": {}}

    @app.get("/api/tools/toolsets/{name}/provider")
    async def rest_toolset_provider(name: str) -> dict[str, Any]:
        return {"provider": None}

    @app.patch("/api/tools/toolsets/{name}/provider")
    async def rest_toolset_provider_set(name: str) -> dict[str, Any]:
        return {"ok": True}

    # -- Logs --------------------------------------------------------------

    @app.get("/api/logs")
    async def rest_logs(request: Request) -> dict[str, Any]:
        return {"logs": [], "lines": []}

    # -- Messaging ---------------------------------------------------------

    @app.get("/api/messaging/platforms")
    async def rest_messaging_platforms(request: Request) -> dict[str, Any]:
        return {"platforms": []}

    @app.put("/api/messaging/platforms/{platform_id}")
    async def rest_messaging_platform_update(platform_id: str) -> dict[str, Any]:
        return {"ok": True, "platform": platform_id}

    @app.post("/api/messaging/platforms/{platform_id}/test")
    async def rest_messaging_platform_test(platform_id: str) -> dict[str, Any]:
        return {"ok": True, "connected": False}

    # -- Cron --------------------------------------------------------------

    @app.get("/api/cron/jobs")
    async def rest_cron_jobs(request: Request) -> list:
        return []

    @app.post("/api/cron/jobs")
    async def rest_cron_create(request: Request) -> dict[str, Any]:
        return {"id": "", "ok": True}

    @app.get("/api/cron/jobs/{job_id}")
    async def rest_cron_job(job_id: str) -> dict[str, Any]:
        return {"id": job_id}

    @app.patch("/api/cron/jobs/{job_id}")
    async def rest_cron_update(job_id: str) -> dict[str, Any]:
        return {"ok": True}

    @app.put("/api/cron/jobs/{job_id}")
    async def rest_cron_update_put(job_id: str) -> dict[str, Any]:
        return {"ok": True}

    @app.delete("/api/cron/jobs/{job_id}")
    async def rest_cron_delete(job_id: str) -> dict[str, Any]:
        return {"ok": True}

    @app.post("/api/cron/jobs/{job_id}/pause")
    async def rest_cron_pause(job_id: str) -> dict[str, Any]:
        return {"ok": True, "id": job_id}

    @app.post("/api/cron/jobs/{job_id}/resume")
    async def rest_cron_resume(job_id: str) -> dict[str, Any]:
        return {"ok": True, "id": job_id}

    @app.post("/api/cron/jobs/{job_id}/trigger")
    async def rest_cron_trigger(job_id: str) -> dict[str, Any]:
        return {"ok": True, "id": job_id}

    # -- Analytics ---------------------------------------------------------

    @app.get("/api/analytics/usage")
    async def rest_analytics_usage(request: Request) -> dict[str, Any]:
        return {"daily": [], "models": [], "skills": [],
                "totals": {}, "skills_summary": {}}

    # -- Audio -------------------------------------------------------------

    @app.post("/api/audio/transcribe")
    async def rest_audio_transcribe(request: Request) -> dict[str, Any]:
        return {"text": ""}

    @app.post("/api/audio/speak")
    async def rest_audio_speak(request: Request) -> dict[str, Any]:
        return {"audio_url": ""}

    @app.get("/api/audio/elevenlabs/voices")
    async def rest_elevenlabs_voices(request: Request) -> dict[str, Any]:
        return {"voices": []}

    # -- Gateway / Updates / Actions ----------------------------------------

    @app.post("/api/gateway/restart")
    async def rest_gateway_restart(request: Request) -> dict[str, Any]:
        return {"ok": True}

    @app.post("/api/hermes/update")
    async def rest_hermes_update(request: Request) -> dict[str, Any]:
        return {"ok": True, "updated": False}

    @app.get("/api/actions/{name}/status")
    async def rest_action_status(name: str) -> dict[str, Any]:
        return {"name": name, "status": "idle", "running": False, "lines": []}

    # -- Auth (WebSocket ticket for OAuth) ---------------------------------

    @app.post("/api/auth/ws-ticket")
    async def rest_auth_ws_ticket(request: Request) -> dict[str, Any]:
        return {"ticket": ""}

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

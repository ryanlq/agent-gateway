"""
FastAPI application for nexus-agent integration.

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
from typing import Any, Callable, Optional

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from agent_gateway.server.agent_status import detect_agents, get_installed_agent_types
from agent_gateway.server.dispatcher import Dispatcher
from agent_gateway.server.session_manager import SessionManager
from agent_gateway.server.session_store import SessionStore

logger = logging.getLogger(__name__)


def create_app(token: str, runner: Any = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        token: Authentication token for WebSocket connections.
        runner: Optional ``GatewayRunner`` for platform adapters (Email, etc.).
    """

    store = SessionStore()
    sessions = SessionManager(session_store=store)
    dispatcher = Dispatcher(sessions)

    # -- Cron manager -------------------------------------------------------
    from agent_gateway.cron.manager import CronManager
    cron_manager = CronManager(store, runner=runner)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Graceful startup/shutdown: manage sessions and platform adapters."""
        logger.info("Agent Gateway starting up")

        # Start platform adapters if runner is provided
        if runner:
            try:
                await runner.start()
                adapter_names = list(runner.adapters.keys())
                if adapter_names:
                    logger.info("Platform adapters started: %s", ", ".join(adapter_names))
                    print(f"[agent-gateway] Platform adapters: {', '.join(adapter_names)}", file=__import__('sys').stderr)
            except Exception as exc:
                logger.error("Failed to start platform adapters: %s", exc)

        # Start cron scheduler
        try:
            await cron_manager.start()
        except Exception as exc:
            logger.error("Failed to start cron scheduler: %s", exc)

        yield

        # Stop cron scheduler
        try:
            await cron_manager.stop()
        except Exception as exc:
            logger.error("Error stopping cron scheduler: %s", exc)

        # Stop platform adapters
        if runner:
            try:
                await runner.shutdown()
                logger.info("Platform adapters stopped")
            except Exception as exc:
                logger.error("Error shutting down platform adapters: %s", exc)

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

    # Expose store on app.state so the runner can share the same instance
    app.state.desktop_store = store

    # Register RPC method handlers
    from agent_gateway.server import methods as m
    dispatcher.register("session.create", m.handle_session_create)
    dispatcher.register("session.resume", m.handle_session_resume)
    dispatcher.register("session.close", m.handle_session_close)
    dispatcher.register("session.list", m.handle_session_list)
    dispatcher.register("session.interrupt", m.handle_session_interrupt)
    dispatcher.register("session.steer", m.handle_session_steer)
    dispatcher.register("prompt.submit", m.handle_prompt_submit)
    dispatcher.register("model.options", m.handle_model_options)
    dispatcher.register("commands.catalog", m.handle_commands_catalog)
    dispatcher.register("config.get", m.handle_config_get)
    dispatcher.register("config.set", m.handle_config_set)
    dispatcher.register("tools.list", m.handle_tools_list)
    dispatcher.register("setup.status", m.handle_setup_status)
    dispatcher.register("setup.runtime_check", m.handle_setup_runtime_check)

    # Phase 1: Core UX methods
    dispatcher.register("session.title", m.handle_session_title)
    dispatcher.register("slash.exec", m.handle_slash_exec)
    dispatcher.register("complete.path", m.handle_complete_path)
    dispatcher.register("complete.slash", m.handle_complete_slash)
    dispatcher.register("approval.respond", m.handle_approval_respond)
    dispatcher.register("sudo.respond", m.handle_sudo_respond)
    dispatcher.register("secret.respond", m.handle_secret_respond)
    dispatcher.register("clarify.respond", m.handle_clarify_respond)

    # Phase 2: Enhanced UX
    dispatcher.register("session.cwd.set", m.handle_session_cwd_set)

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
        # Per-agent params: { "claude-code": { model: "..." }, "pi": { mode: "..." } }
        all_params: dict[str, dict] = store.get_config("agent_params", {}) if store else {}
        current = sessions.default_agent_type
        return {
            "agents": agents,
            "current": current,
            "current_params": all_params.get(current, {}),
            # Send ALL per-agent params so the frontend can restore every agent's settings
            "all_params": all_params,
        }

    @app.post("/api/agents/switch")
    async def rest_agents_switch(request: Request) -> dict[str, Any]:
        """Switch the default agent type."""
        body = await request.json() if await request.body() else {}
        agent_type = body.get("agent", "")
        session_id = body.get("session_id")
        agent_params = body.get("agent_params")

        if session_id:
            await sessions.set_agent(session_id, agent_type, agent_params=agent_params)
        else:
            sessions.default_agent_type = agent_type
            # Persist agent params per-agent so changing one agent's settings
            # doesn't clobber another's.
            if agent_params and store:
                all_params: dict[str, dict] = store.get_config("agent_params", {})
                if not isinstance(all_params, dict):
                    all_params = {}
                all_params[agent_type] = agent_params
                store.set_config("agent_params", all_params)

        return {"ok": True, "agent": agent_type}

    # ------------------------------------------------------------------
    # HTTP REST stubs — nexus-agent expects these endpoints via
    # window.hermesDesktop.api().  Return minimal valid responses so the
    # renderer doesn't crash on 404s.
    # ------------------------------------------------------------------

    # -- Sessions ----------------------------------------------------------

    @app.get("/api/sessions")
    async def rest_sessions(request: Request) -> dict[str, Any]:
        params = dict(request.query_params)
        limit = int(params.get("limit", 40))
        offset = int(params.get("offset", 0))
        min_messages = int(params.get("min_messages", 0))
        archived = params.get("archived", "exclude")
        order = params.get("order", "recent")
        session_list, total = store.list_sessions(
            limit=limit, offset=offset, min_messages=min_messages,
            archived=archived, order=order,
        )
        return {
            "sessions": [store.to_session_info(s) for s in session_list],
            "total": total,
            "offset": offset,
        }

    @app.get("/api/sessions/search")
    async def rest_sessions_search(request: Request) -> dict[str, Any]:
        q = request.query_params.get("q", "")
        results = store.search(q) if q else []
        return {"results": results}

    @app.post("/api/sessions")
    async def rest_sessions_create(request: Request) -> dict[str, Any]:
        body = await request.json() if await request.body() else {}
        s = await sessions.create_session(cwd=body.get("cwd"))
        return {"session_id": s.session_id, "stored_session_id": s.session_id}

    @app.get("/api/sessions/{session_id}")
    async def rest_session_detail(session_id: str) -> dict[str, Any]:
        # Check in-memory first, then persisted store
        s = sessions.get_session(session_id)
        if s:
            return store.to_session_info(store.get(session_id)) if store.get(session_id) else s.to_dict()
        persisted = store.get(session_id)
        if persisted:
            return store.to_session_info(persisted)
        return {"id": session_id, "title": "Chat", "message_count": 0,
                "created_at": 0, "archived": False}

    @app.patch("/api/sessions/{session_id}")
    async def rest_session_update(session_id: str, request: Request) -> dict[str, Any]:
        body = {}
        if await request.body():
            try:
                body = await request.json()
            except Exception:
                pass
        if body.get("archived") is True:
            store.archive(session_id)
        elif body.get("archived") is False:
            store.unarchive(session_id)
        if "title" in body:
            store.update(session_id, title=body["title"])
        return {"ok": True}

    @app.delete("/api/sessions/{session_id}")
    async def rest_session_delete(session_id: str) -> dict[str, Any]:
        store.delete(session_id)
        # Also close in-memory session if active
        if sessions.get_session(session_id):
            await sessions.close_session(session_id)
        return {"ok": True}

    @app.get("/api/sessions/{session_id}/messages")
    async def rest_session_messages(session_id: str) -> dict[str, Any]:
        s = sessions.get_session(session_id)
        if s:
            return {"messages": s.history, "session_id": session_id}
        persisted = store.get(session_id)
        if persisted:
            return {"messages": persisted.history, "session_id": session_id}
        return {"messages": [], "session_id": session_id}

    @app.post("/api/sessions/{session_id}/resume")
    async def rest_session_resume(session_id: str) -> dict[str, Any]:
        s = await sessions.resume_session(session_id)
        if s:
            return {
                "session_id": s.session_id,
                "stored_session_id": s.session_id,
                "resumed": True,
                "messages": s.history,
            }
        return {"error": "not found"}

    @app.post("/api/sessions/{session_id}/branch")
    async def rest_session_branch(session_id: str) -> dict[str, Any]:
        s = await sessions.create_session()
        return {"session_id": s.session_id, "branched": True}

    @app.post("/api/sessions/{session_id}/handoff/email")
    async def rest_session_handoff_email(session_id: str) -> dict[str, Any]:
        """Send the session conversation to email for cross-platform continuation."""
        if not runner or "email" not in runner.adapters:
            return JSONResponse(status_code=400, content={"error": "Email adapter not available"})

        email_adapter = runner.adapters["email"]

        # Load session from persistent store
        session = store.get(session_id)
        if not session:
            return JSONResponse(status_code=404, content={"error": "Session not found"})

        # Determine recipient: EMAIL_HOME_ADDRESS > first allowed user
        import os as _os
        recipient = _os.environ.get("EMAIL_HOME_ADDRESS", "").strip()
        if not recipient and email_adapter._allowed_users:
            recipient = next(iter(email_adapter._allowed_users))
        if not recipient:
            return JSONResponse(status_code=400, content={"error": "No email recipient configured"})

        # Format email body from session history
        title = session.title or "Agent Session"
        body_lines = [f"Session: {title}", ""]
        for msg in session.history:
            role = "You" if msg.get("role") == "user" else "Agent"
            content = msg.get("content", "")
            # Handle content blocks (tool calls etc.)
            if isinstance(content, list):
                texts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        texts.append(block.get("text", ""))
                content = "\n".join(texts)
            if content:
                body_lines.append(f"[{role}]: {content}")
                body_lines.append("")

        body_lines.append("--- Reply to this email to continue the conversation. ---")
        body = "\n".join(body_lines)

        # Truncate if too long
        if len(body) > 48_000:
            body = body[:47_000] + "\n\n... [truncated, use the desktop app for full history]\n\n--- Reply to this email to continue the conversation. ---"

        subject = f"[Agent] {title}"

        # Send email via adapter (SMTP is blocking — run in executor)
        loop = asyncio.get_running_loop()
        message_id = await loop.run_in_executor(
            None,
            lambda: email_adapter._send_new_email(recipient, subject, body),
        )

        # Register in adapter threading maps
        normalized_subject = subject.strip().lower()
        email_adapter._thread_context[(recipient, normalized_subject)] = {
            "subject": subject,
            "message_id": message_id,
        }
        email_adapter._msg_id_to_thread[message_id] = (recipient, normalized_subject)

        # Store message ID in session for In-Reply-To routing
        msg_ids = list(session._email_msg_ids or [])
        if message_id not in msg_ids:
            msg_ids.append(message_id)
        store.update(session_id, _email_msg_ids=msg_ids)

        return {"ok": True, "message_id": message_id, "recipient": recipient}

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
        params = dict(request.query_params)
        limit = int(params.get("limit", 40))
        offset = int(params.get("offset", 0))
        min_messages = int(params.get("min_messages", 0))
        archived = params.get("archived", "exclude")
        order = params.get("order", "recent")
        session_list, total = store.list_sessions(
            limit=limit, offset=offset, min_messages=min_messages,
            archived=archived, order=order,
        )
        return {
            "sessions": [store.to_session_info(s) for s in session_list],
            "total": total,
            "offset": offset,
            "profile_totals": {"default": total},
        }

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

    # -- Config ------------------------------------------------------------
    # Frontend config is stored under a single "hermes_config" key in
    # gateway-config.json to isolate it from gateway-internal keys like
    # "default_agent" and prevent nesting/leaking of "agents" arrays.

    @app.get("/api/config")
    async def rest_config(request: Request) -> dict[str, Any]:
        agents = detect_agents()
        config = store.get_config("hermes_config", {}) if store else {}
        return {
            "config": config,
            "agents": agents,
        }

    @app.get("/api/config/defaults")
    async def rest_config_defaults(request: Request) -> dict[str, Any]:
        """Return default config values for agent-gateway mode."""
        return {
            "defaults": {
                "display.language": "en",
                "terminal.cwd": "",
                "approvals.mode": "suggest",
                "approvals.timeout": "300",
                "security.redact_secrets": "true",
            },
        }

    @app.get("/api/config/schema")
    async def rest_config_schema(request: Request) -> dict[str, Any]:
        """Return config field schema for the settings page."""
        return {
            "category_order": ["model", "chat", "workspace", "safety", "advanced"],
            "fields": {
                "display.language": {
                    "category": "chat",
                    "type": "select",
                    "options": ["en", "zh", "ja", "ko"],
                    "description": "UI display language.",
                },
                "terminal.cwd": {
                    "category": "workspace",
                    "type": "string",
                    "description": "Default working directory for new sessions.",
                },
                "approvals.mode": {
                    "category": "safety",
                    "type": "select",
                    "options": ["suggest", "auto", "strict"],
                    "description": "Approval mode: suggest (ask when unsure), auto (allow most), strict (ask always).",
                },
                "approvals.timeout": {
                    "category": "safety",
                    "type": "number",
                    "description": "Seconds before an approval request times out.",
                },
                "security.redact_secrets": {
                    "category": "safety",
                    "type": "boolean",
                    "description": "Redact API keys and secrets from conversation output.",
                },
            },
        }

    @app.patch("/api/config")
    async def rest_config_set(request: Request) -> dict[str, Any]:
        body = await request.json()
        if store and isinstance(body, dict):
            # Merge into existing hermes_config, stripping non-config keys
            clean = {k: v for k, v in body.items() if k not in ("agents", "current", "current_params")}
            current = store.get_config("hermes_config", {})
            if isinstance(current, dict):
                current.update(clean)
            else:
                current = dict(clean)
            store.set_config("hermes_config", current)
        return {"updated": True}

    @app.put("/api/config")
    async def rest_config_put(request: Request) -> dict[str, Any]:
        body = await request.json()
        config = body.get("config", {})
        if store and isinstance(config, dict):
            # Strip non-config fields the frontend may echo back
            clean = {k: v for k, v in config.items() if k not in ("agents", "current", "current_params")}
            store.set_config("hermes_config", clean)
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

    # -- Skills & Tools ----------------------------------------------------

    @app.get("/api/skills")
    async def rest_skills(request: Request) -> list:
        from agent_gateway.server.skills_scanner import scan_skills
        return scan_skills(sessions.default_agent_type, store)

    @app.put("/api/skills/toggle")
    async def rest_skills_toggle(request: Request) -> dict[str, Any]:
        from agent_gateway.server.skills_scanner import toggle_skill
        body = await request.json() if await request.body() else {}
        return toggle_skill(body.get("name", ""), body.get("enabled", True), store)

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

    # -- Messaging ---------------------------------------------------------

    def _get_platform_env() -> dict[str, dict[str, str]]:
        """Read persisted platform env vars from gateway-config.json."""
        return store.get_config("platform_env", {})

    def _set_platform_env(data: dict[str, dict[str, str]]) -> None:
        """Persist platform env vars to gateway-config.json."""
        store.set_config("platform_env", data)

    # Mapping from gateway.yaml config keys to their env var equivalents.
    # Used to detect existing config set via YAML (not just platform_env/os.environ).
    _YAML_TO_ENV: dict[str, dict[str, str]] = {
        "email": {
            "address": "EMAIL_ADDRESS",
            "password": "EMAIL_PASSWORD",
            "imap_host": "EMAIL_IMAP_HOST",
            "imap_port": "EMAIL_IMAP_PORT",
            "smtp_host": "EMAIL_SMTP_HOST",
            "smtp_port": "EMAIL_SMTP_PORT",
            "poll_interval": "EMAIL_POLL_INTERVAL",
            "allowed_users": "EMAIL_ALLOWED_USERS",
            "allow_all_users": "EMAIL_ALLOW_ALL_USERS",
            "home_channel": "EMAIL_HOME_ADDRESS",
        },
    }

    def _get_yaml_env(name: str) -> dict[str, str]:
        """Read env-var-equivalent values from gateway.yaml for a platform.

        Returns a dict mapping env var names to their YAML values (as strings).
        """
        import os
        result: dict[str, str] = {}
        if not runner or not hasattr(runner, "config"):
            return result
        pcfg = runner.config.get_platform(name)
        if not pcfg:
            return result
        mapping = _YAML_TO_ENV.get(name, {})
        extra = pcfg.extra or {}
        for yaml_key, env_key in mapping.items():
            val = extra.get(yaml_key)
            if val is not None:
                result[env_key] = str(val)
        # Also check pcfg.token → {NAME}_TOKEN
        if pcfg.token:
            result[f"{name.upper()}_TOKEN"] = pcfg.token
        return result

    def _build_env_vars(
        entry: Any,
        persisted: dict[str, str],
        yaml_env: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Build env_vars list for a platform entry.

        Combines the static ``env_var_defs`` metadata with values from
        three sources (in priority order): persisted platform_env,
        os.environ, and gateway.yaml config.
        """
        import os
        yaml = yaml_env or {}
        result: list[dict[str, Any]] = []
        for defn in getattr(entry, "env_var_defs", []):
            value = persisted.get(defn.key) or os.environ.get(defn.key) or yaml.get(defn.key, "")
            is_set = bool(value)
            if is_set and defn.is_password:
                redacted = "••••••••"
            elif is_set:
                # For non-password fields, show a masked hint so the user
                # knows something is configured without exposing the full value.
                # Show first 2 chars + "••" for short values, or first 3 + "•••" for longer.
                if len(value) <= 4:
                    redacted = value[:1] + "••"
                else:
                    redacted = value[:3] + "•••"
            else:
                redacted = None
            result.append({
                "key": defn.key,
                "description": defn.description,
                "prompt": defn.prompt,
                "is_password": defn.is_password,
                "required": defn.required,
                "advanced": defn.advanced,
                "url": defn.url or None,
                "is_set": is_set,
                "redacted_value": redacted,
            })
        return result

    @app.get("/api/messaging/platforms")
    async def rest_messaging_platforms(request: Request) -> dict[str, Any]:
        """Return status of ALL registered messaging platforms.

        Iterates the adapter registry so unconfigured platforms appear too.
        Runtime adapter state (connected/error) is merged from ``runner.adapters``.
        """
        from agent_gateway.core.registry import registry as platform_registry

        all_env = _get_platform_env()
        platforms: list[dict[str, Any]] = []

        for entry in platform_registry.all_entries():
            # Runtime adapter state (if running)
            adapter = runner.adapters.get(entry.name) if runner else None
            is_connected = adapter.is_connected if adapter else False
            has_error = adapter.has_fatal_error if adapter else False

            # Persisted env vars for this platform
            platform_env = all_env.get(entry.name, {})

            # Env vars from gateway.yaml config (for platforms configured
            # before the platform_env persistence was added)
            yaml_env = _get_yaml_env(entry.name)

            # Check if required env vars are set (platform_env > os.environ > yaml)
            import os as _os
            required_set = all(
                bool(platform_env.get(k) or _os.environ.get(k) or yaml_env.get(k))
                for k in entry.required_env
            )

            # Determine state
            if is_connected:
                state = "connected"
            elif has_error:
                state = adapter.fatal_error_code if adapter else "error"
            elif not required_set:
                state = "not_configured"
            else:
                state = "disconnected"

            platforms.append({
                "id": entry.name,
                "name": entry.label,
                "description": entry.platform_hint[:120] if entry.platform_hint else entry.label,
                "docs_url": "",
                "enabled": entry.name in (runner.adapters if runner else {}),
                "configured": required_set,
                "gateway_running": runner is not None and runner._running,
                "state": state,
                "env_vars": _build_env_vars(entry, platform_env, yaml_env),
                "error_message": adapter.fatal_error_message if has_error else None,
                "error_code": adapter.fatal_error_code if has_error else None,
            })

        return {"platforms": platforms}

    @app.put("/api/messaging/platforms/{platform_id}")
    async def rest_messaging_platform_update(
        platform_id: str,
        request: Request,
    ) -> dict[str, Any]:
        """Update a platform's configuration and hot-restart if needed.

        Accepts ``{enabled?, env?: {...}, clear_env?: [...]}``.
        Persists env vars to ``gateway-config.json`` and restarts the adapter.
        """
        from agent_gateway.core.registry import registry as platform_registry
        import os

        body = await request.json()
        entry = platform_registry.get(platform_id)
        if entry is None:
            return JSONResponse({"error": f"Unknown platform: {platform_id}"}, status_code=404)

        # Load current persisted env
        all_env = _get_platform_env()
        platform_env = dict(all_env.get(platform_id, {}))

        # Apply env updates
        new_env = body.get("env")
        if new_env:
            for k, v in new_env.items():
                if v:  # skip empty values
                    platform_env[k] = v
                    os.environ[k] = v  # set in-process so adapter reads it

        # Clear env vars
        clear_keys = body.get("clear_env", [])
        for k in clear_keys:
            platform_env.pop(k, None)
            os.environ.pop(k, None)

        # Persist
        all_env[platform_id] = platform_env
        _set_platform_env(all_env)

        # Handle enable/disable
        enabled = body.get("enabled")
        should_run = enabled if enabled is not None else (platform_id in (runner.adapters if runner else {}))

        if not runner:
            return {"ok": True, "platform": platform_id}

        if should_run:
            # Build config dict from persisted env vars
            config_dict = dict(platform_env)
            success = await runner.restart_adapter(platform_id, config_dict)
            if not success:
                logger.warning("Failed to restart adapter '%s'", platform_id)
        else:
            await runner.stop_adapter(platform_id)

        return {"ok": True, "platform": platform_id}

    @app.post("/api/messaging/platforms/{platform_id}/test")
    async def rest_messaging_platform_test(
        platform_id: str,
        request: Request,
    ) -> dict[str, Any]:
        """Test a platform's connection using current or provided credentials.

        Creates a temporary adapter instance, calls ``connect()``, and returns
        the result.  The temporary instance is discarded after testing.
        """
        from agent_gateway.core.registry import registry as platform_registry
        import os

        entry = platform_registry.get(platform_id)
        if entry is None:
            return {"ok": False, "message": f"Unknown platform: {platform_id}"}

        # Check if deps are available
        if not entry.check_fn():
            return {"ok": False, "message": f"Dependencies not installed: {entry.install_hint}"}

        # Load persisted env vars
        all_env = _get_platform_env()
        platform_env = dict(all_env.get(platform_id, {}))

        # Merge with request body (test with new credentials before saving)
        try:
            body = await request.json()
        except Exception:
            body = {}
        test_env = body.get("env", {})
        merged_env = {**platform_env, **test_env}

        # Temporarily set env vars for adapter init
        prev_vals: dict[str, str | None] = {}
        for k, v in merged_env.items():
            prev_vals[k] = os.environ.get(k)
            if v:
                os.environ[k] = v

        try:
            config_dict = dict(merged_env)
            adapter = platform_registry.create_adapter(platform_id, config_dict)
            if adapter is None:
                return {"ok": False, "message": "Failed to create adapter instance. Check credentials."}

            connected = await adapter.connect()
            try:
                await adapter.disconnect()
            except Exception:
                pass

            if connected:
                return {"ok": True, "message": f"Successfully connected to {entry.label}", "state": "connected"}
            else:
                return {"ok": False, "message": f"Connection to {entry.label} failed. Check credentials."}

        except Exception as exc:
            return {"ok": False, "message": f"Connection error: {exc}"}
        finally:
            # Restore original env vars
            for k, prev in prev_vals.items():
                if prev is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = prev

    # -- Cron --------------------------------------------------------------

    def _job_to_api(job: dict | None) -> dict[str, Any]:
        """Map internal job dict to the frontend CronJob type."""
        if job is None:
            return {}
        return {
            "id": job.get("id", ""),
            "name": job.get("name") or None,
            "prompt": job.get("prompt") or None,
            "script": job.get("script") or None,
            "deliver": job.get("deliver") or None,
            "schedule": job.get("schedule"),
            "schedule_display": job.get("schedule_display") or None,
            "enabled": job.get("enabled", True),
            "state": job.get("state") or None,
            "last_run_at": job.get("last_run_at") or None,
            "last_error": job.get("last_error") or None,
            "next_run_at": job.get("next_run_at") or None,
            "no_agent": job.get("no_agent", False),
            "context_from": job.get("context_from") or [],
        }

    @app.get("/api/cron/jobs")
    async def rest_cron_jobs(request: Request) -> list:
        return [_job_to_api(j) for j in cron_manager.list_jobs()]

    @app.post("/api/cron/jobs")
    async def rest_cron_create(request: Request) -> dict[str, Any]:
        body = await request.json() if await request.body() else {}
        try:
            job = cron_manager.create_job(
                prompt=body.get("prompt", ""),
                schedule=body.get("schedule", ""),
                name=body.get("name"),
                deliver=body.get("deliver"),
                script=body.get("script"),
                no_agent=body.get("no_agent", False),
                context_from=body.get("context_from"),
            )
            return _job_to_api(job)
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

    @app.get("/api/cron/jobs/{job_id}")
    async def rest_cron_job(job_id: str) -> dict[str, Any]:
        job = cron_manager.get_job(job_id)
        if not job:
            return JSONResponse(status_code=404, content={"error": "Job not found"})
        return _job_to_api(job)

    @app.patch("/api/cron/jobs/{job_id}")
    async def rest_cron_update(job_id: str, request: Request) -> dict[str, Any]:
        body = await request.json() if await request.body() else {}
        job = cron_manager.update_job(job_id, body)
        if not job:
            return JSONResponse(status_code=404, content={"error": "Job not found"})
        return _job_to_api(job)

    @app.put("/api/cron/jobs/{job_id}")
    async def rest_cron_update_put(job_id: str, request: Request) -> dict[str, Any]:
        body = await request.json() if await request.body() else {}
        updates = body.get("updates", body)
        job = cron_manager.update_job(job_id, updates)
        if not job:
            return JSONResponse(status_code=404, content={"error": "Job not found"})
        return _job_to_api(job)

    @app.delete("/api/cron/jobs/{job_id}")
    async def rest_cron_delete(job_id: str) -> dict[str, Any]:
        ok = cron_manager.delete_job(job_id)
        return {"ok": ok}

    @app.post("/api/cron/jobs/{job_id}/pause")
    async def rest_cron_pause(job_id: str) -> dict[str, Any]:
        job = cron_manager.pause_job(job_id)
        if not job:
            return JSONResponse(status_code=404, content={"error": "Job not found"})
        return _job_to_api(job)

    @app.post("/api/cron/jobs/{job_id}/resume")
    async def rest_cron_resume(job_id: str) -> dict[str, Any]:
        job = cron_manager.resume_job(job_id)
        if not job:
            return JSONResponse(status_code=404, content={"error": "Job not found"})
        return _job_to_api(job)

    @app.post("/api/cron/jobs/{job_id}/trigger")
    async def rest_cron_trigger(job_id: str) -> dict[str, Any]:
        job = cron_manager.trigger_job(job_id)
        if not job:
            return JSONResponse(status_code=404, content={"error": "Job not found"})
        return _job_to_api(job)

    # -- Gateway / Updates -------------------------------------------------

    @app.post("/api/gateway/restart")
    async def rest_gateway_restart(request: Request) -> dict[str, Any]:
        return {"ok": True}

    # -- Auth (WebSocket ticket) -------------------------------------------

    @app.post("/api/auth/ws-ticket")
    async def rest_auth_ws_ticket(request: Request) -> dict[str, Any]:
        return {"ticket": ""}

    # -- Frontend startup stubs (called during app boot, return empty data) --

    @app.get("/api/logs")
    async def rest_logs(request: Request) -> dict[str, Any]:
        return {"logs": [], "lines": []}

    @app.get("/api/analytics/usage")
    async def rest_analytics_usage(request: Request) -> dict[str, Any]:
        return {"daily": [], "models": [], "skills": [],
                "totals": {}, "skills_summary": {}}

    @app.post("/api/hermes/update")
    async def rest_hermes_update(request: Request) -> dict[str, Any]:
        return {"ok": True, "updated": False}

    @app.get("/api/providers/oauth")
    async def rest_providers_oauth(request: Request) -> dict[str, Any]:
        return {"providers": []}

    @app.get("/api/audio/elevenlabs/voices")
    async def rest_elevenlabs_voices(request: Request) -> dict[str, Any]:
        return {"voices": []}

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

        # Wire runner's desktop_emit so platform messages (email, etc.)
        # also push streaming events to the desktop client.
        if runner:
            runner.desktop_emit = emit

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
            # Unwire runner's desktop_emit
            if runner:
                runner.desktop_emit = None
            # Cleanup sessions on disconnect
            await sessions.close_all()

    return app


def _ws_auth_ok(provided: str | None, expected: str) -> bool:
    """Constant-time token comparison."""
    if not provided:
        return False
    return hmac.compare_digest(provided.encode(), expected.encode())

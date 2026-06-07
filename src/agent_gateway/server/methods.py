"""
JSON-RPC method handlers for the agent-gateway server.

Each handler receives ``(params, emit, sessions)`` where:
- ``params`` is the JSON-RPC params dict
- ``emit`` is an async callback ``emit(event_type, payload, session_id)``
- ``sessions`` is the ``SessionManager``

Handlers return a result dict that gets wrapped in a JSON-RPC response.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agent_gateway.agents.base import CLIAgentError
from agent_gateway.server.agent_status import detect_agents
from agent_gateway.server.session_manager import SessionManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Session methods
# ---------------------------------------------------------------------------

async def handle_session_create(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Create a new chat session."""
    session = await sessions.create_session(
        agent_type=params.get("agent_type"),
        cwd=params.get("cwd"),
    )
    return {
        "session_id": session.session_id,
        "stored_session_id": session.session_id,
        "info": _session_info(session),
    }


async def handle_session_resume(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Resume an existing session (in-memory or rehydrated from file store)."""
    session_id = params.get("session_id") or params.get("stored_session_id")
    session = await sessions.resume_session(session_id)
    if session is None:
        return {"error": f"Session {session_id} not found"}
    return {
        "session_id": session.session_id,
        "stored_session_id": session.session_id,
        "resumed": True,
        "messages": session.history,
        "message_count": len(session.history),
        "info": _session_info(session),
    }


async def handle_session_close(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Close a session."""
    session_id = params.get("session_id", "")
    closed = await sessions.close_session(session_id)
    return {"closed": closed}


async def handle_session_list(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """List all active sessions."""
    return {
        "sessions": [s.to_dict() for s in sessions.list_sessions()],
    }


# ---------------------------------------------------------------------------
# Prompt / chat
# ---------------------------------------------------------------------------

async def handle_prompt_submit(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Submit a prompt and stream the response back as events.

    Returns immediately so the JSON-RPC response doesn't block on the
    (potentially long-running) CLI invocation.  The actual streaming
    happens in a background asyncio task that emits events:

      - message.start  — before the first chunk
      - message.delta* — each text chunk as it arrives
      - message.complete — after the last chunk
    """
    session_id = params.get("session_id", "")
    text = params.get("text", "")

    if not text:
        await emit("error", {"message": "Empty prompt"}, session_id)
        return {"status": "error", "message": "Empty prompt"}

    # Auto-create session if not exists
    session = sessions.get_session(session_id)
    if session is None:
        session = await sessions.create_session()
        session_id = session.session_id

    # Fire-and-forget: run the actual streaming in a background task
    asyncio.create_task(
        _run_prompt(session_id, text, session, emit, sessions),
    )

    # Return immediately — events will arrive asynchronously
    return {"status": "ok"}


async def _run_prompt(
    session_id: str,
    text: str,
    session: Any,
    emit: Any,
    sessions: SessionManager,
) -> None:
    """Background task that streams a prompt and emits events."""
    # Push message.start
    await emit("message.start", {}, session_id)

    full_text: list[str] = []
    try:
        async for chunk in session.bridge.stream(
            session_key=session_id,
            message=text,
            history=session.history,
            system_extra="",
            session_ref=session.backend_session_ref,
        ):
            full_text.append(chunk)
            await emit("message.delta", {"text": chunk}, session_id)

    except CLIAgentError as exc:
        logger.error("Agent error: %s", exc)
        error_msg = str(exc)
        full_text.append(f"\n\n⚠️ Agent error: {error_msg}")
        await emit("message.delta", {"text": error_msg}, session_id)

    except Exception as exc:
        logger.exception("Unexpected error in prompt.submit")
        error_msg = f"Unexpected error: {exc}"
        full_text.append(error_msg)
        await emit("error", {"message": error_msg}, session_id)

    response_text = "".join(full_text)

    # Update history
    session.history.append({"role": "user", "content": text})
    session.history.append({"role": "assistant", "content": response_text})

    # Persist to file store
    sessions.persist_session(session_id)

    # Push message.complete
    await emit("message.complete", {"text": response_text}, session_id)


# ---------------------------------------------------------------------------
# Model / agent selection
# ---------------------------------------------------------------------------

async def handle_model_options(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Return available agents as model providers."""
    agents = detect_agents()
    providers = [
        {
            "slug": a["slug"],
            "name": a["name"],
            "models": ["default"],
            "is_current": a["slug"] == sessions.default_agent_type,
            "installed": a["installed"],
        }
        for a in agents
    ]
    return {
        "model": "default",
        "provider": sessions.default_agent_type,
        "providers": providers,
    }


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

async def handle_commands_catalog(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Return available slash commands."""
    return {
        "pairs": [
            ["/new", "Start a new session"],
            ["/reset", "Reset current session history"],
            ["/agent", "Switch agent type: /agent <claude-code|pi|codex>"],
            ["/help", "Show available commands"],
        ],
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

async def handle_config_get(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Return current configuration."""
    return {
        "default_agent": sessions.default_agent_type,
        "available_agents": ["claude-code", "pi", "codex"],
    }


async def handle_config_set(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Set a configuration value."""
    key = params.get("key", "")
    value = params.get("value")

    if key == "default_agent" and isinstance(value, str):
        sessions.default_agent_type = value
        return {"updated": True, "key": key, "value": value}

    if key == "agent" and isinstance(value, str):
        # Switch agent for a specific session
        session_id = params.get("session_id", "")
        agent_params = params.get("agent_params")
        if session_id:
            await sessions.set_agent(session_id, value, agent_params=agent_params)
            return {"updated": True, "key": key, "value": value}
        sessions.default_agent_type = value
        return {"updated": True, "key": key, "value": value}

    return {"updated": False, "message": f"Unknown config key: {key}"}


# ---------------------------------------------------------------------------
# Setup / readiness (agent-gateway mode: always ready, no provider needed)
# ---------------------------------------------------------------------------

async def handle_setup_status(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Report provider setup status. Agent-gateway uses local CLIs — always configured."""
    return {"provider_configured": True}


async def handle_setup_runtime_check(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Runtime readiness check. Agent-gateway is always ready."""
    return {"ok": True}


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

async def handle_tools_list(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Return available toolsets (stub for Phase 1)."""
    return {
        "toolsets": [
            {
                "name": "agent-tools",
                "description": "Tools provided by the active agent (claude-code / pi / codex)",
                "tool_count": 0,
                "enabled": True,
                "tools": [],
            },
        ],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _session_info(session: Any) -> dict[str, Any]:
    """Build a session info dict."""
    return {
        "session_id": session.session_id,
        "agent_type": session.agent_type,
        "created_at": session.created_at,
        "cwd": session.cwd,
        "title": session.title or f"Chat ({session.agent_type})",
        "message_count": len(session.history),
        "backend_session_ref": getattr(session, "backend_session_ref", None),
        "model": getattr(session, "model", None),
    }

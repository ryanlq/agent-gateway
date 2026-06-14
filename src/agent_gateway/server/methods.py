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
import glob
import json
import logging
import os
import re
import time
from typing import Any

from agent_gateway.agents.base import CLIAgentError
from agent_gateway.server.agent_status import detect_agents
from agent_gateway.server.session_manager import SessionManager

# Module-level reference to the shared CronManager, set during app startup.
# When set, the desktop client path injects cron awareness into agent prompts
# and post-processes responses for cron operations.
_cron_manager: Any = None

logger = logging.getLogger(__name__)

# Track running prompt tasks per session so session.interrupt can cancel them.
_running_prompts: dict[str, asyncio.Task] = {}


# ---------------------------------------------------------------------------
# Session methods
# ---------------------------------------------------------------------------

async def handle_session_create(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Create a new chat session."""
    cwd = params.get("cwd")
    # Fallback: if no cwd provided, try hermes_config.terminal.cwd
    if not cwd and sessions._store:
        hermes_cfg = sessions._store.get_config("hermes_config", {})
        if isinstance(hermes_cfg, dict):
            tc = hermes_cfg.get("terminal")
            if isinstance(tc, dict) and tc.get("cwd"):
                cwd = tc["cwd"]

    # Load stored per-agent params so model, bare, mode etc persist across
    # new sessions (the frontend model picker doesn't send agent_params).
    agent_type = params.get("agent_type") or sessions.default_agent_type
    agent_params = params.get("agent_params")
    if not agent_params and sessions._store:
        all_params: dict = sessions._store.get_config("agent_params", {})
        if isinstance(all_params, dict):
            agent_params = all_params.get(agent_type)

    session = await sessions.create_session(
        agent_type=agent_type,
        cwd=cwd,
        agent_params=agent_params,
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


async def handle_session_interrupt(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Interrupt a running prompt in a session.

    Cancels the background streaming task which in turn kills the subprocess
    (handled by ``_run_subprocess_streaming``'s ``CancelledError`` branch).
    """
    session_id = params.get("session_id", "")
    task = _running_prompts.get(session_id)
    if task is not None and not task.done():
        task.cancel()
        logger.info("Interrupted session %s", session_id)
        return {"status": "interrupted", "session_id": session_id}
    return {"status": "idle", "session_id": session_id}


async def handle_session_steer(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Steer a live turn by appending text to the next tool result.

    The CLI bridge doesn't expose a live tool window, so steering is not
    supported — the frontend falls back to queueing the text for the next turn.
    """
    return {"status": "rejected"}


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
    prompt_name = (params.get("system_prompt") or "").strip()

    if not text:
        await emit("error", {"message": "Empty prompt"}, session_id)
        return {"status": "error", "message": "Empty prompt"}

    # Auto-create session if not exists
    session = sessions.get_session(session_id)
    if session is None:
        session = await sessions.create_session()
        session_id = session.session_id

    # Fire-and-forget: run the actual streaming in a background task
    task = asyncio.create_task(
        _run_prompt(session_id, text, session, emit, sessions, prompt_name=prompt_name),
    )
    _running_prompts[session_id] = task

    def _cleanup(t: asyncio.Task, sid: str = session_id) -> None:
        _running_prompts.pop(sid, None)

    task.add_done_callback(_cleanup)

    # Return immediately — events will arrive asynchronously
    return {"status": "ok"}


async def _run_prompt(
    session_id: str,
    text: str,
    session: Any,
    emit: Any,
    sessions: SessionManager,
    prompt_name: str = "",
) -> None:
    """Background task that streams a prompt and emits events."""
    # Build system_extra with cron awareness if available
    from agent_gateway.core.session import build_session_context_prompt
    system_extra = build_session_context_prompt(
        _make_lightweight_session(session),
        cron_enabled=bool(_cron_manager),
    )

    # Append a user-defined custom prompt (requested via the system_prompt
    # param on prompt.submit). Mirrors the channel_prompt merge in
    # core/runner.py — body is resolved from the persisted custom_prompts map.
    if prompt_name:
        entry = _load_custom_prompts(sessions).get(prompt_name)
        body = entry.get("content") if isinstance(entry, dict) else None
        if body:
            system_extra += f"\n\n{body}"

    # Push message.start
    await emit("message.start", {}, session_id)

    full_text: list[str] = []
    try:
        async for chunk in session.bridge.stream(
            session_key=session_id,
            message=text,
            history=session.history,
            system_extra=system_extra,
            session_ref=session.backend_session_ref,
        ):
            full_text.append(chunk)
            await emit("message.delta", {"text": chunk}, session_id)

    except CLIAgentError as exc:
        logger.error("Agent error: %s", exc)
        error_msg = str(exc)
        full_text.append(f"\n\n⚠️ Agent error: {error_msg}")
        await emit("message.delta", {"text": error_msg}, session_id)

    except asyncio.CancelledError:
        logger.info("Prompt task cancelled for session %s", session_id)
        response_text = "".join(full_text)
        if response_text:
            session.history.append({"role": "user", "content": text})
            session.history.append({"role": "assistant", "content": response_text})
            sessions.persist_session(session_id)
        await emit("message.complete", {"text": response_text}, session_id)
        raise

    except Exception as exc:
        logger.exception("Unexpected error in prompt.submit")
        error_msg = f"Unexpected error: {exc}"
        full_text.append(error_msg)
        await emit("error", {"message": error_msg}, session_id)

    response_text = "".join(full_text)

    # Post-process: execute any cron operations embedded in the response
    cron_confirm_text = ""
    if _cron_manager and response_text:
        try:
            from agent_gateway.core.cron_tool import CronToolParser, CronToolExecutor
            ops = CronToolParser.extract_operations(response_text)
            if ops:
                executor = CronToolExecutor(_cron_manager)
                results = await executor.execute_all(
                    ops, origin=None, session_key=session_id,
                )
                response_text = CronToolParser.replace_operations(response_text, results)
                # Send confirmation as a follow-up message
                for cr in results:
                    icon = "✅" if cr.success else "❌"
                    cron_confirm_text += f"\n{icon} {cr.message}"
                logger.info(
                    "Processed %d cron operation(s) for desktop session %s",
                    len(ops), session_id,
                )
        except Exception as exc:
            logger.warning("Cron tool desktop post-processing failed: %s", exc)

    # Update history
    session.history.append({"role": "user", "content": text})
    session.history.append({"role": "assistant", "content": response_text})

    # Persist to file store
    sessions.persist_session(session_id)

    # Push cron confirmation as a follow-up if any operations were executed
    if cron_confirm_text:
        await emit("message.delta", {"text": cron_confirm_text}, session_id)

    # Push message.complete
    await emit("message.complete", {"text": response_text}, session_id)


def _make_lightweight_session(desktop_session: Any):
    """Create a lightweight Session-like object from a DesktopSession
    for ``build_session_context_prompt()``."""
    from agent_gateway.core.session import Session

    return Session(
        key=desktop_session.session_id,
        platform="desktop",
        user_id="desktop",
        chat_id=desktop_session.session_id,
    )


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
            ["/agent", "Switch agent type: /agent <claude-code|pi>"],
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
        "available_agents": ["claude-code", "pi"],
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

    if key == "reasoning" and isinstance(value, str):
        session_id = params.get("session_id", "")
        if session_id:
            await sessions.set_reasoning_fast(session_id, reasoning=value)
            return {"updated": True, "key": key, "value": value}
        return {"updated": False, "message": "No session_id for reasoning config"}

    if key == "fast" and isinstance(value, str):
        session_id = params.get("session_id", "")
        if session_id:
            await sessions.set_reasoning_fast(session_id, fast=value)
            return {"updated": True, "key": key, "value": value}
        return {"updated": False, "message": "No session_id for fast config"}

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
    """Return available toolsets and skills for the active agent."""
    from agent_gateway.server.skills_scanner import scan_skills

    store = sessions._store
    skills = scan_skills(sessions.default_agent_type, store) if store else []
    return {
        "toolsets": [
            {
                "name": "agent-tools",
                "description": "Tools provided by the active agent (claude-code / pi)",
                "tool_count": 0,
                "enabled": True,
                "tools": [],
            },
        ],
        "skills": skills,
    }


# ---------------------------------------------------------------------------
# Session title
# ---------------------------------------------------------------------------

async def handle_session_title(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Set the title for a session and persist it."""
    session_id = params.get("session_id", "")
    title = params.get("title", "")

    session = sessions.get_session(session_id)
    if session is None:
        return {"title": None, "pending": True}

    session.title = title
    sessions.persist_session(session_id)

    # Notify frontend of the updated session info
    await emit("session.info", _session_info(session), session_id)

    return {"title": title, "session_key": session_id}


# ---------------------------------------------------------------------------
# Custom prompts
# ---------------------------------------------------------------------------

_CUSTOM_PROMPTS_KEY = "custom_prompts"


def _now_ts() -> float:
    return time.time()


def _load_custom_prompts(sessions: SessionManager) -> dict[str, dict[str, Any]]:
    """Read the persisted custom-prompts map: name -> {content, updated_at}."""
    if not sessions._store:
        return {}
    raw = sessions._store.get_config(_CUSTOM_PROMPTS_KEY, {})
    if not isinstance(raw, dict):
        return {}
    return {name: entry for name, entry in raw.items() if isinstance(entry, dict)}


def _save_custom_prompts(sessions: SessionManager, prompts: dict[str, dict[str, Any]]) -> None:
    """Persist the custom-prompts map to gateway-config.json."""
    if sessions._store:
        sessions._store.set_config(_CUSTOM_PROMPTS_KEY, prompts)


async def handle_prompts_list(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """List all saved custom prompts."""
    prompts = _load_custom_prompts(sessions)
    items = [
        {
            "name": name,
            "content": entry.get("content", ""),
            "updated_at": entry.get("updated_at"),
        }
        for name, entry in prompts.items()
    ]
    return {"prompts": items}


async def handle_prompts_add(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Create a new custom prompt. Fails if the name already exists."""
    name = (params.get("name") or "").strip()
    content = params.get("content") or ""
    if not name:
        return {"ok": False, "error": "name is required"}
    prompts = _load_custom_prompts(sessions)
    if name in prompts:
        return {"ok": False, "error": f"prompt '{name}' already exists"}
    prompts[name] = {"content": content, "updated_at": _now_ts()}
    _save_custom_prompts(sessions, prompts)
    return {"ok": True, "name": name}


async def handle_prompts_update(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Update an existing prompt's content (upsert by name)."""
    name = (params.get("name") or "").strip()
    content = params.get("content") or ""
    if not name:
        return {"ok": False, "error": "name is required"}
    prompts = _load_custom_prompts(sessions)
    prompts[name] = {"content": content, "updated_at": _now_ts()}
    _save_custom_prompts(sessions, prompts)
    return {"ok": True, "name": name}


async def handle_prompts_delete(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Delete a custom prompt by name."""
    name = (params.get("name") or "").strip()
    prompts = _load_custom_prompts(sessions)
    if name not in prompts:
        return {"ok": False, "error": f"prompt '{name}' not found"}
    del prompts[name]
    _save_custom_prompts(sessions, prompts)
    return {"ok": True, "name": name}


# ---------------------------------------------------------------------------
# Slash command execution
# ---------------------------------------------------------------------------

# Built-in commands that don't need special handling
_BUILTIN_COMMANDS = {
    "new": "Create a new session",
    "reset": "Reset current session history",
    "agent": "Switch agent type: /agent <claude-code|pi>",
    "help": "Show available commands",
    "cron": "Manage cron jobs: /cron list|create|delete|pause|resume|trigger",
    "jobs": "List cron jobs (alias for /cron list)",
    "schedule": "Quick create: /schedule <schedule> <prompt>",
}


async def _handle_cron_command(
    args: str,
    session_id: str,
    sessions: SessionManager,
    emit: Any,
) -> dict[str, Any]:
    """Handle ``/cron`` subcommands for the desktop client.

    Supports: list, create, delete, pause, resume, trigger.
    If the first word is not a known subcommand, treats the entire input
    as natural language and delegates to the agent to parse a schedule.
    """
    parts = args.split(maxsplit=1)
    sub = parts[0].lower().strip() if parts else ""
    rest = parts[1].strip() if len(parts) > 1 else ""

    if sub in ("", "list", "ls"):
        return _cron_list_result()

    if sub in ("delete", "del", "rm"):
        job_id = rest.strip()
        if not job_id:
            return {"warning": "Usage: /cron delete <job_id>"}
        ok = _cron_manager.delete_job(job_id)
        if ok:
            return {"output": f"✅ 已删除定时任务 (ID: {job_id})"}
        return {"warning": f"未找到任务 {job_id}"}

    if sub == "pause":
        job_id = rest.strip()
        if not job_id:
            return {"warning": "Usage: /cron pause <job_id>"}
        job = _cron_manager.pause_job(job_id)
        if job:
            return {"output": f"⏸️ 已暂停 \"{job.get('name', job_id)}\" (ID: {job_id})"}
        return {"warning": f"未找到任务 {job_id}"}

    if sub == "resume":
        job_id = rest.strip()
        if not job_id:
            return {"warning": "Usage: /cron resume <job_id>"}
        job = _cron_manager.resume_job(job_id)
        if job:
            return {"output": f"▶️ 已恢复 \"{job.get('name', job_id)}\" (ID: {job_id})\n• 下次执行: {job.get('next_run_at', '?')}"}
        return {"warning": f"未找到任务 {job_id}"}

    if sub in ("trigger", "run", "exec"):
        job_id = rest.strip()
        if not job_id:
            return {"warning": "Usage: /cron trigger <job_id>"}
        job = _cron_manager.trigger_job(job_id)
        if job:
            return {"output": f"⚡ 已触发 \"{job.get('name', job_id)}\"，将在下次 tick 执行"}
        return {"warning": f"未找到任务 {job_id}"}

    # If 'create' is explicit, strip it; otherwise treat entire args as
    # natural language to be parsed by the agent.
    if sub == "create":
        create_args = rest
    else:
        # Not a known subcommand — treat the whole input as natural language
        create_args = args

    return await _cron_create_from_desktop(create_args, session_id, sessions, emit)


def _cron_list_result() -> dict[str, Any]:
    """Build the result dict for listing cron jobs."""
    jobs = _cron_manager.list_jobs()
    if not jobs:
        return {"output": "📋 当前没有任何定时任务。"}
    lines = [f"📋 **定时任务列表** ({len(jobs)} 个):", ""]
    for j in jobs:
        state_icon = {
            "scheduled": "🟢", "paused": "⏸️",
            "completed": "✅", "error": "❌",
        }.get(j.get("state", ""), "❓")
        job_id = j.get("id", "?")
        name = j.get("name", "?")
        schedule = j.get("schedule_display", "?")
        next_run = j.get("next_run_at", "?")
        lines.append(
            f"{state_icon} **{name}** (ID: `{job_id}`)\n"
            f"   计划: {schedule} | 下次: {next_run}"
        )
    return {"output": "\n".join(lines)}


async def _cron_create_from_desktop(
    args: str,
    session_id: str,
    sessions: SessionManager,
    emit: Any,
) -> dict[str, Any]:
    """Create a cron job from the desktop client.

    Two modes:
      1. Explicit: ``/cron "0 9 * * *" 检查服务器`` — schedule is the first arg
      2. Natural language: ``/cron 每天早上9点检查服务器`` — agent parses schedule

    Uses ``shlex.split`` to properly handle quoted schedule expressions like
    ``"0 9 * * *"`` as a single argument.
    """
    if not args:
        return {"warning": "Usage: /cron <schedule> <prompt> 或 /cron <自然语言描述>\n示例:\n  /cron \"0 9 * * *\" 检查服务器状态\n  /cron 每天早上9点提醒我打卡"}

    # Use shlex to properly handle quoted schedule expressions
    import shlex
    try:
        parts = shlex.split(args)
    except ValueError:
        # Fallback to simple split if shlex fails (e.g. unbalanced quotes)
        parts = args.split(maxsplit=1)

    if not parts:
        return {"warning": "请提供任务描述"}

    # Handle "every <duration>" as a single schedule token
    # shlex splits "every 30m check server" → ["every", "30m", "check", "server"]
    # but we need schedule="every 30m", prompt="check server"
    if parts[0].lower() == "every" and len(parts) >= 2:
        schedule = f"{parts[0]} {parts[1]}"
        prompt = " ".join(parts[2:]).strip()
        if prompt:
            return _do_create_cron_job(schedule, prompt)
        # If no prompt after schedule, fall through to agent parsing

    first = parts[0].strip()
    prompt = " ".join(parts[1:]).strip() if len(parts) > 1 else ""

    explicit_schedule_patterns = [
        r'^[\d\*\-,/]+\s+[\d\*\-,/]+\s+[\d\*\-,/]+\s+[\d\*\-,/]+\s+[\d\*\-,/]+',  # cron 5-field
        r'^every\s+',       # "every 30m"
        r'^\d+[mhd]$',      # "30m", "2h", "1d"
        r'^\d{4}-\d{2}-\d{2}T',  # ISO timestamp
    ]

    is_explicit = any(re.match(p, first, re.IGNORECASE) for p in explicit_schedule_patterns)

    if is_explicit and prompt:
        # Mode 1: explicit schedule + prompt
        return _do_create_cron_job(first, prompt)

    # Mode 2: natural language — use agent to parse schedule
    return await _cron_create_via_agent(args, session_id, sessions, emit)


def _do_create_cron_job(schedule: str, prompt: str, name: str = None) -> dict[str, Any]:
    """Actually create a cron job via CronManager."""
    try:
        job = _cron_manager.create_job(
            prompt=prompt,
            schedule=schedule,
            name=name,
            deliver="local",
        )
    except ValueError as e:
        return {"warning": f"❌ 创建失败: {e}"}
    except Exception as e:
        return {"warning": f"❌ 创建失败: {e}"}

    return {
        "output": (
            f"✅ 已创建定时任务 \"{job.get('name', 'cron job')}\"\n"
            f"• ID: {job.get('id', '?')}\n"
            f"• 计划: {job.get('schedule_display', schedule)}\n"
            f"• 下次执行: {job.get('next_run_at', '?')}"
        ),
    }


async def _cron_create_via_agent(
    natural_language: str,
    session_id: str,
    sessions: SessionManager,
    emit: Any,
) -> dict[str, Any]:
    """Use the agent to parse natural language into a cron schedule + prompt,
    then create the job.

    The agent receives a focused prompt and is asked to respond with a JSON
    block containing ``schedule`` and ``prompt`` fields.
    """
    session = sessions.get_session(session_id)
    if not session:
        return {"warning": "No active session to parse schedule."}

    parse_prompt = (
        "You are a schedule parser. The user wants to create a scheduled task.\n"
        "Parse their request into a cron schedule expression and a task prompt.\n\n"
        "Respond with ONLY a JSON object (no markdown, no explanation):\n"
        '{"schedule": "<schedule expression>", "prompt": "<task description>", "name": "<short name>"}\n\n'
        "Schedule format options:\n"
        '- Cron: "0 9 * * *" (daily at 9:00), "*/30 * * * *" (every 30 min)\n'
        '- Interval: "every 30m", "every 2h", "every 1d"\n'
        '- One-shot: "30m" (once in 30 min)\n\n'
        f'User request: "{natural_language}"\n\n'
        "JSON:"
    )

    # Use the session's bridge to parse
    chunks: list[str] = []
    try:
        async for chunk in session.bridge.stream(
            session_key=session_id,
            message=parse_prompt,
            history=[],
            system_extra="",
        ):
            if isinstance(chunk, str):
                chunks.append(chunk)
    except Exception as exc:
        return {"warning": f"Agent parsing failed: {exc}"}

    raw_response = "".join(chunks).strip()

    # Extract JSON from the response (agent may wrap it in markdown)
    json_match = re.search(r'\{[^{}]*"schedule"[^{}]*\}', raw_response, re.DOTALL)
    if not json_match:
        return {"warning": f"Could not parse schedule from agent response: {raw_response[:200]}"}

    try:
        parsed = json.loads(json_match.group(0))
    except json.JSONDecodeError as e:
        return {"warning": f"Invalid JSON from agent: {e}"}

    schedule = parsed.get("schedule", "").strip()
    prompt = parsed.get("prompt", "").strip()
    name = parsed.get("name", "").strip() or None

    if not schedule or not prompt:
        return {"warning": f"Agent did not provide schedule/prompt: {raw_response[:200]}"}

    return _do_create_cron_job(schedule, prompt, name=name)


async def handle_slash_exec(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Execute a slash command.

    Frontend sends: { session_id, command } where command has no leading ``/``.
    """
    session_id = params.get("session_id", "")
    raw = params.get("command", "").strip()
    # Strip any remaining leading slashes
    command = raw.lstrip("/")

    if not command:
        return {"output": "No command specified."}

    parts = command.split(None, 1)
    cmd_name = parts[0].lower()
    cmd_arg = parts[1] if len(parts) > 1 else ""

    # -- /new ----------------------------------------------------------------
    if cmd_name == "new":
        new_session = await sessions.create_session()
        await emit(
            "session.info",
            _session_info(new_session),
            new_session.session_id,
        )
        return {
            "output": f"Created new session {new_session.session_id}",
        }

    # -- /reset --------------------------------------------------------------
    if cmd_name == "reset":
        session = sessions.get_session(session_id)
        if session:
            session.history.clear()
            sessions.persist_session(session_id)
            return {"output": "Session history cleared."}
        return {"warning": "No active session to reset."}

    # -- /agent <type> -------------------------------------------------------
    if cmd_name == "agent":
        if not cmd_arg:
            agents = detect_agents()
            names = ", ".join(a["slug"] for a in agents)
            return {"output": f"Available agents: {names}\nUsage: /agent <type>"}
        agent_type = cmd_arg.strip().lower()
        agents = detect_agents()
        valid = {a["slug"] for a in agents}
        if agent_type not in valid:
            return {"warning": f"Unknown agent '{agent_type}'. Available: {', '.join(sorted(valid))}"}
        if session_id:
            await sessions.set_agent(session_id, agent_type)
            session = sessions.get_session(session_id)
            if session:
                await emit("session.info", _session_info(session), session_id)
            return {"output": f"Switched to {agent_type}."}
        return {"warning": "No active session."}

    # -- /help ---------------------------------------------------------------
    if cmd_name == "help":
        lines = ["Available commands:"]
        for name, desc in _BUILTIN_COMMANDS.items():
            lines.append(f"  /{name} — {desc}")
        return {"output": "\n".join(lines)}

    # -- /cron ---------------------------------------------------------------
    if cmd_name == "cron":
        if not _cron_manager:
            return {"warning": "Cron system is not available."}
        return await _handle_cron_command(cmd_arg, session_id, sessions, emit)

    # -- /jobs (alias for /cron list) ----------------------------------------
    if cmd_name == "jobs":
        if not _cron_manager:
            return {"warning": "Cron system is not available."}
        return await _handle_cron_command("list", session_id, sessions, emit)

    # -- /schedule <schedule> <prompt> (alias for /cron create) --------------
    if cmd_name == "schedule":
        if not _cron_manager:
            return {"warning": "Cron system is not available."}
        return await _handle_cron_command(f"create {cmd_arg}", session_id, sessions, emit)

    return {"warning": f"Unknown command: /{cmd_name}"}


# ---------------------------------------------------------------------------
# Completion
# ---------------------------------------------------------------------------

async def handle_complete_path(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Return file/directory path completions for ``@file:`` references.

    Frontend sends: { word, session_id?, cwd? }
    """
    word = params.get("word", "")
    cwd = params.get("cwd")

    # Resolve cwd: explicit param > session cwd > home dir
    if not cwd:
        session_id = params.get("session_id", "")
        session = sessions.get_session(session_id)
        if session and session.cwd:
            cwd = session.cwd
    if not cwd:
        cwd = os.path.expanduser("~")

    # Extract the path fragment after '@file:' or use the whole word
    prefix = word
    for marker in ("@file:", "@folder:", "@dir:", "@"):
        if word.startswith(marker):
            prefix = word[len(marker):]
            break

    # Expand ~ and make absolute
    prefix = os.path.expanduser(prefix)
    if not os.path.isabs(prefix):
        prefix = os.path.join(cwd, prefix)

    # Glob for matches
    base_dir = os.path.dirname(prefix)
    pattern = os.path.basename(prefix) + "*"
    try:
        entries = sorted(glob.glob(os.path.join(base_dir, pattern)))
    except (OSError, ValueError):
        entries = []

    items: list[dict[str, str]] = []
    for entry in entries[:50]:  # Cap results
        name = os.path.basename(entry)
        is_dir = os.path.isdir(entry)
        items.append({
            "text": name + ("/" if is_dir else ""),
            "display": name,
            "meta": "dir" if is_dir else os.path.splitext(name)[1] or "file",
        })

    return {"items": items}


async def handle_complete_slash(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Return slash command completions.

    Frontend sends: { text }
    """
    text = params.get("text", "").lstrip("/").lower()
    items: list[dict[str, str]] = []
    for name, desc in _BUILTIN_COMMANDS.items():
        if not text or name.startswith(text):
            items.append({
                "text": f"/{name}",
                "display": f"/{name}",
                "meta": desc,
            })
    return {"items": items}


# ---------------------------------------------------------------------------
# Session working directory
# ---------------------------------------------------------------------------

async def handle_session_cwd_set(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """Change the working directory for a session's agent subprocess.

    Frontend sends: { session_id, cwd }
    """
    session_id = params.get("session_id", "")
    cwd = params.get("cwd", "")

    if not cwd:
        return {"error": "cwd is required"}

    # Expand ~ and validate
    cwd = os.path.expanduser(cwd)
    if not os.path.isdir(cwd):
        return {"error": f"Not a directory: {cwd}"}

    session = sessions.get_session(session_id)
    if session is None:
        return {"error": f"Session {session_id} not found"}

    session.cwd = cwd
    session.workspace_name = os.path.basename(cwd)
    # Propagate to bridge subprocess
    session.bridge.config.cwd = cwd

    sessions.persist_session(session_id)
    await emit("session.info", _session_info(session), session_id)

    return {"session_id": session_id, "cwd": cwd}


# ---------------------------------------------------------------------------
# Approval / sudo / secret / clarify interaction
# ---------------------------------------------------------------------------

# In-memory store of pending approval requests that the agent subprocess
# is waiting on.  Keyed by request_id, each holds an asyncio.Event and the
# user's response once resolved.
_pending_approvals: dict[str, dict[str, Any]] = {}


async def _wait_for_approval(request_id: str, timeout: float = 300.0) -> dict[str, Any] | None:
    """Block until the user responds to an approval request, or timeout."""
    evt = asyncio.Event()
    _pending_approvals[request_id] = {"event": evt, "response": None}
    try:
        await asyncio.wait_for(evt.wait(), timeout=timeout)
        return _pending_approvals[request_id]["response"]
    except asyncio.TimeoutError:
        return None
    finally:
        _pending_approvals.pop(request_id, None)


async def handle_approval_respond(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """User responds to an approval request (approve / reject)."""
    choice = params.get("choice", "reject")
    session_id = params.get("session_id")

    # For now, resolve any pending approval for this session
    # (the request_id matching is handled via the event system in a
    # future iteration with full agent protocol support)
    resolved = False
    for rid, pending in list(_pending_approvals.items()):
        if not pending.get("event").is_set():
            pending["response"] = {"choice": choice, "session_id": session_id}
            pending["event"].set()
            resolved = True
            break

    return {"resolved": resolved}


async def handle_sudo_respond(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """User provides a sudo password."""
    request_id = params.get("request_id", "")
    password = params.get("password", "")

    pending = _pending_approvals.get(request_id)
    if pending and not pending["event"].is_set():
        pending["response"] = {"type": "sudo", "password": password}
        pending["event"].set()
        return {"status": "ok"}

    return {"status": "no_pending_request"}


async def handle_secret_respond(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """User provides a secret/API key value."""
    request_id = params.get("request_id", "")
    value = params.get("value", "")

    pending = _pending_approvals.get(request_id)
    if pending and not pending["event"].is_set():
        pending["response"] = {"type": "secret", "value": value}
        pending["event"].set()
        return {"status": "ok"}

    return {"status": "no_pending_request"}


async def handle_clarify_respond(
    params: dict[str, Any],
    emit: Any,
    sessions: SessionManager,
) -> dict[str, Any]:
    """User answers a clarification question from the agent."""
    request_id = params.get("request_id", "")
    answer = params.get("answer", "")

    pending = _pending_approvals.get(request_id)
    if pending and not pending["event"].is_set():
        pending["response"] = {"type": "clarify", "answer": answer}
        pending["event"].set()
        return {"ok": True}

    return {"ok": False, "message": "No pending request found"}


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
        "desktop_contract": 1,
        "running": True,
        "reasoning_effort": getattr(session, "reasoning", None),
        "fast": getattr(session, "fast", None) == "fast",
    }

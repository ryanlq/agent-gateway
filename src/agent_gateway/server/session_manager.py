"""
Session manager for the agent-gateway server.

Each desktop session maps to an agent bridge instance and maintains
conversation history in memory. When a ``SessionStore`` is injected,
sessions are also persisted to disk so they survive gateway restarts.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agent_gateway.agents.base import CLIAgentBridge
from agent_gateway.server.agent_factory import create_bridge

if TYPE_CHECKING:
    from agent_gateway.server.session_store import SessionStore

logger = logging.getLogger(__name__)

# Cap per-session history stored to disk.
_MAX_HISTORY = 500


@dataclass
class DesktopSession:
    """A single desktop chat session."""

    session_id: str
    agent_type: str
    bridge: CLIAgentBridge
    history: list[dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    cwd: str | None = None
    title: str | None = None
    # DEPRECATED legacy ref (kept for back-compat / _session_info). Not passed
    # to the CLI.
    backend_session_ref: str | None = None
    # The CLI's native session id, captured from turn-1 output and used as the
    # --resume / --session target from turn 2+. None until the first turn.
    cli_session_id: str | None = None
    status: str = "active"
    workspace_name: str | None = None
    reasoning: str | None = None
    fast: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "agent_type": self.agent_type,
            "created_at": self.created_at,
            "cwd": self.cwd,
            "title": self.title,
            "message_count": len(self.history),
            "backend_session_ref": self.backend_session_ref,
            "cli_session_id": self.cli_session_id,
            "status": self.status,
            "workspace_name": self.workspace_name,
            "reasoning": self.reasoning,
            "fast": self.fast,
        }


class SessionManager:
    """Manages desktop sessions with optional file-backed persistence."""

    def __init__(
        self,
        default_agent_type: str = "claude-code",
        session_store: SessionStore | None = None,
    ) -> None:
        self._sessions: dict[str, DesktopSession] = {}
        self._store = session_store
        # Restore persisted default agent if available
        if self._store:
            persisted = self._store.get_config("default_agent")
            if persisted:
                default_agent_type = persisted
        self.default_agent_type = default_agent_type

    @property
    def default_agent_type(self) -> str:
        return self._default_agent_type

    @default_agent_type.setter
    def default_agent_type(self, value: str) -> None:
        self._default_agent_type = value
        if self._store:
            self._store.set_config("default_agent", value)

    async def create_session(
        self,
        agent_type: str | None = None,
        cwd: str | None = None,
        agent_params: dict[str, Any] | None = None,
        session_id: str | None = None,
        backend_session_ref: str | None = None,
    ) -> DesktopSession:
        """Create a new session with the given agent type and parameters.

        Args:
            session_id: Override the auto-generated ID (used when rehydrating
                a persisted session on resume).
            backend_session_ref: Override the auto-generated CLI session ref
                (used when rehydrating a persisted session on resume).
        """
        atype = agent_type or self.default_agent_type
        sid = session_id or uuid.uuid4().hex[:16]
        ref = backend_session_ref or str(uuid.uuid4())
        bridge = create_bridge(atype, **(agent_params or {}))
        # Propagate cwd to the subprocess so agent CLIs run in the right directory
        if cwd:
            bridge.config.cwd = cwd
        ws_name = os.path.basename(cwd) if cwd else None
        session = DesktopSession(
            session_id=sid,
            agent_type=atype,
            bridge=bridge,
            cwd=cwd,
            backend_session_ref=ref,
            workspace_name=ws_name,
        )
        self._sessions[sid] = session

        # Persist to file store if available
        if self._store:
            self._store.create(
                session_id=sid,
                agent_type=atype,
                workspace=cwd,
                backend_session_ref=ref,
                model=agent_params.get("model") if agent_params else None,
            )

        logger.info(
            "Created session %s with agent %s (ref=%s, params=%s)",
            sid, atype, ref, agent_params,
        )
        return session

    async def resume_session(self, session_id: str) -> DesktopSession | None:
        """Return an existing in-memory session, or rehydrate from the file store."""
        session = self._sessions.get(session_id)
        if session is not None:
            return session

        # Try to rehydrate from persisted store
        if self._store is None:
            return None

        persisted = self._store.get(session_id)
        if persisted is None or persisted.status == "deleted":
            return None

        # Re-create the in-memory session with the original IDs
        atype = persisted.agent_type

        # Restore reasoning param for bridge construction
        reasoning = getattr(persisted, "reasoning", None)
        fast_val = getattr(persisted, "fast", None)
        bridge_params: dict[str, Any] = {}
        if reasoning:
            bridge_params["reasoning"] = reasoning
        # Also load stored per-agent params (model, bare, mode, etc.)
        # so resumed sessions pick up the user's configured defaults.
        if self._store:
            all_params: dict[str, dict] = self._store.get_config("agent_params", {})
            if isinstance(all_params, dict):
                stored = all_params.get(atype, {})
                if isinstance(stored, dict):
                    # Stored params take lower priority than explicit overrides
                    for k, v in stored.items():
                        bridge_params.setdefault(k, v)
        bridge = create_bridge(atype, **bridge_params)

        # Restore the captured native CLI session id (if any). Old records
        # persisted before this field existed load as None; the next turn then
        # re-seeds from history text and captures a fresh id (self-healing).
        restored_cli_id = getattr(persisted, "cli_session_id", None)

        # Propagate cwd to the bridge so agent CLIs run in the right directory
        if persisted.workspace:
            bridge.config.cwd = persisted.workspace

        session = DesktopSession(
            session_id=persisted.session_id,
            agent_type=atype,
            bridge=bridge,
            history=list(persisted.history),
            created_at=persisted.created_at,
            cwd=persisted.workspace,
            title=persisted.title,
            backend_session_ref=persisted.backend_session_ref,
            cli_session_id=restored_cli_id,
            status=persisted.status,
            workspace_name=persisted.workspace_name,
            reasoning=reasoning,
            fast=fast_val,
        )
        self._sessions[persisted.session_id] = session
        logger.info(
            "Rehydrated session %s from store (agent=%s, ref=%s)",
            persisted.session_id, atype, persisted.backend_session_ref,
        )
        return session

    async def close_session(self, session_id: str) -> bool:
        """Close and remove a session. Returns True if it existed."""
        session = self._sessions.pop(session_id, None)
        if session is None:
            return False
        # Persist final state before tearing down
        self.persist_session(session_id, session)
        try:
            await asyncio.wait_for(session.bridge.shutdown(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Timed out shutting down bridge for session %s", session_id)
        except Exception as exc:
            logger.error("Error shutting down bridge for session %s: %s", session_id, exc)
        logger.info("Closed session %s", session_id)
        return True

    async def close_all(self) -> int:
        """Close all sessions concurrently with timeout per session."""
        session_ids = list(self._sessions)
        if not session_ids:
            return 0

        # Close all sessions in parallel for faster shutdown
        results = await asyncio.gather(
            *[self.close_session(sid) for sid in session_ids],
            return_exceptions=True,
        )
        closed = sum(1 for r in results if r is True)
        logger.info("Closed %d/%d sessions", closed, len(session_ids))
        return closed

    def get_session(self, session_id: str) -> DesktopSession | None:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    def persist_session(self, session_id: str, session: DesktopSession | None = None) -> None:
        """Sync in-memory session state to the file store."""
        if self._store is None:
            return
        s = session or self._sessions.get(session_id)
        if s is None:
            return
        # Extract preview from first user message
        preview = None
        for msg in s.history:
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str) and content.strip():
                    preview = content.strip()[:120]
                    break
        # Fall back to existing preview from store if we didn't find one
        if preview is None:
            existing = self._store.get(session_id)
            if existing and existing.preview:
                preview = existing.preview
        updates: dict[str, Any] = {
            "history": s.history[-_MAX_HISTORY:],
            "message_count": len(s.history),
            "last_active": time.time(),
            "cli_session_id": s.cli_session_id,
            "agent_type": s.agent_type,
        }
        if s.title:
            updates["title"] = s.title
        if preview:
            updates["preview"] = preview
        if s.reasoning is not None:
            updates["reasoning"] = s.reasoning
        if s.fast is not None:
            updates["fast"] = s.fast
        self._store.update(session_id, **updates)
        self._store.auto_title(session_id)

    def list_sessions(self) -> list[DesktopSession]:
        """Return all active sessions."""
        return list(self._sessions.values())

    async def set_agent(
        self,
        session_id: str,
        agent_type: str,
        agent_params: dict[str, Any] | None = None,
    ) -> DesktopSession | None:
        """Switch the agent type for an existing session."""
        session = self._sessions.get(session_id)
        if session is None:
            return None
        # Shutdown old bridge with timeout
        try:
            await asyncio.wait_for(session.bridge.shutdown(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Timed out shutting down old bridge for session %s", session_id)
        except Exception as exc:
            logger.error("Error shutting down old bridge for session %s: %s", session_id, exc)
        # Create new bridge with params — fall back to stored per-agent params
        # when the caller (e.g. chat UI model picker) doesn't supply them.
        if not agent_params and self._store:
            all_params: dict[str, dict] = self._store.get_config("agent_params", {})
            if isinstance(all_params, dict):
                agent_params = all_params.get(agent_type)
        previous_type = session.agent_type
        session.agent_type = agent_type
        session.bridge = create_bridge(agent_type, **(agent_params or {}))
        # Preserve cwd from session when switching agents
        if session.cwd:
            session.bridge.config.cwd = session.cwd
        # A captured native CLI session id belongs to the previous agent's
        # CLI; switching engines (e.g. claude-code <-> pi) invalidates it.
        # Drop it so the next turn re-seeds from text history (the new agent
        # gets a recap, then captures its own fresh id). Same-agent param
        # refreshes keep the id — the native session is still valid.
        if previous_type != agent_type:
            session.cli_session_id = None
            self.persist_session(session_id)
        # Persist agent params per-agent
        if agent_params and self._store:
            all_params: dict[str, dict] = self._store.get_config("agent_params", {})
            if not isinstance(all_params, dict):
                all_params = {}
            all_params[agent_type] = agent_params
            self._store.set_config("agent_params", all_params)
        logger.info("Switched session %s to agent %s (params=%s)", session_id, agent_type, agent_params)
        return session

    async def set_reasoning_fast(
        self,
        session_id: str,
        *,
        reasoning: str | None = None,
        fast: str | None = None,
    ) -> DesktopSession | None:
        """Update reasoning effort and/or fast mode for a session.

        Recreates the bridge because reasoning/thinking flags are baked into
        CLI args at construction time.
        """
        session = self._sessions.get(session_id)
        if session is None:
            return None

        # Update session fields
        if reasoning is not None:
            session.reasoning = reasoning
        if fast is not None:
            session.fast = fast

        # Recreate bridge with updated reasoning param
        try:
            await asyncio.wait_for(session.bridge.shutdown(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Timed out shutting down bridge for session %s", session_id)
        except Exception as exc:
            logger.error("Error shutting down bridge for session %s: %s", session_id, exc)

        bridge_params: dict[str, Any] = {}
        if session.reasoning:
            bridge_params["reasoning"] = session.reasoning
        session.bridge = create_bridge(session.agent_type, **bridge_params)

        # Persist global reasoning default so runner's dynamic bridge picks it up
        if reasoning is not None and self._store:
            self._store.set_config("default_reasoning", reasoning)

        logger.info(
            "Updated session %s: reasoning=%s, fast=%s",
            session_id, session.reasoning, session.fast,
        )
        return session

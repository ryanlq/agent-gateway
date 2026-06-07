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
    backend_session_ref: str | None = None
    status: str = "active"
    workspace_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "agent_type": self.agent_type,
            "created_at": self.created_at,
            "cwd": self.cwd,
            "title": self.title,
            "message_count": len(self.history),
            "backend_session_ref": self.backend_session_ref,
            "status": self.status,
            "workspace_name": self.workspace_name,
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
        bridge = create_bridge(atype)
        session = DesktopSession(
            session_id=persisted.session_id,
            agent_type=atype,
            bridge=bridge,
            history=list(persisted.history),
            created_at=persisted.created_at,
            cwd=persisted.workspace,
            title=persisted.title,
            backend_session_ref=persisted.backend_session_ref,
            status=persisted.status,
            workspace_name=persisted.workspace_name,
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
        }
        if s.title:
            updates["title"] = s.title
        if preview:
            updates["preview"] = preview
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
        # Create new bridge with params
        session.agent_type = agent_type
        session.bridge = create_bridge(agent_type, **(agent_params or {}))
        logger.info("Switched session %s to agent %s (params=%s)", session_id, agent_type, agent_params)
        return session

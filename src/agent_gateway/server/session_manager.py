"""
Session manager for the agent-gateway server.

Each desktop session maps to an agent bridge instance and maintains
conversation history in memory.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from agent_gateway.agents.base import CLIAgentBridge
from agent_gateway.server.agent_factory import create_bridge

logger = logging.getLogger(__name__)


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "agent_type": self.agent_type,
            "created_at": self.created_at,
            "cwd": self.cwd,
            "title": self.title,
            "message_count": len(self.history),
        }


class SessionManager:
    """Manages desktop sessions."""

    def __init__(self, default_agent_type: str = "claude-code") -> None:
        self._sessions: dict[str, DesktopSession] = {}
        self.default_agent_type = default_agent_type

    async def create_session(
        self,
        agent_type: str | None = None,
        cwd: str | None = None,
        agent_params: dict[str, Any] | None = None,
    ) -> DesktopSession:
        """Create a new session with the given agent type and parameters."""
        atype = agent_type or self.default_agent_type
        session_id = uuid.uuid4().hex[:16]
        bridge = create_bridge(atype, **(agent_params or {}))
        session = DesktopSession(
            session_id=session_id,
            agent_type=atype,
            bridge=bridge,
            cwd=cwd,
        )
        self._sessions[session_id] = session
        logger.info("Created session %s with agent %s (params=%s)", session_id, atype, agent_params)
        return session

    async def resume_session(self, session_id: str) -> DesktopSession | None:
        """Return an existing session or None."""
        return self._sessions.get(session_id)

    async def close_session(self, session_id: str) -> bool:
        """Close and remove a session. Returns True if it existed."""
        session = self._sessions.pop(session_id, None)
        if session is None:
            return False
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

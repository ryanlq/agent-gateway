"""
Session manager for the agent-gateway server.

Each desktop session maps to an agent bridge instance and maintains
conversation history in memory.
"""

from __future__ import annotations

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
    ) -> DesktopSession:
        """Create a new session with the given agent type."""
        atype = agent_type or self.default_agent_type
        session_id = uuid.uuid4().hex[:16]
        bridge = create_bridge(atype)
        session = DesktopSession(
            session_id=session_id,
            agent_type=atype,
            bridge=bridge,
            cwd=cwd,
        )
        self._sessions[session_id] = session
        logger.info("Created session %s with agent %s", session_id, atype)
        return session

    async def resume_session(self, session_id: str) -> DesktopSession | None:
        """Return an existing session or None."""
        return self._sessions.get(session_id)

    async def close_session(self, session_id: str) -> bool:
        """Close and remove a session. Returns True if it existed."""
        session = self._sessions.pop(session_id, None)
        if session is None:
            return False
        await session.bridge.shutdown()
        logger.info("Closed session %s", session_id)
        return True

    async def close_all(self) -> int:
        """Close all sessions. Returns count closed."""
        count = 0
        for sid in list(self._sessions):
            if await self.close_session(sid):
                count += 1
        return count

    def get_session(self, session_id: str) -> DesktopSession | None:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[DesktopSession]:
        """Return all active sessions."""
        return list(self._sessions.values())

    async def set_agent(self, session_id: str, agent_type: str) -> DesktopSession | None:
        """Switch the agent type for an existing session."""
        session = self._sessions.get(session_id)
        if session is None:
            return None
        # Shutdown old bridge
        await session.bridge.shutdown()
        # Create new bridge
        session.agent_type = agent_type
        session.bridge = create_bridge(agent_type)
        logger.info("Switched session %s to agent %s", session_id, agent_type)
        return session

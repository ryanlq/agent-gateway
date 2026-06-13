"""
Session management for multi-platform conversations.

Each conversation is uniquely identified by a four-dimensional key:

    (platform, user_id, chat_id, thread_id)

This provides strong isolation: different users get independent sessions,
and the same user in different groups also gets independent sessions.

Supports automatic idle cleanup and configurable reset policies.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SessionResetPolicy(Enum):
    """When to automatically reset conversation history."""

    DAILY = "daily"       # Reset at midnight (local time)
    IDLE = "idle"         # Reset after idle timeout
    BOTH = "both"         # Reset on either condition
    NONE = "none"         # Never auto-reset


@dataclass
class Session:
    """
    A single conversation session.

    Tracks history, metadata, and timing information for one conversation
    thread across one platform.
    """

    key: str
    """Unique session identifier (from ``MessageSource.session_key()``)."""

    platform: str
    """Platform name."""

    user_id: str
    """Sender's platform-specific user ID."""

    chat_id: str
    """Conversation / channel / group ID."""

    thread_id: Optional[str] = None
    """Thread / topic / forum ID (None for flat chats)."""

    # -- Timing --------------------------------------------------------------
    created_at: float = field(default_factory=time.time)
    """Unix timestamp of session creation."""

    last_active: float = field(default_factory=time.time)
    """Unix timestamp of last activity."""

    # -- Content -------------------------------------------------------------
    history: list[dict[str, Any]] = field(default_factory=list)
    """Conversation history as a list of role/content dicts."""

    system_prompt_extra: str = ""
    """Additional text to inject into the system prompt."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Arbitrary adapter-specific metadata."""

    # -- Helpers -------------------------------------------------------------

    def touch(self) -> None:
        """Update last-active timestamp."""
        self.last_active = time.time()

    def is_idle(self, max_idle_seconds: float) -> bool:
        """Return True if the session has been idle longer than *max_idle_seconds*."""
        return (time.time() - self.last_active) > max_idle_seconds

    def add_message(self, role: str, content: str, **kwargs: Any) -> None:
        """Append a message to history and touch the session."""
        entry: dict[str, Any] = {"role": role, "content": content}
        entry.update(kwargs)
        self.history.append(entry)
        self.touch()

    def clear_history(self) -> None:
        """Clear conversation history but keep session identity."""
        self.history.clear()
        self.system_prompt_extra = ""

    def get_context_window(self, max_messages: int = 50) -> list[dict[str, Any]]:
        """Return the last *max_messages* entries from history."""
        if len(self.history) <= max_messages:
            return list(self.history)
        return self.history[-max_messages:]


class SessionStore:
    """
    Manages all active sessions with automatic idle cleanup.

    Usage::

        store = SessionStore(max_idle_seconds=3600)
        session = store.get_or_create(source)
        session.add_message("user", "Hello!")
        store.cleanup_idle()  # remove stale sessions
    """

    def __init__(
        self,
        max_idle_seconds: float = 3600.0,
        max_history: int = 200,
        reset_policy: SessionResetPolicy = SessionResetPolicy.IDLE,
    ) -> None:
        self._sessions: dict[str, Session] = {}
        self.max_idle_seconds = max_idle_seconds
        self.max_history = max_history
        self.reset_policy = reset_policy
        self._last_cleanup: float = time.time()

    # -- Core operations -----------------------------------------------------

    def get_or_create(self, source: Any) -> Session:
        """Look up an existing session or create a new one.

        *source* must have ``platform``, ``user_id``, ``chat_id``,
        ``thread_id`` attributes (a ``MessageSource``).
        """
        key = source.session_key()

        if key in self._sessions:
            session = self._sessions[key]
            session.touch()
            return session

        session = Session(
            key=key,
            platform=source.platform,
            user_id=source.user_id,
            chat_id=source.chat_id,
            thread_id=source.thread_id,
        )
        self._sessions[key] = session
        logger.debug("Created new session: %s", key)
        return session

    def get(self, key: str) -> Optional[Session]:
        """Look up a session by key.  Returns None if not found."""
        return self._sessions.get(key)

    def reset(self, key: str) -> bool:
        """Clear a session's history.  Returns True if session existed."""
        session = self._sessions.get(key)
        if session is None:
            return False
        session.clear_history()
        logger.info("Session reset: %s", key)
        return True

    def remove(self, key: str) -> bool:
        """Remove a session entirely.  Returns True if session existed."""
        existed = self._sessions.pop(key, None) is not None
        if existed:
            logger.debug("Session removed: %s", key)
        return existed

    # -- Query ---------------------------------------------------------------

    def all_sessions(self) -> list[Session]:
        """Return all active sessions."""
        return list(self._sessions.values())

    def active_count(self) -> int:
        """Return the number of active sessions."""
        return len(self._sessions)

    def sessions_for_user(self, platform: str, user_id: str) -> list[Session]:
        """Return all sessions for a specific user on a platform."""
        return [
            s for s in self._sessions.values()
            if s.platform == platform and s.user_id == user_id
        ]

    # -- Maintenance ---------------------------------------------------------

    def cleanup_idle(self) -> int:
        """Remove sessions idle beyond ``max_idle_seconds``.  Returns count removed."""
        now = time.time()
        self._last_cleanup = now

        expired = [
            key for key, session in self._sessions.items()
            if session.is_idle(self.max_idle_seconds)
        ]
        for key in expired:
            del self._sessions[key]

        if expired:
            logger.info("Cleaned up %d idle sessions", len(expired))
        return len(expired)

    def trim_histories(self) -> None:
        """Trim all session histories to ``max_history`` entries."""
        for session in self._sessions.values():
            if len(session.history) > self.max_history:
                session.history = session.history[-self.max_history:]

    def check_reset_policy(self) -> int:
        """Apply the configured reset policy.  Returns count of reset sessions."""
        count = 0
        now = time.time()

        for session in self._sessions.values():
            should_reset = False

            if self.reset_policy in (SessionResetPolicy.IDLE, SessionResetPolicy.BOTH):
                if session.is_idle(self.max_idle_seconds):
                    should_reset = True

            if self.reset_policy in (SessionResetPolicy.DAILY, SessionResetPolicy.BOTH):
                # Reset if created on a previous calendar day
                from datetime import datetime
                created_day = datetime.fromtimestamp(session.created_at).date()
                today = datetime.fromtimestamp(now).date()
                if created_day < today:
                    should_reset = True

            if should_reset:
                session.clear_history()
                count += 1

        if count:
            logger.info("Reset %d sessions per policy %s", count, self.reset_policy.value)
        return count


def build_session_context_prompt(session: Session, *, cron_enabled: bool = False) -> str:
    """Build a dynamic system prompt fragment from session metadata.

    Injects platform, user, and timing context so the agent is aware of
    its conversation environment.  When ``cron_enabled`` is True, also
    injects instructions for creating scheduled tasks via the
    ``<!--CRON_OPERATION ... -->`` protocol.
    """
    from datetime import datetime

    lines: list[str] = []

    # Platform context
    lines.append(f"Platform: {session.platform}")

    # Timing context
    now = datetime.now()
    lines.append(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # Session metadata
    if session.thread_id:
        lines.append(f"Thread/Topic: {session.thread_id}")

    # Chat type hint
    chat_type = "group" if session.thread_id or ":" in session.chat_id else "direct message"
    lines.append(f"Chat type: {chat_type}")

    # Cron capabilities
    if cron_enabled:
        lines.append("")
        lines.append(_CRON_CAPABILITY_PROMPT)

    return "\n".join(lines)


_CRON_CAPABILITY_PROMPT = """\
## Scheduled Task (Cron) Capabilities

You can create and manage scheduled tasks (cron jobs) for the user. When the user asks you to set up a recurring task, scheduled check, reminder, or automation, include a special block in your response.

### Create a scheduled task
Include this block anywhere in your response:
<!--CRON_OPERATION
```json
{"action": "create_job", "params": {"prompt": "<what the task should do>", "schedule": "<schedule>", "name": "<friendly name>"}}
```
-->

Schedule formats:
- Cron: `"0 9 * * *"` (daily at 9:00), `"*/30 * * * *"` (every 30 min), `"0 9 * * 1-5"` (weekdays 9:00)
- Interval: `"every 30m"`, `"every 2h"`, `"every 1d"`
- One-shot: `"30m"` (once in 30 min), `"2h"`, `"2026-06-14T10:00"`

### Create an automation script first
<!--CRON_OPERATION
```json
{"action": "create_script", "params": {"filename": "check-server.sh", "content": "#!/bin/bash\\ncurl -s http://localhost/health"}}
```
-->
Then reference it: `"script": "check-server.sh"` in a create_job.

### Manage existing tasks
- List: `{"action": "list_jobs", "params": {}}`
- Pause: `{"action": "pause_job", "params": {"job_id": "<id>"}}`
- Resume: `{"action": "resume_job", "params": {"job_id": "<id>"}}`
- Delete: `{"action": "delete_job", "params": {"job_id": "<id>"}}`

IMPORTANT: Always include the block exactly as shown. The system will detect it and create the task automatically. You can include explanatory text before/after the block."""

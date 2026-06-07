"""
Persistent session store backed by a JSON file.

Stores session metadata and conversation history in
``~/.nexus-agent/sessions.json`` with atomic writes to prevent corruption.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Cap per-session history to prevent unbounded JSON growth.
_MAX_HISTORY_PER_SESSION = 500

# Default directory for persistent data.
_DEFAULT_STORE_DIR = os.path.expanduser("~/.nexus-agent")


@dataclass
class PersistedSession:
    """A single persisted session."""

    session_id: str
    title: str | None = None
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    backend_session_ref: str = ""
    workspace: str | None = None
    workspace_name: str | None = None
    model: str | None = None
    agent_type: str = "claude-code"
    status: str = "active"  # "active" | "archived" | "deleted"
    message_count: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)
    preview: str | None = None


class SessionStore:
    """File-backed session store.

    The on-disk format is a JSON object keyed by ``session_id``::

        {
          "a1b2c3d4...": { <PersistedSession as dict> },
          ...
        }

    All writes go through :meth:`_save` which uses an atomic temp-file +
    rename strategy so a crash mid-write never leaves a truncated file.
    """

    def __init__(self, store_dir: str | None = None) -> None:
        self._dir = Path(store_dir or _DEFAULT_STORE_DIR)
        self._file = self._dir / "sessions.json"
        self._lock = asyncio.Lock()
        self._data: dict[str, dict[str, Any]] = self._load()

    # -- File I/O ---------------------------------------------------------------

    @property
    def file_path(self) -> Path:
        return self._file

    def _load(self) -> dict[str, dict[str, Any]]:
        """Read sessions.json from disk. Returns empty dict on missing/corrupt."""
        if not self._file.exists():
            return {}
        try:
            raw = self._file.read_text(encoding="utf-8")
            data = json.loads(raw)
            if isinstance(data, dict):
                # Filter out soft-deleted sessions that are older than 30 days
                cutoff = time.time() - 30 * 86400
                return {
                    k: v
                    for k, v in data.items()
                    if not (
                        v.get("status") == "deleted"
                        and v.get("last_active", 0) < cutoff
                    )
                }
            return {}
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load sessions from %s: %s", self._file, exc)
            return {}

    def _save(self, data: dict[str, dict[str, Any]]) -> None:
        """Atomic write: write to temp file then rename over the target."""
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=str(self._dir), prefix=".sessions-", suffix=".json.tmp"
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, str(self._file))
        except OSError as exc:
            logger.error("Failed to save sessions to %s: %s", self._file, exc)
            # Clean up temp file if it still exists
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # -- CRUD -------------------------------------------------------------------

    def create(
        self,
        *,
        session_id: str,
        agent_type: str = "claude-code",
        workspace: str | None = None,
        backend_session_ref: str | None = None,
        model: str | None = None,
        title: str | None = None,
    ) -> PersistedSession:
        """Create a new persisted session and write to disk."""
        ref = backend_session_ref or uuid.uuid4().hex
        ws_name = os.path.basename(workspace) if workspace else None
        session = PersistedSession(
            session_id=session_id,
            backend_session_ref=ref,
            agent_type=agent_type,
            workspace=workspace,
            workspace_name=ws_name,
            model=model,
            title=title,
        )
        self._data[session_id] = asdict(session)
        self._save(self._data)
        logger.info("Persisted session %s (agent=%s, ref=%s)", session_id, agent_type, ref)
        return session

    def get(self, session_id: str) -> PersistedSession | None:
        """Retrieve a session by ID."""
        raw = self._data.get(session_id)
        if raw is None:
            return None
        return PersistedSession(**raw)

    def list_sessions(
        self,
        *,
        status: str = "active",
        limit: int = 40,
        offset: int = 0,
        min_messages: int = 0,
        archived: str = "exclude",  # "exclude" | "include" | "only"
        order: str = "recent",  # "recent" | "created"
    ) -> tuple[list[PersistedSession], int]:
        """List sessions matching criteria. Returns (sessions, total_count)."""
        candidates: list[PersistedSession] = []
        for raw in self._data.values():
            s = PersistedSession(**raw)
            if s.status == "deleted":
                continue
            if archived == "exclude" and s.status == "archived":
                continue
            if archived == "only" and s.status != "archived":
                continue
            if min_messages > 0 and s.message_count < min_messages:
                continue
            candidates.append(s)

        # Sort
        if order == "created":
            candidates.sort(key=lambda s: s.created_at, reverse=True)
        else:
            candidates.sort(key=lambda s: s.last_active, reverse=True)

        total = len(candidates)
        page = candidates[offset : offset + limit]
        return page, total

    def update(self, session_id: str, **fields: Any) -> PersistedSession | None:
        """Partial update a session and persist."""
        raw = self._data.get(session_id)
        if raw is None:
            return None
        raw.update(fields)
        self._data[session_id] = raw
        self._save(self._data)
        return PersistedSession(**raw)

    def archive(self, session_id: str) -> bool:
        """Mark a session as archived."""
        return self.update(session_id, status="archived") is not None

    def unarchive(self, session_id: str) -> bool:
        """Mark an archived session as active again."""
        return self.update(session_id, status="active") is not None

    def delete(self, session_id: str) -> bool:
        """Soft-delete a session."""
        result = self.update(session_id, status="deleted")
        return result is not None

    def update_history(
        self,
        session_id: str,
        history: list[dict[str, Any]],
    ) -> PersistedSession | None:
        """Replace conversation history, capped at _MAX_HISTORY_PER_SESSION."""
        capped = history[-_MAX_HISTORY_PER_SESSION:]
        return self.update(
            session_id,
            history=capped,
            message_count=len(capped),
            last_active=time.time(),
        )

    def auto_title(self, session_id: str) -> str | None:
        """If no title is set, derive one from the first user message."""
        session = self.get(session_id)
        if session is None:
            return None
        if session.title:
            return session.title
        for msg in session.history:
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str) and content.strip():
                    title = content.strip()[:80]
                    self.update(session_id, title=title)
                    return title
        return None

    # -- Conversion -------------------------------------------------------------

    def to_session_info(self, session: PersistedSession) -> dict[str, Any]:
        """Convert a PersistedSession to the frontend ``SessionInfo`` shape."""
        return {
            "id": session.session_id,
            "title": session.title,
            "started_at": session.created_at,
            "last_active": session.last_active,
            "ended_at": None,
            "message_count": session.message_count,
            "preview": session.preview,
            "cwd": session.workspace,
            "model": session.model,
            "is_active": session.status == "active",
            "archived": session.status == "archived",
            "input_tokens": 0,
            "output_tokens": 0,
            "tool_call_count": 0,
            "source": None,
            "_lineage_root_id": None,
            "profile": "default",
            "is_default_profile": True,
        }

    # -- Search -----------------------------------------------------------------

    def search(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Simple text search across title and preview fields."""
        q = query.lower()
        results: list[dict[str, Any]] = []
        for raw in self._data.values():
            s = PersistedSession(**raw)
            if s.status == "deleted":
                continue
            title = (s.title or "").lower()
            preview = (s.preview or "").lower()
            if q in title or q in preview:
                results.append(
                    {
                        "session_id": s.session_id,
                        "lineage_root": None,
                        "model": s.model,
                        "role": None,
                        "session_started": s.created_at,
                        "snippet": s.preview or s.title or "",
                        "source": None,
                    }
                )
                if len(results) >= limit:
                    break
        return results

    # -- Async wrappers (for use from async handlers with lock) ------------------

    async def async_create(self, **kwargs: Any) -> PersistedSession:
        async with self._lock:
            return self.create(**kwargs)

    async def async_update(self, session_id: str, **fields: Any) -> PersistedSession | None:
        async with self._lock:
            return self.update(session_id, **fields)

    async def async_update_history(
        self, session_id: str, history: list[dict[str, Any]]
    ) -> PersistedSession | None:
        async with self._lock:
            return self.update_history(session_id, history)

    async def async_auto_title(self, session_id: str) -> str | None:
        async with self._lock:
            return self.auto_title(session_id)

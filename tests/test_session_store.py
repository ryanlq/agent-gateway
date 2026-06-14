"""Tests for the file-backed desktop SessionStore (sessions.json)."""

import time

from agent_gateway.server.session_store import PersistedSession, SessionStore


def _store(tmp_path) -> SessionStore:
    return SessionStore(store_dir=str(tmp_path))


def test_persistedsession_defaults_cli_session_id_none():
    s = PersistedSession(session_id="x")
    assert s.cli_session_id is None


def test_create_then_get_has_no_cli_session_id(tmp_path):
    store = _store(tmp_path)
    store.create(session_id="s1", agent_type="claude-code")
    got = store.get("s1")
    assert got is not None
    assert got.cli_session_id is None


def test_update_cli_session_id_round_trips(tmp_path):
    store = _store(tmp_path)
    store.create(session_id="s1")
    store.update("s1", cli_session_id="cli-uuid-123")
    got = store.get("s1")
    assert got.cli_session_id == "cli-uuid-123"


def test_legacy_record_without_cli_session_id_loads(tmp_path):
    """Records persisted before the field existed must load with default None."""
    store = _store(tmp_path)
    store._data["legacy"] = {
        "session_id": "legacy",
        "title": None,
        "created_at": time.time(),
        "last_active": time.time(),
        "backend_session_ref": "old-uuid",
        "workspace": None,
        "workspace_name": None,
        "model": None,
        "agent_type": "claude-code",
        "status": "active",
        "message_count": 0,
        "history": [],
        "preview": None,
        "_email_msg_ids": [],
        "reasoning": None,
        "fast": None,
    }
    store._save(store._data)

    # Reload from disk in a fresh store to exercise PersistedSession(**raw).
    reloaded = _store(tmp_path)
    got = reloaded.get("legacy")
    assert got is not None
    assert got.cli_session_id is None
    assert got.backend_session_ref == "old-uuid"


def test_to_session_info_lineage_root_is_session_id(tmp_path):
    store = _store(tmp_path)
    store.create(session_id="s1")
    info = store.to_session_info(store.get("s1"))
    assert info["_lineage_root_id"] == "s1"


def test_search_lineage_root_is_session_id(tmp_path):
    store = _store(tmp_path)
    store.create(session_id="s1")
    store.update("s1", title="hello world")
    results = store.search("hello")
    assert len(results) == 1
    assert results[0]["lineage_root"] == "s1"

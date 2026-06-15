"""Tests for the file-backed desktop SessionStore (sessions.json)."""

import logging
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


def test_forward_version_skew_unknown_keys_dropped_not_crash(tmp_path, caplog):
    """A record written by a NEWER gateway (fields this build doesn't know) must
    load without crashing, dropping the unknown keys silently. Exercises every
    disk-read path — the v0.4.0 brick surfaced via list_sessions, but the same
    ``**raw`` unpacking lived in get/search/find/update too."""
    store = _store(tmp_path)
    store._data["future"] = {
        "session_id": "future",
        "title": "from a newer build",
        "created_at": time.time(),
        "last_active": time.time(),
        "backend_session_ref": "ref-1",
        "workspace": "/tmp/ws",
        "workspace_name": "ws",
        "model": "claude-sonnet",
        "agent_type": "claude-code",
        "status": "active",
        "message_count": 3,
        "history": [{"role": "user", "content": "hello"}],
        "preview": "hello preview",
        "_email_msg_ids": ["<msg-1@example.com>"],
        "reasoning": None,
        "fast": None,
        # Unknown-to-this-build keys (forward version skew):
        "cli_session_id": "cli-future-id",  # the actual v0.4.0 regression trigger
        "future_blob": {"anything": 42},  # a truly hypothetical future field
    }
    store._save(store._data)

    reloaded = _store(tmp_path)

    # get() — primary lookup path.
    got = reloaded.get("future")
    assert got is not None
    assert got.session_id == "future"
    assert got.title == "from a newer build"
    assert got.backend_session_ref == "ref-1"
    assert got._email_msg_ids == ["<msg-1@example.com>"]  # underscore field survives
    assert got.message_count == 3
    assert not hasattr(got, "future_blob")

    # list_sessions() — the path that returned 500 in the incident.
    sessions, total = reloaded.list_sessions()
    assert total == 1
    assert sessions[0].session_id == "future"

    # search()
    results = reloaded.search("newer")
    assert len(results) == 1
    assert results[0]["session_id"] == "future"

    # find_by_email_message_id()
    found = reloaded.find_by_email_message_id("<msg-1@example.com>")
    assert found is not None
    assert found.session_id == "future"

    # update() — mutates raw then reconstructs; must still drop unknowns.
    updated = reloaded.update("future", title="renamed")
    assert updated is not None
    assert updated.title == "renamed"

    # Diagnosability: dropped keys logged at DEBUG (off by default → no noise).
    with caplog.at_level(logging.DEBUG, logger="agent_gateway.server.session_store"):
        reloaded.get("future")
    assert any("ignoring unknown fields" in r.message for r in caplog.records)


def test_corrupt_non_dict_entry_skipped(tmp_path, caplog):
    """A non-dict entry (corrupt/foreign shape) must be skipped, not crash _load
    with an AttributeError → 500. A valid sibling record still loads."""
    store = _store(tmp_path)
    store._data["corrupt"] = ["not", "a", "dict"]  # foreign shape
    store._data["good"] = {
        "session_id": "good",
        "title": "survives",
        "created_at": time.time(),
        "last_active": time.time(),
        "agent_type": "claude-code",
        "status": "active",
        "message_count": 0,
        "history": [],
        "_email_msg_ids": [],
    }
    store._save(store._data)

    with caplog.at_level(logging.WARNING, logger="agent_gateway.server.session_store"):
        reloaded = _store(tmp_path)

    assert reloaded.get("corrupt") is None
    assert reloaded.get("good") is not None
    assert reloaded.get("good").title == "survives"
    assert any("non-dict session entry" in r.message for r in caplog.records)

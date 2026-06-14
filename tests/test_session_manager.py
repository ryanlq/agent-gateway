"""Tests for SessionManager desktop-session lifecycle (create/resume/persist)."""

import pytest

from agent_gateway.server.session_manager import SessionManager
from agent_gateway.server.session_store import SessionStore


def _mgr(tmp_path) -> SessionManager:
    store = SessionStore(store_dir=str(tmp_path))
    return SessionManager(default_agent_type="claude-code", session_store=store)


@pytest.mark.asyncio
async def test_create_session_cli_session_id_is_none(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    assert session.cli_session_id is None


@pytest.mark.asyncio
async def test_resume_restores_persisted_cli_session_id(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.cli_session_id = "cli-captured"
    mgr.persist_session(session.session_id)

    # Force rehydration from the store.
    mgr._sessions.pop(session.session_id)
    resumed = await mgr.resume_session(session.session_id)
    assert resumed is not None
    assert resumed.cli_session_id == "cli-captured"


@pytest.mark.asyncio
async def test_resume_legacy_record_without_cli_session_id_is_none(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    mgr.persist_session(session.session_id)
    mgr._sessions.pop(session.session_id)

    resumed = await mgr.resume_session(session.session_id)
    assert resumed is not None
    # A record with no captured id resumes as None → next turn re-seeds.
    assert resumed.cli_session_id is None


@pytest.mark.asyncio
async def test_persist_session_writes_cli_session_id(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.cli_session_id = "xyz"
    mgr.persist_session(session.session_id)
    assert mgr._store.get(session.session_id).cli_session_id == "xyz"


@pytest.mark.asyncio
async def test_set_agent_different_type_resets_cli_session_id(tmp_path):
    """Switching engines (claude-code -> pi) drops the captured native id: it
    belongs to the previous CLI and would be a bogus --resume/--session target.
    Persisted too, so a gateway restart doesn't reload the stale id."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.cli_session_id = "claude-native-id"
    mgr.persist_session(session.session_id)
    assert mgr._store.get(session.session_id).cli_session_id == "claude-native-id"

    await mgr.set_agent(session.session_id, "pi")

    assert session.agent_type == "pi"
    assert session.cli_session_id is None
    assert mgr._store.get(session.session_id).cli_session_id is None


@pytest.mark.asyncio
async def test_set_agent_same_type_preserves_cli_session_id(tmp_path):
    """Refreshing params on the same agent keeps the native id — the CLI
    session is still valid for the same engine."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.cli_session_id = "claude-native-id"

    await mgr.set_agent(session.session_id, "claude-code")

    assert session.cli_session_id == "claude-native-id"


@pytest.mark.asyncio
async def test_persist_session_writes_agent_type(tmp_path):
    """persist_session must write agent_type so a switch survives a gateway
    restart (resume_session rebuilds the bridge from persisted.agent_type)."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.agent_type = "pi"
    mgr.persist_session(session.session_id)
    assert mgr._store.get(session.session_id).agent_type == "pi"

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

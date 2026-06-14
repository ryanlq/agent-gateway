"""Tests for methods._run_prompt: native session capture, resume, and truncate."""

import asyncio

import pytest

from agent_gateway.server import methods
from agent_gateway.server.methods import _run_prompt, _truncate_history
from agent_gateway.server.session_manager import SessionManager
from agent_gateway.server.session_store import SessionStore


def _mgr(tmp_path) -> SessionManager:
    store = SessionStore(store_dir=str(tmp_path))
    return SessionManager(default_agent_type="claude-code", session_store=store)


class _Collector:
    """Async emit() sink that records events."""

    def __init__(self):
        self.events = []

    async def __call__(self, event_type, payload, session_id):
        self.events.append((event_type, payload, session_id))


def _stub_stream(bridge, chunks, *, capture_value=None):
    """Replace bridge.stream with a stub that records the session_ref it was
    called with, optionally latches a captured native id, and yields chunks."""
    seen = {}

    async def fake_stream(*, session_key, message, history, system_extra, session_ref):
        seen["session_ref"] = session_ref
        if capture_value is not None:
            bridge.captured_cli_session_id = capture_value
        for c in chunks:
            yield c

    bridge.stream = fake_stream
    return seen


@pytest.mark.asyncio
async def test_turn1_passes_no_ref_and_latches_capture(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    seen = _stub_stream(session.bridge, ["Hello"], capture_value="cli-1")
    emit = _Collector()

    await _run_prompt(session.session_id, "hi", session, emit, mgr)

    assert seen["session_ref"] is None          # turn 1: no native id yet
    assert session.cli_session_id == "cli-1"    # captured id latched
    # Persisted to disk too.
    assert mgr._store.get(session.session_id).cli_session_id == "cli-1"


@pytest.mark.asyncio
async def test_turn2_passes_captured_ref(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.cli_session_id = "cli-1"
    seen = _stub_stream(session.bridge, ["World"], capture_value="cli-1")
    emit = _Collector()

    await _run_prompt(session.session_id, "again", session, emit, mgr)

    assert seen["session_ref"] == "cli-1"       # turn 2+: resume target


@pytest.mark.asyncio
async def test_dead_resume_drops_stale_id(tmp_path):
    """A resume attempt that yields no captured id (CLI session gone) drops the
    stale id so the next turn re-seeds instead of looping on a failed resume."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.cli_session_id = "dead-id"
    seen = _stub_stream(session.bridge, ["err"])  # no capture_value → stays None
    emit = _Collector()

    await _run_prompt(session.session_id, "hi", session, emit, mgr)

    assert seen["session_ref"] == "dead-id"
    assert session.cli_session_id is None


@pytest.mark.asyncio
async def test_truncate_resets_history_and_reseeds(tmp_path):
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.history = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "u2"},
        {"role": "assistant", "content": "a2"},
        {"role": "user", "content": "u3"},
        {"role": "assistant", "content": "a3"},
    ]
    session.cli_session_id = "old"
    seen = _stub_stream(session.bridge, ["fresh"], capture_value="new-id")
    emit = _Collector()

    await _run_prompt(session.session_id, "edited-u3", session, emit, mgr,
                      truncate_ordinal=2)

    # First two user/assistant pairs kept; the edited message is the new turn.
    assert [m["content"] for m in session.history if m["role"] == "user"] == \
        ["u1", "u2", "edited-u3"]
    assert [m["content"] for m in session.history if m["role"] == "assistant"] == \
        ["a1", "a2", "fresh"]
    # Truncate forced a reseed: no ref passed, fresh id captured.
    assert seen["session_ref"] is None
    assert session.cli_session_id == "new-id"


# -- _truncate_history unit tests -------------------------------------------

def test_truncate_history_zero_returns_empty():
    assert _truncate_history([{"role": "user", "content": "x"}], 0) == []


def test_truncate_history_keeps_first_n_pairs():
    h = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "u2"},
        {"role": "assistant", "content": "a2"},
        {"role": "user", "content": "u3"},
        {"role": "assistant", "content": "a3"},
    ]
    assert _truncate_history(h, 2) == [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "u2"},
        {"role": "assistant", "content": "a2"},
    ]


def test_truncate_history_n_exceeds_count_keeps_all():
    h = [{"role": "user", "content": "u1"}, {"role": "assistant", "content": "a1"}]
    assert _truncate_history(h, 5) == h


def test_truncate_history_missing_assistant_reply():
    h = [
        {"role": "user", "content": "u1"},
        {"role": "user", "content": "u2"},
        {"role": "assistant", "content": "a2"},
    ]
    # Keep the first user turn only; u1 has no assistant reply following it.
    assert _truncate_history(h, 1) == [{"role": "user", "content": "u1"}]


# -- concurrency guard: one active prompt task per session ---------------

async def _noop() -> None:
    return None


@pytest.mark.asyncio
async def test_concurrent_submit_rejected_with_error(tmp_path):
    """A second prompt.submit while a turn is streaming is rejected with an
    error event (not just a status) — the client ignores response status and
    needs the event to release its busy flag."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    sid = session.session_id

    # Simulate a turn already in flight: a pending task in the registry.
    blocker = asyncio.Event()

    async def _hold() -> None:
        await blocker.wait()

    running = asyncio.create_task(_hold())
    methods._running_prompts[sid] = running
    try:
        emit = _Collector()
        result = await methods.handle_prompt_submit(
            {"session_id": sid, "text": "hi"}, emit, mgr,
        )
        assert result["status"] == "busy"
        # An error event was emitted so the client can settle.
        assert any(e[0] == "error" for e in emit.events)
        # The in-flight task is untouched; no second task was created.
        assert methods._running_prompts[sid] is running
    finally:
        blocker.set()
        await running
        methods._running_prompts.pop(sid, None)


@pytest.mark.asyncio
async def test_done_running_task_does_not_block_new_submit(tmp_path):
    """A finished task still in the registry (cleanup callback pending) must
    not trip the guard — the ``.done()`` check avoids a false reject."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    sid = session.session_id
    _stub_stream(session.bridge, ["ok"], capture_value="new-id")
    emit = _Collector()

    done_task = asyncio.create_task(_noop())
    await done_task
    methods._running_prompts[sid] = done_task
    try:
        result = await methods.handle_prompt_submit(
            {"session_id": sid, "text": "hi"}, emit, mgr,
        )
        assert result["status"] == "ok"
        new_task = methods._running_prompts[sid]
        assert new_task is not done_task
        await new_task
    finally:
        methods._running_prompts.pop(sid, None)


@pytest.mark.asyncio
async def test_running_prompt_slot_cleared_after_completion(tmp_path):
    """Normal completion still evicts the slot (self-aware cleanup owns it)."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    sid = session.session_id
    _stub_stream(session.bridge, ["done"], capture_value="x")
    emit = _Collector()

    await methods.handle_prompt_submit({"session_id": sid, "text": "hi"}, emit, mgr)
    task = methods._running_prompts[sid]
    assert task is not None
    await task
    # Yield control so the done_callback (call_soon) can run and clear the slot.
    for _ in range(20):
        if methods._running_prompts.get(sid) is None:
            break
        await asyncio.sleep(0)
    assert methods._running_prompts.get(sid) is None


@pytest.mark.asyncio
async def test_cron_parse_uses_chat_not_stream(tmp_path):
    """Natural-language /cron parsing must use chat() (an independent
    subprocess that never touches the capture field) — not stream(), which
    resets/latches bridge.captured_cli_session_id and would clobber an
    in-flight prompt turn's id (slash commands bypass the busy queue)."""
    mgr = _mgr(tmp_path)
    session = await mgr.create_session(agent_type="claude-code")
    session.bridge.captured_cli_session_id = "main-turn-id"
    calls = {"stream": 0, "chat": 0}

    async def fake_stream(**kw):
        calls["stream"] += 1
        if False:  # pragma: no cover - must never be iterated
            yield ""

    async def fake_chat(*, session_key, message, history, system_extra, session_ref=None):
        calls["chat"] += 1
        return '{"schedule": "0 9 * * *", "prompt": "check server", "name": "daily"}'

    session.bridge.stream = fake_stream
    session.bridge.chat = fake_chat

    class _FakeCron:
        def create_job(self, **kw):
            return {"id": "j1", "name": kw.get("name"),
                    "schedule_display": kw.get("schedule"), "next_run_at": "?"}

    methods._cron_manager = _FakeCron()
    try:
        await methods._cron_create_via_agent(
            "每天9点检查", session.session_id, mgr, _Collector())
    finally:
        methods._cron_manager = None

    assert calls["stream"] == 0
    assert calls["chat"] == 1
    # The in-flight prompt's captured id is intact.
    assert session.bridge.captured_cli_session_id == "main-turn-id"

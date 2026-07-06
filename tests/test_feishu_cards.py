"""Tests for the Feishu task-status card builder and throttled patcher."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from agent_gateway.adapters.feishu_cards import (
    EDIT_SOFT_CAP,
    TaskStatusCard,
    ThrottledCardPatcher,
)


# ---------------------------------------------------------------------------
# TaskStatusCard render states
# ---------------------------------------------------------------------------


def test_initial_state_is_running():
    card = TaskStatusCard()
    rendered = card.render()
    assert rendered["header"]["template"] == "blue"
    assert rendered["header"]["title"]["content"] == "💬 任务执行中"
    body = rendered["elements"][0]["text"]["content"]
    assert "正在处理" in body


def test_finalize_done():
    card = TaskStatusCard()
    card.finalize("done")
    rendered = card.render()
    assert rendered["header"]["template"] == "green"
    assert rendered["header"]["title"]["content"] == "✓ 任务完成"


def test_finalize_failed():
    card = TaskStatusCard()
    card.finalize("failed")
    rendered = card.render()
    assert rendered["header"]["template"] == "red"
    assert rendered["header"]["title"]["content"] == "⚠ 任务失败"


def test_finalize_interrupted():
    card = TaskStatusCard()
    card.finalize("interrupted")
    rendered = card.render()
    assert rendered["header"]["template"] == "orange"
    assert rendered["header"]["title"]["content"] == "⏸ 任务中断"


def test_finalize_unknown_outcome_defaults_to_done():
    card = TaskStatusCard()
    card.finalize("nonsense")
    assert card.outcome == "done"


def test_render_json_size_is_reasonable():
    card = TaskStatusCard()
    # Status card body is tiny — well under any Feishu cap.
    assert card.render_json_size() < 500


# ---------------------------------------------------------------------------
# ThrottledCardPatcher
# ---------------------------------------------------------------------------


class FakeSender:
    def __init__(self, *, create_mid="om_1", patch_ok=True):
        self.create_mid = create_mid
        self.patch_ok = patch_ok
        self.creates: list[str] = []
        self.patches: list[str] = []

    async def create_tool_card(self, chat_id, card_json, *, reply_to, metadata):
        self.creates.append(card_json)
        return self.create_mid

    async def patch_tool_card(self, message_id, card_json):
        self.patches.append(card_json)
        return self.patch_ok


class FakeClock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


@pytest.fixture
def frozen_clock():
    clock = FakeClock()
    with patch("agent_gateway.adapters.feishu_cards.time.monotonic", new=clock):
        yield clock


def _patcher(sender, clock):
    card = TaskStatusCard()
    return ThrottledCardPatcher(sender, card, "chat_x"), card


async def test_flush_creates_card_on_first_call(frozen_clock):
    sender = FakeSender()
    patcher, card = _patcher(sender, frozen_clock)
    patcher.mark_pending()
    await patcher.flush_if_due()
    assert len(sender.creates) == 1
    assert patcher.message_id == "om_1"
    assert sender.patches == []  # create, not patch


async def test_finalize_pushes_terminal_state(frozen_clock):
    sender = FakeSender()
    patcher, card = _patcher(sender, frozen_clock)
    # Create the "running" card first.
    patcher.mark_pending()
    await patcher.flush_if_due()
    # Finalize → one patch with the "done" header.
    await patcher.finalize("done")
    assert len(sender.patches) == 1
    import json as _json
    final = _json.loads(sender.patches[-1])
    assert final["header"]["template"] == "green"
    assert final["header"]["title"]["content"] == "✓ 任务完成"


async def test_finalize_without_create_still_sends(frozen_clock):
    """A round that never flushed mid-stream still gets a final card."""
    sender = FakeSender()
    patcher, card = _patcher(sender, frozen_clock)
    # No prior flush — finalize() must create + the patch is implicit in create.
    await patcher.finalize("done")
    # finalize() with no message_id yet → create fires with the terminal state.
    assert len(sender.creates) == 1
    import json as _json
    created = _json.loads(sender.creates[-1])
    assert created["header"]["template"] == "green"


async def test_create_failure_disables_card(frozen_clock):
    sender = FakeSender(create_mid=None)  # create returns None
    patcher, card = _patcher(sender, frozen_clock)
    patcher.mark_pending()
    await patcher.flush_if_due()
    assert patcher.message_id is None
    assert sender.patches == []


async def test_patch_failure_freezes_card(frozen_clock):
    sender = FakeSender(patch_ok=False)
    patcher, card = _patcher(sender, frozen_clock)
    patcher.mark_pending()
    await patcher.flush_if_due()  # create succeeds
    await patcher.finalize("done")  # patch fails → frozen
    assert len(sender.patches) == 1
    assert patcher.edits == 1


async def test_soft_cap_still_applies(frozen_clock):
    """The safeguard is retained: hitting EDIT_SOFT_CAP stops mid-stream patches
    but finalize() still lands one last update."""
    sender = FakeSender()
    patcher, card = _patcher(sender, frozen_clock)
    patcher.mark_pending()
    await patcher.flush_if_due()  # create
    # Force the cap by directly driving flushes past EDIT_SOFT_CAP.
    for _ in range(EDIT_SOFT_CAP):
        patcher.mark_pending()
        patcher.mark_pending()
        patcher.mark_pending()  # cross the change threshold
        await patcher.flush_if_due()
        if patcher.edits >= EDIT_SOFT_CAP:
            break
    assert patcher.edits >= EDIT_SOFT_CAP
    edits_at_cap = patcher.edits
    await patcher.finalize("done")
    assert patcher.edits == edits_at_cap + 1  # finalize bypasses cap


# ---------------------------------------------------------------------------
# Adapter-level hook drive
# ---------------------------------------------------------------------------


async def test_adapter_hooks_drive_card_lifecycle(frozen_clock):
    """Full runner-driven lifecycle: begin (running card created) → tools
    (no extra patches) → end (final patch)."""
    from agent_gateway.adapters.feishu import FeishuAdapter

    adapter = FeishuAdapter({})
    adapter._client = object()  # truthy sentinel; real SDK calls are faked below

    sender = FakeSender()

    async def fake_create(chat_id, card_json, *, reply_to, metadata):
        return await sender.create_tool_card(chat_id, card_json, reply_to=reply_to, metadata=metadata)

    async def fake_patch(message_id, card_json):
        return await sender.patch_tool_card(message_id, card_json)

    adapter.create_tool_card = fake_create  # type: ignore[assignment]
    adapter.patch_tool_card = fake_patch  # type: ignore[assignment]

    handle = await adapter.begin_tool_round("chat_x")
    assert handle is not None
    # begin_tool_round immediately creates the "running" card.
    assert len(sender.creates) == 1
    assert sender.patches == []

    # Tool events are no-ops for the card.
    await adapter.tool_round_start(handle, {"name": "Read", "tool_id": "t1",
                                            "input": {"file_path": "a.py"}})
    await adapter.tool_round_complete(handle, {"name": "Read", "tool_id": "t1",
                                               "is_error": False})
    assert sender.patches == []  # still no mid-stream patches

    # A failed tool flips the terminal outcome but still doesn't patch mid-stream.
    await adapter.tool_round_start(handle, {"name": "Bash", "tool_id": "t2",
                                            "input": {"command": "pytest"}})
    await adapter.tool_round_complete(handle, {"name": "Bash", "tool_id": "t2",
                                               "is_error": True,
                                               "error_message": "1 test failed"})
    assert sender.patches == []

    await adapter.end_tool_round(handle, success=True)

    # Exactly one final patch, red header (any_failed=True).
    assert len(sender.patches) == 1
    import json as _json
    final = _json.loads(sender.patches[-1])
    assert final["header"]["template"] == "red"
    assert final["header"]["title"]["content"] == "⚠ 任务失败"


async def test_adapter_end_with_success_yields_green(frozen_clock):
    """A round with no failed tools finalizes to green "任务完成"."""
    from agent_gateway.adapters.feishu import FeishuAdapter

    adapter = FeishuAdapter({})
    adapter._client = object()
    sender = FakeSender()
    adapter.create_tool_card = lambda *a, **k: sender.create_tool_card(*a, **k)  # type: ignore
    adapter.patch_tool_card = lambda *a, **k: sender.patch_tool_card(*a, **k)  # type: ignore

    handle = await adapter.begin_tool_round("chat_x")
    await adapter.tool_round_start(handle, {"name": "Read", "tool_id": "t1", "input": {}})
    await adapter.tool_round_complete(handle, {"name": "Read", "tool_id": "t1", "is_error": False})
    await adapter.end_tool_round(handle, success=True)

    assert len(sender.patches) == 1
    import json as _json
    final = _json.loads(sender.patches[-1])
    assert final["header"]["template"] == "green"
    assert final["header"]["title"]["content"] == "✓ 任务完成"


async def test_adapter_end_with_exception_yields_orange(frozen_clock):
    """success=False from the runner produces the interrupted (orange) state."""
    from agent_gateway.adapters.feishu import FeishuAdapter

    adapter = FeishuAdapter({})
    adapter._client = object()
    sender = FakeSender()
    adapter.create_tool_card = lambda *a, **k: sender.create_tool_card(*a, **k)  # type: ignore
    adapter.patch_tool_card = lambda *a, **k: sender.patch_tool_card(*a, **k)  # type: ignore

    handle = await adapter.begin_tool_round("chat_x")
    await adapter.end_tool_round(handle, success=False)

    import json as _json
    final = _json.loads(sender.patches[-1])
    assert final["header"]["template"] == "orange"
    assert final["header"]["title"]["content"] == "⏸ 任务中断"

"""Tests for the Feishu tool-call summary card builder and throttled patcher."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from agent_gateway.adapters.feishu_cards import (
    ARG_SUMMARY_MAX,
    EDIT_SOFT_CAP,
    FLUSH_AFTER_CHANGES,
    FLUSH_AFTER_SECONDS,
    ThrottledCardPatcher,
    ToolCardBuilder,
    summarize_arg,
)


# ---------------------------------------------------------------------------
# summarize_arg
# ---------------------------------------------------------------------------


def test_summarize_arg_picks_relevant_field():
    assert summarize_arg("Read", {"file_path": "src/app.tsx"}) == "src/app.tsx"
    assert summarize_arg("Bash", {"command": "pytest -xvs"}) == "pytest -xvs"
    assert summarize_arg("Grep", {"pattern": "TODO", "output_mode": "content"}) == "TODO"
    assert summarize_arg("Write", {"file_path": "a/b.py", "content": "x"}) == "a/b.py"


def test_summarize_arg_clips_long_values():
    long_cmd = "git " + "x" * 200
    out = summarize_arg("Bash", {"command": long_cmd})
    assert len(out) == ARG_SUMMARY_MAX
    assert out.endswith("…")


def test_summarize_arg_fallback_first_string():
    assert summarize_arg("MysteryTool", {"a": "first", "b": "second"}) == "first"
    assert summarize_arg("Whatever", {}) == ""
    assert summarize_arg("Whatever", None) == ""


# ---------------------------------------------------------------------------
# ToolCardBuilder render states
# ---------------------------------------------------------------------------


def _card_text(builder: ToolCardBuilder) -> str:
    """Concatenate all lark_md / plain_text contents for easy assertions."""
    parts = []
    for el in builder.render()["elements"]:
        text = el.get("text")
        if isinstance(text, dict) and "content" in text:
            parts.append(text["content"])
        for sub in el.get("elements", []) or []:
            if isinstance(sub, dict) and "content" in sub:
                parts.append(sub["content"])
    return "\n".join(parts)


def test_render_running_state():
    b = ToolCardBuilder()
    b.add_start("t1", "Read", {"file_path": "foo.py"})
    card = b.render()
    assert card["header"]["template"] == "blue"
    assert card["header"]["title"]["content"] == "💬 处理中"
    assert "🔄 Read" in _card_text(b)


def test_render_all_done_after_finalize():
    b = ToolCardBuilder()
    b.add_start("t1", "Read", {"file_path": "foo.py"})
    b.add_complete("t1", elapsed=0.3, is_error=False)
    b.finalize("done", 0.3)
    card = b.render()
    assert card["header"]["template"] == "green"
    assert card["header"]["title"]["content"] == "✓ 已完成"
    txt = _card_text(b)
    assert "✓ Read" in txt
    assert "0.3s" in txt
    assert "1 tools" in txt


def test_render_failure_adds_error_note():
    b = ToolCardBuilder()
    b.add_start("t1", "Bash", {"command": "pytest"})
    b.add_complete("t1", elapsed=5.2, is_error=True, error="1 test failed: test_x")
    b.finalize("failed", 5.2)
    card = b.render()
    assert card["header"]["template"] == "red"
    assert card["header"]["title"]["content"] == "⚠ 部分失败"
    txt = _card_text(b)
    assert "✗ Bash" in txt
    assert "1 test failed: test_x" in txt  # error surfaces in a note element


def test_render_interrupted_marks_running_tools_failed():
    b = ToolCardBuilder()
    b.add_start("t1", "Read", {"file_path": "foo.py"})
    b.finalize("interrupted", 1.0)  # t1 never completed
    card = b.render()
    assert card["header"]["template"] == "orange"
    assert card["header"]["title"]["content"] == "⏸ 已中断"
    txt = _card_text(b)
    assert "✗ Read" in txt
    assert "interrupted" in txt


def test_complete_without_start_is_recorded():
    b = ToolCardBuilder()
    b.add_complete("orphan", elapsed=0.5, is_error=False)
    b.finalize("done", 0.5)
    assert "✓ tool" in _card_text(b)


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
    builder = ToolCardBuilder()
    return ThrottledCardPatcher(sender, builder, "chat_x"), builder


async def test_zero_tools_never_creates_card(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    await patcher.finalize("done", 0.0)
    assert sender.creates == []
    assert sender.patches == []
    assert patcher.message_id is None


async def test_first_tool_creates_card(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    builder.add_start("t1", "Read", {"file_path": "a.py"})
    patcher.mark_pending()
    await patcher.flush_if_due()
    assert len(sender.creates) == 1
    assert patcher.message_id == "om_1"
    assert sender.patches == []  # create, not patch


async def test_coalesce_three_changes_then_flush(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    # First change → create.
    builder.add_start("t1", "Read", {"file_path": "a.py"})
    patcher.mark_pending()
    await patcher.flush_if_due()
    # Two more changes under the threshold → no patch yet.
    builder.add_complete("t1", elapsed=0.2, is_error=False)
    patcher.mark_pending()
    await patcher.flush_if_due()
    builder.add_start("t2", "Edit", {"file_path": "b.py"})
    patcher.mark_pending()
    await patcher.flush_if_due()
    assert len(sender.patches) == 0
    # Third pending change crosses FLUSH_AFTER_CHANGES → one patch.
    builder.add_complete("t2", elapsed=0.1, is_error=False)
    patcher.mark_pending()
    await patcher.flush_if_due()
    assert len(sender.patches) == 1


async def test_time_based_flush(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    builder.add_start("t1", "Read", {"file_path": "a.py"})
    patcher.mark_pending()
    await patcher.flush_if_due()  # create
    builder.add_complete("t1", elapsed=0.2, is_error=False)
    patcher.mark_pending()
    await patcher.flush_if_due()  # pending=1, no time → no flush
    assert len(sender.patches) == 0
    frozen_clock.advance(FLUSH_AFTER_SECONDS + 0.01)
    await patcher.flush_if_due()  # timer elapsed → flush
    assert len(sender.patches) == 1


async def test_error_flushes_immediately(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    builder.add_start("t1", "Bash", {"command": "pytest"})
    patcher.mark_pending()
    await patcher.flush_if_due()  # create
    builder.add_complete("t1", elapsed=5.0, is_error=True, error="boom")
    patcher.mark_pending()
    await patcher.flush_if_due(is_error=True)  # no time passed, but error
    assert len(sender.patches) == 1


async def test_finalize_renders_full_state(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    for i in range(4):
        tid = f"t{i}"
        builder.add_start(tid, "Read", {"file_path": f"f{i}.py"})
        patcher.mark_pending()
        await patcher.flush_if_due(is_error=(i == 3))
        builder.add_complete(tid, elapsed=0.1 * i, is_error=(i == 3),
                             error="fail" if i == 3 else "")
        patcher.mark_pending()
        await patcher.flush_if_due(is_error=(i == 3))
    await patcher.finalize("failed", 1.0)
    # The last patch (finalize) contains all four tools + green->red header.
    final_card = __import__("json").loads(sender.patches[-1])
    names = [e["text"]["content"] for e in final_card["elements"]
             if e.get("text", {}).get("tag") == "lark_md"]
    assert len(names) == 4
    assert final_card["header"]["template"] == "red"


async def test_soft_cap_stops_midstream_patches(frozen_clock):
    sender = FakeSender()
    patcher, builder = _patcher(sender, frozen_clock)
    # Create the card first.
    builder.add_start("t0", "Read", {"file_path": "a.py"})
    patcher.mark_pending()
    await patcher.flush_if_due()
    # Force FLUSH_AFTER_CHANGES pending then flush, EDIT_SOFT_CAP times.
    for i in range(EDIT_SOFT_CAP):
        builder.add_start(f"x{i}", "Read", {"file_path": f"f{i}.py"})
        patcher.mark_pending()
        for _ in range(FLUSH_AFTER_CHANGES):
            patcher.mark_pending()
        await patcher.flush_if_due()
        if patcher.edits >= EDIT_SOFT_CAP:
            break
    assert patcher.edits == EDIT_SOFT_CAP
    edits_at_cap = patcher.edits
    # Further mid-stream flushes are suppressed.
    builder.add_start("zzz", "Read", {"file_path": "z.py"})
    patcher.mark_pending()
    for _ in range(FLUSH_AFTER_CHANGES):
        patcher.mark_pending()
    await patcher.flush_if_due()
    assert patcher.edits == edits_at_cap
    # finalize still gets one last patch through.
    await patcher.finalize("done", 1.0)
    assert patcher.edits == edits_at_cap + 1


async def test_create_failure_disables_card(frozen_clock):
    sender = FakeSender(create_mid=None)  # create returns None
    patcher, builder = _patcher(sender, frozen_clock)
    builder.add_start("t1", "Read", {"file_path": "a.py"})
    patcher.mark_pending()
    await patcher.flush_if_due()
    assert patcher.message_id is None
    builder.add_complete("t1", elapsed=0.1, is_error=True, error="x")
    patcher.mark_pending()
    await patcher.flush_if_due(is_error=True)  # disabled → no patch
    assert sender.patches == []


# ---------------------------------------------------------------------------
# Adapter-level hook drive
# ---------------------------------------------------------------------------


async def test_adapter_hooks_drive_card_lifecycle(frozen_clock):
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

    await adapter.tool_round_start(handle, {"name": "Read", "tool_id": "t1",
                                            "input": {"file_path": "a.py"}})
    assert len(sender.creates) == 1  # first tool → immediate create
    frozen_clock.advance(0.2)
    await adapter.tool_round_complete(handle, {"name": "Read", "tool_id": "t1",
                                               "is_error": False})
    await adapter.tool_round_start(handle, {"name": "Bash", "tool_id": "t2",
                                            "input": {"command": "pytest"}})
    frozen_clock.advance(5.0)
    await adapter.tool_round_complete(handle, {"name": "Bash", "tool_id": "t2",
                                               "is_error": True,
                                               "error_message": "1 test failed"})
    await adapter.end_tool_round(handle, success=True)

    # Final patch reflects: t1 done, t2 failed, red header (any_failed).
    import json as _json
    final = _json.loads(sender.patches[-1])
    assert final["header"]["template"] == "red"
    rows = [e["text"]["content"] for e in final["elements"]
            if e.get("text", {}).get("tag") == "lark_md"]
    assert any("✓ Read" in r and "a.py" in r for r in rows)
    assert any("✗ Bash" in r and "pytest" in r for r in rows)


async def test_adapter_zero_tools_leaves_no_card(frozen_clock):
    from agent_gateway.adapters.feishu import FeishuAdapter

    adapter = FeishuAdapter({})
    adapter._client = object()
    sender = FakeSender()
    adapter.create_tool_card = lambda *a, **k: sender.create_tool_card(*a, **k)  # type: ignore
    adapter.patch_tool_card = lambda *a, **k: sender.patch_tool_card(*a, **k)  # type: ignore

    handle = await adapter.begin_tool_round("chat_x")
    await adapter.end_tool_round(handle, success=True)
    assert sender.creates == []
    assert sender.patches == []

"""
Feishu tool-call summary card — streaming card builder + throttled patcher.

Renders a single agent round's ``tool_use`` calls as one Feishu interactive
card that grows in place:

    ┌─ 💬 处理中 ──────────────────┐
    │  🔄 Read   src/app.tsx  · …   │
    │  ✓ Edit    src/app.tsx  · 0.1s│
    │  ✗ Bash    pytest     · 5.2s  │
    │  ─────────────────────────── │
    │  🤖 Claude Code · 3 tools · 5.6s │
    └───────────────────────────────┘

The card is updated via ``PATCH /im/v1/messages/:id`` (``msg_type=interactive``).
Feishu caps a message at 20 edits, so :class:`ThrottledCardPatcher` coalesces
updates and soft-stops before the cap. Card body is hard-capped at 30 KB, so
only one-line status rows are emitted — input/output stay out of the card.

Schema is classic Feishu card v1 (``config`` + ``header.template`` + top-level
``elements``): the most broadly supported form for ``interactive`` messages
and PATCH streaming. :class:`ToolCardBuilder` isolates all card-JSON
construction, so swapping individual elements (e.g. to ``expandable_note``
later) is a single-method change.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional, Protocol

logger = logging.getLogger(__name__)

# Feishu caps a single message at 20 edits. We stop auto-patching a few short
# of that so the final ``finalize()`` always has headroom for one last patch.
EDIT_SOFT_CAP = 17

# Coalesce policy: flush after this many buffered tool changes, or this many
# seconds since the last patch — whichever comes first. A failure always
# flushes immediately.
FLUSH_AFTER_CHANGES = 3
FLUSH_AFTER_SECONDS = 1.5

# Safety margin under Feishu's 30 KB card-body cap. A final card that would
# exceed this is dropped (the card stays at its last good mid-stream state)
# rather than erroring on every patch.
CARD_BODY_MAX = 28_000

# Cap a tool's one-line argument summary so a long command/path can't blow up
# a row (and the 30 KB card budget).
ARG_SUMMARY_MAX = 48
ERROR_NOTE_MAX = 1000

_RUNNING = "running"
_DONE = "done"
_FAILED = "failed"


@dataclass
class ToolEntry:
    """One tool invocation within a round."""

    tool_id: str
    name: str
    arg: str = ""
    status: str = _RUNNING
    elapsed: Optional[float] = None
    error: str = ""


def summarize_arg(name: str, tool_input: Optional[dict[str, Any]]) -> str:
    """Pick the most relevant single-line argument for a tool.

    Read/Edit/Write → file_path, Bash → command, Grep/Glob → pattern, etc.
    Everything else falls back to the first string-valued argument.
    """
    if not tool_input:
        return ""
    key = {
        "Read": "file_path",
        "Edit": "file_path",
        "Write": "file_path",
        "MultiEdit": "file_path",
        "NotebookEdit": "notebook_path",
    }.get(name)
    if key:
        val = tool_input.get(key)
        if isinstance(val, str):
            return _clip(val)
    if name == "Bash":
        return _clip(str(tool_input.get("command", "")))
    if name in ("Grep", "Glob"):
        return _clip(str(tool_input.get("pattern", "")))
    if name in ("WebFetch", "WebSearch"):
        return _clip(str(tool_input.get("url") or tool_input.get("query") or ""))
    if name in ("Agent", "Task"):
        return _clip(str(tool_input.get("description") or tool_input.get("prompt") or ""))
    # Generic fallback: first string value.
    for val in tool_input.values():
        if isinstance(val, str) and val:
            return _clip(val)
    return ""


def _clip(text: str) -> str:
    text = text.strip().replace("\n", " ")
    if len(text) > ARG_SUMMARY_MAX:
        return text[: ARG_SUMMARY_MAX - 1] + "…"
    return text


class ToolCardBuilder:
    """Holds the canonical card state and renders the Feishu card dict.

    The builder is the single source of truth; the patcher decides *when* to
    push ``render()`` to the API. Mutations are cheap (in-memory); the
    expensive PATCH only fires on the patcher's schedule.
    """

    def __init__(self, agent_label: str = "Claude Code") -> None:
        self._agent_label = agent_label
        self._tools: dict[str, ToolEntry] = {}
        self._order: list[str] = []
        self._outcome: str = "running"  # running | done | failed | interrupted
        self._total_elapsed: Optional[float] = None

    # -- mutation -------------------------------------------------------

    def add_start(self, tool_id: str, name: str, tool_input: Optional[dict[str, Any]]) -> None:
        if tool_id in self._tools:
            return
        self._tools[tool_id] = ToolEntry(
            tool_id=tool_id, name=name, arg=summarize_arg(name, tool_input)
        )
        self._order.append(tool_id)

    def add_complete(
        self,
        tool_id: str,
        *,
        elapsed: Optional[float],
        is_error: bool,
        error: str = "",
    ) -> None:
        entry = self._tools.get(tool_id)
        if entry is None:
            # Complete without a seen start — record it as a finished tool.
            entry = ToolEntry(tool_id=tool_id, name="tool")
            self._tools[tool_id] = entry
            self._order.append(tool_id)
        entry.elapsed = elapsed
        if is_error:
            entry.status = _FAILED
            entry.error = (error or "failed")[:ERROR_NOTE_MAX]
        else:
            entry.status = _DONE

    def finalize(self, outcome: str, total_elapsed: Optional[float]) -> None:
        # Any tool still "running" when the round ends was interrupted.
        for entry in self._tools.values():
            if entry.status == _RUNNING:
                entry.status = _FAILED
                if not entry.error:
                    entry.error = "interrupted"
        self._outcome = outcome
        self._total_elapsed = total_elapsed

    @property
    def tool_count(self) -> int:
        return len(self._order)

    # -- render ---------------------------------------------------------

    def render(self) -> dict[str, Any]:
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": self._header_title()},
                "template": self._header_template(),
            },
            "elements": self._render_elements(),
        }

    def render_json_size(self) -> int:
        return len(json.dumps(self.render(), ensure_ascii=False))

    def _header_title(self) -> str:
        if self._outcome == "done":
            return "✓ 已完成"
        if self._outcome == "failed":
            return "⚠ 部分失败"
        if self._outcome == "interrupted":
            return "⏸ 已中断"
        return "💬 处理中"

    def _header_template(self) -> str:
        if self._outcome == "done":
            return "green"
        if self._outcome == "failed":
            return "red"
        if self._outcome == "interrupted":
            return "orange"
        return "blue"

    def _render_elements(self) -> list[dict[str, Any]]:
        elements: list[dict[str, Any]] = []
        for tool_id in self._order:
            entry = self._tools[tool_id]
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": _render_row(entry)}})
            if entry.status == _FAILED and entry.error:
                elements.append({
                    "tag": "note",
                    "elements": [{"tag": "plain_text", "content": f"⚠ {entry.error}"}],
                })
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": self._render_footer()}],
        })
        return elements

    def _render_footer(self) -> str:
        parts = [f"🤖 {self._agent_label}", f"{self.tool_count} tools"]
        if self._total_elapsed is not None:
            parts.append(f"{self._total_elapsed:.1f}s")
        return " · ".join(parts)


def _render_row(entry: ToolEntry) -> str:
    if entry.status == _RUNNING:
        icon, suffix = "🔄", " · …"
    elif entry.status == _FAILED:
        icon = "✗"
        suffix = f" · {_fmt_elapsed(entry.elapsed)}"
    else:
        icon = "✓"
        suffix = f" · {_fmt_elapsed(entry.elapsed)}"
    name = entry.name or "tool"
    if entry.arg:
        return f"**{icon} {name}**  `{entry.arg}`{suffix}"
    return f"**{icon} {name}**{suffix}"


def _fmt_elapsed(elapsed: Optional[float]) -> str:
    return "…" if elapsed is None else f"{elapsed:.1f}s"


class CardSender(Protocol):
    """The slice of the Feishu adapter the patcher needs (duck-typed)."""

    async def create_tool_card(
        self, chat_id: str, card_json: str, *,
        reply_to: Optional[str], metadata: Optional[dict[str, Any]],
    ) -> Optional[str]: ...

    async def patch_tool_card(self, message_id: str, card_json: str) -> bool: ...


class ThrottledCardPatcher:
    """Owns the card message lifecycle: lazy create, then throttled PATCH.

    The card message is created on the first flush (so a round with zero
    tools never produces a card). After creation, tool changes are coalesced:
    a PATCH fires on failure, every ``FLUSH_AFTER_CHANGES`` buffered changes,
    or every ``FLUSH_AFTER_SECONDS`` — whichever is first. Mid-stream
    auto-patching soft-stops at ``EDIT_SOFT_CAP`` so :meth:`finalize` always
    has room for one final patch well under Feishu's 20-edit ceiling.
    """

    def __init__(
        self,
        sender: CardSender,
        builder: ToolCardBuilder,
        chat_id: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        self._sender = sender
        self._builder = builder
        self._chat_id = chat_id
        self._reply_to = reply_to
        self._metadata = metadata
        self._message_id: Optional[str] = None
        self._edits = 0
        self._pending_changes = 0
        self._last_flush = 0.0
        self._stopped = False  # hit soft cap or a fatal patch error
        self._lock = asyncio.Lock()

    @property
    def message_id(self) -> Optional[str]:
        return self._message_id

    @property
    def edits(self) -> int:
        return self._edits

    def mark_pending(self) -> None:
        """Record that the builder changed; call after each add_start/complete."""
        self._pending_changes += 1

    async def flush_if_due(self, *, is_error: bool = False) -> None:
        """Push the current builder state to the API if a threshold is met.

        A failure (``is_error``) flushes immediately regardless of the
        timer/change-count, so failures surface without waiting on coalescing.
        """
        async with self._lock:
            now = time.monotonic()
            timed_out = (now - self._last_flush) >= FLUSH_AFTER_SECONDS
            enough_changes = self._pending_changes >= FLUSH_AFTER_CHANGES
            first = self._message_id is None
            if not (is_error or timed_out or enough_changes or first):
                return
            await self._flush_locked()

    async def finalize(self, outcome: str, total_elapsed: Optional[float]) -> None:
        async with self._lock:
            # A round with zero tools never created a card — leave no trace.
            if self._message_id is None and self._builder.tool_count == 0:
                return
            self._builder.finalize(outcome, total_elapsed)
            if self._builder.render_json_size() > CARD_BODY_MAX:
                # Too big to PATCH safely — keep the last good mid-stream card.
                logger.warning(
                    "[Feishu] tool card final state %d bytes > %d cap — skipping final patch",
                    self._builder.render_json_size(), CARD_BODY_MAX,
                )
                return
            # finalize always gets one best-effort patch, bypassing the soft cap.
            self._stopped = False
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if self._stopped:
            return
        card_json = json.dumps(self._builder.render(), ensure_ascii=False)
        try:
            if self._message_id is None:
                mid = await self._sender.create_tool_card(
                    self._chat_id, card_json,
                    reply_to=self._reply_to, metadata=self._metadata,
                )
                self._last_flush = time.monotonic()
                self._pending_changes = 0
                if mid:
                    self._message_id = mid
                else:
                    logger.warning("[Feishu] tool card create failed — disabling card for this round")
                    self._stopped = True
                return

            ok = await self._sender.patch_tool_card(self._message_id, card_json)
            self._edits += 1
            self._last_flush = time.monotonic()
            self._pending_changes = 0
            if not ok:
                logger.warning(
                    "[Feishu] tool card patch failed at edit #%d — freezing card",
                    self._edits,
                )
                self._stopped = True
            elif self._edits >= EDIT_SOFT_CAP:
                logger.info(
                    "[Feishu] tool card hit soft edit cap (%d) — suppressing mid-stream patches",
                    self._edits,
                )
                self._stopped = True
        except Exception as exc:  # never let card delivery break the round
            logger.warning("[Feishu] tool card flush error: %s", exc)
            self._stopped = True

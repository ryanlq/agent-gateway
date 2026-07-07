"""
Feishu task-status card — streaming card builder + throttled patcher.

Renders a single agent round as one Feishu interactive card that reflects the
overall task state instead of per-tool detail:

    ┌─ 💬 任务执行中 ────────────┐
    │  正在处理您的请求...          │
    └─────────────────────────────┘

Two state transitions, exactly two PATCH calls per round:

  1. ``begin_tool_round``  →  create card with state ``running``
  2. ``end_tool_round``    →  patch card to ``done`` / ``failed`` / ``interrupted``

The card body is hard-capped at 30 KB and a single message at 20 edits; the
:class:`ThrottledCardPatcher` keeps the existing safeguards for that.  Feishu
has no native auto-ticking client-side element, so no time / count is shown —
the state label is the only signal.

Schema is classic Feishu card v1 (``config`` + ``header.template`` + top-level
``elements``): the most broadly supported form for ``interactive`` messages
and PATCH streaming.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional, Protocol

logger = logging.getLogger(__name__)

# Feishu caps a single message at 20 edits. ``finalize()`` bypasses the soft
# cap below to always land one last patch.
EDIT_SOFT_CAP = 17

# Safety margin under Feishu's 30 KB card-body cap. A final card that would
# exceed this is dropped (the card stays at its last good mid-stream state)
# rather than erroring on every patch.
CARD_BODY_MAX = 28_000

_RUNNING = "running"
_DONE = "done"
_FAILED = "failed"
_INTERRUPTED = "interrupted"

# Outcome → (header title, header template color, body status text)
_OUTCOME_DISPLAY: dict[str, tuple[str, str, str]] = {
    _RUNNING: ("💬 任务执行中", "blue", "正在处理您的请求..."),
    _DONE: ("✓ 任务完成", "green", "已完成您的请求"),
    _FAILED: ("⚠ 任务失败", "red", "部分处理失败"),
    _INTERRUPTED: ("⏸ 任务中断", "orange", "处理已被中断"),
}


class TaskStatusCard:
    """Holds the canonical task-state and renders the Feishu card dict.

    The card is the single source of truth; the patcher decides *when* to
    push ``render()`` to the API.  Mutations are cheap (in-memory); the
    expensive PATCH only fires on the patcher's schedule.
    """

    def __init__(self) -> None:
        self._outcome: str = _RUNNING
        self._content: str = ""  # Agent response content

    def finalize(self, outcome: str, content: str = "") -> None:
        """Transition the card to its terminal state.

        ``outcome`` is one of ``"done"`` / ``"failed"`` / ``"interrupted"``;
        any other value is coerced to ``"done"``.
        """
        if outcome in (_DONE, _FAILED, _INTERRUPTED):
            self._outcome = outcome
        else:
            self._outcome = _DONE
        if content:
            self._content = content

    def update_content(self, content: str) -> None:
        """Update the card content (agent response text)."""
        self._content = content

    @property
    def outcome(self) -> str:
        return self._outcome

    @property
    def content(self) -> str:
        return self._content

    # -- render ---------------------------------------------------------

    def render(self) -> dict[str, Any]:
        title, template, body = _OUTCOME_DISPLAY[self._outcome]
        elements: list[dict[str, Any]] = []

        # Show content if available (agent response)
        # Use div+lark_md for PATCH compatibility (tag:markdown may not work with PATCH API)
        if self._content:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": self._content}})
        else:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})

        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": template,
            },
            "elements": elements,
        }

    def render_json_size(self) -> int:
        return len(json.dumps(self.render(), ensure_ascii=False))


class CardSender(Protocol):
    """The slice of the Feishu adapter the patcher needs (duck-typed)."""

    async def create_tool_card(
        self, chat_id: str, card_json: str, *,
        reply_to: Optional[str], metadata: Optional[dict[str, Any]],
    ) -> Optional[str]: ...

    async def patch_tool_card(self, message_id: str, card_json: str) -> bool: ...


class ThrottledCardPatcher:
    """Owns the card message lifecycle: lazy create, then throttled PATCH.

    For the task-status card the lifecycle is intentionally minimal — the
    runner drives ``flush_if_due`` to create the initial "running" card and
    ``finalize`` to push the terminal state.  Mid-stream hooks are no-ops,
    so the soft cap and time/change thresholds almost never fire, but the
    safeguards are kept so a misbehaving caller cannot blow past Feishu's
    limits.
    """

    def __init__(
        self,
        sender: CardSender,
        card: TaskStatusCard,
        chat_id: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        self._sender = sender
        self._card = card
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
        """Record that the card state changed; call after each mutation."""
        self._pending_changes += 1

    async def flush_if_due(self, *, is_error: bool = False) -> None:
        """Push the current card state to the API if a threshold is met.

        For the task-status card this is mainly used for the *initial* create
        (``_message_id is None``) triggered by ``begin_tool_round``.  A
        failure (``is_error``) always flushes immediately.
        """
        async with self._lock:
            now = time.monotonic()
            first = self._message_id is None
            if not (is_error or first or self._pending_changes >= 3
                    or (now - self._last_flush) >= 1.5):
                return
            await self._flush_locked()

    async def finalize(self, outcome: str, content: str = "") -> None:
        async with self._lock:
            self._card.finalize(outcome, content)
            if self._card.render_json_size() > CARD_BODY_MAX:
                logger.warning(
                    "[Feishu] task card final state %d bytes > %d cap — skipping final patch",
                    self._card.render_json_size(), CARD_BODY_MAX,
                )
                return
            # finalize always gets one best-effort patch, bypassing the soft cap.
            self._stopped = False
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if self._stopped:
            return
        card_json = json.dumps(self._card.render(), ensure_ascii=False)
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
                    logger.warning("[Feishu] task card create failed — disabling card for this round")
                    self._stopped = True
                return

            ok = await self._sender.patch_tool_card(self._message_id, card_json)
            self._edits += 1
            self._last_flush = time.monotonic()
            self._pending_changes = 0
            if not ok:
                logger.warning(
                    "[Feishu] task card patch failed at edit #%d — freezing card",
                    self._edits,
                )
                self._stopped = True
            elif self._edits >= EDIT_SOFT_CAP:
                logger.info(
                    "[Feishu] task card hit soft edit cap (%d) — suppressing mid-stream patches",
                    self._edits,
                )
                self._stopped = True
        except Exception as exc:  # never let card delivery break the round
            logger.warning("[Feishu] task card flush error: %s", exc)
            self._stopped = True

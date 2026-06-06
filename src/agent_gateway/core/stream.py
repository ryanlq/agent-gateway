"""
Stream consumer — bridges agent streaming output to platform delivery.

Converts incremental text deltas into platform-native messages using one
of two strategies:

  1. **Draft streaming** (preferred): the adapter supports ``send_draft()``
     which renders an animated streaming preview.

  2. **Edit streaming** (fallback): send an initial message, then repeatedly
     ``edit_message`` as more text arrives.

Both strategies include adaptive throttling to respect platform rate limits.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from agent_gateway.core.message import SendResult

logger = logging.getLogger(__name__)


@dataclass
class StreamConsumerConfig:
    """Configuration for the stream consumer."""

    min_edit_interval: float = 0.8
    """Minimum seconds between edit/draft requests."""

    use_draft: bool = False
    """Prefer draft streaming when available."""

    max_message_length: int = 0
    """Platform message length limit (0 = unlimited)."""

    tool_progress_mode: str = "all"
    """Tool progress display: all / new / verbose / none."""

    tool_preview_length: int = 40
    """Max characters of tool argument preview."""


class StreamConsumer:
    """
    Consumes streaming text deltas and pushes them to a platform adapter.

    Usage::

        consumer = StreamConsumer(adapter, chat_id, config)
        for chunk in agent.stream(prompt):
            consumer.on_delta(chunk)
        await consumer.finish(final_text)
    """

    def __init__(
        self,
        adapter: Any,
        chat_id: str,
        config: StreamConsumerConfig | None = None,
        *,
        metadata: dict[str, Any] | None = None,
        reply_to: str | None = None,
    ) -> None:
        self.adapter = adapter
        self.chat_id = chat_id
        self.config = config or StreamConsumerConfig()
        self.metadata = metadata
        self.reply_to = reply_to

        # Internal state
        self._buffer: str = ""
        self._message_id: Optional[str] = None
        self._draft_id: int = 0
        self._last_edit_time: float = 0.0
        self._use_draft: bool = False
        self._segment_count: int = 0
        self._tool_count: int = 0
        self._finished: bool = False

        # Check draft support
        if self.config.use_draft:
            try:
                if adapter.supports_draft_streaming():
                    self._use_draft = True
                    self._draft_id = int(time.time() * 1000) % (2**31)
            except Exception:
                self._use_draft = False

    # -- Public API ----------------------------------------------------------

    def on_delta(self, text: str) -> None:
        """Called for each incremental text chunk.

        Buffers the text and schedules a platform update if enough time has
        passed since the last one (adaptive throttling).
        """
        if self._finished:
            return

        self._buffer += text

        now = time.monotonic()
        if now - self._last_edit_time >= self.config.min_edit_interval:
            self._schedule_flush()

    def on_segment_break(self) -> None:
        """Called when a text segment ends (e.g. before a tool call)."""
        self._segment_count += 1

    def on_commentary(self, text: str) -> None:
        """Called for agent commentary / thinking output.

        By default, commentary is appended to the buffer with a visual
        separator.  Adapters can override ``render_message_event`` to
        customise this.
        """
        # Commentary is typically not shown on messaging platforms
        # (it's thinking/internal), but we track it for completeness.
        pass

    async def on_tool_call(self, tool_name: str, preview: str = "",
                           args: dict | None = None) -> None:
        """Called when a tool is invoked.

        Renders tool progress on the platform if the adapter supports it.
        """
        if self.config.tool_progress_mode == "none":
            return

        self._tool_count += 1

        # Build tool progress text
        tool_text = f"⚙️ {tool_name}"
        if preview:
            cap = self.config.tool_preview_length or 40
            if len(preview) > cap:
                preview = preview[:cap - 3] + "..."
            tool_text += f': "{preview}"'
        elif args and self.config.tool_progress_mode == "verbose":
            import json
            args_str = json.dumps(args, ensure_ascii=False, default=str)
            if len(args_str) > 80:
                args_str = args_str[:77] + "..."
            tool_text += f"({list(args.keys())})\n{args_str}"
        else:
            tool_text += "..."

        # Flush current text buffer first, then send tool progress
        await self._flush()

        try:
            await self.adapter.send(
                self.chat_id,
                tool_text,
                metadata=self.metadata,
            )
        except Exception as exc:
            logger.debug("Tool progress send failed: %s", exc)

    async def finish(self, final_content: str | None = None) -> SendResult:
        """Signal that streaming is complete.

        If *final_content* is provided, it replaces the buffer entirely
        (useful for formatted final output).

        Returns the ``SendResult`` of the final message.
        """
        self._finished = True
        content = final_content or self._buffer

        if not content:
            return SendResult(success=True)

        # If we've been editing a message, do the final edit
        if self._message_id and not self._use_draft:
            try:
                result = await self.adapter.edit_message(
                    self.chat_id, self._message_id, content,
                    finalize=True,
                )
                if result.success:
                    return result
            except Exception as exc:
                logger.debug("Final edit failed, falling back to send: %s", exc)

        # Otherwise send the final content as a fresh message
        try:
            result = await self.adapter.send(
                self.chat_id,
                content,
                reply_to=self.reply_to,
                metadata=self.metadata,
            )
            # Clean up the old streaming message if we sent a fresh one
            if self._message_id and result.success and result.message_id:
                try:
                    await self.adapter.delete_message(self.chat_id, self._message_id)
                except Exception:
                    pass  # Best-effort cleanup

            return result
        except Exception as exc:
            logger.error("Final send failed: %s", exc)
            return SendResult(success=False, error=str(exc))

    # -- Internal ------------------------------------------------------------

    def _schedule_flush(self) -> None:
        """Schedule an async flush.  Called from sync ``on_delta`` context."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._flush())
        except RuntimeError:
            pass  # No running loop

    async def _flush(self) -> None:
        """Push the current buffer content to the platform."""
        if not self._buffer:
            return

        self._last_edit_time = time.monotonic()
        content = self._buffer

        # Truncate if platform has a length limit
        if self.config.max_message_length > 0:
            len_fn = getattr(self.adapter, "message_len_fn", len)
            if len_fn(content) > self.config.max_message_length:
                content = content[:self.config.max_message_length - 20] + "\n\n..."

        try:
            if self._use_draft and self._draft_id:
                # Draft streaming path
                result = await self.adapter.send_draft(
                    self.chat_id, self._draft_id, content,
                    metadata=self.metadata,
                )
            elif self._message_id:
                # Edit path
                result = await self.adapter.edit_message(
                    self.chat_id, self._message_id, content,
                )
            else:
                # Initial send
                result = await self.adapter.send(
                    self.chat_id, content,
                    reply_to=self.reply_to,
                    metadata=self.metadata,
                )
                if result.success and result.message_id:
                    self._message_id = result.message_id

        except Exception as exc:
            logger.debug("Stream flush failed: %s", exc)

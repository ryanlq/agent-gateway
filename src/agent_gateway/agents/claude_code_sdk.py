"""
Claude Code SDK bridge.

Uses the official ``claude-code-sdk`` Python library (``pip install claude-code-sdk``),
which wraps the Claude Code CLI in a structured async API.

Strictly superior to parsing ``claude --print --output-format stream-json`` stdout:

  - Structured ``Message`` events (no stdout parsing)
  - Explicit ``ResultMessage`` (clear success / failure signal)
  - Native exception hierarchy (no more exit-code guesswork)
  - Native session resume via ``ClaudeCodeOptions.resume`` + ``continue_conversation``
  - Auth reuses Claude Code CLI's OAuth — **Max plan users are not billed additionally**

Requirements::

    pip install claude-code-sdk
    claude --version          # underlying CLI must be installed + logged in
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from agent_gateway.agents.base import (
    CLIAgentBridge,
    CLIAgentError,
    CLICrashError,
    CLITimeoutError,
    SubprocessConfig,
)
from agent_gateway.agents.events import AgentEvent

logger = logging.getLogger(__name__)


def _check_sdk_deps() -> bool:
    """Return True if ``claude_code_sdk`` is importable."""
    try:
        import claude_code_sdk  # noqa: F401

        return True
    except ImportError:
        return False


class ClaudeCodeSdkBridge(CLIAgentBridge):
    """Claude Code bridge backed by the official Python SDK.

    Drop-in replacement for :class:`ClaudeCodeBridge`. The ``GatewayRunner``
    interface (``chat`` / ``stream`` / ``shutdown`` / ``captured_cli_session_id``)
    is preserved so callers do not need changes.

    Parameters mirror the CLI bridge where they overlap, plus a few
    SDK-specific options (``thinking``, ``max_budget_usd``, ``can_use_tool``).
    """

    def __init__(
        self,
        *,
        model: str | None = None,
        max_turns: int = 20,
        timeout: float | None = None,
        permission_mode: str = "acceptEdits",
        allowed_tools: str | None = None,
        disallowed_tools: str | None = None,
        bare: bool = False,
        effort: str | None = None,
        thinking: dict | None = None,
        max_budget_usd: float | None = None,
        include_partial_messages: bool = True,
    ) -> None:
        # SubprocessConfig is only used by the base class for bookkeeping
        # (e.g. the timeout field); the SDK manages its own subprocess.
        super().__init__(
            SubprocessConfig(
                command=["claude-code-sdk"],
                timeout=timeout,
            )
        )
        self.model = model
        self.max_turns = max_turns
        self.permission_mode = permission_mode or "acceptEdits"
        self.allowed_tools = (
            [t.strip() for t in allowed_tools.split(",") if t.strip()]
            if allowed_tools
            else None
        )
        self.disallowed_tools = (
            [t.strip() for t in disallowed_tools.split(",") if t.strip()]
            if disallowed_tools
            else None
        )
        self.bare = bare
        self.effort = effort
        self.thinking = thinking
        self.max_budget_usd = max_budget_usd
        self.include_partial_messages = include_partial_messages

        # Gateway reads this after stream() to pass as session_ref on the next
        # turn, enabling cross-process session resume.
        self.captured_cli_session_id: str | None = None

    # -- Internal helpers ---------------------------------------------------

    def _build_options(
        self,
        session_ref: str | None,
        system_extra: str,
    ) -> Any:
        """Construct a ``ClaudeCodeOptions`` for a single invocation."""
        from claude_code_sdk import ClaudeCodeOptions

        kwargs: dict[str, Any] = {
            "permission_mode": self.permission_mode,
            "continue_conversation": bool(session_ref),
        }
        # max_turns=None (client "Unlimited") → omit it so the SDK runs until
        # the agent naturally stops, instead of forwarding None.
        if self.max_turns is not None:
            kwargs["max_turns"] = self.max_turns
        if self.model:
            kwargs["model"] = self.model
        if session_ref:
            kwargs["resume"] = session_ref
        if system_extra:
            kwargs["append_system_prompt"] = system_extra
        if self.allowed_tools:
            kwargs["allowed_tools"] = self.allowed_tools
        if self.disallowed_tools:
            kwargs["disallowed_tools"] = self.disallowed_tools
        if self.bare:
            # Skip CLAUDE.md auto-discovery / hooks / plugins
            kwargs["settings"] = "none"
        if self.thinking:
            # Forward as extra_args if SDK exposes thinking via CLI flag
            kwargs.setdefault("extra_args", {})["thinking"] = str(self.thinking)
        if self.max_budget_usd is not None:
            kwargs.setdefault("extra_args", {})["max-budget-usd"] = str(
                self.max_budget_usd
            )
        if self.effort:
            kwargs.setdefault("extra_args", {})["effort"] = self.effort
        kwargs["include_partial_messages"] = self.include_partial_messages
        return ClaudeCodeOptions(**kwargs)

    def _build_prompt(
        self,
        message: str,
        history: list[dict[str, Any]],
        session_ref: str | None,
    ) -> str:
        """Build the user prompt.

        When ``session_ref`` is set the CLI's own transcript is the source of
        truth — do NOT replay history text (that would double the context and
        lose tool-call structure). Same rule as the CLI bridge.
        """
        if session_ref:
            return message
        parts: list[str] = []
        history_text = self._format_history(history)
        if history_text:
            parts.append("Previous conversation:\n" + history_text)
        parts.append(message)
        return "\n\n".join(parts)

    def _format_history(self, history: list[dict[str, Any]]) -> str:
        """Format history as Human/Assistant blocks (Claude convention)."""
        if not history:
            return ""
        parts: list[str] = []
        for entry in history:
            role = entry.get("role", "")
            content = entry.get("content", "")
            if role == "user":
                parts.append(f"Human: {content}")
            elif role == "assistant":
                parts.append(f"Assistant: {content}")
            else:
                parts.append(f"{role.capitalize()}: {content}")
        return "\n\n".join(parts)

    # -- CLIAgentBridge overrides ------------------------------------------

    def _build_args(self, *args: Any, **kwargs: Any) -> list[str]:
        # Unused by the SDK path; base class only calls it from the
        # subprocess-based chat()/stream(). We override both.
        return []

    async def _parse_output(self, raw_stdout: str, session_key: str) -> str:
        return raw_stdout  # unused

    async def chat(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        session_ref: str | None = None,
    ) -> str:
        """Non-streaming: collect text deltas and return the final string.

        Non-text events (reasoning, tool_start, tool_complete) are consumed
        but not included in the returned text — they're only meaningful for
        the streaming path where the runner routes them to the right sink.
        """
        chunks: list[str] = []
        async for event in self.stream(
            session_key,
            message,
            history,
            system_extra,
            session_ref=session_ref,
        ):
            if isinstance(event, AgentEvent):
                if event.kind == "text_delta" and event.text:
                    chunks.append(event.text)
            elif isinstance(event, str):
                chunks.append(event)
        return "".join(chunks)

    async def stream(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        session_ref: str | None = None,
    ) -> AsyncIterator[str]:
        """Yield incremental text chunks from the Claude Code SDK."""
        # Reset captured id for this invocation; filled from ResultMessage
        self.captured_cli_session_id = None

        try:
            from claude_code_sdk import (
                AssistantMessage,
                ResultMessage,
                SystemMessage,
                query,
            )
        except ImportError as exc:
            raise CLIAgentError(
                "claude-code-sdk is not installed. Install with: "
                "pip install claude-code-sdk"
            ) from exc

        options = self._build_options(session_ref, system_extra)
        prompt = self._build_prompt(message, history, session_ref)

        # Track content block indices already surfaced via StreamEvent
        # (token-level streaming). When the final AssistantMessage arrives,
        # blocks whose index is in this set are skipped for TextBlock /
        # ThinkingBlock to avoid double-emit. ToolUseBlock / ToolResultBlock
        # are handled via ``seen_tool_ids`` below.
        announced_indices: set[int] = set()
        # Track whether a "thinking..." lead-in was emitted for the current
        # thinking block; subsequent thinking_delta yields actual content.
        thinking_active = False
        # Track tool_use IDs we've emitted tool_start for, so we can emit
        # tool_complete when the matching ToolResultBlock arrives.
        seen_tool_ids: dict[str, str] = {}  # tool_use_id → tool_name

        self._logger.info(
            "[claude-code-sdk] Starting query (prompt_len=%d, resume=%s, model=%s)",
            len(prompt),
            session_ref,
            self.model,
        )
        event_count = 0
        # Track whether any real text_delta was emitted. If the model spends
        # all its budget on reasoning and produces zero text (rare but possible
        # with very short prompts), the platform chat would otherwise show
        # "no response". We accumulate reasoning as a fallback and promote it
        # to text_delta at the end if nothing else was sent.
        saw_text_delta = False
        reasoning_fallback: list[str] = []
        try:
            # Per-event timeout: if the SDK hangs (subprocess stuck, anyio
            # stream blocked), asyncio.wait_for on each __anext__() raises
            # TimeoutError -> caught by the except asyncio.TimeoutError branch.
            async def _sdk_events():
                async for _m in query(prompt=prompt, options=options):
                    yield _m

            _aiter = _sdk_events()
            while True:
                try:
                    msg = await asyncio.wait_for(
                        _aiter.__anext__(),
                        timeout=self.config.timeout,
                    )
                except StopAsyncIteration:
                    break
                event_count += 1
                if event_count <= 3 or event_count % 50 == 0:
                    self._logger.info(
                        "[claude-code-sdk] event #%d type=%s",
                        event_count,
                        type(msg).__name__,
                    )
                if isinstance(msg, AssistantMessage) or self._is_user_message(msg):
                    # Walk blocks in the message. For TextBlock / ThinkingBlock
                    # we dedupe by announced_indices (already streamed inline).
                    # For ToolUseBlock / ToolResultBlock we emit structured
                    # tool_start / tool_complete events.
                    for i, block in enumerate(getattr(msg, "content", []) or []):
                        btype = type(block).__name__
                        if btype == "TextBlock":
                            if i in announced_indices:
                                continue
                            text = getattr(block, "text", "") or ""
                            if text:
                                saw_text_delta = True
                                yield AgentEvent.text_delta(text)
                        elif btype == "ThinkingBlock":
                            if i in announced_indices:
                                continue
                            thinking = getattr(block, "thinking", "") or ""
                            if thinking:
                                reasoning_fallback.append(thinking)
                                yield AgentEvent.reasoning_delta(thinking)
                        elif btype == "ToolUseBlock":
                            tid = getattr(block, "id", "") or ""
                            tname = getattr(block, "name", "") or "tool"
                            if tid and tid not in seen_tool_ids:
                                seen_tool_ids[tid] = tname
                                yield AgentEvent.tool_start(
                                    name=tname,
                                    tool_id=tid,
                                    tool_input=getattr(block, "input", {}) or {},
                                )
                        elif btype == "ToolResultBlock":
                            tid = getattr(block, "tool_use_id", "") or ""
                            tname = seen_tool_ids.get(tid, "")
                            if tid and tname:
                                is_err = bool(getattr(block, "is_error", False))
                                result = getattr(block, "content", None)
                                yield AgentEvent.tool_complete(
                                    name=tname,
                                    tool_id=tid,
                                    result=result,
                                    is_error=is_err,
                                )
                    # Reset per-turn text/thinking dedup state. Tool id map is
                    # preserved across turns because ToolResultBlocks for a
                    # turn N tool may arrive in turn N+1's UserMessage.
                    announced_indices = set()
                    thinking_active = False

                elif isinstance(msg, ResultMessage):
                    if msg.session_id:
                        self.captured_cli_session_id = msg.session_id
                    if msg.is_error:
                        err_text = msg.result or "Claude Code SDK returned an error"
                        raise CLIAgentError(err_text)
                    self._logger.debug(
                        "SDK finished: turns=%d cost=$%.4f session=%s",
                        msg.num_turns,
                        msg.total_cost_usd or 0.0,
                        msg.session_id,
                    )

                elif isinstance(msg, SystemMessage):
                    self._logger.debug("SystemMessage subtype=%s", msg.subtype)

                else:
                    # StreamEvent (raw Anthropic streaming delta) — only when
                    # include_partial_messages=True. Yields typed AgentEvents.
                    for event, meta in self._iter_stream_event(msg, thinking_active):
                        if meta.get("thinking_started"):
                            thinking_active = True
                        idx = meta.get("block_index")
                        if idx is not None:
                            announced_indices.add(idx)
                        tid = meta.get("tool_id")
                        tname = meta.get("tool_name")
                        if tid and tname and tid not in seen_tool_ids:
                            seen_tool_ids[tid] = tname
                        if event is not None:
                            if event.kind == "text_delta":
                                saw_text_delta = True
                            elif event.kind == "reasoning_delta":
                                reasoning_fallback.append(event.text)
                            yield event

            self._logger.info(
                "[claude-code-sdk] Query finished: %d events emitted", event_count
            )

            # Safety net: if the model produced ONLY reasoning with no text,
            # promote the reasoning to a text_delta so the platform chat shows
            # something instead of an empty "no response" message. The
            # reasoning panel already got the same text, so the desktop user
            # sees it in both panels — that's the lesser evil vs total silence.
            if not saw_text_delta and reasoning_fallback:
                fallback_text = (
                    "\n\n💭 *model reasoning (no final text):*\n\n"
                    + "".join(reasoning_fallback)
                )
                self._logger.warning(
                    "[claude-code-sdk] No text_delta emitted; promoting %d chars "
                    "of reasoning as fallback text_delta",
                    len(fallback_text),
                )
                yield AgentEvent.text_delta(fallback_text)
        except asyncio.CancelledError:
            # User clicked interrupt. Re-raise cleanly so the runner's
            # interrupt propagates — the SDK's anyio cancel-scope cleanup
            # may later raise a noisy RuntimeError from its generator's
            # finally block; that's handled by the generic branch below.
            self._logger.info("[claude-code-sdk] Cancelled by user (interrupt)")
            raise
        except CLIAgentError:
            raise
        except asyncio.TimeoutError as exc:
            raise CLITimeoutError(self.config.timeout, "claude-code-sdk") from exc
        except Exception as exc:
            # Map SDK-specific exceptions to our hierarchy
            msg_text = str(exc)
            # Suppress the anyio cancel-scope RuntimeError that the SDK raises
            # during generator cleanup after a Task.cancel() — it's noise
            # caused by the SDK using anyio in an asyncio context. If the
            # current task is actually being cancelled, re-raise CancelledError
            # so the interrupt takes effect.
            if "cancel scope" in msg_text:
                cur = asyncio.current_task()
                if cur is not None and cur.cancelling():
                    self._logger.debug(
                        "[claude-code-sdk] Suppressing cancel-scope error during interrupt"
                    )
                    raise asyncio.CancelledError() from exc
            etype = type(exc).__name__
            if etype == "ProcessError":
                # exc.exit_code / exc.stderr available
                exit_code = getattr(exc, "exit_code", None) or -1
                stderr = getattr(exc, "stderr", "") or ""
                raise CLICrashError(exit_code, stderr, "claude-code-sdk") from exc
            if etype == "CLINotFoundError":
                raise CLIAgentError(
                    "Claude Code CLI not found. Install it and run "
                    "'claude --version' to verify."
                ) from exc
            raise CLIAgentError(f"Claude Code SDK error: {exc}") from exc

    # -- Stream / block helpers --------------------------------------------

    @staticmethod
    def _is_user_message(msg: Any) -> bool:
        """Duck-type check for UserMessage without importing the class."""
        return type(msg).__name__ == "UserMessage"

    def _iter_stream_event(
        self, event: Any, thinking_active: bool
    ) -> list[tuple[AgentEvent | None, dict]]:
        """Parse a raw Anthropic ``StreamEvent`` into typed :class:`AgentEvent`.

        Returns a list of ``(event, meta)`` tuples (usually 0 or 1). The
        caller yields ``event`` (if not None) and merges ``meta`` into the
        per-turn dedup state.

        Meta keys:
          - ``block_index`` (int): index announced → skip matching
            TextBlock/ThinkingBlock in the final AssistantMessage
          - ``thinking_started`` (bool): flip ``thinking_active`` so
            subsequent thinking_delta events stream actual content
          - ``tool_id`` / ``tool_name``: register a new tool_use id so
            matching ToolResultBlocks can emit tool_complete

        Event handling:
          - ``content_block_start`` (text)     → silent (first delta via content_block_delta)
          - ``content_block_start`` (tool_use) → AgentEvent.tool_start
          - ``content_block_start`` (thinking) → silent marker; flip thinking_active
          - ``content_block_delta`` (text_delta)     → AgentEvent.text_delta
          - ``content_block_delta`` (thinking_delta) → AgentEvent.reasoning_delta
                                                       (only when thinking_active)
          - ``content_block_delta`` (input_json_delta) → silent
        """
        raw = getattr(event, "event", None)
        if not isinstance(raw, dict):
            return []
        etype = raw.get("type")
        index = raw.get("index")

        if etype == "content_block_delta":
            delta = raw.get("delta", {}) or {}
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                text = delta.get("text", "") or ""
                if not text:
                    return []
                meta = {"block_index": index} if index is not None else {}
                return [(AgentEvent.text_delta(text), meta)]
            if delta_type == "thinking_delta":
                text = delta.get("thinking", "") or ""
                if not text or not thinking_active:
                    return []
                meta = {"block_index": index} if index is not None else {}
                return [(AgentEvent.reasoning_delta(text), meta)]
            # input_json_delta (tool input) — silent (too noisy for chat)
            return []

        if etype == "content_block_start":
            block = raw.get("content_block", {}) or {}
            btype = block.get("type")
            if btype == "tool_use":
                name = block.get("name", "tool")
                tid = block.get("id", "")
                meta: dict = {}
                if index is not None:
                    meta["block_index"] = index
                if tid:
                    meta["tool_id"] = tid
                    meta["tool_name"] = name
                return [
                    (AgentEvent.tool_start(name=name, tool_id=tid, tool_input={}), meta)
                ]
            if btype == "thinking":
                meta = {"thinking_started": True}
                if index is not None:
                    meta["block_index"] = index
                # Don't emit a placeholder — the desktop already shows a
                # "thinking" affordance when reasoning.delta starts flowing.
                return [(None, meta)]
            # text content_block_start — first delta arrives via
            # content_block_delta; nothing to emit here.
            return []

        # Other events (message_start, message_delta, ping, ...) — silent
        return []

    async def shutdown(self) -> None:
        # The SDK manages its own subprocess lifecycle; nothing to clean up.
        return None

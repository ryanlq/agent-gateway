"""Tests for ClaudeCodeSdkBridge."""

import asyncio
import pytest
from unittest.mock import patch

from agent_gateway.agents.claude_code_sdk import (
    ClaudeCodeSdkBridge,
    _check_sdk_deps,
)
from agent_gateway.agents.base import CLIAgentError, CLICrashError, CLITimeoutError


# ---------------------------------------------------------------------------
# Dep check
# ---------------------------------------------------------------------------


class TestDepsCheck:
    def test_sdk_available(self):
        assert _check_sdk_deps() is True

    def test_sdk_missing(self):
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *a, **kw):
            if name == "claude_code_sdk":
                raise ImportError("simulated")
            return real_import(name, *a, **kw)

        with patch.object(builtins, "__import__", side_effect=fake_import):
            assert _check_sdk_deps() is False


# ---------------------------------------------------------------------------
# Construction / options builder
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_defaults(self):
        b = ClaudeCodeSdkBridge()
        assert b.model is None
        assert b.max_turns == 20
        assert b.permission_mode == "acceptEdits"
        assert b.allowed_tools is None
        assert b.captured_cli_session_id is None

    def test_parse_allowed_tools(self):
        b = ClaudeCodeSdkBridge(allowed_tools="Bash,Read,Edit")
        assert b.allowed_tools == ["Bash", "Read", "Edit"]

    def test_parse_disallowed_tools(self):
        b = ClaudeCodeSdkBridge(disallowed_tools="WebSearch,WebFetch")
        assert b.disallowed_tools == ["WebSearch", "WebFetch"]

    def test_options_basic(self):
        b = ClaudeCodeSdkBridge(model="claude-sonnet-4-6", max_turns=5)
        opts = b._build_options(session_ref=None, system_extra="Be concise")
        assert opts.model == "claude-sonnet-4-6"
        assert opts.max_turns == 5
        assert opts.append_system_prompt == "Be concise"
        assert opts.resume is None
        assert opts.continue_conversation is False

    def test_options_resume(self):
        b = ClaudeCodeSdkBridge()
        opts = b._build_options(session_ref="sess_abc", system_extra="")
        assert opts.resume == "sess_abc"
        assert opts.continue_conversation is True

    def test_options_effort_via_extra_args(self):
        b = ClaudeCodeSdkBridge(effort="high")
        opts = b._build_options(None, "")
        assert opts.extra_args.get("effort") == "high"


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------


class TestPromptBuilding:
    def test_prompt_no_history(self):
        b = ClaudeCodeSdkBridge()
        p = b._build_prompt("hello", [], None)
        assert p == "hello"

    def test_prompt_with_history(self):
        b = ClaudeCodeSdkBridge()
        history = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
        ]
        p = b._build_prompt("now", history, None)
        assert "Human: first" in p
        assert "Assistant: reply" in p
        assert p.endswith("now")

    def test_prompt_with_session_ref_skips_history(self):
        b = ClaudeCodeSdkBridge()
        history = [{"role": "user", "content": "old"}]
        p = b._build_prompt("new msg", history, "sess_abc")
        assert p == "new msg"  # history intentionally skipped


# ---------------------------------------------------------------------------
# Streaming — fake the SDK
# ---------------------------------------------------------------------------


def _make_fake_messages(events):
    """Build an async generator that yields the given events."""

    async def _gen(**kwargs):
        for ev in events:
            yield ev

    return _gen


class TestStream:
    @pytest.mark.asyncio
    async def test_yields_text_blocks(self):
        from claude_code_sdk import (
            AssistantMessage,
            ResultMessage,
            TextBlock,
        )

        events = [
            AssistantMessage(
                content=[TextBlock(text="Hello "), TextBlock(text="world")],
                model="claude-sonnet-4-6",
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success",
                duration_ms=1000,
                duration_api_ms=900,
                is_error=False,
                num_turns=1,
                session_id="sess_xyz",
                total_cost_usd=0.001,
                usage=None,
                result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            events_out = []
            async for e in bridge.stream("sk", "hi", [], ""):
                events_out.append(e)

        assert [e.text for e in events_out if e.kind == "text_delta"] == ["Hello ", "world"]
        assert bridge.captured_cli_session_id == "sess_xyz"

    @pytest.mark.asyncio
    async def test_result_error_raises(self):
        from claude_code_sdk import ResultMessage

        events = [
            ResultMessage(
                subtype="error",
                duration_ms=100,
                duration_api_ms=50,
                is_error=True,
                num_turns=0,
                session_id=None,
                total_cost_usd=None,
                usage=None,
                result="API failure: overloaded",
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            with pytest.raises(CLIAgentError, match="API failure"):
                async for _ in bridge.stream("sk", "hi", [], ""):
                    pass

    @pytest.mark.asyncio
    async def test_result_error_empty_message(self):
        from claude_code_sdk import ResultMessage

        events = [
            ResultMessage(
                subtype="error",
                duration_ms=0,
                duration_api_ms=0,
                is_error=True,
                num_turns=0,
                session_id=None,
                total_cost_usd=None,
                usage=None,
                result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            with pytest.raises(CLIAgentError, match="returned an error"):
                async for _ in bridge.stream("sk", "hi", [], ""):
                    pass

    @pytest.mark.asyncio
    async def test_tool_use_not_yielded(self):
        from claude_code_sdk import (
            AssistantMessage,
            ResultMessage,
            TextBlock,
            ToolUseBlock,
        )

        events = [
            AssistantMessage(
                content=[
                    TextBlock(text="thinking..."),
                    ToolUseBlock(id="tu_1", name="Bash", input={"command": "ls"}),
                ],
                model="claude-sonnet-4-6",
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success",
                duration_ms=0,
                duration_api_ms=0,
                is_error=False,
                num_turns=1,
                session_id="sess_1",
                total_cost_usd=None,
                usage=None,
                result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            chunks = []
            async for c in bridge.stream("sk", "hi", [], ""):
                chunks.append(c)

        # TextBlock yielded as text_delta; ToolUseBlock yielded as tool_start
        kinds = [e.kind for e in chunks]
        assert kinds == ["text_delta", "tool_start"]
        assert chunks[0].text == "thinking..."
        assert chunks[1].tool_name == "Bash"

    @pytest.mark.asyncio
    async def test_process_error_maps_to_crash(self):
        from claude_code_sdk import ProcessError

        async def boom(**kwargs):
            raise ProcessError("CLI crashed", exit_code=2, stderr="boom")
            yield  # pragma: no cover — makes this an async generator

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", new=boom):
            with pytest.raises(CLICrashError) as exc_info:
                async for _ in bridge.stream("sk", "hi", [], ""):
                    pass
        assert exc_info.value.returncode == 2
        assert "boom" in exc_info.value.stderr

    @pytest.mark.asyncio
    async def test_timeout_mapped(self):
        async def slow(**kwargs):
            raise asyncio.TimeoutError()
            yield  # pragma: no cover

        bridge = ClaudeCodeSdkBridge(timeout=30.0)
        with patch("claude_code_sdk.query", new=slow):
            with pytest.raises(CLITimeoutError):
                async for _ in bridge.stream("sk", "hi", [], ""):
                    pass

    @pytest.mark.asyncio
    async def test_cli_not_found_mapped(self):
        from claude_code_sdk import CLINotFoundError

        async def missing(**kwargs):
            raise CLINotFoundError()
            yield  # pragma: no cover

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", new=missing):
            with pytest.raises(CLIAgentError, match="Claude Code CLI not found"):
                async for _ in bridge.stream("sk", "hi", [], ""):
                    pass


# ---------------------------------------------------------------------------
# chat() — non-streaming wrapper
# ---------------------------------------------------------------------------



    @pytest.mark.asyncio
    async def test_stream_event_text_delta(self):
        """include_partial_messages=True emits StreamEvent with per-token deltas."""
        from claude_code_sdk import AssistantMessage, ResultMessage, TextBlock
        from claude_code_sdk.types import StreamEvent

        events = [
            StreamEvent(
                uuid="u1",
                session_id="s1",
                event={
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "Hel"},
                },
                parent_tool_use_id=None,
            ),
            StreamEvent(
                uuid="u2",
                session_id="s1",
                event={
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "lo"},
                },
                parent_tool_use_id=None,
            ),
            # Final AssistantMessage must NOT re-emit the same text
            AssistantMessage(
                content=[TextBlock(text="Hello")],
                model="claude-sonnet-4-6",
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success",
                duration_ms=0,
                duration_api_ms=0,
                is_error=False,
                num_turns=1,
                session_id="s1",
                total_cost_usd=None,
                usage=None,
                result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            chunks = []
            async for e in bridge.stream("sk", "hi", [], ""):
                chunks.append(e)
        # Two token deltas → 2 text_delta events; final AssistantMessage deduped
        assert [(e.kind, e.text) for e in chunks] == [
            ("text_delta", "Hel"),
            ("text_delta", "lo"),
        ]

    @pytest.mark.asyncio
    async def test_stream_event_tool_use(self):
        """content_block_start tool_use emits a progress marker."""
        from claude_code_sdk import ResultMessage
        from claude_code_sdk.types import StreamEvent

        events = [
            StreamEvent(
                uuid="u1",
                session_id="s1",
                event={
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use",
                        "id": "tu_99",
                        "name": "Read",
                        "input": {},
                    },
                },
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success",
                duration_ms=0,
                duration_api_ms=0,
                is_error=False,
                num_turns=1,
                session_id="s1",
                total_cost_usd=None,
                usage=None,
                result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            chunks = []
            async for e in bridge.stream("sk", "hi", [], ""):
                chunks.append(e)
        assert len(chunks) == 1
        assert chunks[0].kind == "tool_start"
        assert chunks[0].tool_name == "Read"
        assert chunks[0].tool_id == "tu_99"



    @pytest.mark.asyncio
    async def test_stream_event_thinking_content(self):
        """thinking_delta streams the actual thinking text (not just indicator)."""
        from claude_code_sdk import ResultMessage
        from claude_code_sdk.types import StreamEvent

        events = [
            StreamEvent(
                uuid="u1", session_id="s1",
                event={
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "thinking", "thinking": ""},
                },
                parent_tool_use_id=None,
            ),
            StreamEvent(
                uuid="u2", session_id="s1",
                event={
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "thinking_delta", "thinking": "Let me think..."},
                },
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success", duration_ms=0, duration_api_ms=0,
                is_error=False, num_turns=1, session_id="s1",
                total_cost_usd=None, usage=None, result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            chunks = []
            async for e in bridge.stream("sk", "hi", [], ""):
                chunks.append(e)
        # The reasoning_delta carries the content. Because no text_delta
        # was produced by the model, the bridge's safety net promotes the
        # reasoning text as a fallback text_delta so the platform chat
        # isn't left showing "no response".
        kinds = [e.kind for e in chunks]
        assert kinds[0] == "reasoning_delta"
        assert chunks[0].text == "Let me think..."
        assert "text_delta" in kinds  # fallback kicked in
        fallback = [e for e in chunks if e.kind == "text_delta"][0]
        assert "Let me think..." in fallback.text
        assert "no final text" in fallback.text

    @pytest.mark.asyncio
    async def test_no_duplicate_after_streamed_turn(self):
        """Final AssistantMessage must NOT re-emit blocks already streamed."""
        from claude_code_sdk import (
            AssistantMessage, ResultMessage, TextBlock, ToolUseBlock, ThinkingBlock, ToolResultBlock,
        )
        from claude_code_sdk.types import StreamEvent

        events = [
            # Streaming: thinking start + delta
            StreamEvent(
                uuid="u1", session_id="s1",
                event={
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "thinking", "thinking": ""},
                },
                parent_tool_use_id=None,
            ),
            StreamEvent(
                uuid="u2", session_id="s1",
                event={
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "thinking_delta", "thinking": "reasoning..."},
                },
                parent_tool_use_id=None,
            ),
            # Streaming: tool use announced
            StreamEvent(
                uuid="u3", session_id="s1",
                event={
                    "type": "content_block_start",
                    "index": 1,
                    "content_block": {"type": "tool_use", "id": "tu_1", "name": "Bash", "input": {}},
                },
                parent_tool_use_id=None,
            ),
            # Streaming: text content_block_start + delta
            StreamEvent(
                uuid="u3b", session_id="s1",
                event={
                    "type": "content_block_start",
                    "index": 3,
                    "content_block": {"type": "text", "text": ""},
                },
                parent_tool_use_id=None,
            ),
            StreamEvent(
                uuid="u4", session_id="s1",
                event={
                    "type": "content_block_delta",
                    "index": 3,
                    "delta": {"type": "text_delta", "text": "Done."},
                },
                parent_tool_use_id=None,
            ),
            # Final AssistantMessage with ALL 3 blocks — must NOT re-emit any
            AssistantMessage(
                content=[
                    ThinkingBlock(thinking="reasoning...", signature="sig_1"),  # index 0
                    ToolUseBlock(id="tu_1", name="Bash", input={}),  # index 1
                    ToolResultBlock(tool_use_id="tu_1", content="ok", is_error=False),  # index 2
                    TextBlock(text="Done."),  # index 3
                ],
                model="claude-sonnet-4-6",
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success", duration_ms=0, duration_api_ms=0,
                is_error=False, num_turns=1, session_id="s1",
                total_cost_usd=None, usage=None, result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            chunks = []
            async for e in bridge.stream("sk", "hi", [], ""):
                chunks.append(e)

        # Expected: reasoning_delta (content), tool_start, text_delta, tool_complete
        # Each appears exactly once; final AssistantMessage does NOT re-emit.
        kinds = [e.kind for e in chunks]
        assert kinds.count("reasoning_delta") == 1
        assert kinds.count("tool_start") == 1
        assert kinds.count("text_delta") == 1
        assert kinds.count("tool_complete") == 1
        # Text content
        text_events = [e for e in chunks if e.kind == "text_delta"]
        assert text_events[0].text == "Done."
        # Tool event payloads
        tool_start = [e for e in chunks if e.kind == "tool_start"][0]
        assert tool_start.tool_name == "Bash"
        assert tool_start.tool_id == "tu_1"
        tool_complete = [e for e in chunks if e.kind == "tool_complete"][0]
        assert tool_complete.tool_id == "tu_1"


class TestChat:
    @pytest.mark.asyncio
    async def test_chat_concatenates(self):
        from claude_code_sdk import AssistantMessage, ResultMessage, TextBlock

        events = [
            AssistantMessage(
                content=[TextBlock(text="foo"), TextBlock(text="bar")],
                model="claude-sonnet-4-6",
                parent_tool_use_id=None,
            ),
            ResultMessage(
                subtype="success",
                duration_ms=0,
                duration_api_ms=0,
                is_error=False,
                num_turns=1,
                session_id="sess_q",
                total_cost_usd=None,
                usage=None,
                result=None,
            ),
        ]

        bridge = ClaudeCodeSdkBridge()
        with patch("claude_code_sdk.query", side_effect=_make_fake_messages(events)):
            result = await bridge.chat("sk", "hi", [], "")
        assert result == "foobar"
        assert bridge.captured_cli_session_id == "sess_q"


# ---------------------------------------------------------------------------
# shutdown() — no-op
# ---------------------------------------------------------------------------


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_is_noop(self):
        bridge = ClaudeCodeSdkBridge()
        await bridge.shutdown()  # Should not raise

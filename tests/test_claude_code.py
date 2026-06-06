"""Tests for ClaudeCodeBridge."""

import json
import pytest
from unittest.mock import AsyncMock, patch

from agent_gateway.agents.claude_code import ClaudeCodeBridge
from agent_gateway.agents.base import CLITimeoutError, SubprocessConfig


class TestClaudeCodeBuildArgs:
    def test_basic_args(self):
        bridge = ClaudeCodeBridge()
        args = bridge._build_args("s:1", "hello", [], "")
        assert args[0] == "claude"
        assert "--print" in args
        assert "--output-format" in args
        assert "json" in args

    def test_model_arg(self):
        bridge = ClaudeCodeBridge(model="claude-sonnet-4-6")
        args = bridge._build_args("s:1", "hi", [], "")
        assert "--model" in args
        idx = args.index("--model")
        assert args[idx + 1] == "claude-sonnet-4-6"

    def test_extra_args(self):
        bridge = ClaudeCodeBridge(extra_args=["--flag", "value"])
        args = bridge._build_args("s:1", "hi", [], "")
        assert "--flag" in args
        assert "value" in args

    def test_max_turns(self):
        bridge = ClaudeCodeBridge()
        args = bridge._build_args("s:1", "hi", [], "")
        assert "--max-turns" in args


class TestClaudeCodeParseOutput:
    @pytest.mark.asyncio
    async def test_parse_json_result(self):
        bridge = ClaudeCodeBridge()
        output = json.dumps({"type": "result", "result": "Hello back!"})
        result = await bridge._parse_output(output, "s:1")
        assert result == "Hello back!"

    @pytest.mark.asyncio
    async def test_parse_json_content(self):
        bridge = ClaudeCodeBridge()
        output = json.dumps({"content": "Response text"})
        result = await bridge._parse_output(output, "s:1")
        assert result == "Response text"

    @pytest.mark.asyncio
    async def test_parse_json_content_list(self):
        bridge = ClaudeCodeBridge()
        output = json.dumps({"content": [
            {"type": "text", "text": "Hello"},
            {"type": "text", "text": "World"},
        ]})
        result = await bridge._parse_output(output, "s:1")
        assert "Hello" in result
        assert "World" in result

    @pytest.mark.asyncio
    async def test_parse_plain_text_fallback(self):
        bridge = ClaudeCodeBridge()
        result = await bridge._parse_output("Just plain text response", "s:1")
        assert result == "Just plain text response"

    @pytest.mark.asyncio
    async def test_parse_empty(self):
        bridge = ClaudeCodeBridge()
        result = await bridge._parse_output("", "s:1")
        assert result == ""

    @pytest.mark.asyncio
    async def test_parse_multiline_json(self):
        bridge = ClaudeCodeBridge()
        # Simulate multiple JSON lines (e.g. tool use + result)
        lines = [
            json.dumps({"type": "tool_use", "tool": "read"}),
            json.dumps({"type": "result", "result": "Final answer"}),
        ]
        result = await bridge._parse_output("\n".join(lines), "s:1")
        assert result == "Final answer"


class TestClaudeCodeHistoryFormat:
    def test_format_history_empty(self):
        bridge = ClaudeCodeBridge()
        assert bridge._format_history([]) == ""

    def test_format_history_conversation(self):
        bridge = ClaudeCodeBridge()
        history = [
            {"role": "user", "content": "Hi"},
            {"role": "assistant", "content": "Hello!"},
        ]
        result = bridge._format_history(history)
        assert "Human: Hi" in result
        assert "Assistant: Hello!" in result

    def test_format_prompt_simple(self):
        bridge = ClaudeCodeBridge()
        result = bridge._format_prompt("What is 2+2?", [], "")
        assert "What is 2+2?" in result

    def test_format_prompt_with_history_and_system(self):
        bridge = ClaudeCodeBridge()
        result = bridge._format_prompt(
            "Follow-up",
            [{"role": "user", "content": "First"}],
            "Be concise",
        )
        assert "Be concise" in result
        assert "First" in result
        assert "Follow-up" in result


class TestClaudeCodeStreamParsing:
    def test_parse_stream_line_text_delta(self):
        bridge = ClaudeCodeBridge()
        line = json.dumps({
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": "Hello"},
        })
        assert bridge._parse_stream_line(line) == "Hello"

    def test_parse_stream_line_plain_text(self):
        bridge = ClaudeCodeBridge()
        assert bridge._parse_stream_line("Just text") == "Just text"

    def test_parse_stream_line_empty(self):
        bridge = ClaudeCodeBridge()
        assert bridge._parse_stream_line("") == ""
        assert bridge._parse_stream_line("  ") == ""

    def test_parse_stream_line_no_text(self):
        bridge = ClaudeCodeBridge()
        line = json.dumps({"type": "message_start", "message": {}})
        assert bridge._parse_stream_line(line) == ""


class TestClaudeCodeChat:
    @pytest.mark.asyncio
    async def test_chat_success(self):
        bridge = ClaudeCodeBridge()
        mock_output = json.dumps({"type": "result", "result": "Hi there!"})
        with patch.object(bridge, "_run_subprocess", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = (mock_output, "", 0)
            result = await bridge.chat("s:1", "hello", [])
            assert result == "Hi there!"

    @pytest.mark.asyncio
    async def test_chat_timeout_returns_error(self):
        bridge = ClaudeCodeBridge(timeout=0.1)
        with patch.object(bridge, "_run_subprocess", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = CLITimeoutError(0.1, "claude")
            result = await bridge.chat("s:1", "hello", [])
            assert "timeout" in result.lower()

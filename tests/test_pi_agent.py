"""Tests for PiAgentBridge."""

import json
import pytest
from unittest.mock import AsyncMock, patch

from agent_gateway.agents.pi_agent import PiAgentBridge, _strip_ansi
from agent_gateway.agents.base import CLIConnectionError


class TestPiAgentPrintMode:
    def test_default_is_print_mode(self):
        bridge = PiAgentBridge()
        assert bridge.mode == "print"
        assert bridge._pool is None

    def test_default_command(self):
        bridge = PiAgentBridge()
        assert bridge.config.command == ["pi"]

    def test_custom_command(self):
        bridge = PiAgentBridge(command="/usr/local/bin/pi")
        assert bridge.config.command[0] == "/usr/local/bin/pi"

    def test_extra_args(self):
        bridge = PiAgentBridge(extra_args=["--verbose"])
        args = bridge._build_args("s:1", "hi", [], "")
        assert "--verbose" in args

    def test_build_args_print(self):
        bridge = PiAgentBridge(mode="print")
        args = bridge._build_args("s:1", "hello", [], "")
        assert args == ["pi", "--print"]

    def test_build_args_json(self):
        bridge = PiAgentBridge(mode="json")
        args = bridge._build_args("s:1", "hello", [], "")
        assert args == ["pi", "--mode", "json"]

    def test_build_args_rpc(self):
        bridge = PiAgentBridge(mode="rpc")
        args = bridge._build_args("s:1", "hello", [], "")
        assert "--mode" in args and "rpc" in args


class TestPiAgentRPCMode:
    def test_rpc_creates_pool(self):
        bridge = PiAgentBridge(mode="rpc")
        assert bridge.mode == "rpc"
        assert bridge._pool is not None

    def test_rpc_command(self):
        bridge = PiAgentBridge(mode="rpc")
        assert "--mode" in bridge.config.command
        assert "rpc" in bridge.config.command


class TestPiAgentParseOutput:
    @pytest.mark.asyncio
    async def test_parse_print_output(self):
        bridge = PiAgentBridge(mode="print")
        result = await bridge._parse_output("Hello from Pi!", "s:1")
        assert result == "Hello from Pi!"

    @pytest.mark.asyncio
    async def test_parse_print_strips_ansi(self):
        bridge = PiAgentBridge(mode="print")
        result = await bridge._parse_output("\x1b[32mHello\x1b[0m", "s:1")
        assert result == "Hello"

    @pytest.mark.asyncio
    async def test_parse_json_output_message_end(self):
        bridge = PiAgentBridge(mode="json")
        jsonl = "\n".join([
            '{"type":"session","version":3}',
            '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Hello world"}]}}',
        ])
        result = await bridge._parse_output(jsonl, "s:1")
        assert result == "Hello world"

    @pytest.mark.asyncio
    async def test_parse_json_output_empty(self):
        bridge = PiAgentBridge(mode="json")
        result = await bridge._parse_output("", "s:1")
        assert result == ""

    def test_parse_rpc_response_result(self):
        bridge = PiAgentBridge(mode="rpc")
        raw = json.dumps({"result": "Hello back!"})
        result = bridge._parse_rpc_response(raw)
        assert result == "Hello back!"

    def test_parse_rpc_response_error(self):
        bridge = PiAgentBridge(mode="rpc")
        raw = json.dumps({"error": {"message": "Something went wrong"}})
        result = bridge._parse_rpc_response(raw)
        assert "Something went wrong" in result

    def test_parse_rpc_response_plain_text(self):
        bridge = PiAgentBridge(mode="rpc")
        result = bridge._parse_rpc_response("plain text response")
        assert result == "plain text response"

    def test_parse_rpc_response_empty(self):
        bridge = PiAgentBridge(mode="rpc")
        result = bridge._parse_rpc_response("")
        assert result == ""


class TestPiAgentJsonStreaming:
    def test_parse_json_stream_line_text_delta(self):
        bridge = PiAgentBridge(mode="json")
        line = json.dumps({
            "type": "message_update",
            "assistantMessageEvent": {"type": "text_delta", "delta": "Hello"},
        })
        assert bridge._parse_json_stream_line(line) == "Hello"

    def test_parse_json_stream_line_empty(self):
        bridge = PiAgentBridge(mode="json")
        assert bridge._parse_json_stream_line("") == ""
        assert bridge._parse_json_stream_line("  ") == ""

    def test_parse_json_stream_line_non_json(self):
        bridge = PiAgentBridge(mode="json")
        assert bridge._parse_json_stream_line("plain text") == "plain text"

    def test_parse_json_stream_line_session_event(self):
        bridge = PiAgentBridge(mode="json")
        line = json.dumps({"type": "session", "version": 3})
        assert bridge._parse_json_stream_line(line) == ""

    def test_parse_json_stream_line_message_end(self):
        bridge = PiAgentBridge(mode="json")
        line = json.dumps({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Final answer"}],
            },
        })
        assert bridge._parse_json_stream_line(line) == "Final answer"


class TestPiAgentPromptFormat:
    def test_format_prompt_print_mode_with_history(self):
        bridge = PiAgentBridge(mode="print")
        result = bridge._format_prompt(
            "message",
            [{"role": "user", "content": "prev"}],
            "system instructions",
        )
        assert "system instructions" in result
        assert "prev" in result
        assert "message" in result

    def test_format_prompt_rpc_mode_just_message(self):
        bridge = PiAgentBridge(mode="rpc")
        result = bridge._format_prompt("hello", [], "")
        assert result == "hello"


class TestPiAgentChat:
    @pytest.mark.asyncio
    async def test_chat_print_mode_with_mock(self):
        bridge = PiAgentBridge(mode="print")
        with patch.object(bridge, "_run_subprocess", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = ("Hello from Pi!", "", 0)
            result = await bridge.chat("s:1", "hello", [])
            assert result == "Hello from Pi!"

    @pytest.mark.asyncio
    async def test_chat_rpc_mode_success(self):
        bridge = PiAgentBridge(mode="rpc")

        async def mock_send_recv(session_key, input_data, **kwargs):
            return json.dumps({"result": "Hello from Pi!"})

        bridge._pool.send_and_recv = mock_send_recv
        bridge._pool.get_or_create = AsyncMock()

        result = await bridge.chat("s:1", "hello", [])
        assert result == "Hello from Pi!"

    @pytest.mark.asyncio
    async def test_chat_rpc_connection_error_retries(self):
        bridge = PiAgentBridge(mode="rpc")

        call_count = 0

        async def mock_send_recv(session_key, input_data, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise CLIConnectionError("s:1", "Connection lost")
            return json.dumps({"result": "Retried successfully!"})

        bridge._pool.send_and_recv = mock_send_recv
        bridge._pool.get_or_create = AsyncMock()
        bridge._pool.terminate = AsyncMock()

        result = await bridge.chat("s:1", "hello", [])
        assert result == "Retried successfully!"
        assert call_count == 2


class TestStripAnsi:
    def test_basic(self):
        assert _strip_ansi("\x1b[32mHello\x1b[0m") == "Hello"

    def test_no_ansi(self):
        assert _strip_ansi("plain text") == "plain text"

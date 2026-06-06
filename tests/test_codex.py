"""Tests for CodexBridge."""

import pytest
from unittest.mock import AsyncMock, patch

from agent_gateway.agents.codex import CodexBridge, _ANSI_RE
from agent_gateway.agents.base import CLITimeoutError


class TestCodexBuildArgs:
    def test_basic_args(self):
        bridge = CodexBridge()
        args = bridge._build_args("s:1", "hello", [], "")
        assert args[0] == "codex"
        assert "--quiet" in args

    def test_model_arg(self):
        bridge = CodexBridge(model="codex-mini")
        args = bridge._build_args("s:1", "hi", [], "")
        assert "--model" in args

    def test_extra_args(self):
        bridge = CodexBridge(extra_args=["--flag"])
        args = bridge._build_args("s:1", "hi", [], "")
        assert "--flag" in args


class TestCodexParseOutput:
    @pytest.mark.asyncio
    async def test_strips_ansi(self):
        bridge = CodexBridge()
        ansi_text = "\x1b[32mHello\x1b[0m \x1b[1mWorld\x1b[0m"
        result = await bridge._parse_output(ansi_text, "s:1")
        assert result == "Hello World"

    @pytest.mark.asyncio
    async def test_strips_trailing_whitespace(self):
        bridge = CodexBridge()
        result = await bridge._parse_output("  hello  \n\n", "s:1")
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_plain_text(self):
        bridge = CodexBridge()
        result = await bridge._parse_output("Just text", "s:1")
        assert result == "Just text"


class TestANSIRegex:
    def test_basic_color(self):
        assert _ANSI_RE.sub("", "\x1b[32mgreen\x1b[0m") == "green"

    def test_bold(self):
        assert _ANSI_RE.sub("", "\x1b[1mbold\x1b[0m") == "bold"

    def test_no_ansi(self):
        assert _ANSI_RE.sub("", "plain text") == "plain text"

    def test_complex(self):
        text = "\x1b[38;5;196mRed\x1b[0m and \x1b[1;32mBold Green\x1b[0m"
        assert _ANSI_RE.sub("", text) == "Red and Bold Green"


class TestCodexChat:
    @pytest.mark.asyncio
    async def test_chat_success(self):
        bridge = CodexBridge()
        with patch.object(bridge, "_run_subprocess", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = ("Hello!", "", 0)
            result = await bridge.chat("s:1", "hi", [])
            assert result == "Hello!"

    @pytest.mark.asyncio
    async def test_chat_strips_ansi(self):
        bridge = CodexBridge()
        with patch.object(bridge, "_run_subprocess", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = ("\x1b[32mResponse\x1b[0m", "", 0)
            result = await bridge.chat("s:1", "hi", [])
            assert result == "Response"

    @pytest.mark.asyncio
    async def test_chat_timeout(self):
        bridge = CodexBridge(timeout=0.1)
        with patch.object(bridge, "_run_subprocess", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = CLITimeoutError(0.1, "codex")
            result = await bridge.chat("s:1", "hi", [])
            assert "timeout" in result.lower()

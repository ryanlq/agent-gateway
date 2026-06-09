"""
Claude Code CLI bridge.

Wraps the ``claude`` CLI tool (``claude --print``) into the gateway's
agent interface.  Each invocation is stateless — history is reconstructed
in the prompt.

Supports:
  - Structured output via ``--output-format json``
  - Streaming via ``--output-format stream-json``
  - Model selection via ``--model``
  - Custom system prompts via ``--system-prompt``

Requirements::

    # Claude Code CLI must be installed
    # See: https://docs.anthropic.com/en/docs/claude-code
"""

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator

from agent_gateway.agents.base import (
    CLIAgentBridge,
    CLIParseError,
    SubprocessConfig,
)

logger = logging.getLogger(__name__)


class ClaudeCodeBridge(CLIAgentBridge):
    """
    Adapter for the Claude Code CLI (``claude --print``).

    Usage::

        bridge = ClaudeCodeBridge(model="claude-sonnet-4-6")
        runner = GatewayRunner(config, agent=bridge)
    """

    def __init__(
        self,
        *,
        model: str | None = None,
        max_turns: int = 10,
        timeout: float = 120.0,
        max_output_bytes: int = 2_000_000,
        extra_args: list[str] | None = None,
        command: str = "claude",
        bare: bool = False,
        reasoning: str | None = None,
    ) -> None:
        config = SubprocessConfig(
            command=[command],
            timeout=timeout,
            max_output_bytes=max_output_bytes,
        )
        super().__init__(config)
        self.model = model
        self.max_turns = max_turns
        self.extra_args = extra_args or []
        self.command = command
        self.bare = bare
        self.reasoning = reasoning

    # -- Reasoning effort ---------------------------------------------------

    def _effort_args(self) -> list[str]:
        """Return ``--effort`` flags when reasoning is configured."""
        if self.reasoning and self.reasoning != "none":
            return ["--effort", self.reasoning]
        return []

    # -- CLIAgentBridge overrides ------------------------------------------

    def _build_args(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
        *,
        session_ref: str | None = None,
    ) -> list[str]:
        """Build ``claude --print`` command arguments."""
        args = [self.command, "--print", "--output-format", "json"]

        if self.model:
            args.extend(["--model", self.model])

        # Allow enough turns for Claude to use tools (read files, etc.)
        args.extend(["--max-turns", str(self.max_turns)])

        # Pass session ref for CLI-level session continuity
        if session_ref:
            args.extend(["--session-id", session_ref])

        # Reasoning effort level
        args.extend(self._effort_args())

        # Bare mode: skip hooks, plugins, tools, CLAUDE.md auto-discovery
        if self.bare:
            args.extend(["--bare", "--disable-slash-commands", "--tools", ""])

        # Add any extra user-provided args
        args.extend(self.extra_args)

        return args

    async def _parse_output(self, raw_stdout: str, session_key: str) -> str:
        """Parse JSON output from ``claude --output-format json``.

        Expected envelope::

            {"type": "result", "result": "response text", ...}

        Falls back to raw text if JSON parsing fails.
        """
        if not raw_stdout or not raw_stdout.strip():
            return ""

        # The CLI may output multiple JSON lines; take the last one
        # that looks like a result envelope.
        lines = raw_stdout.strip().splitlines()
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Handle various envelope formats
            if isinstance(data, dict):
                # Standard result envelope
                if "result" in data:
                    return str(data["result"])
                # Some versions use "content" or "text"
                if "content" in data:
                    content = data["content"]
                    if isinstance(content, list):
                        texts = [
                            b.get("text", "") for b in content
                            if isinstance(b, dict) and b.get("type") == "text"
                        ]
                        return "\n".join(texts)
                    return str(content)
                if "text" in data:
                    return str(data["text"])

        # Fallback: return raw text (might be plain-text output)
        return raw_stdout.strip()

    # -- Streaming override -------------------------------------------------

    async def stream(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        session_ref: str | None = None,
    ) -> AsyncIterator[str]:
        """Stream output using ``--output-format stream-json``.

        Yields text deltas parsed from JSONL events.
        Requires ``--verbose`` for stream-json output.
        """
        args = [self.command, "--print", "--output-format", "stream-json", "--verbose"]

        if self.model:
            args.extend(["--model", self.model])

        args.extend(["--max-turns", str(self.max_turns)])

        if session_ref:
            args.extend(["--session-id", session_ref])

        # Reasoning effort level
        args.extend(self._effort_args())

        # Bare mode: skip hooks, plugins, tools, CLAUDE.md auto-discovery
        if self.bare:
            args.extend(["--bare", "--disable-slash-commands", "--tools", ""])

        args.extend(self.extra_args)

        prompt = self._format_prompt(message, history, system_extra)

        async for line in self._run_subprocess_streaming(args, input_text=prompt):
            # Try to parse as JSONL
            text = self._parse_stream_line(line)
            if text:
                yield text

    def _parse_stream_line(self, line: str) -> str:
        """Parse a single stream-json line, return text delta or empty.

        Handles the real ``claude --print --output-format stream-json --verbose``
        output format observed in Claude Code v2.1+:

          - ``{"type":"system",...}`` — init / hook events (skipped)
          - ``{"type":"assistant","message":{"content":[{"type":"text","text":"..."}]}}``
          - ``{"type":"result","result":"..."}`` — final result
        """
        if not line or not line.strip():
            return ""
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            # Non-JSON line — yield as plain text
            return line

        if not isinstance(data, dict):
            return ""

        event_type = data.get("type", "")

        # Result envelope — skip; the full text was already yielded via
        # assistant / content_block_delta events above.
        if event_type == "result":
            return ""

        # Assistant message — extract text from content blocks
        if event_type == "assistant":
            msg = data.get("message", {})
            if isinstance(msg, dict):
                content = msg.get("content", [])
                if isinstance(content, list):
                    texts = [
                        b.get("text", "")
                        for b in content
                        if isinstance(b, dict) and b.get("type") == "text"
                    ]
                    return "\n".join(t for t in texts if t)
            return ""

        # Anthropic-style content_block_delta
        if event_type == "content_block_delta":
            delta = data.get("delta", {})
            if isinstance(delta, dict):
                return delta.get("text", "")

        # System / other events — skip
        return ""

    # -- History formatting -------------------------------------------------

    def _format_history(self, history: list[dict[str, Any]]) -> str:
        """Format history for Claude Code's expected input.

        Uses Human/Assistant labels matching Claude's convention.
        """
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

    def _format_prompt(
        self,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> str:
        """Build the full prompt for Claude Code."""
        blocks: list[str] = []

        if system_extra:
            blocks.append(f"System instructions: {system_extra}")

        history_text = self._format_history(history)
        if history_text:
            blocks.append("Previous conversation:\n" + history_text)

        blocks.append(message)

        return "\n\n".join(blocks)

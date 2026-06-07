"""
OpenAI Codex CLI bridge.

Wraps the ``codex`` CLI tool into the gateway's agent interface.
Stateless per invocation — history is reconstructed in the prompt.

Requirements::

    # Codex CLI must be installed
    # See: https://github.com/openai/codex
"""

from __future__ import annotations

import re
import logging
from typing import Any, AsyncIterator

from agent_gateway.agents.base import (
    CLIAgentBridge,
    SubprocessConfig,
)

logger = logging.getLogger(__name__)

# ANSI escape code regex
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07|\x1b\[.*?[a-zA-Z]")


class CodexBridge(CLIAgentBridge):
    """
    Adapter for the OpenAI Codex CLI (``codex``).

    Usage::

        bridge = CodexBridge(model="codex-mini")
        runner = GatewayRunner(config, agent=bridge)
    """

    def __init__(
        self,
        *,
        model: str | None = None,
        approval_mode: str | None = None,
        timeout: float = 120.0,
        max_output_bytes: int = 2_000_000,
        extra_args: list[str] | None = None,
        command: str = "codex",
    ) -> None:
        config = SubprocessConfig(
            command=[command],
            timeout=timeout,
            max_output_bytes=max_output_bytes,
        )
        super().__init__(config)
        self.model = model
        self.approval_mode = approval_mode
        self.extra_args = extra_args or []
        self.command = command

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
        """Build ``codex`` command arguments."""
        args = [self.command, "--quiet"]

        if self.model:
            args.extend(["--model", self.model])

        if self.approval_mode:
            args.extend(["--approval-mode", self.approval_mode])

        # Codex does not support session flags — session_ref is ignored.

        args.extend(self.extra_args)

        # The prompt is passed via stdin
        return args

    async def _parse_output(self, raw_stdout: str, session_key: str) -> str:
        """Strip ANSI escape codes and trailing whitespace from codex output."""
        cleaned = _ANSI_RE.sub("", raw_stdout)
        return cleaned.strip()

    # -- Streaming override -------------------------------------------------

    async def stream(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        session_ref: str | None = None,
    ) -> AsyncIterator[str]:
        """Stream output from codex, stripping ANSI codes per line."""
        args = self._build_args(session_key, message, history, system_extra, session_ref=session_ref)
        prompt = self._format_prompt(message, history, system_extra)

        async for line in self._run_subprocess_streaming(args, input_text=prompt):
            cleaned = _ANSI_RE.sub("", line).strip()
            if cleaned:
                yield cleaned

    # -- History formatting -------------------------------------------------

    def _format_prompt(
        self,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> str:
        """Build prompt for Codex. Keeps it simple — the message itself."""
        blocks: list[str] = []

        if system_extra:
            blocks.append(system_extra)

        history_text = self._format_history(history)
        if history_text:
            blocks.append("Previous conversation:\n" + history_text)

        blocks.append(message)

        return "\n\n".join(blocks)

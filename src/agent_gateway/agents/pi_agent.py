"""
Pi Agent CLI bridge.

Wraps the Pi Agent CLI into the gateway's agent interface.  Supports three
modes controlled by ``mode`` parameter:

  - ``"print"`` (default) — Uses ``pi --print`` for simple text-in/text-out.
    Stateless per invocation, reliable.
  - ``"json"`` — Uses ``pi --mode json`` for structured JSONL streaming.
    Parses ``text_delta`` events for incremental output.
  - ``"rpc"`` — Uses ``pi --mode rpc`` for stateful JSON-RPC sessions.
    Persistent per-session subprocess via ``SubprocessPool``.

Requirements::

    # Pi Agent CLI must be installed
    # See: https://github.com/NousResearch/pi-agent
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, AsyncIterator

from agent_gateway.agents.base import (
    CLIAgentBridge,
    CLIConnectionError,
    CLITimeoutError,
    SubprocessConfig,
    SubprocessPool,
)

logger = logging.getLogger(__name__)


class PiAgentBridge(CLIAgentBridge):
    """
    Adapter for Pi Agent CLI.

    Usage::

        # Default: simple --print mode
        bridge = PiAgentBridge()
        runner = GatewayRunner(config, agent=bridge)

        # JSON streaming mode
        bridge = PiAgentBridge(mode="json")

        # RPC mode (stateful, per-session subprocesses)
        bridge = PiAgentBridge(mode="rpc", idle_timeout=300)
    """

    def __init__(
        self,
        *,
        command: str = "pi",
        mode: str = "print",
        timeout: float = 120.0,
        max_output_bytes: int = 2_000_000,
        idle_timeout: float = 300.0,
        max_concurrent: int = 10,
        extra_args: list[str] | None = None,
    ) -> None:
        config = SubprocessConfig(
            command=[command],
            timeout=timeout,
            max_output_bytes=max_output_bytes,
            idle_timeout=idle_timeout,
            max_concurrent=max_concurrent,
        )
        super().__init__(config)
        self.command = command
        self.mode = mode
        self.extra_args = extra_args or []
        self._pool: SubprocessPool | None = None

        if mode == "rpc":
            config.command = [command, "--mode", "rpc"] + self.extra_args
            self._pool = SubprocessPool(config)

    # -- CLIAgentBridge overrides ------------------------------------------

    def _build_args(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> list[str]:
        """Build CLI args based on mode."""
        if self.mode == "print":
            args = [self.command, "--print"]
            args.extend(self.extra_args)
            return args
        elif self.mode == "json":
            args = [self.command, "--mode", "json"]
            args.extend(self.extra_args)
            return args
        else:  # rpc
            return self.config.command

    async def _parse_output(self, raw_stdout: str, session_key: str) -> str:
        """Parse output based on mode."""
        if self.mode == "print":
            # Simple text output
            return _strip_ansi(raw_stdout.strip())
        elif self.mode == "json":
            # JSONL — extract the last assistant text content
            return self._extract_text_from_jsonl(raw_stdout)
        else:  # rpc
            return self._parse_rpc_response(raw_stdout)

    def _extract_text_from_jsonl(self, raw_stdout: str) -> str:
        """Extract the final assistant text from a ``--mode json`` JSONL stream.

        Scans all lines for ``message_end`` events with assistant content blocks.
        Falls back to the last ``text`` field found.
        """
        if not raw_stdout or not raw_stdout.strip():
            return ""

        last_text = ""
        for line in raw_stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(data, dict):
                continue

            event_type = data.get("type", "")

            # message_end carries the final assistant content
            if event_type == "message_end":
                msg = data.get("message", {})
                if isinstance(msg, dict) and msg.get("role") == "assistant":
                    content = msg.get("content", [])
                    if isinstance(content, list):
                        texts = [
                            b.get("text", "")
                            for b in content
                            if isinstance(b, dict) and b.get("type") == "text"
                        ]
                        combined = "\n".join(t for t in texts if t)
                        if combined:
                            last_text = combined

            # Also check message_update for text content
            if event_type == "message_update":
                ame = data.get("assistantMessageEvent", {})
                if isinstance(ame, dict) and ame.get("type") == "text_delta":
                    last_text = ame.get("delta", "")

        return last_text or raw_stdout.strip()

    # -- GatewayRunner interface -------------------------------------------

    async def chat(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
    ) -> str:
        """Send a prompt and return the response."""
        if self.mode == "rpc" and self._pool:
            return await self._chat_rpc(session_key, message, history, system_extra)
        # print / json modes: use base class (spawn + collect)
        return await super().chat(session_key, message, history, system_extra)

    async def stream(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
    ) -> AsyncIterator[str]:
        """Stream response chunks."""
        if self.mode == "json":
            async for chunk in self._stream_json(session_key, message, history, system_extra):
                yield chunk
        elif self.mode == "rpc" and self._pool:
            async for chunk in self._stream_rpc(session_key, message, history, system_extra):
                yield chunk
        else:
            # print mode: use base streaming (line-by-line stdout)
            args = self._build_args(session_key, message, history, system_extra)
            prompt = self._format_prompt(message, history, system_extra)
            async for line in self._run_subprocess_streaming(args, input_text=prompt):
                cleaned = _strip_ansi(line).strip()
                if cleaned:
                    yield cleaned

    # -- JSON mode streaming -----------------------------------------------

    async def _stream_json(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> AsyncIterator[str]:
        """Stream using ``pi --mode json``, parsing JSONL events."""
        args = [self.command, "--mode", "json"]
        args.extend(self.extra_args)
        prompt = self._format_prompt(message, history, system_extra)

        async for line in self._run_subprocess_streaming(args, input_text=prompt):
            text = self._parse_json_stream_line(line)
            if text:
                yield text

    def _parse_json_stream_line(self, line: str) -> str:
        """Parse a single ``--mode json`` JSONL event.

        Pi Agent JSON format uses events like:
          - ``{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"..."}}``
          - ``{"type":"turn_end",...}``
          - ``{"type":"message_end","message":{"role":"assistant","content":[...]}}``
        """
        if not line or not line.strip():
            return ""
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            return _strip_ansi(line)

        event_type = data.get("type", "")

        # text_delta events carry incremental text
        if event_type == "message_update":
            ame = data.get("assistantMessageEvent", {})
            if isinstance(ame, dict):
                inner_type = ame.get("type", "")
                if inner_type == "text_delta":
                    return ame.get("delta", "")

        # message_end contains the final content
        if event_type == "message_end":
            msg = data.get("message", {})
            if isinstance(msg, dict):
                content = msg.get("content", [])
                if isinstance(content, list):
                    texts = []
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            texts.append(block.get("text", ""))
                    return "\n".join(texts)

        return ""

    # -- RPC mode ----------------------------------------------------------

    async def _chat_rpc(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> str:
        """Send a prompt via RPC and return the response."""
        prompt = self._format_prompt(message, history, system_extra)
        request = {"type": "prompt", "text": prompt}

        try:
            raw = await self._pool.send_and_recv(session_key, json.dumps(request))
            return self._parse_rpc_response(raw)
        except CLITimeoutError as exc:
            return f"⚠️ Agent timeout: {exc}"
        except CLIConnectionError as exc:
            logger.warning("RPC connection error, retrying: %s", exc)
            await self._pool.terminate(session_key)
            try:
                raw = await self._pool.send_and_recv(session_key, json.dumps(request))
                return self._parse_rpc_response(raw)
            except Exception as retry_exc:
                return f"⚠️ Agent error after retry: {retry_exc}"
        except Exception as exc:
            return f"⚠️ Agent error: {exc}"

    async def _stream_rpc(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> AsyncIterator[str]:
        """Stream response chunks via RPC."""
        if not self._pool:
            return

        prompt = self._format_prompt(message, history, system_extra)
        request = json.dumps({"type": "prompt", "text": prompt})

        try:
            pp = await self._pool.get_or_create(session_key)
            async with pp.reader_lock:
                pp.process.stdin.write((request + "\n").encode())
                await pp.process.stdin.drain()

                while True:
                    try:
                        raw = await asyncio.wait_for(
                            pp.process.stdout.readline(),
                            timeout=self.config.timeout,
                        )
                    except asyncio.TimeoutError:
                        break
                    if not raw:
                        break
                    line = raw.decode().rstrip("\n")
                    if not line:
                        continue
                    try:
                        resp = json.loads(line)
                    except json.JSONDecodeError:
                        yield line
                        continue
                    if resp.get("type") in ("done", "turn_end"):
                        break
                    text = resp.get("result", "") or resp.get("text", "") or resp.get("delta", "")
                    if text:
                        yield text
            pp.touch()
        except Exception as exc:
            logger.error("Pi Agent RPC stream error: %s", exc)
            yield f"⚠️ Stream error: {exc}"

    def _parse_rpc_response(self, raw: str) -> str:
        """Parse an RPC response envelope."""
        if not raw or not raw.strip():
            return ""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return raw.strip()

        if isinstance(data, dict):
            if "result" in data:
                r = data["result"]
                return r if isinstance(r, str) else str(r)
            if "error" in data:
                err = data["error"]
                return f"Error: {err.get('message', err) if isinstance(err, dict) else err}"
        return raw.strip()

    # -- Prompt formatting --------------------------------------------------

    def _format_prompt(
        self,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> str:
        """Build prompt — for --print mode, just the message."""
        if self.mode == "print":
            blocks: list[str] = []
            if system_extra:
                blocks.append(system_extra)
            history_text = self._format_history(history)
            if history_text:
                blocks.append("Previous conversation:\n" + history_text)
            blocks.append(message)
            return "\n\n".join(blocks)
        return message

    # -- Cleanup ------------------------------------------------------------

    async def shutdown(self) -> None:
        """Clean up pooled subprocesses."""
        if self._pool:
            count = await self._pool.terminate_all()
            if count:
                logger.info("Shut down %d Pi Agent processes", count)


# -- Utility ---------------------------------------------------------------

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07|\x1b\[.*?[a-zA-Z]")


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return _ANSI_RE.sub("", text)

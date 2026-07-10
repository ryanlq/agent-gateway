"""
CLI Agent Bridge — base classes, subprocess pool, and exception hierarchy.

Provides the infrastructure for wrapping subprocess-based CLI agent tools
(Claude Code, Pi Agent) into the async callable interface
that ``GatewayRunner`` expects.

Usage::

    from agent_gateway.agents import ClaudeCodeSdkBridge

    bridge = ClaudeCodeSdkBridge()
    runner = GatewayRunner(config, agent=bridge)
    # or: runner = GatewayRunner(config, agent_callback=bridge.as_callback())
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CLIAgentError(Exception):
    """Base exception for CLI agent errors."""


class CLITimeoutError(CLIAgentError):
    """Subprocess exceeded the configured timeout."""

    def __init__(self, timeout: float, command: str) -> None:
        self.timeout = timeout
        self.command = command
        super().__init__(f"CLI '{command}' timed out after {timeout:.1f}s")


class CLICrashError(CLIAgentError):
    """Subprocess exited with non-zero return code."""

    def __init__(self, returncode: int, stderr: str, command: str) -> None:
        self.returncode = returncode
        self.stderr = stderr
        self.command = command
        super().__init__(f"CLI '{command}' crashed (exit {returncode}): {stderr[:200]}")


class CLIOutputTooLargeError(CLIAgentError):
    """Subprocess output exceeded max_output_bytes."""

    def __init__(self, size: int, limit: int) -> None:
        self.size = size
        self.limit = limit
        super().__init__(f"CLI output too large ({size} > {limit} bytes)")


class CLIParseError(CLIAgentError):
    """Failed to parse structured output from the CLI tool."""

    def __init__(self, raw_output: str, detail: str) -> None:
        self.raw_output = raw_output
        super().__init__(f"CLI parse error: {detail}")


class CLIConnectionError(CLIAgentError):
    """Failed to communicate with a pooled persistent process."""

    def __init__(self, session_key: str, detail: str) -> None:
        self.session_key = session_key
        super().__init__(f"CLI connection error for '{session_key}': {detail}")


# ---------------------------------------------------------------------------
# SubprocessConfig
# ---------------------------------------------------------------------------


@dataclass
class SubprocessConfig:
    """Configuration for CLI subprocess management."""

    command: list[str]
    """CLI command and fixed arguments, e.g. ``["claude", "--print"]``."""

    timeout: float | None = None
    """Max seconds per invocation. ``None`` means no limit."""

    max_output_bytes: int = 1_000_000
    """Max stdout bytes to capture (prevents runaway output)."""

    idle_timeout: float = 300.0
    """Seconds before idle pooled processes are cleaned up."""

    max_concurrent: int = 10
    """Max simultaneous pooled processes."""

    env: dict[str, str] | None = None
    """Extra environment variables (merged with os.environ)."""

    cwd: str | None = None
    """Working directory for the subprocess."""


# ---------------------------------------------------------------------------
# PooledProcess — tracks a persistent subprocess
# ---------------------------------------------------------------------------


@dataclass
class PooledProcess:
    """A persistent subprocess tracked in the pool."""

    session_key: str
    process: asyncio.subprocess.Process
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    reader_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def touch(self) -> None:
        self.last_active = time.time()

    def is_idle(self, max_idle: float) -> bool:
        return (time.time() - self.last_active) > max_idle


# ---------------------------------------------------------------------------
# SubprocessPool
# ---------------------------------------------------------------------------


class SubprocessPool:
    """
    Manages per-session persistent subprocess instances.

    Used by stateful CLI tools (e.g. Pi Agent RPC mode) that maintain a
    conversation over a long-lived process.  Stateless tools (Claude Code)
    use ``CLIAgentBridge._run_subprocess`` directly instead.
    """

    def __init__(self, config: SubprocessConfig) -> None:
        self.config = config
        self._pool: dict[str, PooledProcess] = {}

    # -- Lifecycle -----------------------------------------------------------

    async def get_or_create(self, session_key: str) -> PooledProcess:
        """Get an existing process or spawn a new one for *session_key*."""
        if session_key in self._pool:
            pp = self._pool[session_key]
            if pp.process.returncode is None:
                pp.touch()
                return pp
            # Dead process — remove and recreate
            del self._pool[session_key]

        if len(self._pool) >= self.config.max_concurrent:
            # Evict the most idle process
            oldest_key = min(self._pool, key=lambda k: self._pool[k].last_active)
            await self.terminate(oldest_key)

        process = await self._spawn()
        pp = PooledProcess(session_key=session_key, process=process)
        self._pool[session_key] = pp
        logger.debug("Spawned pooled process for %s (pid %s)", session_key, process.pid)
        return pp

    async def send_and_recv(
        self,
        session_key: str,
        input_data: str,
        *,
        timeout: float | None = None,
    ) -> str:
        """Send *input_data* to a pooled process and read one response line."""
        pp = await self.get_or_create(session_key)

        async with pp.reader_lock:
            try:
                pp.process.stdin.write((input_data + "\n").encode())
                await pp.process.stdin.drain()
            except Exception as exc:
                raise CLIConnectionError(session_key, f"Write error: {exc}") from exc

            try:
                raw = await asyncio.wait_for(
                    pp.process.stdout.readline(),
                    timeout=timeout or self.config.timeout,
                )
            except asyncio.TimeoutError:
                raise CLITimeoutError(timeout or self.config.timeout, self.config.command[0])

            if not raw:
                raise CLIConnectionError(session_key, "Process closed stdout")

            pp.touch()
            line = raw.decode().rstrip("\n")

            if len(line.encode()) > self.config.max_output_bytes:
                raise CLIOutputTooLargeError(len(line.encode()), self.config.max_output_bytes)

            return line

    async def terminate(self, session_key: str) -> bool:
        """Terminate the process for *session_key*. Returns True if it existed."""
        pp = self._pool.pop(session_key, None)
        if pp is None:
            return False
        return await self._kill(pp)

    async def terminate_all(self) -> int:
        """Terminate all pooled processes. Returns count terminated."""
        count = 0
        for key in list(self._pool):
            if await self.terminate(key):
                count += 1
        return count

    async def cleanup_idle(self) -> int:
        """Terminate processes idle beyond ``idle_timeout``. Returns count."""
        expired = [key for key, pp in self._pool.items() if pp.is_idle(self.config.idle_timeout)]
        for key in expired:
            await self.terminate(key)
        return len(expired)

    def terminate_all_sync(self) -> int:
        """Force-kill all pooled processes synchronously (last-resort cleanup)."""
        count = 0
        for key in list(self._pool):
            pp = self._pool.pop(key, None)
            if pp is None:
                continue
            try:
                pp.process.kill()
                count += 1
            except ProcessLookupError:
                pass
            except Exception:
                pass
        return count

    # -- Properties ----------------------------------------------------------

    @property
    def active_count(self) -> int:
        return len(self._pool)

    @property
    def session_keys(self) -> list[str]:
        return list(self._pool.keys())

    # -- Internal ------------------------------------------------------------

    async def _spawn(self) -> asyncio.subprocess.Process:
        """Spawn a new subprocess."""
        env = dict(os.environ)
        if self.config.env:
            env.update(self.config.env)

        return await asyncio.create_subprocess_exec(
            *self.config.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=self.config.cwd,
        )

    @staticmethod
    async def _kill(pp: PooledProcess) -> bool:
        """Kill a pooled process."""
        try:
            pp.process.terminate()
            try:
                await asyncio.wait_for(pp.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pp.process.kill()
                await pp.process.wait()
            return True
        except ProcessLookupError:
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# CLIAgentBridge — abstract base for all CLI agent bridges
# ---------------------------------------------------------------------------


class CLIAgentBridge(ABC):
    """
    Abstract base class for wrapping CLI agent tools into the gateway's
    agent interface.

    Subclasses implement:
      - ``_build_args(session_key, message, history, system_extra) -> list[str]``
      - ``_parse_output(raw_stdout, session_key) -> str``

    Optional overrides:
      - ``_format_history(history) -> str``
      - ``_format_prompt(message, history, system_extra) -> str``

    The resulting object can be passed to ``GatewayRunner`` as the
    ``agent`` parameter (implements ``chat()`` and ``stream()``).
    """

    def __init__(self, config: SubprocessConfig) -> None:
        self.config = config
        self._pool: SubprocessPool | None = None  # Only for stateful tools
        self._logger = logging.getLogger(self.__class__.__name__)

    # -- GatewayRunner interface -------------------------------------------

    async def chat(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        session_ref: str | None = None,
    ) -> str:
        """Agent interface called by ``GatewayRunner._invoke_agent()``.

        Spawns a subprocess, sends the formatted prompt, and returns
        the parsed response string.
        """
        args = self._build_args(
            session_key, message, history, system_extra, session_ref=session_ref
        )
        prompt = self._format_prompt(message, history, system_extra)

        try:
            stdout, stderr, returncode = await self._run_subprocess(
                args,
                input_text=prompt,
            )
        except CLITimeoutError as exc:
            self._logger.error("Timeout: %s", exc)
            return f"⚠️ Agent timeout: {exc}"
        except CLICrashError as exc:
            self._logger.error("Crash: %s", exc)
            return f"⚠️ Agent error (exit {exc.returncode}): {exc.stderr[:200]}"
        except CLIOutputTooLargeError as exc:
            self._logger.error("Output too large: %s", exc)
            return f"⚠️ Agent output exceeded limit ({exc.limit} bytes)"
        except CLIConnectionError as exc:
            self._logger.error("Connection error: %s", exc)
            return f"⚠️ Agent connection error: {exc}"

        if returncode != 0:
            self._logger.error("Non-zero exit %d: %s", returncode, stderr[:200])
            return f"⚠️ Agent error (exit {returncode}): {stderr[:200]}"

        try:
            return await self._parse_output(stdout, session_key)
        except CLIParseError as exc:
            self._logger.warning("Parse error, returning raw output: %s", exc)
            return stdout.strip() if stdout.strip() else "⚠️ Agent returned empty response."

    async def stream(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        session_ref: str | None = None,
    ) -> AsyncIterator[str]:
        """Streaming interface called by ``GatewayRunner._call_agent_streaming()``.

        Yields incremental text chunks from subprocess stdout.
        """
        args = self._build_args(
            session_key, message, history, system_extra, session_ref=session_ref
        )
        prompt = self._format_prompt(message, history, system_extra)

        async for chunk in self._run_subprocess_streaming(args, input_text=prompt):
            yield chunk

    def as_callback(self) -> Callable[..., Awaitable[str]]:
        """Return an async callable for ``GatewayRunner(agent_callback=...)``."""

        async def _callback(**kwargs: Any) -> str:
            return await self.chat(
                session_key=kwargs.get("session_key", ""),
                message=kwargs.get("message", ""),
                history=kwargs.get("history", []),
                system_extra=kwargs.get("system_extra", ""),
            )

        return _callback

    # -- Subclass hooks (abstract) -----------------------------------------

    @abstractmethod
    def _build_args(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
        *,
        session_ref: str | None = None,
    ) -> list[str]:
        """Build CLI command arguments for this invocation."""

    @abstractmethod
    async def _parse_output(self, raw_stdout: str, session_key: str) -> str:
        """Parse raw subprocess stdout into the response string."""

    # -- Subclass hooks (optional) -----------------------------------------

    def _format_history(self, history: list[dict[str, Any]]) -> str:
        """Convert OpenAI-style history dicts into a prompt string.

        Default: formats as ``[User]: ...`` / ``[Assistant]: ...`` blocks.
        """
        if not history:
            return ""
        parts: list[str] = []
        for entry in history:
            role = entry.get("role", "unknown").capitalize()
            content = entry.get("content", "")
            parts.append(f"[{role}]: {content}")
        return "\n\n".join(parts)

    def _format_prompt(
        self,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> str:
        """Build the full prompt combining history, system_extra, and message."""
        blocks: list[str] = []

        if system_extra:
            blocks.append(f"[System]: {system_extra}")

        history_text = self._format_history(history)
        if history_text:
            blocks.append(history_text)

        blocks.append(f"[User]: {message}")

        return "\n\n".join(blocks)

    # -- Internal subprocess management ------------------------------------

    async def _run_subprocess(
        self,
        args: list[str],
        input_text: str | None = None,
        *,
        timeout: float | None = None,
    ) -> tuple[str, str, int]:
        """Execute a subprocess with timeout and output limits.

        Returns ``(stdout, stderr, return_code)``.
        """
        effective_timeout = timeout or self.config.timeout

        env = dict(os.environ)
        if self.config.env:
            env.update(self.config.env)

        self._logger.debug("Running: %s", " ".join(args))

        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=self.config.cwd,
        )

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(input=input_text.encode() if input_text else None),
                timeout=effective_timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise CLITimeoutError(effective_timeout, args[0])

        stdout = stdout_bytes.decode(errors="replace")
        stderr = stderr_bytes.decode(errors="replace")

        if len(stdout_bytes) > self.config.max_output_bytes:
            raise CLIOutputTooLargeError(len(stdout_bytes), self.config.max_output_bytes)

        returncode = proc.returncode or 0
        self._logger.debug(
            "CLI finished (exit %d, stdout %d bytes, stderr %d bytes)",
            returncode,
            len(stdout_bytes),
            len(stderr_bytes),
        )

        return stdout, stderr, returncode

    async def _run_subprocess_streaming(
        self,
        args: list[str],
        input_text: str | None = None,
        *,
        timeout: float | None = None,
    ) -> AsyncIterator[str]:
        """Execute a subprocess, yielding stdout lines as they arrive.

        Uses ``procstream`` for cross-platform real-time streaming. Unlike stdbuf,
        this works on Windows/macOS/Linux by using thread-backed pipe reading
        or PTY where available.
        """
        effective_timeout = timeout or self.config.timeout

        env = dict(os.environ)
        if self.config.env:
            env.update(self.config.env)

        self._logger.debug("Streaming: %s", " ".join(args))

        try:
            from procstream import arun
        except ImportError:
            raise CLIAgentError(
                "procstream is required for streaming. Install with: pip install procstream"
            )

        try:
            proc = await arun(
                args,
                timeout=effective_timeout,
                cwd=self.config.cwd,
                env=env,
                stdin=input_text or None,
            )
        except OSError as exc:
            # The child died before/while accepting stdin: procstream's arun()
            # drains stdin before handing back a process handle, and propagates
            # a ConnectionResetError (child exited before reading stdin — e.g.
            # a startup/auth crash) or a FileNotFoundError (CLI binary missing)
            # instead of returning. We never get a proc, so there's no
            # returncode/stderr to inspect — but this is a real crash that must
            # surface as CLICrashError, not leak to the client as a raw OSError
            # ("unexpected error"). returncode=-1 marks "exited at startup".
            self._logger.error("Agent process failed to start (stdin drain): %s", exc)
            raise CLICrashError(-1, str(exc), args[0] if args else "agent") from exc

        total_bytes = 0
        overflow = False
        stderr_lines: list[str] = []
        try:
            async for line in proc.stream():
                if line.is_stderr:
                    # Buffer stderr (capped) so a crash report can surface it;
                    # never yielded — it must not pollute the response stream.
                    stderr_lines.append(line.text)
                    self._logger.debug("Agent stderr: %s", line.text[:200])
                    continue
                total_bytes += len(line.text.encode())
                if total_bytes > self.config.max_output_bytes:
                    self._logger.warning(
                        "Streaming output exceeded %d bytes, killing process",
                        self.config.max_output_bytes,
                    )
                    proc.kill()
                    overflow = True
                    break
                if line.text:
                    yield line.text
        except asyncio.CancelledError:
            self._logger.debug("Streaming cancelled, killing subprocess")
            proc.kill()
            raise
        except OSError as exc:
            # The child exited abruptly (typical when it dies fast with a
            # non-zero exit and buffered stdout); procstream surfaces this as
            # a pipe error from stream() rather than a clean EOF. Don't let it
            # mask the real outcome — fall through to the returncode check so a
            # crash still surfaces as CLICrashError, not a raw ConnectionReset.
            self._logger.debug("Agent stream pipe error (checking exit code): %s", exc)
        finally:
            await proc.wait()

        # Surface crashes: the non-streaming path (chat) raises on non-zero
        # exit, so streaming must too — otherwise a crashed/missing CLI turn
        # reads as an empty success (returncode was never checked, stderr was
        # only debug-logged). Only reached when the stream was fully consumed
        # (a GeneratorExit tears the generator down and skips this). Skip when
        # we killed the process ourselves for overflowing output (prior
        # behavior: partial output yielded as-is).
        rc = proc.returncode or 0
        if rc != 0 and not overflow:
            stderr = "".join(stderr_lines)[-4000:]
            self._logger.error("Agent stream crashed (exit %d): %s", rc, stderr[:200])
            raise CLICrashError(rc, stderr, args[0] if args else "agent")

    # -- Cleanup ------------------------------------------------------------

    async def shutdown(self) -> None:
        """Clean up any pooled subprocesses."""
        if self._pool:
            try:
                count = await asyncio.wait_for(self._pool.terminate_all(), timeout=10.0)
                if count:
                    self._logger.info("Shut down %d pooled processes", count)
            except asyncio.TimeoutError:
                self._logger.warning("Timed out shutting down pooled processes, forcing kill")
                self._pool.terminate_all_sync()

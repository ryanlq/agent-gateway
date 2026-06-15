"""Tests for the CLI agent bridge base infrastructure."""

import pytest

from agent_gateway.agents.base import (
    CLIAgentBridge,
    CLICrashError,
    CLITimeoutError,
    PooledProcess,
    SubprocessConfig,
    SubprocessPool,
)


# ---------------------------------------------------------------------------
# SubprocessConfig tests
# ---------------------------------------------------------------------------

class TestSubprocessConfig:
    def test_defaults(self):
        cfg = SubprocessConfig(command=["echo"])
        assert cfg.timeout == 1200.0
        assert cfg.max_output_bytes == 1_000_000
        assert cfg.idle_timeout == 300.0
        assert cfg.max_concurrent == 10

    def test_custom(self):
        cfg = SubprocessConfig(command=["claude"], timeout=60, max_output_bytes=500_000)
        assert cfg.timeout == 60
        assert cfg.max_output_bytes == 500_000


# ---------------------------------------------------------------------------
# SubprocessPool tests (using real echo subprocess)
# ---------------------------------------------------------------------------

class TestSubprocessPool:
    @pytest.mark.asyncio
    async def test_get_or_create(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        pp = await pool.get_or_create("session:1")
        assert isinstance(pp, PooledProcess)
        assert pp.session_key == "session:1"
        assert pp.process.returncode is None
        assert pool.active_count == 1
        await pool.terminate_all()

    @pytest.mark.asyncio
    async def test_reuse_existing(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        pp1 = await pool.get_or_create("session:1")
        pp2 = await pool.get_or_create("session:1")
        assert pp1 is pp2
        assert pool.active_count == 1
        await pool.terminate_all()

    @pytest.mark.asyncio
    async def test_different_sessions(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        pp1 = await pool.get_or_create("session:1")
        pp2 = await pool.get_or_create("session:2")
        assert pp1 is not pp2
        assert pool.active_count == 2
        await pool.terminate_all()

    @pytest.mark.asyncio
    async def test_terminate(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        await pool.get_or_create("session:1")
        assert await pool.terminate("session:1") is True
        assert pool.active_count == 0

    @pytest.mark.asyncio
    async def test_terminate_nonexistent(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        assert await pool.terminate("nonexistent") is False

    @pytest.mark.asyncio
    async def test_terminate_all(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        await pool.get_or_create("s:1")
        await pool.get_or_create("s:2")
        count = await pool.terminate_all()
        assert count == 2
        assert pool.active_count == 0

    @pytest.mark.asyncio
    async def test_session_keys(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"]))
        await pool.get_or_create("s:1")
        await pool.get_or_create("s:2")
        keys = pool.session_keys
        assert "s:1" in keys
        assert "s:2" in keys
        await pool.terminate_all()

    @pytest.mark.asyncio
    async def test_max_concurrent_eviction(self):
        pool = SubprocessPool(SubprocessConfig(command=["cat"], max_concurrent=2))
        await pool.get_or_create("s:1")
        await pool.get_or_create("s:2")
        # Third session should evict the most idle
        await pool.get_or_create("s:3")
        assert pool.active_count == 2
        assert "s:3" in pool.session_keys
        await pool.terminate_all()


# ---------------------------------------------------------------------------
# CLIAgentBridge._format_history tests (via a minimal concrete subclass)
# ---------------------------------------------------------------------------

class _DummyBridge(CLIAgentBridge):
    """Minimal concrete bridge for testing base class methods."""

    def _build_args(self, session_key, message, history, system_extra, session_ref=None):
        return ["echo", message]

    async def _parse_output(self, raw_stdout, session_key):
        return raw_stdout.strip()


class TestCLIAgentBridgeBase:
    def test_format_history_empty(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        assert bridge._format_history([]) == ""

    def test_format_history_single(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        result = bridge._format_history([{"role": "user", "content": "hello"}])
        assert "[User]: hello" == result

    def test_format_history_multi(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        history = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
            {"role": "user", "content": "how are you?"},
        ]
        result = bridge._format_history(history)
        assert "[User]: hi" in result
        assert "[Assistant]: hello" in result
        assert "[User]: how are you?" in result

    def test_format_prompt_no_history(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        result = bridge._format_prompt("hello", [], "")
        assert result == "[User]: hello"

    def test_format_prompt_with_system(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        result = bridge._format_prompt("hi", [], "be helpful")
        assert "[System]: be helpful" in result
        assert "[User]: hi" in result

    def test_format_prompt_full(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        result = bridge._format_prompt(
            "message",
            [{"role": "user", "content": "prev"}],
            "sys",
        )
        assert "System" in result
        assert "prev" in result
        assert "message" in result

    def test_as_callback_returns_callable(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        cb = bridge.as_callback()
        assert callable(cb)


# ---------------------------------------------------------------------------
# CLIAgentBridge._run_subprocess tests (using real echo)
# ---------------------------------------------------------------------------

class TestCLIAgentBridgeSubprocess:
    @pytest.mark.asyncio
    async def test_run_subprocess_echo(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        stdout, stderr, rc = await bridge._run_subprocess(["echo", "hello world"])
        assert rc == 0
        assert "hello world" in stdout

    @pytest.mark.asyncio
    async def test_run_subprocess_with_input(self):
        bridge = _DummyBridge(SubprocessConfig(command=["cat"]))
        stdout, stderr, rc = await bridge._run_subprocess(["cat"], input_text="piped input")
        assert rc == 0
        assert "piped input" in stdout

    @pytest.mark.asyncio
    async def test_run_subprocess_timeout(self):
        bridge = _DummyBridge(SubprocessConfig(command=["sleep"], timeout=0.1))
        with pytest.raises(CLITimeoutError):
            await bridge._run_subprocess(["sleep", "10"])

    @pytest.mark.asyncio
    async def test_run_subprocess_crash(self):
        bridge = _DummyBridge(SubprocessConfig(command=["false"]))
        stdout, stderr, rc = await bridge._run_subprocess(["false"])
        assert rc != 0

    @pytest.mark.asyncio
    async def test_chat_with_echo(self):
        """End-to-end: chat() uses _build_args + _run_subprocess + _parse_output."""
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        result = await bridge.chat("s:1", "hello", [])
        assert "hello" in result

    @pytest.mark.asyncio
    async def test_chat_timeout_returns_error_string(self):
        bridge = _DummyBridge(SubprocessConfig(command=["sleep"], timeout=0.1))
        # Override _build_args to actually use sleep
        bridge._build_args = lambda *a, **kw: ["sleep", "10"]
        result = await bridge.chat("s:1", "x", [])
        assert "timeout" in result.lower() or "error" in result.lower()

    @pytest.mark.asyncio
    async def test_shutdown_no_pool(self):
        bridge = _DummyBridge(SubprocessConfig(command=["echo"]))
        await bridge.shutdown()  # Should be a no-op

    @pytest.mark.asyncio
    async def test_streaming_success_yields_output(self):
        # Drain stdin (cat >/dev/null) like the crash-path tests below —
        # otherwise procstream's arun() trips its stdin-drain race against
        # ``echo`` (which exits without reading stdin) and raises CLICrashError
        # instead of yielding. This exercises the deterministic success path.
        bridge = _DummyBridge(SubprocessConfig(command=["sh"]))
        bridge._build_args = lambda *a, **kw: ["sh", "-c", "cat >/dev/null; echo hello world"]
        chunks = [c async for c in bridge.stream("s:1", "hello world", [])]
        assert any("hello world" in c for c in chunks)

    @pytest.mark.asyncio
    async def test_streaming_crash_raises_CLICrashError(self):
        """A non-zero exit during streaming must surface (previously silent:
        returncode was never checked, stderr only debug-logged).

        The command drains stdin (``cat >/dev/null``) so procstream's arun()
        doesn't trip its stdin-drain race — we exercise the post-loop
        returncode check deterministically rather than the startup-crash path.
        """
        bridge = _DummyBridge(SubprocessConfig(command=["sh"]))
        bridge._build_args = lambda *a, **kw: ["sh", "-c", "cat >/dev/null; echo partial; exit 3"]
        chunks: list[str] = []
        with pytest.raises(CLICrashError) as ei:
            async for chunk in bridge.stream("s:1", "x", []):
                chunks.append(chunk)
        assert ei.value.returncode == 3
        # Partial stdout streamed before the exit is still delivered.
        assert any("partial" in c for c in chunks)

    @pytest.mark.asyncio
    async def test_streaming_crash_surfaces_stderr(self):
        """Buffered stderr is included in the crash error so failures are
        diagnosable, not just an opaque exit code. stdin is drained so the
        crash deterministically reaches the post-loop returncode check."""
        bridge = _DummyBridge(SubprocessConfig(command=["sh"]))
        bridge._build_args = lambda *a, **kw: ["sh", "-c", "cat >/dev/null; echo boom details >&2; exit 7"]
        with pytest.raises(CLICrashError) as ei:
            async for _ in bridge.stream("s:1", "x", []):
                pass
        assert ei.value.returncode == 7
        assert "boom details" in ei.value.stderr

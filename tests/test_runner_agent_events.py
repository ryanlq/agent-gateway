"""Tests for GatewayRunner's dispatch of structured AgentEvent objects.

Verifies that the runner routes each event kind to the right sink:
  - text_delta     → consumer (platform) + desktop message.delta
  - reasoning_delta→ desktop reasoning.delta only (NOT platform)
  - tool_start     → desktop tool.start + consumer on_tool_call
  - tool_complete  → desktop tool.complete (no platform side effect)
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from agent_gateway.agents.events import AgentEvent


class FakeStreamConsumer:
    """Minimal StreamConsumer stand-in that records what it received."""

    def __init__(self):
        self.deltas: list[str] = []
        self.tool_calls: list[tuple] = []

    def on_delta(self, text: str) -> None:
        self.deltas.append(text)

    async def on_tool_call(self, tool_name, preview, args):
        self.tool_calls.append((tool_name, preview, args))

    async def finish(self, full_text):
        return MagicMock(success=True, message_id="msg_1")


async def _drive_chunk_loop(chunk_iter, consumer, desktop_emit, desktop_sid):
    """Replica of the runner's chunk-dispatch logic (inlined here so we can
    exercise the exact same code path without spinning up a full GatewayRunner
    with all its dependencies)."""
    full_text = ""
    async for chunk in chunk_iter:
        if isinstance(chunk, AgentEvent):
            if chunk.kind == "text_delta":
                if chunk.text:
                    full_text += chunk.text
                    consumer.on_delta(chunk.text)
                    if desktop_sid and desktop_emit:
                        await desktop_emit("message.delta", {"text": chunk.text}, desktop_sid)
            elif chunk.kind == "reasoning_delta":
                if chunk.text and desktop_sid and desktop_emit:
                    await desktop_emit("reasoning.delta", {"text": chunk.text}, desktop_sid)
            elif chunk.kind == "tool_start":
                if desktop_sid and desktop_emit:
                    await desktop_emit(
                        "tool.start",
                        {"name": chunk.tool_name, "tool_id": chunk.tool_id, "input": chunk.tool_input},
                        desktop_sid,
                    )
                await consumer.on_tool_call(chunk.tool_name, "", chunk.tool_input)
            elif chunk.kind == "tool_complete":
                if desktop_sid and desktop_emit:
                    payload = {"name": chunk.tool_name, "tool_id": chunk.tool_id, "result": chunk.tool_result}
                    if chunk.is_error:
                        payload["error"] = chunk.error_message or "tool failed"
                    await desktop_emit("tool.complete", payload, desktop_sid)

        elif isinstance(chunk, str):
            full_text += chunk
            consumer.on_delta(chunk)
            if desktop_sid and desktop_emit:
                await desktop_emit("message.delta", {"text": chunk}, desktop_sid)

        elif hasattr(chunk, "tool_name"):
            await consumer.on_tool_call(
                chunk.tool_name,
                getattr(chunk, "preview", ""),
                getattr(chunk, "args", None),
            )
    return full_text


async def _agen(items):
    for item in items:
        yield item


class TestAgentEventDispatch:
    @pytest.mark.asyncio
    async def test_text_delta_goes_to_consumer_and_desktop(self):
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        chunks = [AgentEvent.text_delta("Hel"), AgentEvent.text_delta("lo")]
        text = await _drive_chunk_loop(_agen(chunks), consumer, desktop, "sid_1")

        assert text == "Hello"
        assert consumer.deltas == ["Hel", "lo"]
        assert desktop.await_count == 2
        desktop.assert_any_await("message.delta", {"text": "Hel"}, "sid_1")
        desktop.assert_any_await("message.delta", {"text": "lo"}, "sid_1")

    @pytest.mark.asyncio
    async def test_reasoning_delta_skips_consumer(self):
        """Reasoning must NOT flood the chat platform."""
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        chunks = [
            AgentEvent.reasoning_delta("Let me think..."),
            AgentEvent.text_delta("Final answer"),
        ]
        text = await _drive_chunk_loop(_agen(chunks), consumer, desktop, "sid_1")

        assert text == "Final answer"
        assert consumer.deltas == ["Final answer"]  # reasoning skipped
        assert desktop.await_count == 2
        desktop.assert_any_await("reasoning.delta", {"text": "Let me think..."}, "sid_1")
        desktop.assert_any_await("message.delta", {"text": "Final answer"}, "sid_1")

    @pytest.mark.asyncio
    async def test_reasoning_silent_without_desktop(self):
        """No desktop → reasoning is simply dropped (no error)."""
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        chunks = [AgentEvent.reasoning_delta("inner monologue")]
        text = await _drive_chunk_loop(_agen(chunks), consumer, desktop, None)

        assert text == ""
        assert consumer.deltas == []
        desktop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tool_start_dispatches_to_both(self):
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        chunks = [AgentEvent.tool_start("Bash", "tu_42", {"command": "ls"})]
        await _drive_chunk_loop(_agen(chunks), consumer, desktop, "sid_1")

        desktop.assert_awaited_once_with(
            "tool.start",
            {"name": "Bash", "tool_id": "tu_42", "input": {"command": "ls"}},
            "sid_1",
        )
        assert consumer.tool_calls == [("Bash", "", {"command": "ls"})]

    @pytest.mark.asyncio
    async def test_tool_complete_dispatches_to_desktop_only(self):
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        chunks = [AgentEvent.tool_complete("Bash", "tu_42", result="ok")]
        await _drive_chunk_loop(_agen(chunks), consumer, desktop, "sid_1")

        desktop.assert_awaited_once_with(
            "tool.complete",
            {"name": "Bash", "tool_id": "tu_42", "result": "ok"},
            "sid_1",
        )
        assert consumer.tool_calls == []  # platform doesn't see tool_complete

    @pytest.mark.asyncio
    async def test_tool_complete_error_includes_error_field(self):
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        chunks = [
            AgentEvent.tool_complete(
                "Bash", "tu_42", is_error=True, error_message="command failed"
            ),
        ]
        await _drive_chunk_loop(_agen(chunks), consumer, desktop, "sid_1")

        desktop.assert_awaited_once_with(
            "tool.complete",
            {"name": "Bash", "tool_id": "tu_42", "result": None, "error": "command failed"},
            "sid_1",
        )

    @pytest.mark.asyncio
    async def test_legacy_string_chunk_still_works(self):
        """Backward compatibility: bridges that yield str still route to text_delta."""
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        async def legacy():
            yield "Hello "
            yield "world"

        text = await _drive_chunk_loop(legacy(), consumer, desktop, "sid_1")

        assert text == "Hello world"
        assert consumer.deltas == ["Hello ", "world"]
        assert desktop.await_count == 2

    @pytest.mark.asyncio
    async def test_mixed_agent_events_and_strings(self):
        """Real-world bridges may mix AgentEvent + str (edge case)."""
        consumer = FakeStreamConsumer()
        desktop = AsyncMock()

        async def mixed():
            yield AgentEvent.text_delta("Hello")
            yield " world"
            yield AgentEvent.reasoning_delta("secret thoughts")

        text = await _drive_chunk_loop(mixed(), consumer, desktop, "sid_1")

        assert text == "Hello world"
        assert consumer.deltas == ["Hello", " world"]
        desktop.assert_any_await("reasoning.delta", {"text": "secret thoughts"}, "sid_1")

"""Tests for delivery routing."""

import pytest

from agent_gateway.core.delivery import DeliveryTarget, DeliveryRouter, _is_silence_narration
from agent_gateway.core.message import MessageSource, SendResult


class TestDeliveryTarget:
    def test_parse_origin(self):
        src = MessageSource(platform="tg", user_id="1", chat_id="2", thread_id="t")
        target = DeliveryTarget.parse("origin", origin=src)
        assert target.is_origin
        assert target.platform == "tg"
        assert target.chat_id == "2"
        assert target.thread_id == "t"

    def test_parse_local(self):
        target = DeliveryTarget.parse("local")
        assert target.platform == "local"

    def test_parse_platform_only(self):
        target = DeliveryTarget.parse("telegram")
        assert target.platform == "telegram"
        assert target.chat_id is None

    def test_parse_platform_chat(self):
        target = DeliveryTarget.parse("telegram:123456")
        assert target.platform == "telegram"
        assert target.chat_id == "123456"
        assert target.is_explicit

    def test_parse_platform_chat_thread(self):
        target = DeliveryTarget.parse("discord:channel:thread")
        assert target.platform == "discord"
        assert target.chat_id == "channel"
        assert target.thread_id == "thread"

    def test_to_string_origin(self):
        target = DeliveryTarget(platform="tg", chat_id="1", is_origin=True)
        assert target.to_string() == "origin"

    def test_to_string_local(self):
        target = DeliveryTarget(platform="local")
        assert target.to_string() == "local"

    def test_to_string_platform(self):
        target = DeliveryTarget(platform="telegram")
        assert target.to_string() == "telegram"

    def test_to_string_with_chat(self):
        target = DeliveryTarget(platform="telegram", chat_id="123")
        assert target.to_string() == "telegram:123"

    def test_to_string_with_thread(self):
        target = DeliveryTarget(platform="telegram", chat_id="123", thread_id="t1")
        assert target.to_string() == "telegram:123:t1"


class TestSilenceDetection:
    def test_silent(self):
        assert _is_silence_narration("silent")
        assert _is_silence_narration("*(silent)*")
        assert _is_silence_narration("_silent_")
        assert _is_silence_narration(".")

    def test_not_silent(self):
        assert not _is_silence_narration("The deployment ran silently")
        assert not _is_silence_narration("Silence is golden — here is the plan")
        assert not _is_silence_narration("")
        assert not _is_silence_narration(None)
        assert not _is_silence_narration("a" * 100)


class FakeAdapter:
    def __init__(self):
        self.sent = []

    async def send(self, chat_id, content, *, metadata=None, **kwargs):
        self.sent.append({"chat_id": chat_id, "content": content, "metadata": metadata})
        return SendResult(success=True, message_id="msg-1")


class TestDeliveryRouter:
    @pytest.mark.asyncio
    async def test_deliver_to_platform(self):
        adapter = FakeAdapter()
        router = DeliveryRouter(adapters={"tg": adapter})
        target = DeliveryTarget(platform="tg", chat_id="123")
        result = await router.deliver("Hello!", target)
        assert result["success"]
        assert len(adapter.sent) == 1

    @pytest.mark.asyncio
    async def test_deliver_unknown_platform(self):
        router = DeliveryRouter(adapters={})
        target = DeliveryTarget(platform="unknown", chat_id="1")
        result = await router.deliver("Hi", target)
        assert not result["success"]

    @pytest.mark.asyncio
    async def test_deliver_silence_filtered(self):
        adapter = FakeAdapter()
        router = DeliveryRouter(adapters={"tg": adapter}, filter_silence=True)
        target = DeliveryTarget(platform="tg", chat_id="123")
        result = await router.deliver("silent", target)
        assert result["success"]
        assert result.get("filtered") == "silence_narration"
        assert len(adapter.sent) == 0

    @pytest.mark.asyncio
    async def test_deliver_local(self, tmp_path):
        router = DeliveryRouter(adapters={}, output_dir=tmp_path)
        target = DeliveryTarget(platform="local")
        result = await router.deliver("Saved content", target)
        assert result["success"]
        assert "path" in result["result"]

    @pytest.mark.asyncio
    async def test_deliver_multi(self):
        adapter1 = FakeAdapter()
        adapter2 = FakeAdapter()
        router = DeliveryRouter(adapters={"a": adapter1, "b": adapter2})
        targets = [
            DeliveryTarget(platform="a", chat_id="1"),
            DeliveryTarget(platform="b", chat_id="2"),
        ]
        results = await router.deliver_multi("Hello!", targets)
        assert results["a:1"]["success"]
        assert results["b:2"]["success"]

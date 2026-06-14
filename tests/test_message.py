"""Tests for core message types."""


from agent_gateway.core.message import (
    ChatType,
    EphemeralReply,
    MessageEvent,
    MessageSource,
    MessageType,
    SendResult,
    coerce_plaintext_gateway_command,
)


class TestMessageSource:
    def test_session_key_basic(self):
        src = MessageSource(platform="telegram", user_id="123", chat_id="456")
        assert src.session_key() == "telegram:123:456"

    def test_session_key_with_thread(self):
        src = MessageSource(platform="discord", user_id="u1", chat_id="c1", thread_id="t1")
        assert src.session_key() == "discord:u1:c1:t1"

    def test_session_key_no_thread(self):
        src = MessageSource(platform="slack", user_id="u1", chat_id="c1")
        assert src.session_key() == "slack:u1:c1"

    def test_different_platforms_different_keys(self):
        s1 = MessageSource(platform="telegram", user_id="1", chat_id="2")
        s2 = MessageSource(platform="discord", user_id="1", chat_id="2")
        assert s1.session_key() != s2.session_key()

    def test_different_chats_different_keys(self):
        s1 = MessageSource(platform="telegram", user_id="1", chat_id="2")
        s2 = MessageSource(platform="telegram", user_id="1", chat_id="3")
        assert s1.session_key() != s2.session_key()


class TestMessageEvent:
    def test_is_command(self):
        event = MessageEvent(text="/start")
        assert event.is_command()

    def test_is_not_command(self):
        event = MessageEvent(text="hello")
        assert not event.is_command()

    def test_get_command(self):
        event = MessageEvent(text="/help some args")
        assert event.get_command() == "help"

    def test_get_command_with_bot_suffix(self):
        event = MessageEvent(text="/start@mybot")
        assert event.get_command() == "start"

    def test_get_command_args(self):
        event = MessageEvent(text="/search python async")
        assert event.get_command_args() == "python async"

    def test_get_command_no_args(self):
        event = MessageEvent(text="/start")
        assert event.get_command_args() == ""

    def test_non_command_returns_none(self):
        event = MessageEvent(text="hello")
        assert event.get_command() is None

    def test_file_path_rejected_as_command(self):
        event = MessageEvent(text="/etc/passwd")
        assert event.get_command() is None

    def test_default_values(self):
        event = MessageEvent(text="hi")
        assert event.message_type == MessageType.TEXT
        assert event.media_urls == []
        assert event.media_types == []
        assert event.internal is False


class TestSendResult:
    def test_success(self):
        r = SendResult(success=True, message_id="123")
        assert r.success
        assert r.message_id == "123"
        assert not r.retryable

    def test_failure(self):
        r = SendResult(success=False, error="timeout")
        assert not r.success
        assert r.retryable is False

    def test_retryable_failure(self):
        r = SendResult(success=False, error="connection reset", retryable=True)
        assert r.retryable


class TestEphemeralReply:
    def test_is_str(self):
        reply = EphemeralReply("Done", ttl_seconds=10)
        assert isinstance(reply, str)
        assert str(reply) == "Done"

    def test_ttl(self):
        reply = EphemeralReply("Done", ttl_seconds=30)
        assert reply.ttl_seconds == 30
        assert reply.text == "Done"

    def test_no_ttl(self):
        reply = EphemeralReply("Done")
        assert reply.ttl_seconds is None


class TestCoercePlaintextCommand:
    def test_restart_gateway(self):
        event = MessageEvent(
            text="please restart the gateway",
            source=MessageSource("tg", "1", "2", chat_type=ChatType.DM),
        )
        coerce_plaintext_gateway_command(event)
        assert event.text == "/restart"

    def test_normal_text_unchanged(self):
        event = MessageEvent(
            text="What's the weather?",
            source=MessageSource("tg", "1", "2", chat_type=ChatType.DM),
        )
        coerce_plaintext_gateway_command(event)
        assert event.text == "What's the weather?"

    def test_group_message_unchanged(self):
        event = MessageEvent(
            text="restart gateway",
            source=MessageSource("tg", "1", "2", chat_type=ChatType.GROUP),
        )
        coerce_plaintext_gateway_command(event)
        assert event.text == "restart gateway"

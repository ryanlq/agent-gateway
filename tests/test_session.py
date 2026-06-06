"""Tests for session management."""

import time

import pytest

from agent_gateway.core.message import MessageSource, ChatType
from agent_gateway.core.session import Session, SessionStore, SessionResetPolicy


class TestSession:
    def test_touch_updates_last_active(self):
        s = Session(key="test", platform="tg", user_id="1", chat_id="2")
        old = s.last_active
        time.sleep(0.01)
        s.touch()
        assert s.last_active > old

    def test_add_message(self):
        s = Session(key="test", platform="tg", user_id="1", chat_id="2")
        s.add_message("user", "hello")
        s.add_message("assistant", "hi there")
        assert len(s.history) == 2
        assert s.history[0]["role"] == "user"
        assert s.history[1]["content"] == "hi there"

    def test_clear_history(self):
        s = Session(key="test", platform="tg", user_id="1", chat_id="2")
        s.add_message("user", "hello")
        s.system_prompt_extra = "extra"
        s.clear_history()
        assert len(s.history) == 0
        assert s.system_prompt_extra == ""

    def test_is_idle(self):
        s = Session(key="test", platform="tg", user_id="1", chat_id="2")
        assert not s.is_idle(3600)
        s.last_active = time.time() - 7200
        assert s.is_idle(3600)

    def test_get_context_window(self):
        s = Session(key="test", platform="tg", user_id="1", chat_id="2")
        for i in range(100):
            s.add_message("user", f"msg {i}")
        window = s.get_context_window(max_messages=10)
        assert len(window) == 10
        assert window[-1]["content"] == "msg 99"


class TestSessionStore:
    def test_get_or_create_new(self):
        store = SessionStore()
        src = MessageSource(platform="tg", user_id="1", chat_id="2")
        s = store.get_or_create(src)
        assert s.key == "tg:1:2"
        assert s.platform == "tg"

    def test_get_or_create_existing(self):
        store = SessionStore()
        src = MessageSource(platform="tg", user_id="1", chat_id="2")
        s1 = store.get_or_create(src)
        s1.add_message("user", "hello")
        s2 = store.get_or_create(src)
        assert s2 is s1
        assert len(s2.history) == 1

    def test_different_sessions(self):
        store = SessionStore()
        s1 = store.get_or_create(MessageSource("tg", "1", "2"))
        s2 = store.get_or_create(MessageSource("tg", "1", "3"))
        assert s1 is not s2
        assert store.active_count() == 2

    def test_reset(self):
        store = SessionStore()
        src = MessageSource(platform="tg", user_id="1", chat_id="2")
        s = store.get_or_create(src)
        s.add_message("user", "hello")
        assert store.reset(s.key) is True
        assert len(s.history) == 0

    def test_reset_nonexistent(self):
        store = SessionStore()
        assert store.reset("nonexistent") is False

    def test_remove(self):
        store = SessionStore()
        src = MessageSource(platform="tg", user_id="1", chat_id="2")
        store.get_or_create(src)
        key = src.session_key()
        assert store.remove(key) is True
        assert store.active_count() == 0

    def test_cleanup_idle(self):
        store = SessionStore(max_idle_seconds=0.01)
        src = MessageSource(platform="tg", user_id="1", chat_id="2")
        store.get_or_create(src)
        assert store.active_count() == 1
        time.sleep(0.02)
        removed = store.cleanup_idle()
        assert removed == 1
        assert store.active_count() == 0

    def test_sessions_for_user(self):
        store = SessionStore()
        store.get_or_create(MessageSource("tg", "u1", "c1"))
        store.get_or_create(MessageSource("tg", "u1", "c2"))
        store.get_or_create(MessageSource("tg", "u2", "c1"))
        sessions = store.sessions_for_user("tg", "u1")
        assert len(sessions) == 2

    def test_trim_histories(self):
        store = SessionStore(max_history=5)
        src = MessageSource(platform="tg", user_id="1", chat_id="2")
        s = store.get_or_create(src)
        for i in range(20):
            s.add_message("user", f"msg {i}")
        assert len(s.history) == 20
        store.trim_histories()
        assert len(s.history) == 5

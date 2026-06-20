"""Tests for platform-aware desktop session sync.

Covers the IM (feishu/telegram) vs email routing split in
``GatewayRunner._sync_to_desktop`` and the chat/topic-keyed session-id
derivation, plus the origin-provenance fields persisted since 2026-06-20.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

from agent_gateway.core.message import ChatType, MessageEvent, MessageSource, MessageType
from agent_gateway.core.runner import GatewayRunner
from agent_gateway.server.session_store import PersistedSession, SessionStore


# ---------------------------------------------------------------------------
# Minimal stand-ins
# ---------------------------------------------------------------------------


class FakeStore:
    """In-memory SessionStore stand-in that records create()/update() calls."""

    def __init__(self) -> None:
        self.data: dict[str, SimpleNamespace] = {}
        self.created: list[dict] = []

    def get(self, sid: str):
        return self.data.get(sid)

    def create(self, **kw):
        s = SimpleNamespace(
            history=[], last_active=0.0, _email_msg_ids=[], **kw
        )
        self.data[kw["session_id"]] = s
        self.created.append(kw)
        return s

    def update_history(self, sid: str, h: list[dict]):
        if sid not in self.data:
            self.create(session_id=sid, agent_type="claude-code-sdk")
        self.data[sid].history = list(h)

    def update(self, sid: str, **kw):
        if sid in self.data:
            self.data[sid].__dict__.update(kw)

    def get_config(self, key, default=None):
        return default or "claude-code-sdk"

    def find_by_email_message_id(self, message_id):  # no email threading here
        return None


def _runner(store: FakeStore) -> GatewayRunner:
    r = GatewayRunner.__new__(GatewayRunner)
    r._desktop_store = store
    return r


def _feishu(chat_id: str, thread_id: str | None = None,
            chat_type: ChatType = ChatType.GROUP) -> MessageSource:
    return MessageSource(
        platform="feishu", user_id="ou_1", chat_id=chat_id,
        thread_id=thread_id, chat_type=chat_type, display_name="U",
    )


def _event(source: MessageSource) -> MessageEvent:
    return MessageEvent(
        text="hi", message_type=MessageType.TEXT, source=source,
        message_id="m1", raw_message={},
    )


# ---------------------------------------------------------------------------
# Session-id derivation
# ---------------------------------------------------------------------------


def test_chat_session_id_plain_group():
    assert GatewayRunner._chat_desktop_session_id(_feishu("oc_g")) == "feishu-oc_g"


def test_chat_session_id_topic_is_own_session():
    # A topic reply gets its own id — distinct from the parent group session.
    assert GatewayRunner._chat_desktop_session_id(_feishu("oc_g", "om_t")) == "feishu-oc_g-om_t"


# ---------------------------------------------------------------------------
# IM sync: distinct sessions + origin fields
# ---------------------------------------------------------------------------


def test_sync_creates_three_distinct_sessions():
    store = FakeStore()
    r = _runner(store)
    r._sync_to_desktop(_feishu("oc_g"), "普通消息", "ok", _event(_feishu("oc_g")))
    r._sync_to_desktop(_feishu("oc_g", "om_t"), "话题1", "ok",
                       _event(_feishu("oc_g", "om_t")))
    r._sync_to_desktop(_feishu("oc_dm", chat_type=ChatType.DM), "私聊", "ok",
                       _event(_feishu("oc_dm", chat_type=ChatType.DM)))
    assert set(store.data) == {
        "feishu-oc_g", "feishu-oc_g-om_t", "feishu-oc_dm",
    }


def test_sync_persists_origin_fields_and_history():
    store = FakeStore()
    r = _runner(store)
    r._sync_to_desktop(_feishu("oc_g"), "你好", "你好!", _event(_feishu("oc_g")))
    g = store.data["feishu-oc_g"]
    assert g.platform == "feishu"
    assert g.chat_id == "oc_g"
    assert g.chat_type == "group"
    assert g.source == "飞书·群聊"
    assert g.history == [
        {"role": "user", "content": "你好"},
        {"role": "assistant", "content": "你好!"},
    ]


def test_sync_appends_to_existing_topic():
    store = FakeStore()
    r = _runner(store)
    r._sync_to_desktop(_feishu("oc_g", "om_t"), "第一句", "r1",
                       _event(_feishu("oc_g", "om_t")))
    r._sync_to_desktop(_feishu("oc_g", "om_t"), "第二句", "r2",
                       _event(_feishu("oc_g", "om_t")))
    # One topic = one session; the second turn appends rather than creating.
    assert len(store.created) == 1
    assert len(store.data["feishu-oc_g-om_t"].history) == 4  # 2 turns × (user+assistant)


def test_sync_dm_marked_p2p():
    store = FakeStore()
    r = _runner(store)
    r._sync_to_desktop(_feishu("oc_dm", chat_type=ChatType.DM), "hi", "ok",
                       _event(_feishu("oc_dm", chat_type=ChatType.DM)))
    dm = store.data["feishu-oc_dm"]
    assert dm.chat_type == "p2p"
    assert dm.source == "飞书·私聊"


# ---------------------------------------------------------------------------
# Email regression — must keep the legacy id scheme, not IM routing
# ---------------------------------------------------------------------------


def test_email_does_not_route_through_chat_sync():
    """An email source keeps the legacy ``email-{sender}[-{subject_hash}]`` id
    scheme and must NOT be folded into the IM chat-sync path (which would set
    platform/chat_id)."""
    store = FakeStore()
    r = _runner(store)
    src = MessageSource(
        platform="email", user_id="a@b.com", chat_id="a@b.com",
        chat_type=ChatType.DM, display_name="A",
    )
    r._sync_to_desktop(src, "subject: hello", "reply", _event(src))
    assert store.data, "email path should persist a session"
    created_ids = [c["session_id"] for c in store.created]
    assert any(sid.startswith("email-a-b-com") for sid in created_ids), created_ids
    # Email path predates the IM fields and must leave them unset (the create
    # call it makes carries no platform/chat_id kwargs).
    email_create = store.created[0]
    assert "platform" not in email_create
    assert "chat_id" not in email_create


# ---------------------------------------------------------------------------
# PersistedSession / SessionStore provenance (P1)
# ---------------------------------------------------------------------------


def test_persisted_session_legacy_dict_loads_none():
    # Records written before 2026-06-20 lack the origin fields entirely.
    legacy = PersistedSession.from_dict({"session_id": "old-1", "title": "x"})
    assert legacy.platform is None
    assert legacy.chat_id is None
    assert legacy.thread_id is None
    assert legacy.chat_type is None
    assert legacy.source is None


def test_to_session_info_emits_origin_fields():
    store = SessionStore()  # file-backed; we only exercise the converter
    p = PersistedSession(
        session_id="feishu-oc_x", platform="feishu", chat_id="oc_x",
        thread_id=None, chat_type="group", source="飞书·群聊",
    )
    info = store.to_session_info(p)
    assert info["platform"] == "feishu"
    assert info["chat_id"] == "oc_x"
    assert info["chat_type"] == "group"
    assert info["source"] == "飞书·群聊"

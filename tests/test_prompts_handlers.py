"""Tests for the custom-prompts RPC handlers in ``server.methods``.

These exercise the CRUD handlers (``prompts.list/add/update/delete``) with a
minimal fake store — the handlers only touch ``sessions._store.get_config`` /
``set_config``, so a full SessionManager is not required.
"""

from typing import Any

from agent_gateway.server import methods as m


class FakeStore:
    """Minimal stand-in for the SessionManager's config store."""

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = dict(data or {})

    def get_config(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set_config(self, key: str, value: Any) -> None:
        self._data[key] = value


class FakeSessions:
    def __init__(self, store: FakeStore | None = None) -> None:
        self._store = store


# emit is unused by the handlers; a sentinel is fine.
_NO_EMIT = object()


async def test_list_empty():
    sessions = FakeSessions(FakeStore())
    result = await m.handle_prompts_list({}, _NO_EMIT, sessions)
    assert result == {"prompts": []}


async def test_add_then_list_roundtrip():
    sessions = FakeSessions(FakeStore())
    created = await m.handle_prompts_add(
        {"name": "code-review", "content": "Review as a senior engineer."}, _NO_EMIT, sessions
    )
    assert created == {"ok": True, "name": "code-review"}

    listed = await m.handle_prompts_list({}, _NO_EMIT, sessions)
    assert len(listed["prompts"]) == 1
    entry = listed["prompts"][0]
    assert entry["name"] == "code-review"
    assert entry["content"] == "Review as a senior engineer."
    assert isinstance(entry["updated_at"], float)


async def test_add_requires_name():
    sessions = FakeSessions(FakeStore())
    result = await m.handle_prompts_add({"name": "  ", "content": "x"}, _NO_EMIT, sessions)
    assert result["ok"] is False
    assert "name" in result["error"]
    # Nothing was persisted.
    assert (await m.handle_prompts_list({}, _NO_EMIT, sessions))["prompts"] == []


async def test_add_rejects_duplicate_name():
    sessions = FakeSessions(FakeStore({"custom_prompts": {"code-review": {"content": "old", "updated_at": 1.0}}}))
    result = await m.handle_prompts_add({"name": "code-review", "content": "new"}, _NO_EMIT, sessions)
    assert result["ok"] is False
    assert "already exists" in result["error"]
    # Existing content is untouched.
    listed = await m.handle_prompts_list({}, _NO_EMIT, sessions)
    assert listed["prompts"][0]["content"] == "old"


async def test_update_changes_content_and_timestamp():
    sessions = FakeSessions(FakeStore({"custom_prompts": {"x": {"content": "v1", "updated_at": 1.0}}}))
    result = await m.handle_prompts_update({"name": "x", "content": "v2"}, _NO_EMIT, sessions)
    assert result == {"ok": True, "name": "x"}
    listed = await m.handle_prompts_list({}, _NO_EMIT, sessions)
    entry = listed["prompts"][0]
    assert entry["content"] == "v2"
    assert entry["updated_at"] is not None and entry["updated_at"] >= 1.0


async def test_update_upserts_missing_name():
    """update acts as an upsert — a new name is created."""
    sessions = FakeSessions(FakeStore())
    result = await m.handle_prompts_update({"name": "fresh", "content": "body"}, _NO_EMIT, sessions)
    assert result == {"ok": True, "name": "fresh"}
    listed = await m.handle_prompts_list({}, _NO_EMIT, sessions)
    assert [p["name"] for p in listed["prompts"]] == ["fresh"]


async def test_update_requires_name():
    sessions = FakeSessions(FakeStore())
    result = await m.handle_prompts_update({"name": "", "content": "x"}, _NO_EMIT, sessions)
    assert result["ok"] is False


async def test_delete_removes_prompt():
    sessions = FakeSessions(FakeStore({"custom_prompts": {"x": {"content": "1", "updated_at": 1.0}}}))
    result = await m.handle_prompts_delete({"name": "x"}, _NO_EMIT, sessions)
    assert result == {"ok": True, "name": "x"}
    assert (await m.handle_prompts_list({}, _NO_EMIT, sessions))["prompts"] == []


async def test_delete_missing_name_fails():
    sessions = FakeSessions(FakeStore())
    result = await m.handle_prompts_delete({"name": "nope"}, _NO_EMIT, sessions)
    assert result["ok"] is False
    assert "not found" in result["error"]


async def test_load_custom_prompts_guards_against_bad_data():
    """Corrupted config (non-dict value or non-dict entry) is tolerated."""
    sessions = FakeSessions(
        FakeStore(
            {
                "custom_prompts": {
                    "good": {"content": "ok", "updated_at": 1.0},
                    "bad": "not-a-dict",
                }
            }
        )
    )
    loaded = m._load_custom_prompts(sessions)
    assert list(loaded.keys()) == ["good"]


async def test_load_custom_prompts_no_store():
    """A session manager without a store yields an empty map."""
    sessions = FakeSessions(store=None)
    assert m._load_custom_prompts(sessions) == {}

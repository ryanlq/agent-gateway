"""Tests for platform registry."""

import pytest

from agent_gateway.core.registry import PlatformEntry, PlatformRegistry


class FakeAdapter:
    def __init__(self, config):
        self.config = config


class FakeAdapterWithDeps:
    def __init__(self, config):
        self.config = config


def _make_entry(name="test", label="Test", **kwargs):
    return PlatformEntry(
        name=name,
        label=label,
        adapter_factory=lambda cfg: FakeAdapter(cfg),
        check_fn=lambda: True,
        **kwargs,
    )


class TestPlatformRegistry:
    def test_register_and_lookup(self):
        reg = PlatformRegistry()
        entry = _make_entry(name="irc", label="IRC")
        reg.register(entry)
        assert reg.get("irc") is entry
        assert reg.is_registered("irc")

    def test_unregister(self):
        reg = PlatformRegistry()
        reg.register(_make_entry(name="irc"))
        assert reg.unregister("irc") is True
        assert not reg.is_registered("irc")

    def test_unregister_nonexistent(self):
        reg = PlatformRegistry()
        assert reg.unregister("nope") is False

    def test_create_adapter_success(self):
        reg = PlatformRegistry()
        reg.register(_make_entry(name="test"))
        adapter = reg.create_adapter("test", {"token": "abc"})
        assert isinstance(adapter, FakeAdapter)
        assert adapter.config["token"] == "abc"

    def test_create_adapter_not_registered(self):
        reg = PlatformRegistry()
        assert reg.create_adapter("nonexistent", {}) is None

    def test_create_adapter_check_fails(self):
        reg = PlatformRegistry()
        reg.register(PlatformEntry(
            name="broken",
            label="Broken",
            adapter_factory=lambda cfg: FakeAdapter(cfg),
            check_fn=lambda: False,
        ))
        assert reg.create_adapter("broken", {}) is None

    def test_create_adapter_config_validation_fails(self):
        reg = PlatformRegistry()
        reg.register(PlatformEntry(
            name="strict",
            label="Strict",
            adapter_factory=lambda cfg: FakeAdapter(cfg),
            check_fn=lambda: True,
            validate_config=lambda cfg: bool(cfg.get("required_field")),
        ))
        assert reg.create_adapter("strict", {}) is None
        assert reg.create_adapter("strict", {"required_field": "yes"}) is not None

    def test_create_adapter_factory_error(self):
        reg = PlatformRegistry()
        reg.register(PlatformEntry(
            name="crashy",
            label="Crashy",
            adapter_factory=lambda cfg: (_ for _ in ()).throw(RuntimeError("boom")),
            check_fn=lambda: True,
        ))
        assert reg.create_adapter("crashy", {}) is None

    def test_reregistration_overwrites(self):
        reg = PlatformRegistry()
        e1 = _make_entry(name="test", label="V1")
        e2 = _make_entry(name="test", label="V2")
        reg.register(e1)
        reg.register(e2)
        assert reg.get("test").label == "V2"

    def test_all_entries(self):
        reg = PlatformRegistry()
        reg.register(_make_entry(name="a"))
        reg.register(_make_entry(name="b"))
        assert len(reg.all_entries()) == 2

    def test_builtin_vs_plugin(self):
        reg = PlatformRegistry()
        reg.register(_make_entry(name="builtin", source="builtin"))
        reg.register(_make_entry(name="plugin", source="plugin"))
        assert len(reg.builtin_entries()) == 1
        assert len(reg.plugin_entries()) == 1

    def test_platform_names(self):
        reg = PlatformRegistry()
        reg.register(_make_entry(name="alpha"))
        reg.register(_make_entry(name="beta"))
        names = reg.platform_names()
        assert "alpha" in names
        assert "beta" in names

    def test_summary(self):
        reg = PlatformRegistry()
        reg.register(_make_entry(name="test"))
        summary = reg.summary()
        assert "test" in summary

    def test_empty_summary(self):
        reg = PlatformRegistry()
        assert "No platforms" in reg.summary()

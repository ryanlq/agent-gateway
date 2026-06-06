"""Tests for configuration system."""

import os
import pytest

from agent_gateway.core.config import GatewayConfig, PlatformConfig, StreamingConfig, SessionConfig


class TestPlatformConfig:
    def test_from_dict_basic(self):
        pc = PlatformConfig.from_dict("telegram", {"token": "abc123", "enabled": True})
        assert pc.token == "abc123"
        assert pc.enabled

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_TOKEN", "env-token")
        pc = PlatformConfig.from_dict("telegram", {"token": "yaml-token"})
        assert pc.token == "env-token"

    def test_home_channel(self):
        pc = PlatformConfig.from_dict("tg", {"home_channel": "123"})
        assert pc.home_channel == "123"

    def test_dm_policy_default(self):
        pc = PlatformConfig.from_dict("tg", {})
        assert pc.dm_policy == "allowlist"

    def test_allow_from_string(self):
        pc = PlatformConfig.from_dict("tg", {"allow_from": "1, 2, 3"})
        assert pc.allow_from == ["1", "2", "3"]

    def test_allow_from_list(self):
        pc = PlatformConfig.from_dict("tg", {"allow_from": ["1", "2"]})
        assert pc.allow_from == ["1", "2"]


class TestStreamingConfig:
    def test_defaults(self):
        sc = StreamingConfig()
        assert sc.enabled
        assert sc.min_edit_interval == 0.8
        assert sc.tool_progress == "all"

    def test_from_dict(self):
        sc = StreamingConfig.from_dict({"enabled": False, "tool_progress": "verbose"})
        assert not sc.enabled
        assert sc.tool_progress == "verbose"


class TestSessionConfig:
    def test_defaults(self):
        sc = SessionConfig()
        assert sc.max_idle_seconds == 3600
        assert sc.reset_policy == "idle"

    def test_from_dict(self):
        sc = SessionConfig.from_dict({"max_idle_seconds": 7200, "reset_policy": "daily"})
        assert sc.max_idle_seconds == 7200
        assert sc.reset_policy == "daily"


class TestGatewayConfig:
    def test_load_missing_file(self):
        config = GatewayConfig.load("/nonexistent/path.yaml")
        assert len(config.platforms) == 0

    def test_from_dict(self):
        data = {
            "platforms": {
                "telegram": {"token": "abc", "enabled": True},
                "discord": {"token": "def", "enabled": False},
            },
            "streaming": {"enabled": False},
            "session": {"max_idle_seconds": 1800},
            "filter_silence_narration": False,
        }
        config = GatewayConfig.from_dict(data)
        assert len(config.platforms) == 2
        assert not config.streaming.enabled
        assert config.session.max_idle_seconds == 1800
        assert not config.filter_silence_narration

    def test_enabled_platforms(self):
        config = GatewayConfig.from_dict({
            "platforms": {
                "tg": {"enabled": True},
                "dc": {"enabled": False},
            }
        })
        enabled = config.enabled_platforms()
        assert "tg" in enabled
        assert "dc" not in enabled

    def test_summary(self):
        config = GatewayConfig.from_dict({
            "platforms": {
                "telegram": {"token": "abc"},
                "discord": {"enabled": False},
            }
        })
        summary = config.summary()
        assert "telegram" in summary
        assert "discord" in summary

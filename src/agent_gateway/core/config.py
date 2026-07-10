"""
Configuration system for the agent gateway.

Supports layered configuration loading:

    Environment variables > YAML file > defaults

Usage::

    from agent_gateway.core.config import GatewayConfig

    config = GatewayConfig.load("gateway.yaml")
    print(config.platforms["telegram"].token)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Platform config
# ---------------------------------------------------------------------------

@dataclass
class PlatformConfig:
    """Configuration for a single platform connection."""

    enabled: bool = True
    """Whether this platform is active."""

    token: str = ""
    """Primary auth token / API key."""

    extra: dict[str, Any] = field(default_factory=dict)
    """Platform-specific options (parsed from YAML)."""

    # -- Derived helpers -----------------------------------------------------

    @property
    def home_channel(self) -> Optional[str]:
        """Default chat ID for cron / notification delivery."""
        return self.extra.get("home_channel")

    @property
    def dm_policy(self) -> str:
        """Direct-message access policy: ``open`` / ``allowlist`` / ``closed``."""
        return self.extra.get("dm_policy", "allowlist")

    @property
    def group_policy(self) -> str:
        """Group-chat access policy."""
        return self.extra.get("group_policy", "allowlist")

    @property
    def allow_from(self) -> list[str]:
        """List of allowed user/chat IDs."""
        raw = self.extra.get("allow_from", [])
        if isinstance(raw, str):
            return [s.strip() for s in raw.split(",") if s.strip()]
        return list(raw)

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> PlatformConfig:
        """Create a ``PlatformConfig`` from a parsed YAML dict.

        Environment variable ``{NAME}_TOKEN`` overrides the YAML ``token``
        field (env > YAML precedence).
        """
        env_token = os.environ.get(f"{name.upper()}_TOKEN", "").strip()

        return cls(
            enabled=data.get("enabled", True),
            token=env_token or data.get("token", ""),
            extra=data,
        )


# ---------------------------------------------------------------------------
# Streaming config
# ---------------------------------------------------------------------------

@dataclass
class StreamingConfig:
    """Streaming response configuration."""

    enabled: bool = True
    """Whether streaming is enabled."""

    min_edit_interval: float = 0.8
    """Minimum seconds between edit requests (throttle)."""

    use_draft: bool = False
    """Prefer native draft streaming when available."""

    tool_progress: str = "all"
    """Tool progress display mode: ``all`` / ``new`` / ``verbose`` / ``none``."""

    tool_preview_length: int = 40
    """Max characters of tool argument preview."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StreamingConfig:
        return cls(
            enabled=data.get("enabled", True),
            min_edit_interval=data.get("min_edit_interval", 0.8),
            use_draft=data.get("use_draft", False),
            tool_progress=data.get("tool_progress", "all"),
            tool_preview_length=data.get("tool_preview_length", 40),
        )


# ---------------------------------------------------------------------------
# Session config
# ---------------------------------------------------------------------------

@dataclass
class SessionConfig:
    """Session management configuration."""

    max_idle_seconds: float = 3600.0
    """Idle timeout before session reset (seconds)."""

    max_history: int = 200
    """Maximum conversation history entries per session."""

    reset_policy: str = "idle"
    """Reset policy: ``daily`` / ``idle`` / ``both`` / ``none``."""

    cleanup_interval: float = 300.0
    """How often to run idle cleanup (seconds)."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SessionConfig:
        return cls(
            max_idle_seconds=float(data.get("max_idle_seconds", 3600)),
            max_history=int(data.get("max_history", 200)),
            reset_policy=data.get("reset_policy", "idle"),
            cleanup_interval=float(data.get("cleanup_interval", 300)),
        )


# ---------------------------------------------------------------------------
# Gateway config
# ---------------------------------------------------------------------------

@dataclass
class GatewayConfig:
    """
    Top-level gateway configuration.

    Contains all platform configs, session settings, streaming options,
    and operational parameters.
    """

    platforms: dict[str, PlatformConfig] = field(default_factory=dict)
    """All configured platform connections."""

    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    """Streaming response settings."""

    session: SessionConfig = field(default_factory=SessionConfig)
    """Session management settings."""

    filter_silence_narration: bool = True
    """Whether to drop silence-narration outbound messages."""

    agent_timeout: float | None = None
    """Maximum time (seconds) for agent processing before timeout.

    ``None`` (default) means no limit — the agent runs until it naturally
    finishes.  Set to a number of seconds to enforce a hard deadline.
    """

    # -- Loading -------------------------------------------------------------

    @classmethod
    def load(cls, path: str | Path = "gateway.yaml") -> GatewayConfig:
        """Load configuration from a YAML file with env-var overrides.

        Precedence: ``os.environ`` > YAML > defaults.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            A fully resolved ``GatewayConfig``.
        """
        config = cls()

        yaml_path = Path(path)
        if not yaml_path.exists():
            logger.info("Config file not found at %s — using defaults", yaml_path)
            return config

        try:
            import yaml
        except ImportError:
            logger.warning("PyYAML not installed — using defaults")
            return config

        try:
            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.error("Failed to load config from %s: %s", yaml_path, exc)
            return config

        if not isinstance(data, dict):
            logger.warning("Config file is not a dict — using defaults")
            return config

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GatewayConfig:
        """Create from a parsed dict (e.g. from YAML)."""
        config = cls()

        # Platforms
        for name, pdata in data.get("platforms", {}).items():
            if isinstance(pdata, dict):
                config.platforms[name] = PlatformConfig.from_dict(name, pdata)

        # Streaming
        if "streaming" in data and isinstance(data["streaming"], dict):
            config.streaming = StreamingConfig.from_dict(data["streaming"])

        # Session
        if "session" in data and isinstance(data["session"], dict):
            config.session = SessionConfig.from_dict(data["session"])

        # Top-level flags
        config.filter_silence_narration = data.get("filter_silence_narration", True)
        config.agent_timeout = float(data["agent_timeout"]) if data.get("agent_timeout") not in (None, "", "none", "unlimited") else None

        return config

    # -- Helpers -------------------------------------------------------------

    def enabled_platforms(self) -> dict[str, PlatformConfig]:
        """Return only enabled platform configs."""
        return {name: pcfg for name, pcfg in self.platforms.items() if pcfg.enabled}

    def get_platform(self, name: str) -> Optional[PlatformConfig]:
        """Look up a platform config by name."""
        return self.platforms.get(name)

    def summary(self) -> str:
        """Return a human-readable config summary."""
        lines = ["Gateway Configuration:"]
        lines.append(f"  Platforms: {len(self.platforms)} configured, "
                     f"{len(self.enabled_platforms())} enabled")
        lines.append(f"  Streaming: {'on' if self.streaming.enabled else 'off'}")
        lines.append(f"  Session reset: {self.session.reset_policy}")
        lines.append(f"  Silence filter: {'on' if self.filter_silence_narration else 'off'}")

        for name, pcfg in self.platforms.items():
            status = "✅" if pcfg.enabled else "⬜"
            has_token = "🔑" if pcfg.token else "⚠️"
            lines.append(f"    {status} {name} {has_token}")

        return "\n".join(lines)

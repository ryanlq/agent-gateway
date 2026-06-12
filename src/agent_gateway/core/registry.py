"""
Platform adapter registry.

Allows platform adapters (built-in and plugin) to self-register so the
gateway can discover and instantiate them without hardcoded if/elif chains.

Usage (plugin side)::

    from agent_gateway.core.registry import registry, PlatformEntry

    registry.register(PlatformEntry(
        name="irc",
        label="IRC",
        adapter_factory=lambda cfg: IRCAdapter(cfg),
        check_fn=lambda: True,
    ))

Usage (gateway side)::

    adapter = registry.create_adapter("irc", config_dict)

The module-level ``registry`` singleton is the default instance.  You can
also instantiate ``PlatformRegistry`` separately for testing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class EnvVarDef:
    """Metadata for a single platform environment variable.

    Used by the frontend messaging settings UI to render input fields,
    show descriptions, and indicate required/optional status.
    """

    key: str
    """Environment variable name (e.g. ``"TELEGRAM_TOKEN"``)."""

    description: str = ""
    """Human-readable description of what this variable controls."""

    prompt: str = ""
    """Placeholder text shown in the input field."""

    is_password: bool = False
    """If True, the UI treats this as a secret (masked input, redacted display)."""

    required: bool = True
    """Whether this variable must be set for the platform to function."""

    advanced: bool = False
    """If True, shown in the "Advanced" section of the UI."""

    url: str = ""
    """Link to documentation for obtaining this value."""


@dataclass
class PlatformEntry:
    """Metadata and factory for a single platform adapter.

    This mirrors the Hermes gateway's ``PlatformEntry`` design — each entry
    carries everything the registry needs to validate, instantiate, and
    describe a platform adapter.
    """

    name: str
    """Identifier used in config files (e.g. ``"telegram"``, ``"discord"``)."""

    label: str
    """Human-readable label (e.g. ``"Telegram"``, ``"Discord"``)."""

    adapter_factory: Callable[[dict[str, Any]], Any]
    """Factory callable: receives a config dict, returns an adapter instance.

    Using a factory instead of a bare class lets plugins do custom init
    (e.g. passing extra kwargs, wrapping in try/except).
    """

    check_fn: Callable[[], bool]
    """Returns True when the platform's dependencies are available."""

    # -- Optional validation -------------------------------------------------

    validate_config: Optional[Callable[[dict[str, Any]], bool]] = None
    """Given a config dict, is it properly configured?  None = skip validation."""

    is_connected_fn: Optional[Callable[[dict[str, Any]], bool]] = None
    """Given a config dict, is the platform connected/enabled?"""

    # -- Installation --------------------------------------------------------

    required_env: list[str] = field(default_factory=list)
    """Env vars this platform needs (for setup display)."""

    install_hint: str = ""
    """Hint shown when ``check_fn`` returns False."""

    setup_fn: Optional[Callable[[], None]] = None
    """Interactive setup function (prompts user, saves env vars)."""

    # -- Classification ------------------------------------------------------

    source: str = "plugin"
    """``"builtin"`` or ``"plugin"``."""

    # -- Limits --------------------------------------------------------------

    max_message_length: int = 0
    """Max message length for smart-chunking.  0 = no limit."""

    # -- Display -------------------------------------------------------------

    emoji: str = "🔌"
    """Emoji for CLI / status display."""

    platform_hint: str = ""
    """Hint injected into the system prompt (e.g. ``"Do not use markdown."``)."""

    # -- Auth ----------------------------------------------------------------

    allowed_users_env: str = ""
    """Env var for comma-separated allowed user IDs."""

    allow_all_env: str = ""
    """Env var that, if truthy, authorises all users."""

    # -- Cron delivery -------------------------------------------------------

    cron_deliver_env_var: str = ""
    """Env var name for the home channel used by cron delivery."""

    env_var_defs: list[EnvVarDef] = field(default_factory=list)
    """Detailed metadata for each environment variable (for frontend UI)."""

    # -- Standalone sending --------------------------------------------------

    standalone_sender_fn: Optional[Callable[..., Awaitable[dict]]] = None
    """Async coroutine that delivers a message without a live adapter.

    Used when ``cron`` runs in a separate process and the in-process
    adapter is not available.

    Signature::

        async (config, chat_id, message, **kwargs) -> dict
    """

    # -- YAML config bridge --------------------------------------------------

    apply_yaml_config_fn: Optional[Callable[[dict, dict], Optional[dict]]] = None
    """Translate YAML config keys into env vars / PlatformConfig.extra."""

    # -- Privacy -------------------------------------------------------------

    pii_safe: bool = False
    """If True, session descriptions redact PII (phone numbers, etc.)."""

    # -- Permissions ---------------------------------------------------------

    allow_update_command: bool = True
    """Whether /update command is allowed from this platform."""


class PlatformRegistry:
    """Central registry of platform adapters.

    Thread-safe for reads (dict lookups are atomic under GIL).
    Writes happen at startup during sequential discovery.
    """

    def __init__(self) -> None:
        self._entries: dict[str, PlatformEntry] = {}

    # -- Registration --------------------------------------------------------

    def register(self, entry: PlatformEntry) -> None:
        """Register a platform adapter entry.

        If an entry with the same name exists, it is replaced (last-writer
        wins — this lets plugins override built-in adapters if desired).
        """
        if entry.name in self._entries:
            prev = self._entries[entry.name]
            logger.info(
                "Platform '%s' re-registered (was %s, now %s)",
                entry.name, prev.source, entry.source,
            )
        self._entries[entry.name] = entry
        logger.debug("Registered platform adapter: %s (%s)", entry.name, entry.source)

    def unregister(self, name: str) -> bool:
        """Remove a platform entry.  Returns True if it existed."""
        return self._entries.pop(name, None) is not None

    # -- Lookup --------------------------------------------------------------

    def get(self, name: str) -> Optional[PlatformEntry]:
        """Look up a platform entry by name."""
        return self._entries.get(name)

    def is_registered(self, name: str) -> bool:
        return name in self._entries

    def all_entries(self) -> list[PlatformEntry]:
        """Return all registered platform entries."""
        return list(self._entries.values())

    def builtin_entries(self) -> list[PlatformEntry]:
        """Return only built-in platform entries."""
        return [e for e in self._entries.values() if e.source == "builtin"]

    def plugin_entries(self) -> list[PlatformEntry]:
        """Return only plugin-registered platform entries."""
        return [e for e in self._entries.values() if e.source == "plugin"]

    # -- Factory -------------------------------------------------------------

    def create_adapter(self, name: str, config: dict[str, Any]) -> Optional[Any]:
        """Create an adapter instance for the given platform name.

        Returns ``None`` if:
          - No entry registered for *name*
          - ``check_fn()`` returns False (missing deps)
          - ``validate_config()`` returns False (misconfigured)
          - The factory raises an exception
        """
        entry = self._entries.get(name)
        if entry is None:
            return None

        # Check dependencies
        try:
            if not entry.check_fn():
                hint = f" ({entry.install_hint})" if entry.install_hint else ""
                logger.warning("Platform '%s' requirements not met%s", entry.label, hint)
                return None
        except Exception as exc:
            logger.warning("Platform '%s' check_fn error: %s", entry.label, exc)
            return None

        # Validate config
        if entry.validate_config is not None:
            try:
                if not entry.validate_config(config):
                    logger.warning("Platform '%s' config validation failed", entry.label)
                    return None
            except Exception as exc:
                logger.warning("Platform '%s' config validation error: %s", entry.label, exc)
                return None

        # Create adapter
        try:
            adapter = entry.adapter_factory(config)
            logger.info("Created adapter for '%s'", entry.label)
            return adapter
        except Exception as exc:
            logger.error("Failed to create adapter for '%s': %s", entry.label, exc, exc_info=True)
            return None

    # -- Introspection -------------------------------------------------------

    def platform_names(self) -> list[str]:
        """Return all registered platform names."""
        return list(self._entries.keys())

    def summary(self) -> str:
        """Return a human-readable summary of all registered platforms."""
        if not self._entries:
            return "No platforms registered."

        lines = ["Registered platforms:"]
        for entry in self._entries.values():
            status = "✅" if entry.check_fn() else "❌"
            lines.append(f"  {status} {entry.emoji} {entry.label} ({entry.name}) [{entry.source}]")
        return "\n".join(lines)


# Module-level singleton — the default registry instance.
registry = PlatformRegistry()

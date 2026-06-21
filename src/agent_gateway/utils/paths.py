"""Central data-home resolution for agent-gateway.

All gateway runtime data — persisted sessions, logs, adapter state cursors,
media cache, delivery output — must live under one home so the gateway is
self-contained and no longer leaks across ``~/.hermes`` (upstream-Hermes
residue) and ``~/.agent_gateway`` (the old module-name dir). ``resolve_home()``
is the single source of truth; every other module derives its path from it so
``NEXUS_AGENT_HOME`` overrides the whole tree at once.

Default home: ``~/.nexus-agent`` (Windows: ``%LOCALAPPDATA%\\nexus-agent`` is
the desktop's convention, but the gateway honours whatever the desktop pins via
``NEXUS_AGENT_HOME``). ``migrate_legacy_agent_gateway_home()`` performs a
one-time move of the old ``~/.agent_gateway`` subdirs into the new home; call it
once at process start, before any adapter/state/cache dir is created.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def resolve_home() -> Path:
    """Gateway data home.

    ``NEXUS_AGENT_HOME`` (set by the desktop, tests, or the user) overrides the
    default of ``~/.nexus-agent``. All other gateway paths derive from this.
    """
    env_home = os.environ.get("NEXUS_AGENT_HOME")
    return Path(env_home) if env_home else Path.home() / ".nexus-agent"


def migrate_legacy_agent_gateway_home() -> None:
    """One-time move of ``~/.agent_gateway/{state,cache,output}`` into the home.

    The old module-name dir ``~/.agent_gateway`` predates the unification onto
    ``~/.nexus-agent``. Each subdir is moved atomically (same-filesystem
    ``rename``) only when the destination does not yet exist, so re-runs and a
    partially-migrated tree are safe. The legacy dir is left in place (may still
    hold unrelated files) — it is simply no longer read.
    """
    home = resolve_home()
    legacy = Path.home() / ".agent_gateway"
    if not legacy.is_dir():
        return
    for sub in ("state", "cache", "output"):
        src = legacy / sub
        dst = home / sub
        if not src.exists() or dst.exists():
            continue
        try:
            home.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            logger.info("Migrated legacy %s -> %s", src, dst)
        except Exception as exc:  # pragma: no cover - best-effort, never fatal
            logger.warning("Could not migrate %s to %s: %s", src, dst, exc)

"""Persistent state store for adapter offset tracking.

Adapters use this to persist their position cursors (last processed
message ID, update offset, etc.) so backlog recovery can pick up
where it left off after a gateway restart.

State files live under ``~/.agent_gateway/state/<platform>.json``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_STATE_DIR = Path.home() / ".agent_gateway" / "state"


def _ensure_dir() -> Path:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    return _STATE_DIR


def load_state(platform: str) -> dict[str, Any]:
    """Load persisted state for a platform. Returns ``{}`` on any error."""
    path = _STATE_DIR / f"{platform}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to load state for %s: %s", platform, exc)
        return {}


def save_state(platform: str, data: dict[str, Any]) -> None:
    """Persist state for a platform."""
    try:
        d = _ensure_dir()
        path = d / f"{platform}.json"
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to save state for %s: %s", platform, exc)

"""Shared runtime helpers for built-in platform adapters.

Centralizes the two patterns that were copy-pasted — and quietly drifting —
across every adapter:

- :func:`resolve_credential`: the "config value → env var → default" fallback
  used to read tokens and other credentials. Feishu was ``.strip()``-ing its
  values while Discord/Telegram/Slack were not; routing everyone through one
  helper keeps that consistent and makes any future change (e.g. always strip)
  a single edit.
- :func:`sdk_available`: the ``try: import … except ImportError`` probe behind
  each adapter's ``_check_*_deps``. One place to update if dep-checking
  semantics ever change, and the exact probe the frozen-binary smoke test
  (``agent-gateway --check-adapters``) relies on.
"""

from __future__ import annotations

import os


def resolve_credential(
    *candidates,
    env: str,
    default: str = "",
    strip: bool = False,
    cast=None,
):
    """Resolve an adapter setting: first truthy candidate, else env, else default.

    ``candidates`` are explicit config-sourced values tried in order (e.g.
    ``config.get("token")``, ``extra.get("app_id")``) before falling back to the
    ``env`` environment variable (using ``default`` when unset). Pass
    ``strip=True`` to trim a str result, and ``cast=int`` to coerce numeric
    settings such as ports.

    Mirrors the ``cfg.get(k) or os.getenv(ENV, default)`` idiom verbatim — no
    behaviour change vs. the prior inline reads, just centralized.
    """
    value: object = default
    for cand in candidates:
        if cand:
            value = cand
            break
    else:
        value = os.getenv(env, default)
    if strip and isinstance(value, str):
        value = value.strip()
    if cast is int:
        return int(value)
    return value


def sdk_available(import_name: str) -> bool:
    """True if the given top-level SDK module imports.

    This is the probe behind every adapter's ``_check_*_deps`` and the one the
    frozen-binary smoke test exercises: in a PyInstaller build with no runtime
    pip, an SDK that failed to freeze fails this import.
    """
    try:
        __import__(import_name)
        return True
    except ImportError:
        return False

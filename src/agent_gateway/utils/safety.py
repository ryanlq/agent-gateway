"""
Safety utilities for media delivery path validation.

Prevents the agent from exfiltrating host secrets via MEDIA: directives
(e.g. ``MEDIA:/etc/passwd`` or ``MEDIA:~/.ssh/id_rsa``).

Two modes:
  - **Strict**: files must be in a cache dir, an operator-allowlisted root,
    or freshly produced (mtime within a recency window).
  - **Non-strict** (default): accept any regular file not under the denylist.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

# Hard denylist — these prefixes are never allowed for media delivery
_DENIED_PREFIXES = (
    "/etc", "/proc", "/sys", "/dev", "/root", "/boot",
    "/var/log", "/var/lib", "/var/run",
)

# Under $HOME, these subdirectories are denied
_DENIED_HOME_SUBPATHS = (
    ".ssh", ".aws", ".gnupg", ".kube", ".docker", ".config", ".azure",
)

# Default recency window (seconds) for freshly-produced files
_DEFAULT_RECENCY_SECONDS = 600


def validate_media_delivery_path(
    path: str,
    *,
    cache_roots: list[Path] | None = None,
    strict: bool = False,
    recency_seconds: float = _DEFAULT_RECENCY_SECONDS,
) -> Optional[str]:
    """Return a safe absolute path if *path* is valid for media delivery.

    Returns ``None`` if the path is rejected.

    Args:
        path: The file path to validate.
        cache_roots: Additional allowed root directories.
        strict: Enable strict allowlist mode.
        recency_seconds: Window for trusting freshly-produced files.
    """
    if not path:
        return None

    candidate = str(path).strip()
    # Strip surrounding quotes/backticks
    if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in "`\"'":
        candidate = candidate[1:-1].strip()
    candidate = candidate.lstrip("`\"'").rstrip("`\"',.;:)}]")
    if not candidate:
        return None

    try:
        expanded = Path(os.path.expanduser(candidate))
    except (OSError, RuntimeError, ValueError):
        return None

    if not expanded.is_absolute():
        return None

    try:
        resolved = expanded.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return None

    if not resolved.is_file():
        return None

    # Check cache / operator allowlist — always honoured
    roots = list(cache_roots or [])
    for root in roots:
        try:
            root_resolved = Path(root).expanduser().resolve(strict=False)
            resolved.relative_to(root_resolved)
            return str(resolved)
        except ValueError:
            continue

    # Non-strict mode: accept anything not on the denylist
    if not strict:
        if _is_under_denied_prefix(resolved):
            return None
        return str(resolved)

    # Strict mode: check recency window
    if recency_seconds > 0 and not _is_under_denied_prefix(resolved):
        try:
            mtime = resolved.stat().st_mtime
            if (time.time() - mtime) <= recency_seconds:
                return str(resolved)
        except OSError:
            pass

    return None


def _is_under_denied_prefix(resolved: Path) -> bool:
    """Check if *resolved* is under a denied system path."""
    home = Path(os.path.expanduser("~")).resolve(strict=False)

    for prefix in _DENIED_PREFIXES:
        denied = Path(prefix)
        try:
            resolved.relative_to(denied)
            return True
        except ValueError:
            pass

    for subpath in _DENIED_HOME_SUBPATHS:
        denied = home / subpath
        try:
            resolved.relative_to(denied)
            return True
        except ValueError:
            pass

    return False


def safe_url_for_log(url: str, max_len: int = 80) -> str:
    """Return a URL safe for logging (no query/fragment/userinfo)."""
    if not url:
        return ""

    try:
        from urllib.parse import urlsplit
        parsed = urlsplit(str(url))
    except Exception:
        return str(url)[:max_len]

    if parsed.scheme and parsed.netloc:
        netloc = parsed.netloc.rsplit("@", 1)[-1]
        base = f"{parsed.scheme}://{netloc}"
        path = parsed.path or ""
        if path and path != "/":
            basename = path.rsplit("/", 1)[-1]
            safe = f"{base}/.../{basename}" if basename else f"{base}/..."
        else:
            safe = base
    else:
        safe = str(url)

    if len(safe) <= max_len:
        return safe
    return f"{safe[:max_len - 3]}..."

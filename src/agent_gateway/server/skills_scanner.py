"""
Skills scanner for agent-gateway.

Scans agent-specific skills directories, parses SKILL.md frontmatter,
and returns skill metadata matching the desktop client's ``SkillInfo`` interface.

Supported agents:
  - claude-code: ``~/.claude/skills/``
  - pi:          ``~/.pi/agent/skills/``
  - codex:       (no skills concept)
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from agent_gateway.server.session_store import SessionStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Skills directory mapping (agent slug → skills path)
# ---------------------------------------------------------------------------

_SKILLS_DIR_MAP: dict[str, Any] = {
    "claude-code": lambda: Path.home() / ".claude" / "skills",
    "pi": lambda: Path.home() / ".pi" / "agent" / "skills",
    # codex has no skills concept
}

# ---------------------------------------------------------------------------
# Category derivation
# ---------------------------------------------------------------------------

# Prefixes that are too generic to form a meaningful category.
_GENERIC_PREFIXES = frozenset({
    "agent", "api", "brainstorming", "claude", "code", "deep", "experience",
    "fewer", "fiction", "find", "get", "init", "ljg", "loop", "mail",
    "native", "plan", "run", "skill", "simplify", "update", "verify",
})

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---", re.DOTALL)

# TTL cache
_CACHE_TTL = 60  # seconds
_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class SkillEntry:
    """Parsed skill metadata."""
    name: str
    description: str
    category: str
    dir_name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derive_category(name: str) -> str:
    """Derive a category from a skill name.

    Hyphenated names use the prefix before the first hyphen, unless the
    prefix is in the generic set, in which case the category is ``"general"``.
    """
    if "-" not in name:
        return "general"
    prefix = name.split("-", 1)[0]
    return "general" if prefix in _GENERIC_PREFIXES else prefix


def _parse_skill_md(skill_dir: Path) -> SkillEntry | None:
    """Parse a SKILL.md file and return a ``SkillEntry``.

    Returns ``None`` if the file cannot be read or parsed.
    """
    md_path = skill_dir / "SKILL.md"
    dir_name = skill_dir.name

    try:
        content = md_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        logger.debug("Cannot read %s: %s", md_path, exc)
        return None

    # Extract YAML frontmatter
    m = _FM_RE.match(content)
    if m is None:
        logger.debug("No frontmatter in %s, using dir name", md_path)
        return SkillEntry(
            name=dir_name,
            description="",
            category=_derive_category(dir_name),
            dir_name=dir_name,
        )

    try:
        meta = yaml.safe_load(m.group(1))
    except yaml.YAMLError as exc:
        logger.debug("Invalid YAML in %s: %s", md_path, exc)
        return SkillEntry(
            name=dir_name,
            description="",
            category=_derive_category(dir_name),
            dir_name=dir_name,
        )

    if not isinstance(meta, dict):
        meta = {}

    name = str(meta.get("name") or dir_name)
    description = str(meta.get("description") or "")
    # Collapse multi-line descriptions into a single line
    description = " ".join(description.split()).strip()

    return SkillEntry(
        name=name,
        description=description,
        category=_derive_category(name),
        dir_name=dir_name,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scan_skills(
    agent_slug: str,
    store: SessionStore,
    *,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Scan the skills directory for the given agent and return ``SkillInfo`` dicts.

    Results are cached for ``_CACHE_TTL`` seconds unless *force_refresh* is True.
    """
    # Check cache
    now = time.monotonic()
    cached = _cache.get(agent_slug)
    if cached and not force_refresh:
        ts, data = cached
        if now - ts < _CACHE_TTL:
            return data

    # Resolve skills directory
    dir_factory = _SKILLS_DIR_MAP.get(agent_slug)
    if dir_factory is None:
        return []
    skills_dir = dir_factory()

    if not skills_dir.is_dir():
        return []

    # Load disabled set from persisted config
    disabled_list: list[str] = store.get_config("skills_disabled", [])
    disabled_set = set(disabled_list) if disabled_list else set()

    # Scan directory
    results: list[dict[str, Any]] = []
    try:
        entries = list(os.scandir(skills_dir))
    except OSError as exc:
        logger.warning("Cannot scan %s: %s", skills_dir, exc)
        return []

    for entry in entries:
        # Skip hidden dirs and non-directories (follow symlinks)
        if entry.name.startswith("."):
            continue
        if not entry.is_dir(follow_symlinks=True):
            continue

        skill_dir = Path(entry.path)
        # Must contain SKILL.md
        if not (skill_dir / "SKILL.md").is_file():
            continue

        parsed = _parse_skill_md(skill_dir)
        if parsed is None:
            continue

        results.append({
            "name": parsed.name,
            "description": parsed.description,
            "category": parsed.category,
            "enabled": parsed.name not in disabled_set,
        })

    # Sort by name for stable ordering
    results.sort(key=lambda s: s["name"])

    # Update cache
    _cache[agent_slug] = (now, results)
    return results


def toggle_skill(
    name: str,
    enabled: bool,
    store: SessionStore,
) -> dict[str, Any]:
    """Toggle a skill's enabled state and persist the change.

    Returns a dict matching the desktop client's expected response shape.
    """
    disabled_list: list[str] = store.get_config("skills_disabled", [])
    disabled_set = set(disabled_list)

    if enabled:
        disabled_set.discard(name)
    else:
        disabled_set.add(name)

    try:
        store.set_config("skills_disabled", sorted(disabled_set))
    except OSError as exc:
        logger.error("Failed to persist skills_disabled: %s", exc)
        return {"ok": False, "name": name, "enabled": not enabled}

    # Invalidate all caches so the next scan reflects the change
    _cache.clear()

    return {"ok": True, "name": name, "enabled": enabled}

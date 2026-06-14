"""Tests for the skills scanner module."""

from __future__ import annotations

from pathlib import Path

import pytest

from agent_gateway.server.skills_scanner import (
    _derive_category,
    _parse_skill_md,
    scan_skills,
    toggle_skill,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class FakeStore:
    """Minimal SessionStore stand-in with get_config / set_config."""

    def __init__(self, config: dict | None = None):
        self._config = config or {}

    def get_config(self, key: str, default=None):
        return self._config.get(key, default)

    def set_config(self, key: str, value) -> None:
        self._config[key] = value


@pytest.fixture
def fake_store():
    return FakeStore()


@pytest.fixture
def skills_dir(tmp_path):
    """Create a temporary skills directory with mock skills."""
    skills = tmp_path / "skills"
    skills.mkdir()

    # Skill with standard frontmatter
    foo = skills / "foo-tool"
    foo.mkdir()
    (foo / "SKILL.md").write_text(
        "---\nname: foo-tool\ndescription: A foo tool\n---\n\nBody.\n"
    )

    # Skill with multi-line description
    bar = skills / "bar-helper"
    bar.mkdir()
    (bar / "SKILL.md").write_text(
        "---\nname: bar-helper\ndescription: >\n  This is a multi-line\n  description for bar.\n---\n\nBody.\n"
    )

    # Skill without frontmatter
    baz = skills / "plain-skill"
    baz.mkdir()
    (baz / "SKILL.md").write_text("Just some content without frontmatter.\n")

    # Hidden directory (should be skipped)
    hidden = skills / ".hidden-skill"
    hidden.mkdir()
    (hidden / "SKILL.md").write_text("---\nname: hidden\n---\n")

    # Directory without SKILL.md (should be skipped)
    no_md = skills / "no-skill-md"
    no_md.mkdir()

    # Symlink skill
    linked = skills / "linked-skill"
    target = tmp_path / "external-skill"
    target.mkdir()
    (target / "SKILL.md").write_text(
        "---\nname: linked-skill\ndescription: A symlinked skill\n---\n"
    )
    linked.symlink_to(target)

    # Skill with openviking prefix for category testing
    ova = skills / "openviking-analysis"
    ova.mkdir()
    (ova / "SKILL.md").write_text(
        "---\nname: openviking-analysis\ndescription: Analysis tool\n---\n"
    )

    return skills


# ---------------------------------------------------------------------------
# _derive_category
# ---------------------------------------------------------------------------

class TestDeriveCategory:
    def test_no_hyphen(self):
        assert _derive_category("brainstorming") == "general"

    def test_generic_prefix(self):
        assert _derive_category("find-skills") == "general"
        assert _derive_category("get-news") == "general"
        assert _derive_category("mail-send") == "general"
        assert _derive_category("experience-learner") == "general"
        assert _derive_category("native-feel-thing") == "general"

    def test_meaningful_prefix(self):
        assert _derive_category("openviking-analysis") == "openviking"
        assert _derive_category("mcp-builder") == "mcp"
        assert _derive_category("playwright-cli") == "playwright"
        assert _derive_category("mermaid-diagrams") == "mermaid"

    def test_single_hyphen_generic(self):
        assert _derive_category("code-review") == "general"
        assert _derive_category("deep-research") == "general"


# ---------------------------------------------------------------------------
# _parse_skill_md
# ---------------------------------------------------------------------------

class TestParseSkillMd:
    def test_standard_frontmatter(self, tmp_path):
        d = tmp_path / "my-skill"
        d.mkdir()
        (d / "SKILL.md").write_text(
            "---\nname: my-skill\ndescription: A cool skill\n---\n\nBody.\n"
        )
        entry = _parse_skill_md(d)
        assert entry is not None
        assert entry.name == "my-skill"
        assert entry.description == "A cool skill"
        assert entry.dir_name == "my-skill"

    def test_multiline_description(self, tmp_path):
        d = tmp_path / "multi"
        d.mkdir()
        (d / "SKILL.md").write_text(
            "---\nname: multi\ndescription: >\n  Line one.\n  Line two.\n---\n\nBody.\n"
        )
        entry = _parse_skill_md(d)
        assert entry is not None
        assert "Line one." in entry.description
        assert "Line two." in entry.description
        # Should be single-line (collapsed)
        assert "\n" not in entry.description

    def test_no_frontmatter_falls_back_to_dir_name(self, tmp_path):
        d = tmp_path / "fallback-name"
        d.mkdir()
        (d / "SKILL.md").write_text("No frontmatter here.\n")
        entry = _parse_skill_md(d)
        assert entry is not None
        assert entry.name == "fallback-name"
        assert entry.description == ""

    def test_missing_name_uses_dir_name(self, tmp_path):
        d = tmp_path / "dir-name-only"
        d.mkdir()
        (d / "SKILL.md").write_text(
            "---\ndescription: Has desc but no name\n---\n"
        )
        entry = _parse_skill_md(d)
        assert entry is not None
        assert entry.name == "dir-name-only"
        assert entry.description == "Has desc but no name"

    def test_invalid_yaml_falls_back(self, tmp_path):
        d = tmp_path / "bad-yaml"
        d.mkdir()
        (d / "SKILL.md").write_text(
            "---\n: invalid: yaml: [}\n---\n"
        )
        entry = _parse_skill_md(d)
        assert entry is not None
        assert entry.name == "bad-yaml"

    def test_missing_skill_md(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        entry = _parse_skill_md(d)
        assert entry is None

    def test_empty_description(self, tmp_path):
        d = tmp_path / "nodesc"
        d.mkdir()
        (d / "SKILL.md").write_text("---\nname: nodesc\n---\n")
        entry = _parse_skill_md(d)
        assert entry is not None
        assert entry.description == ""


# ---------------------------------------------------------------------------
# scan_skills
# ---------------------------------------------------------------------------

class TestScanSkills:
    def test_scans_directory(self, skills_dir, fake_store, monkeypatch):
        """Patch _SKILLS_DIR_MAP to point at our temp dir."""
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        results = scan_skills("test-agent", fake_store)
        names = [r["name"] for r in results]
        assert "foo-tool" in names
        assert "bar-helper" in names
        assert "plain-skill" in names
        assert "linked-skill" in names
        assert "openviking-analysis" in names
        # Hidden and no-SKILL.md dirs excluded
        assert "hidden" not in names

    def test_hidden_dirs_excluded(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        results = scan_skills("test-agent", fake_store)
        for r in results:
            assert not r["name"].startswith(".")

    def test_symlink_followed(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        results = scan_skills("test-agent", fake_store)
        names = [r["name"] for r in results]
        assert "linked-skill" in names

    def test_unknown_agent_returns_empty(self, fake_store):
        results = scan_skills("nonexistent-agent", fake_store)
        assert results == []

    def test_missing_directory_returns_empty(self, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: Path("/nonexistent/path")},
        )
        mod._cache.clear()

        results = scan_skills("test-agent", fake_store)
        assert results == []

    def test_disabled_state_from_store(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        fake_store.set_config("skills_disabled", ["foo-tool"])

        results = scan_skills("test-agent", fake_store)
        foo = next(r for r in results if r["name"] == "foo-tool")
        assert foo["enabled"] is False

        bar = next(r for r in results if r["name"] == "bar-helper")
        assert bar["enabled"] is True

    def test_category_derived(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        results = scan_skills("test-agent", fake_store)
        by_name = {r["name"]: r for r in results}

        assert by_name["openviking-analysis"]["category"] == "openviking"
        assert by_name["foo-tool"]["category"] == "foo"
        assert by_name["bar-helper"]["category"] == "bar"

    def test_results_sorted_by_name(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        results = scan_skills("test-agent", fake_store)
        names = [r["name"] for r in results]
        assert names == sorted(names)

    def test_cache_returns_same_results(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        r1 = scan_skills("test-agent", fake_store)
        r2 = scan_skills("test-agent", fake_store)
        assert r1 is r2  # same object from cache

    def test_force_refresh_bypasses_cache(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        r1 = scan_skills("test-agent", fake_store)
        r2 = scan_skills("test-agent", fake_store, force_refresh=True)
        assert r1 is not r2  # different objects
        assert r1 == r2  # same data


# ---------------------------------------------------------------------------
# toggle_skill
# ---------------------------------------------------------------------------

class TestToggleSkill:
    def test_disable_skill(self, fake_store):
        mod = __import__(
            "agent_gateway.server.skills_scanner", fromlist=["_cache"]
        )
        mod._cache.clear()

        result = toggle_skill("my-skill", False, fake_store)
        assert result == {"ok": True, "name": "my-skill", "enabled": False}
        assert "my-skill" in fake_store.get_config("skills_disabled", [])

    def test_enable_skill(self, fake_store):
        mod = __import__(
            "agent_gateway.server.skills_scanner", fromlist=["_cache"]
        )
        mod._cache.clear()

        fake_store.set_config("skills_disabled", ["my-skill", "other"])
        result = toggle_skill("my-skill", True, fake_store)
        assert result == {"ok": True, "name": "my-skill", "enabled": True}
        assert "my-skill" not in fake_store.get_config("skills_disabled", [])
        assert "other" in fake_store.get_config("skills_disabled", [])

    def test_cache_cleared_on_toggle(self, skills_dir, fake_store, monkeypatch):
        from agent_gateway.server import skills_scanner as mod

        monkeypatch.setattr(
            mod, "_SKILLS_DIR_MAP",
            {"test-agent": lambda: skills_dir},
        )
        mod._cache.clear()

        # Populate cache
        scan_skills("test-agent", fake_store)
        assert mod._cache.get("test-agent") is not None

        # Toggle should clear cache
        toggle_skill("foo-tool", False, fake_store)
        assert mod._cache == {}

    def test_persisted_sorted(self, fake_store):
        mod = __import__(
            "agent_gateway.server.skills_scanner", fromlist=["_cache"]
        )
        mod._cache.clear()

        toggle_skill("zebra", False, fake_store)
        toggle_skill("alpha", False, fake_store)
        disabled = fake_store.get_config("skills_disabled", [])
        assert disabled == sorted(disabled)

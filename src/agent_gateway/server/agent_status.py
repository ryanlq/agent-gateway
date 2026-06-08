"""
Agent CLI detection.

Checks whether each supported agent CLI is installed and reachable,
returning status info for display in the desktop UI.
"""

from __future__ import annotations

import logging
import shutil
from typing import Any

logger = logging.getLogger(__name__)

# Agent metadata registry
_AGENT_META: dict[str, dict[str, Any]] = {
    "claude-code": {
        "name": "Claude Code",
        "description": "Anthropic's coding agent. Uses Claude Sonnet / Opus models.",
        "cli_command": "claude",
        "docs_url": "https://docs.anthropic.com/en/docs/claude-code",
        "install_hint": "npm install -g @anthropic-ai/claude-code",
        "params": [
            {
                "key": "model",
                "label": "Model",
                "type": "select",
                "options": ["claude-sonnet-4-6", "claude-opus-4-8", "claude-haiku-4-5-20251001"],
                "default": "claude-sonnet-4-6",
                "description": "Claude model to use for responses.",
            },
        ],
    },
    "pi": {
        "name": "Pi Agent",
        "description": "Nous Research's Pi agent. Supports print, json, and rpc modes.",
        "cli_command": "pi",
        "docs_url": "",
        "install_hint": "pip install pi-agent",
        "params": [
            {
                "key": "mode",
                "label": "Mode",
                "type": "select",
                "options": ["print", "json", "rpc"],
                "default": "json",
                "description": "Pi agent communication mode.",
            },
        ],
    },
    "codex": {
        "name": "OpenAI Codex",
        "description": "OpenAI's Codex CLI coding agent.",
        "cli_command": "codex",
        "docs_url": "https://github.com/openai/codex",
        "install_hint": "npm install -g @openai/codex",
        "params": [
            {
                "key": "approval_mode",
                "label": "Approval Mode",
                "type": "select",
                "options": ["suggest", "auto-edit", "full-auto"],
                "default": "suggest",
                "description": "Codex approval mode for tool calls.",
            },
        ],
    },
}


def detect_agents() -> list[dict[str, Any]]:
    """Detect installed agent CLIs and return status info.

    Returns a list of agent descriptors, each with:
      - slug, name, description, docs_url
      - installed (bool), cli_path (str or None)
    """
    results: list[dict[str, Any]] = []

    for slug, meta in _AGENT_META.items():
        cli_cmd = meta["cli_command"]
        cli_path = shutil.which(cli_cmd)

        results.append({
            "slug": slug,
            "name": meta["name"],
            "description": meta["description"],
            "docs_url": meta["docs_url"],
            "install_hint": meta.get("install_hint", ""),
            "params": meta.get("params", []),
            "installed": cli_path is not None,
            "cli_path": cli_path,
        })

    return results


def get_installed_agent_types() -> list[str]:
    """Return slugs of installed agents."""
    return [a["slug"] for a in detect_agents() if a["installed"]]

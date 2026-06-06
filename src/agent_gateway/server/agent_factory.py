"""
Agent bridge factory.

Creates the appropriate ``CLIAgentBridge`` subclass based on agent type.
"""

from __future__ import annotations

import logging

from agent_gateway.agents.base import CLIAgentBridge

logger = logging.getLogger(__name__)

# Lazy imports to avoid hard dependency on all bridges
_AGENT_REGISTRY: dict[str, str] = {
    "claude-code": "agent_gateway.agents.claude_code:ClaudeCodeBridge",
    "pi": "agent_gateway.agents.pi_agent:PiAgentBridge",
    "codex": "agent_gateway.agents.codex:CodexBridge",
}


def create_bridge(agent_type: str, **kwargs: object) -> CLIAgentBridge:
    """Create a bridge instance for the given agent type.

    Parameters
    ----------
    agent_type :
        One of ``"claude-code"``, ``"pi"``, ``"codex"``.
    **kwargs :
        Forwarded to the bridge constructor.

    Returns
    -------
    CLIAgentBridge
    """
    entry = _AGENT_REGISTRY.get(agent_type)
    if entry is None:
        available = ", ".join(sorted(_AGENT_REGISTRY))
        raise ValueError(f"Unknown agent type '{agent_type}'. Available: {available}")

    module_path, class_name = entry.rsplit(":", 1)
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    bridge = cls(**kwargs)
    logger.info("Created %s bridge for agent type '%s'", class_name, agent_type)
    return bridge

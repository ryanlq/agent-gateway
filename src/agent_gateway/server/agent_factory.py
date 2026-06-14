"""
Agent bridge factory.

Creates the appropriate ``CLIAgentBridge`` subclass based on agent type.
"""

from __future__ import annotations

import inspect
import logging

from agent_gateway.agents.base import CLIAgentBridge

logger = logging.getLogger(__name__)

# Lazy imports to avoid hard dependency on all bridges
_AGENT_REGISTRY: dict[str, str] = {
    "claude-code": "agent_gateway.agents.claude_code:ClaudeCodeBridge",
    "pi": "agent_gateway.agents.pi_agent:PiAgentBridge",
}


def _coerce_params(cls: type, kwargs: dict) -> dict:
    """Coerce string param values to expected types based on constructor signature.

    The UI sends all param values as strings (e.g. ``"true"``, ``"false"``),
    but bridge constructors expect native Python types (e.g. ``bool``).
    """
    sig = inspect.signature(cls.__init__)
    coerced = dict(kwargs)
    for key, param in sig.parameters.items():
        if key not in coerced:
            continue
        ann = param.annotation
        # With ``from __future__ import annotations``, annotations are strings.
        ann_name = ann if isinstance(ann, str) else getattr(ann, "__name__", "")
        val = coerced[key]
        if ann_name == "bool" and isinstance(val, str):
            coerced[key] = val.lower() in ("true", "1", "yes")
        elif ann_name == "int" and isinstance(val, str):
            try:
                coerced[key] = int(val)
            except ValueError:
                pass
        elif ann_name == "float" and isinstance(val, str):
            try:
                coerced[key] = float(val)
            except ValueError:
                pass
    return coerced


def create_bridge(agent_type: str, **kwargs: object) -> CLIAgentBridge:
    """Create a bridge instance for the given agent type.

    Parameters
    ----------
    agent_type :
        One of ``"claude-code"``, ``"pi"``.
    **kwargs :
        Forwarded to the bridge constructor (with automatic type coercion).

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
    coerced = _coerce_params(cls, kwargs)
    bridge = cls(**coerced)
    logger.info("Created %s bridge for agent type '%s' (params=%s)", class_name, agent_type, coerced)
    return bridge

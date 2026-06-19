"""
Structured agent event protocol.

Bridges yield :class:`AgentEvent` instances instead of raw strings so the
runner can dispatch each event to the correct channel:

  - ``text_delta``     → platform adapter (message.delta) + desktop message
  - ``reasoning_delta``→ desktop reasoning panel (reasoning.delta); NOT to
                         chat platforms (would spam them)
  - ``tool_start``     → desktop tool card (tool.start) + optional platform
                         "🛠️ 调用工具: **X**" marker
  - ``tool_complete``  → desktop tool card closure (tool.complete); silent on
                         chat platforms

Backward compatibility: bridges that still yield ``str`` are auto-wrapped as
``AgentEvent.text_delta(text)`` by the runner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AgentEvent:
    """A typed event produced by an agent bridge during streaming.

    The runner inspects :attr:`kind` and routes the event to the right sink:
    platform adapter, desktop client, or both.
    """

    kind: str
    """One of ``"text_delta"``, ``"reasoning_delta"``, ``"tool_start"``,
    ``"tool_complete"``."""

    text: str = ""
    """Text payload (used by ``text_delta`` and ``reasoning_delta``)."""

    tool_name: str = ""
    """Tool name (used by ``tool_start`` / ``tool_complete``)."""

    tool_id: str = ""
    """Tool call id (used by ``tool_start`` / ``tool_complete``)."""

    tool_input: dict[str, Any] = field(default_factory=dict)
    """Tool input arguments (``tool_start``)."""

    tool_result: Any = None
    """Tool output (``tool_complete``)."""

    is_error: bool = False
    """If True, the tool call failed (``tool_complete``)."""

    error_message: str = ""
    """Human-readable failure reason (``tool_complete`` when ``is_error``)."""

    # -- Convenience constructors -----------------------------------------

    @classmethod
    def text_delta(cls, text: str) -> "AgentEvent":
        return cls(kind="text_delta", text=text)

    @classmethod
    def reasoning_delta(cls, text: str) -> "AgentEvent":
        return cls(kind="reasoning_delta", text=text)

    @classmethod
    def tool_start(
        cls,
        name: str,
        tool_id: str,
        tool_input: dict[str, Any] | None = None,
    ) -> "AgentEvent":
        return cls(
            kind="tool_start",
            tool_name=name,
            tool_id=tool_id,
            tool_input=tool_input or {},
        )

    @classmethod
    def tool_complete(
        cls,
        name: str,
        tool_id: str,
        result: Any = None,
        *,
        is_error: bool = False,
        error_message: str = "",
    ) -> "AgentEvent":
        return cls(
            kind="tool_complete",
            tool_name=name,
            tool_id=tool_id,
            tool_result=result,
            is_error=is_error,
            error_message=error_message,
        )

"""
Structured stream event types for the gateway.

These events flow from the AI agent to the gateway's delivery layer,
allowing adapters to render streaming updates in platform-native ways.

Event types:
  - ``MessageChunk``   — incremental text delta
  - ``MessageStop``    — text segment boundary
  - ``ToolCallChunk``  — tool invocation progress
  - ``ToolCallFinished`` — tool call completed
  - ``Commentary``     — agent commentary / thinking
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Union


@dataclass(frozen=True)
class MessageChunk:
    """Incremental text produced by the agent."""

    text: str
    """The new text fragment."""

    segment_index: int = 0
    """Which text segment this belongs to (0 = main response)."""


@dataclass(frozen=True)
class MessageStop:
    """Text segment boundary or final stop."""

    final: bool = False
    """True when the entire response is complete."""


@dataclass(frozen=True)
class ToolCallChunk:
    """Progressive information about a tool invocation."""

    tool_name: str
    """Name of the tool being called."""

    tool_call_id: str = ""
    """Unique ID for this tool call."""

    args: Optional[dict[str, Any]] = None
    """Parsed tool arguments."""

    preview: str = ""
    """Short human-readable preview of arguments."""

    index: int = 0
    """Position in the tool call sequence."""


@dataclass(frozen=True)
class ToolCallFinished:
    """A tool call has completed."""

    tool_name: str
    tool_call_id: str = ""

    result_preview: str = ""
    """Short preview of the tool result."""

    success: bool = True


@dataclass(frozen=True)
class Commentary:
    """Agent commentary / thinking output."""

    text: str
    """Commentary text."""


# Union type for all stream events
StreamEvent = Union[MessageChunk, MessageStop, ToolCallChunk, ToolCallFinished, Commentary]

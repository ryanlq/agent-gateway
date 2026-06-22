"""
CLI Agent Bridges — wrap subprocess-based AI agent tools into the
gateway's agent interface.

Bridges available:

  - ``ClaudeCodeSdkBridge`` — Anthropic Claude Code via official Python SDK
  - ``PiAgentBridge`` — Pi Agent RPC mode (``pi --mode rpc``)

Usage::

    from agent_gateway.agents import ClaudeCodeSdkBridge

    bridge = ClaudeCodeSdkBridge(model="claude-sonnet-4-6")
    runner = GatewayRunner(config, agent=bridge)
"""

from agent_gateway.agents.events import AgentEvent
from agent_gateway.agents.base import (
    CLIAgentBridge,
    CLIAgentError,
    CLICrashError,
    CLIConnectionError,
    CLIOutputTooLargeError,
    CLIParseError,
    CLITimeoutError,
    PooledProcess,
    SubprocessConfig,
    SubprocessPool,
)
from agent_gateway.agents.claude_code_sdk import ClaudeCodeSdkBridge
from agent_gateway.agents.pi_agent import PiAgentBridge

__all__ = [
    "CLIAgentBridge",
    "ClaudeCodeSdkBridge",
    "PiAgentBridge",
    "SubprocessConfig",
    "SubprocessPool",
    "PooledProcess",
    "CLIAgentError",
    "CLICrashError",
    "CLIConnectionError",
    "CLIOutputTooLargeError",
    "CLIParseError",
    "CLITimeoutError",
    "AgentEvent",
]

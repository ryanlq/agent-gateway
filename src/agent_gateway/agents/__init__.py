"""
CLI Agent Bridges — wrap subprocess-based AI agent tools into the
gateway's agent interface.

Bridges available:

  - ``ClaudeCodeBridge`` — Anthropic Claude Code CLI (``claude --print``)
  - ``PiAgentBridge`` — Pi Agent RPC mode (``pi --mode rpc``)

Usage::

    from agent_gateway.agents import ClaudeCodeBridge

    bridge = ClaudeCodeBridge(model="claude-sonnet-4-6")
    runner = GatewayRunner(config, agent=bridge)
    # or: runner = GatewayRunner(config, agent_callback=bridge.as_callback())
"""

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
from agent_gateway.agents.claude_code import ClaudeCodeBridge
from agent_gateway.agents.pi_agent import PiAgentBridge

__all__ = [
    # Bridges
    "CLIAgentBridge",
    "ClaudeCodeBridge",
    "PiAgentBridge",
    # Infrastructure
    "SubprocessConfig",
    "SubprocessPool",
    "PooledProcess",
    # Exceptions
    "CLIAgentError",
    "CLICrashError",
    "CLIConnectionError",
    "CLIOutputTooLargeError",
    "CLIParseError",
    "CLITimeoutError",
]

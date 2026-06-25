"""Tests for ``agent_factory._coerce_params`` — the string→typed coercion that
turns client ``agent_params`` into bridge constructor args.

Focus: the "unlimited" sentinel. The desktop agent-settings UI sends all param
values as strings; an empty string (or ``none`` / ``unlimited``) means the user
flipped an "Unlimited" toggle and must reach the bridge as ``None`` — not as a
broken ``""`` that would crash ``int()`` / the SDK. ``None`` is what makes
``timeout`` deadline-less (``asyncio.wait_for(timeout=None)``) and ``max_turns``
run to natural completion.
"""

from agent_gateway.agents.claude_code_sdk import ClaudeCodeSdkBridge
from agent_gateway.server.agent_factory import _coerce_params, create_bridge


def test_coerce_int_string():
    assert _coerce_params(ClaudeCodeSdkBridge, {"max_turns": "20"})["max_turns"] == 20


def test_coerce_float_string():
    assert _coerce_params(ClaudeCodeSdkBridge, {"timeout": "1200"})["timeout"] == 1200.0


def test_coerce_bool_string():
    assert _coerce_params(ClaudeCodeSdkBridge, {"bare": "true"})["bare"] is True
    assert _coerce_params(ClaudeCodeSdkBridge, {"bare": "false"})["bare"] is False


def test_coerce_empty_string_is_none():
    out = _coerce_params(ClaudeCodeSdkBridge, {"max_turns": "", "timeout": ""})
    assert out["max_turns"] is None
    assert out["timeout"] is None


def test_coerce_unlimited_sentinels_are_none():
    for sent in ("none", "None", "unlimited", "UNLIMITED", "  "):
        out = _coerce_params(ClaudeCodeSdkBridge, {"max_turns": sent})
        assert out["max_turns"] is None, sent


def test_coerce_bad_int_left_as_string():
    # Non-numeric, non-sentinel: unchanged (preserves existing lenient behavior).
    assert _coerce_params(ClaudeCodeSdkBridge, {"max_turns": "soon"})["max_turns"] == "soon"


def test_create_bridge_unlimited_reaches_bridge_as_none():
    # End-to-end: the unlimited sentinel flows through coercion into the bridge
    # as None for both max_turns and timeout.
    bridge = create_bridge("claude-code-sdk", max_turns="", timeout="")
    assert bridge.max_turns is None
    assert bridge.config.timeout is None  # None → no asyncio deadline = unlimited


def test_create_bridge_numeric_timeout_reaches_bridge():
    bridge = create_bridge("claude-code-sdk", timeout="900")
    assert bridge.config.timeout == 900.0

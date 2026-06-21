"""
Agent Gateway — A reusable multi-platform messaging gateway for AI agents.

Provides a unified adapter interface to connect any AI agent to 20+ messaging
platforms (Telegram, Discord, Slack, WeChat, etc.) with session management,
streaming responses, and cross-platform delivery routing.

Quick start::

    from agent_gateway import GatewayRunner, GatewayConfig, registry
    from agent_gateway.adapters.telegram import register_telegram

    # Register platforms
    register_telegram()

    # Load config & run
    config = GatewayConfig.load("gateway.yaml")
    runner = GatewayRunner(config, my_agent)
    await runner.start()
"""

from agent_gateway.core.message import (
    MessageEvent,
    MessageSource,
    MessageType,
    SendResult,
    EphemeralReply,
)
from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.session import Session, SessionStore
from agent_gateway.core.registry import PlatformEntry, PlatformRegistry, registry
from agent_gateway.core.delivery import DeliveryTarget, DeliveryRouter
from agent_gateway.core.config import GatewayConfig, PlatformConfig
from agent_gateway.core.runner import GatewayRunner
from agent_gateway.core.stream import StreamConsumer

__all__ = [
    # Message types
    "MessageEvent",
    "MessageSource",
    "MessageType",
    "SendResult",
    "EphemeralReply",
    # Adapter
    "BasePlatformAdapter",
    # Session
    "Session",
    "SessionStore",
    # Registry
    "PlatformEntry",
    "PlatformRegistry",
    "registry",
    # Delivery
    "DeliveryTarget",
    "DeliveryRouter",
    # Config
    "GatewayConfig",
    "PlatformConfig",
    # Runner
    "GatewayRunner",
    # Stream
    "StreamConsumer",
]

__version__ = "0.4.5"

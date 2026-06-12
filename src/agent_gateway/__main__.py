"""
Entry point for ``python -m agent_gateway``.

Starts a FastAPI + WebSocket JSON-RPC server that nexus-agent
connects to for agent chat sessions.  If ``~/.nexus-agent/gateway.yaml``
configures platform adapters (Email, Telegram, etc.), they are started
alongside the desktop server.

Usage::

    AGENT_GATEWAY_SESSION_TOKEN=mytoken python -m agent_gateway --port 9119
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import secrets
import sys
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Directory for persistent data and config
_NEXUS_AGENT_DIR = Path.home() / ".nexus-agent"


# ---------------------------------------------------------------------------
# Built-in adapter registration
# ---------------------------------------------------------------------------

def register_builtin_adapters() -> None:
    """Register all built-in platform adapters with the global registry."""
    from agent_gateway.adapters.email import register_email
    register_email()

    # Register other built-in adapters (gracefully skipped if deps missing)
    for _name, _module in [
        ("telegram", "agent_gateway.adapters.telegram"),
        ("discord", "agent_gateway.adapters.discord"),
        ("slack", "agent_gateway.adapters.slack"),
        ("webhook", "agent_gateway.adapters.webhook"),
    ]:
        try:
            mod = __import__(_module, fromlist=[f"register_{_name}"])
            getattr(mod, f"register_{_name}")()
        except ImportError:
            logger.debug("Adapter '%s' not available (missing deps)", _name)
        except Exception as exc:
            logger.warning("Failed to register adapter '%s': %s", _name, exc)


# ---------------------------------------------------------------------------
# Agent callback — bridges GatewayRunner → CLIAgentBridge
# ---------------------------------------------------------------------------

def make_agent_callback(*, agent_timeout: float = 1800.0) -> Any:
    """Create an async agent callback for the ``GatewayRunner``.

    The callback creates a ``CLIAgentBridge`` (claude-code / pi / codex)
    based on the default agent type stored in ``~/.nexus-agent/gateway-config.json``
    and calls it to produce a response.
    """
    from agent_gateway.server.session_store import SessionStore

    # Share a single SessionStore to read default_agent without re-reading
    # the JSON file on every message.
    _store = SessionStore()

    async def _callback(
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str = "",
        **kw: Any,
    ) -> str:
        from agent_gateway.server.agent_factory import create_bridge

        agent_type = _store.get_config("default_agent", "claude-code")
        bridge = create_bridge(agent_type, timeout=agent_timeout)
        try:
            chunks: list[str] = []
            async for chunk in bridge.stream(
                session_key=session_key,
                message=message,
                history=history,
                system_extra=system_extra,
            ):
                chunks.append(chunk)
            return "".join(chunks)
        except Exception as exc:
            logger.error("Agent callback error: %s", exc)
            return f"⚠️ Agent error: {exc}"
        finally:
            try:
                await asyncio.wait_for(bridge.shutdown(), timeout=5.0)
            except Exception:
                pass

    return _callback


# ---------------------------------------------------------------------------
# Platform gateway initialisation
# ---------------------------------------------------------------------------

def try_create_runner() -> Optional[Any]:
    """Load ``~/.nexus-agent/gateway.yaml`` and create a ``GatewayRunner``.

    Returns ``None`` if no config file exists or no platforms are enabled.
    """
    config_path = _NEXUS_AGENT_DIR / "gateway.yaml"
    if not config_path.exists():
        logger.info("No %s found — skipping platform adapters", config_path)
        return None

    try:
        from agent_gateway.core.config import GatewayConfig
        from agent_gateway.core.runner import GatewayRunner
    except ImportError as exc:
        logger.warning("Gateway runner dependencies not available: %s", exc)
        return None

    gw_config = GatewayConfig.load(config_path)
    enabled = gw_config.enabled_platforms()
    if not enabled:
        logger.info("No enabled platforms in %s", config_path)
        return None

    logger.info(
        "Enabled platform adapters: %s",
        ", ".join(f"{name} (✅)" for name in enabled),
    )

    # Register adapters so the runner can find them
    register_builtin_adapters()

    # Create runner with an agent callback that uses the desktop's bridge system
    runner = GatewayRunner(
        gw_config,
        agent_callback=make_agent_callback(agent_timeout=gw_config.agent_timeout),
        desktop_store=None,  # Will be set in main() after sharing with app
    )
    return runner


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="agent_gateway",
        description="Agent Gateway server for nexus-agent integration",
    )
    parser.add_argument("--port", type=int, default=9119, help="Listen port (default: 9119)")
    parser.add_argument("--host", default="127.0.0.1", help="Listen host (default: 127.0.0.1)")
    args = parser.parse_args()

    # Token auth: read from env or generate a random one
    token = os.environ.get("AGENT_GATEWAY_SESSION_TOKEN", "")
    if not token:
        token = secrets.token_urlsafe(32)
        print(f"[agent-gateway] Generated session token: {token}", file=sys.stderr)

    try:
        import uvicorn
    except ImportError:
        print(
            "Error: uvicorn is required for the server. Install with:\n"
            '  pip install "agent-gateway[desktop]"',
            file=sys.stderr,
        )
        sys.exit(1)

    from agent_gateway.server.app import create_app

    # Try to set up platform adapters (email, telegram, etc.)
    runner = try_create_runner()

    app = create_app(token, runner=runner)

    # Share the desktop session store with the runner so platform
    # conversations (email, etc.) are written to the same store
    # that the desktop server reads from.
    if runner:
        from agent_gateway.server.session_store import SessionStore as DesktopStore
        store = app.state.desktop_store  # The store created inside create_app
        runner._desktop_store = store

    print(f"[agent-gateway] Starting server on {args.host}:{args.port}", file=sys.stderr)

    # Configure uvicorn with graceful shutdown
    config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_level="warning",
        timeout_graceful_shutdown=10,  # Give bridges 10s to clean up
    )
    server = uvicorn.Server(config)

    try:
        asyncio.run(server.serve())
    except KeyboardInterrupt:
        pass  # Server handles cleanup via lifespan


if __name__ == "__main__":
    main()

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

from agent_gateway.utils.paths import migrate_legacy_agent_gateway_home, resolve_home

logger = logging.getLogger(__name__)

# Directory for persistent data and config (derived from the central resolver
# so NEXUS_AGENT_HOME overrides everything at once).
_NEXUS_AGENT_DIR = resolve_home()


# ---------------------------------------------------------------------------
# File logging
# ---------------------------------------------------------------------------

def _resolve_log_dir() -> Path:
    """Gateway log directory: ``<home>/logs`` (see :func:`resolve_home`)."""
    return resolve_home() / "logs"


LOG_DIR = _resolve_log_dir()
_GATEWAY_LOG_PATH = LOG_DIR / "gateway.log"


def setup_file_logging() -> None:
    """Direct Python logging output to ``gateway.log`` in the shared log dir.

    The desktop's ``/api/logs`` endpoint reads this file to display gateway
    logs in the command center UI.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(_GATEWAY_LOG_PATH, encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)


# ---------------------------------------------------------------------------
# Built-in adapter registration
# ---------------------------------------------------------------------------

def register_builtin_adapters() -> None:
    """Register all built-in platform adapters with the global registry."""
    from agent_gateway.adapters.email import register_email
    register_email()

    # Register other built-in adapters. In a dev install an ImportError just
    # means an optional extra isn't installed (debug). In a frozen build,
    # though, every builtin adapter's SDK should be frozen — a missing one is a
    # real defect (the CI ``--check-adapters`` smoke test should have caught it
    # pre-release), so log it loudly instead of silently dropping the platform.
    _frozen = getattr(sys, "frozen", False)
    for _name, _module in [
        ("telegram", "agent_gateway.adapters.telegram"),
        ("discord", "agent_gateway.adapters.discord"),
        ("slack", "agent_gateway.adapters.slack"),
        ("feishu", "agent_gateway.adapters.feishu"),
        ("webhook", "agent_gateway.adapters.webhook"),
    ]:
        try:
            mod = __import__(_module, fromlist=[f"register_{_name}"])
            getattr(mod, f"register_{_name}")()
        except ImportError:
            if _frozen:
                logger.error(
                    "Adapter '%s' unavailable in frozen build — its SDK failed "
                    "to freeze; this should have been caught by --check-adapters",
                    _name,
                )
            else:
                logger.debug("Adapter '%s' not available (missing deps)", _name)
        except Exception as exc:
            logger.warning("Failed to register adapter '%s': %s", _name, exc)


def run_adapter_check() -> int:
    """Import every built-in adapter and verify its SDK imports in this env.

    Returns 0 when all adapters are importable and report their dependencies as
    installed, 1 otherwise. Exposed via the ``--check-adapters`` CLI so CI can
    smoke-test the freshly-built frozen binary: the shipped sidecar has no
    runtime pip, so an SDK that failed to freeze surfaces here as a hard
    failure instead of as a "Dependencies not installed" error in production.
    """
    # (name, module, deps-check function). Adapter modules import their SDK
    # lazily (inside _check_*_deps / connect), so importing the module itself
    # always succeeds — the deps function is what actually probes the SDK.
    checks = [
        ("email", "agent_gateway.adapters.email", "_check_email_deps"),
        ("telegram", "agent_gateway.adapters.telegram", "_check_telegram_deps"),
        ("discord", "agent_gateway.adapters.discord", "_check_discord_deps"),
        ("slack", "agent_gateway.adapters.slack", "_check_slack_deps"),
        ("feishu", "agent_gateway.adapters.feishu", "_check_feishu_deps"),
        ("webhook", "agent_gateway.adapters.webhook", "_check_webhook_deps"),
    ]
    print("[agent-gateway] verifying built-in adapter SDKs...", file=sys.stderr)
    failures: list[str] = []
    for name, module_path, deps_attr in checks:
        try:
            mod = __import__(module_path, fromlist=[deps_attr])
            deps_fn = getattr(mod, deps_attr, None)
            ok = bool(deps_fn()) if callable(deps_fn) else False
        except Exception as exc:  # noqa: BLE001 — any import-time failure counts
            print(f"  ✗ {name:<9} import failed: {exc}", file=sys.stderr)
            failures.append(name)
            continue
        if ok:
            print(f"  ✓ {name:<9} ok", file=sys.stderr)
        else:
            print(f"  ✗ {name:<9} dependencies not installed", file=sys.stderr)
            failures.append(name)
    if failures:
        print(
            f"[agent-gateway] adapter check FAILED for: {', '.join(failures)}",
            file=sys.stderr,
        )
        return 1
    print("[agent-gateway] all built-in adapter SDKs present", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# Agent callback — bridges GatewayRunner → CLIAgentBridge
# ---------------------------------------------------------------------------

def make_agent_callback(*, agent_timeout: float = 1800.0) -> Any:
    """Create an async agent callback for the ``GatewayRunner``.

    The callback creates a ``CLIAgentBridge`` (claude-code / pi)
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

        agent_type = _store.get_config("default_agent", "claude-code-sdk")
        # Forward per-agent params (model, max_turns, timeout, …) so client
        # settings actually take effect on this path. Per-agent timeout
        # (client "Unlimited" → "") overrides the global agent_timeout.
        all_params: dict = _store.get_config("agent_params") or {}
        agent_params = all_params.get(agent_type, {}) if isinstance(all_params, dict) else {}
        agent_params.setdefault("timeout", agent_timeout)
        bridge = create_bridge(agent_type, **agent_params)
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
    parser.add_argument(
        "--check-adapters",
        action="store_true",
        help="Verify every built-in adapter SDK imports, then exit "
        "(CI smoke-tests the frozen binary with this).",
    )
    args = parser.parse_args()

    if args.check_adapters:
        # Smoke-test mode: exit before logging/server setup so CI gets a clean
        # pass/fail from the freshly-built frozen binary.
        sys.exit(run_adapter_check())

    setup_file_logging()
    # Move the old ~/.agent_gateway runtime dirs into the unified home before
    # any adapter/state/cache path is created (idempotent; no-op when done).
    migrate_legacy_agent_gateway_home()

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

    # Create a shared CronManager for both the runner and the REST API.
    # This allows agent responses to create cron jobs (via cron_tool protocol)
    # and the REST endpoints to manage the same jobs.
    cron_manager = None
    try:
        from agent_gateway.cron.manager import CronManager
        from agent_gateway.server.session_store import SessionStore as _Store
        cron_manager = CronManager(_Store(), runner=runner)
    except Exception as exc:
        logger.warning("Failed to create CronManager: %s", exc)

    # Share CronManager with the runner so it can process cron operations
    # in agent responses and handle /cron slash commands.
    if runner and cron_manager:
        runner.cron_manager = cron_manager

    app = create_app(token, runner=runner, cron_manager=cron_manager)

    # Share the desktop session store with the runner so platform
    # conversations (email, etc.) are written to the same store
    # that the desktop server reads from.
    if runner:
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

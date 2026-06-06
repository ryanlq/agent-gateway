"""
Entry point for ``python -m agent_gateway``.

Starts a FastAPI + WebSocket JSON-RPC server that hermes-desktop
connects to for agent chat sessions.

Usage::

    AGENT_GATEWAY_SESSION_TOKEN=mytoken python -m agent_gateway --port 9119
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="agent_gateway",
        description="Agent Gateway server for hermes-desktop integration",
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

    app = create_app(token)
    print(f"[agent-gateway] Starting server on {args.host}:{args.port}", file=sys.stderr)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

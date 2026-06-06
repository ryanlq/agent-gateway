#!/usr/bin/env python3
"""
Custom adapter example — create a new platform adapter.

This demonstrates how to add support for any messaging platform
by implementing just three abstract methods.
"""

import asyncio
import logging
from typing import Any, Optional

from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.message import (
    ChatType,
    MessageEvent,
    MessageSource,
    MessageType,
    SendResult,
)
from agent_gateway.core.registry import PlatformEntry, registry

logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Step 1: Define your adapter
# ---------------------------------------------------------------------------

class ConsoleAdapter(BasePlatformAdapter):
    """
    A simple console adapter that reads from stdin and writes to stdout.

    Useful for testing and development.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._name = "Console"
        self._prompt = config.get("prompt", "You> ")
        self._reader_task: Optional[asyncio.Task] = None

    # -- Required methods ----------------------------------------------------

    async def connect(self) -> bool:
        """Start reading from stdin."""
        self._reader_task = asyncio.create_task(self._read_loop())
        self._mark_connected()
        print("✅ Console adapter connected. Type your messages below.")
        return True

    async def disconnect(self) -> None:
        """Stop reading."""
        if self._reader_task:
            self._reader_task.cancel()
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Print the agent's response to stdout."""
        print(f"\n🤖 Agent: {content}\n")
        return SendResult(success=True, message_id=f"console-{id(content)}")

    # -- Optional overrides --------------------------------------------------

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        print("⏳ Agent is typing...")

    # -- Internal ------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Read lines from stdin and dispatch as messages."""
        loop = asyncio.get_running_loop()

        source = MessageSource(
            platform="console",
            user_id="user",
            chat_id="console",
            chat_type=ChatType.DM,
            display_name="User",
        )

        while True:
            try:
                # Read from stdin (blocking, run in executor)
                line = await loop.run_in_executor(None, lambda: input(self._prompt))
                if not line.strip():
                    continue

                event = MessageEvent(
                    text=line,
                    message_type=MessageType.TEXT,
                    source=source,
                )

                await self.handle_message(event)

            except EOFError:
                break
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logging.error("Console read error: %s", exc)


# ---------------------------------------------------------------------------
# Step 2: Register your adapter
# ---------------------------------------------------------------------------

def register_console() -> None:
    """Register the Console adapter."""
    registry.register(PlatformEntry(
        name="console",
        label="Console",
        adapter_factory=lambda cfg: ConsoleAdapter(cfg),
        check_fn=lambda: True,  # No external dependencies
        emoji="💻",
        platform_hint="You are responding in a terminal console.",
        source="builtin",
    ))


# ---------------------------------------------------------------------------
# Step 3: Use it!
# ---------------------------------------------------------------------------

async def my_agent(session_key: str, message: str, history: list, **kw) -> str:
    """Simple agent that reverses the input."""
    return f"You said: {message[::-1]}"


async def main():
    from agent_gateway import GatewayConfig, GatewayRunner

    register_console()

    config = GatewayConfig()
    config.platforms["console"] = type(config.platforms["console"])(
        enabled=True, token="", extra={"prompt": "You> "},
    )

    runner = GatewayRunner(config, agent_callback=my_agent)
    await runner.start()
    await runner.wait_for_shutdown()


if __name__ == "__main__":
    asyncio.run(main())

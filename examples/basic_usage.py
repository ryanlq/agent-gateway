#!/usr/bin/env python3
"""
Basic usage example — connect an AI agent to Telegram.

Prerequisites:
    pip install agent-gateway[telegram]

Set your bot token:
    export TELEGRAM_TOKEN="your-bot-token"

Then run:
    python basic_usage.py
"""

import asyncio
import logging

from agent_gateway import GatewayConfig, GatewayRunner
from agent_gateway.adapters.telegram import register_telegram

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


# ---------------------------------------------------------------------------
# Define your AI agent — this can be any async function
# ---------------------------------------------------------------------------

async def my_agent_callback(
    session_key: str,
    message: str,
    history: list[dict],
    **kwargs,
) -> str:
    """
    This is where your AI agent logic goes.

    For demo purposes, we just echo the message back.
    Replace this with calls to OpenAI, Anthropic, local LLM, etc.
    """
    # Example: simple echo bot
    return f"Echo: {message}"

    # Example: OpenAI integration
    # import openai
    # client = openai.AsyncOpenAI()
    # response = await client.chat.completions.create(
    #     model="gpt-4o",
    #     messages=[{"role": "system", "content": "You are a helpful assistant."}] + history,
    # )
    # return response.choices[0].message.content

    # Example: Anthropic integration
    # import anthropic
    # client = anthropic.AsyncAnthropic()
    # response = await client.messages.create(
    #     model="claude-sonnet-4-6",
    #     max_tokens=4096,
    #     system="You are a helpful assistant.",
    #     messages=history,
    # )
    # return response.content[0].text


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    # 1. Register the platforms you want to use
    register_telegram()

    # 2. Load configuration
    config = GatewayConfig.load("gateway.yaml")
    print(config.summary())

    # 3. Create the gateway runner with your agent callback
    runner = GatewayRunner(config, agent_callback=my_agent_callback)

    # 4. Install graceful shutdown handlers (Ctrl+C)
    runner.install_signal_handlers()

    # 5. Start all adapters
    await runner.start()

    # 6. Keep running until shutdown
    print("\n🚀 Gateway is running. Press Ctrl+C to stop.\n")
    await runner.wait_for_shutdown()


if __name__ == "__main__":
    asyncio.run(main())

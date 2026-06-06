# Agent Gateway

A reusable, modular multi-platform messaging gateway for AI agents.

**One interface — every messaging platform.**

Connect your AI agent to Telegram, Discord, Slack, webhooks, or any custom platform
with a unified adapter interface. Add a new platform by implementing just **3 methods**.

## Features

- 🔌 **Plugin Architecture** — Register platform adapters at runtime, no core code changes
- 💬 **20+ Platform Ready** — Telegram, Discord, Slack, Webhook out of the box; extend for any platform
- 🌊 **Streaming Responses** — Real-time streaming with adaptive throttling and draft/edit modes
- 🧠 **Session Management** — Automatic conversation isolation, idle cleanup, and history trimming
- 🚀 **Delivery Router** — Cross-platform message routing with truncation and anti-loop protection
- 🔒 **Safety First** — Media path validation, SSRF protection, credential leak prevention
- ⚡ **Async Native** — Built on `asyncio` for high-concurrency workloads
- 🎯 **Minimal Interface** — Adapters only need `connect`, `disconnect`, `send` — everything else is optional

## Quick Start

### Installation

```bash
# Core only (no platform dependencies)
pip install agent-gateway

# With specific platform support
pip install agent-gateway[telegram]
pip install agent-gateway[discord]
pip install agent-gateway[slack]
pip install agent-gateway[webhook]

# Everything
pip install agent-gateway[all]
```

### 5-Line Integration

```python
import asyncio
from agent_gateway import GatewayRunner, GatewayConfig
from agent_gateway.adapters.telegram import register_telegram

async def my_agent(session_key, message, history, **kw):
    return f"You said: {message}"  # Replace with your LLM call

async def main():
    register_telegram()
    config = GatewayConfig.load("gateway.yaml")
    runner = GatewayRunner(config, agent_callback=my_agent)
    await runner.start()
    await runner.wait_for_shutdown()

asyncio.run(main())
```

### Configuration

Create a `gateway.yaml`:

```yaml
platforms:
  telegram:
    enabled: true
    token: ""                    # Or set TELEGRAM_TOKEN env var
    dm_policy: allowlist
    allow_from: ["123456789"]

  discord:
    enabled: false
    token: ""                    # Or set DISCORD_TOKEN env var

streaming:
  enabled: true
  tool_progress: "all"

session:
  max_idle_seconds: 3600
  reset_policy: "idle"
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    GatewayRunner                      │
│         Orchestrates adapters, sessions, routing      │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ Telegram  │  │ Discord  │  │  Slack   │  ...      │
│  │ Adapter   │  │ Adapter  │  │ Adapter  │           │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘         │
│        │              │              │                │
│  ┌─────▼──────────────▼──────────────▼──────┐        │
│  │        BasePlatformAdapter (ABC)          │        │
│  │   connect / send / disconnect (required)  │        │
│  │   edit / delete / media / typing (opt.)   │        │
│  └─────────────────┬────────────────────────┘        │
│                     │ MessageEvent                    │
│  ┌─────────────────▼────────────────────────┐        │
│  │          DeliveryRouter                    │        │
│  │   Cross-platform routing & truncation     │        │
│  └─────────────────┬────────────────────────┘        │
│  ┌─────────────────▼────────────────────────┐        │
│  │        SessionStore + StreamConsumer      │        │
│  └─────────────────┬────────────────────────┘        │
│  ┌─────────────────▼────────────────────────┐        │
│  │            Your AI Agent                   │        │
│  └───────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────┘
```

## Creating a Custom Adapter

Implement just 3 methods to add a new platform:

```python
from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.message import SendResult
from agent_gateway.core.registry import PlatformEntry, registry


class MyPlatformAdapter(BasePlatformAdapter):

    async def connect(self) -> bool:
        # Connect to your platform
        return True

    async def disconnect(self) -> None:
        # Clean up
        pass

    async def send(self, chat_id, content, *, reply_to=None, metadata=None) -> SendResult:
        # Send a message
        return SendResult(success=True, message_id="msg-1")


# Register it
registry.register(PlatformEntry(
    name="my_platform",
    label="My Platform",
    adapter_factory=lambda cfg: MyPlatformAdapter(cfg),
    check_fn=lambda: True,
    emoji="🚀",
))
```

Optional overrides for richer support:

| Method | Purpose |
|--------|---------|
| `edit_message()` | Streaming updates |
| `delete_message()` | Ephemeral messages |
| `send_image()` | Native image delivery |
| `send_voice()` | Audio/voice messages |
| `send_document()` | File attachments |
| `send_typing()` | Typing indicator |
| `send_clarify()` | Multi-choice prompts |
| `supports_draft_streaming()` | Native draft previews |

## Agent Integration Patterns

### Pattern 1: Callback Function (Simplest)

```python
async def agent_callback(session_key, message, history, **kw):
    import openai
    client = openai.AsyncOpenAI()
    resp = await client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": "You are helpful."}] + history,
    )
    return resp.choices[0].message.content

runner = GatewayRunner(config, agent_callback=agent_callback)
```

### Pattern 2: Agent Object

```python
class MyAgent:
    async def chat(self, session_key, message, history, **kw):
        # Your LLM logic here
        return "Response"

    async def stream(self, session_key, message, history, **kw):
        # Yield text chunks for streaming
        yield "Hello"
        yield " world"

runner = GatewayRunner(config, agent=MyAgent())
```

### Pattern 3: Framework Integration

```python
# LangChain
from langchain.chains import ConversationChain
from langchain_openai import ChatOpenAI

chain = ConversationChain(llm=ChatOpenAI())

async def langchain_agent(session_key, message, history, **kw):
    return await chain.arun(message)

runner = GatewayRunner(config, agent_callback=langchain_agent)
```

## API Reference

### Core Types

| Type | Description |
|------|-------------|
| `MessageEvent` | Normalized inbound message (text, media, source) |
| `SendResult` | Outbound send result (success, message_id, retryable) |
| `MessageSource` | 4D identity: (platform, user_id, chat_id, thread_id) |
| `DeliveryTarget` | Routing destination: platform:chat:thread |
| `Session` | Conversation state with history and metadata |

### Core Components

| Component | Description |
|-----------|-------------|
| `GatewayRunner` | Top-level orchestrator |
| `BasePlatformAdapter` | Abstract adapter interface |
| `PlatformRegistry` | Runtime adapter registration |
| `DeliveryRouter` | Cross-platform message routing |
| `SessionStore` | Session lifecycle management |
| `StreamConsumer` | Streaming response delivery |
| `MediaCache` | Media file caching |

## Project Structure

```
agent-gateway/
├── pyproject.toml
├── README.md
├── src/agent_gateway/
│   ├── __init__.py                  # Public API
│   ├── core/
│   │   ├── message.py               # MessageEvent, SendResult, MessageSource
│   │   ├── adapter.py               # BasePlatformAdapter (ABC)
│   │   ├── session.py               # Session, SessionStore
│   │   ├── registry.py              # PlatformRegistry, PlatformEntry
│   │   ├── delivery.py              # DeliveryRouter, DeliveryTarget
│   │   ├── config.py                # GatewayConfig, PlatformConfig
│   │   ├── runner.py                # GatewayRunner
│   │   ├── stream.py                # StreamConsumer
│   │   └── stream_events.py         # Structured stream events
│   ├── adapters/
│   │   ├── telegram.py              # Telegram adapter
│   │   ├── discord.py               # Discord adapter
│   │   ├── slack.py                 # Slack adapter
│   │   └── webhook.py               # Webhook adapter
│   ├── media/
│   │   └── cache.py                 # Media caching utilities
│   └── utils/
│       ├── safety.py                # Path validation, SSRF protection
│       └── proxy.py                 # Proxy resolution
├── examples/
│   ├── basic_usage.py
│   ├── custom_adapter.py
│   └── gateway.yaml
└── tests/
    ├── test_message.py
    ├── test_session.py
    ├── test_registry.py
    ├── test_delivery.py
    └── test_config.py
```

## Design Principles

1. **Minimal Interface** — 3 abstract methods; everything else has graceful defaults
2. **Event-Driven Decoupling** — Adapters never call the agent directly; they invoke the injected handler
3. **Session Isolation** — `(platform, user_id, chat_id, thread_id)` uniquely identifies a conversation
4. **Graceful Degradation** — Draft streaming → edit mode → one-shot send (3-level fallback)
5. **Safety First** — Media path validation, silence-narration anti-loop, credential leak prevention
6. **Plugin Extensibility** — Register new platforms at runtime via the registry

## License

MIT

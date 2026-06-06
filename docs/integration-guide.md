# Agent Gateway 对接文档

> 版本 0.1.0 · 适用于 Python 3.11+
>
> 将任意 AI Agent（Claude Code、Pi Agent、OpenAI Codex、自研 Agent）一键接入
> Telegram / Discord / Slack / Webhook 等 20+ 消息平台。

---

## 目录

- [1. 架构概览](#1-架构概览)
- [2. 安装](#2-安装)
- [3. 快速开始：5 行代码跑通](#3-快速开始5-行代码跑通)
- [4. AI Agent 对接](#4-ai-agent-对接)
  - [4.1 对接模式总览](#41-对接模式总览)
  - [4.2 回调函数模式（最简单）](#42-回调函数模式最简单)
  - [4.3 Agent 对象模式](#43-agent-对象模式)
  - [4.4 Claude Code Bridge](#44-claude-code-bridge)
  - [4.5 Pi Agent Bridge](#45-pi-agent-bridge)
  - [4.6 OpenAI Codex Bridge](#46-openai-codex-bridge)
  - [4.7 自定义 CLI Agent Bridge](#47-自定义-cli-agent-bridge)
- [5. 消息平台适配器](#5-消息平台适配器)
  - [5.1 Telegram](#51-telegram)
  - [5.2 Discord](#52-discord)
  - [5.3 Slack](#53-slack)
  - [5.4 Webhook](#54-webhook)
  - [5.5 自定义平台适配器](#55-自定义平台适配器)
- [6. 配置参考](#6-配置参考)
  - [6.1 YAML 配置](#61-yaml-配置)
  - [6.2 环境变量覆盖](#62-环境变量覆盖)
  - [6.3 Python 代码配置](#63-python-代码配置)
- [7. 会话管理](#7-会话管理)
- [8. 流式响应](#8-流式响应)
- [9. 跨平台投递路由](#9-跨平台投递路由)
- [10. 媒体处理](#10-媒体处理)
- [11. 安全机制](#11-安全机制)
- [12. 完整示例](#12-完整示例)
- [13. 故障排查](#13-故障排查)
- [14. API 速查表](#14-api-速查表)

---

## 1. 架构概览

```
用户 ──▶ Telegram ──┐
用户 ──▶ Discord  ──┤                        ┌──────────────────┐
用户 ──▶ Slack    ──┼──▶ Platform Adapter ──▶ │  GatewayRunner   │
用户 ──▶ Webhook  ──┤    (消息标准化)         │                  │
用户 ──▶ 自定义... ─┘                         │  ┌─ SessionStore │
                                             │  ├─ DeliveryRouter│
                                             │  ├─ StreamConsumer│
                                             │  └─ MediaCache   │
                                             │                  │
                                             │  agent_callback()│
                                             └────────┬─────────┘
                                                      │
                                    ┌─────────────────┼─────────────────┐
                                    │                 │                   │
                              ┌─────▼─────┐  ┌───────▼───────┐  ┌──────▼──────┐
                              │Claude Code│  │   Pi Agent    │  │ 自定义 Agent│
                              │  Bridge   │  │   Bridge      │  │  Callback  │
                              │(subprocess)│  │(subprocess)   │  │(Python 函数)│
                              └───────────┘  └───────────────┘  └─────────────┘
```

**三层解耦**：

| 层 | 职责 | 模块 |
|---|------|------|
| **平台适配层** | 将各消息平台统一为 `MessageEvent` | `adapters/` |
| **网关核心层** | 会话管理、流式响应、路由投递 | `core/` |
| **Agent 桥接层** | 将 AI Agent 包装为统一接口 | `agents/` |

三层独立替换——换平台不改 Agent，换 Agent 不改平台。

---

## 2. 安装

```bash
# 基础安装（无平台依赖）
pip install agent-gateway
# 或使用 uv
uv pip install agent-gateway

# 按需安装平台依赖
pip install agent-gateway[telegram]    # Telegram
pip install agent-gateway[discord]     # Discord
pip install agent-gateway[slack]       # Slack
pip install agent-gateway[webhook]     # Webhook (FastAPI + uvicorn)
pip install agent-gateway[all]         # 全部平台

# 开发依赖
pip install agent-gateway[dev]
```

**CLI Agent 要求**（Agent 桥接层需要）：

```bash
# Claude Code（需要本地安装 claude CLI）
# 参考: https://docs.anthropic.com/en/docs/claude-code
npm install -g @anthropic-ai/claude-code

# Pi Agent（需要本地安装 pi CLI）
# 参考: https://github.com/NousResearch/pi-agent

# OpenAI Codex（需要本地安装 codex CLI）
# 参考: https://github.com/openai/codex
```

---

## 3. 快速开始：5 行代码跑通

### 最小示例：回调函数 + Telegram

```python
# app.py
import asyncio
from agent_gateway import GatewayRunner, GatewayConfig
from agent_gateway.adapters.telegram import register_telegram

async def my_agent(session_key, message, history, **kw):
    """你的 Agent 逻辑——这里用简单回复演示"""
    return f"Echo: {message}"

async def main():
    register_telegram()                               # ① 注册平台
    config = GatewayConfig.load("gateway.yaml")       # ② 加载配置
    runner = GatewayRunner(config, agent_callback=my_agent)  # ③ 创建 Runner
    await runner.start()                              # ④ 启动
    await runner.wait_for_shutdown()                  # ⑤ 等待退出

asyncio.run(main())
```

配置文件 `gateway.yaml`：

```yaml
platforms:
  telegram:
    enabled: true
    token: ""              # 或设置环境变量 TELEGRAM_TOKEN
```

运行：

```bash
export TELEGRAM_TOKEN="your-bot-token"
python app.py
```

### 最小示例：Claude Code + Pi Agent

```python
# cli_agent_app.py
import asyncio
from agent_gateway import GatewayRunner, GatewayConfig
from agent_gateway.adapters.webhook import register_webhook
from agent_gateway.agents import ClaudeCodeBridge

async def main():
    register_webhook()

    config = GatewayConfig()
    config.platforms["webhook"] = GatewayConfig.__dataclass_fields__  # 简写

    # 用 Claude Code 作为 Agent 后端
    bridge = ClaudeCodeBridge(model="claude-sonnet-4-6", timeout=60)
    runner = GatewayRunner(config, agent=bridge)

    await runner.start()
    print("Gateway running at http://127.0.0.1:8080/webhook")
    await runner.wait_for_shutdown()

asyncio.run(main())
```

---

## 4. AI Agent 对接

### 4.1 对接模式总览

| 模式 | 适用场景 | 复杂度 |
|------|---------|--------|
| **回调函数** | 自研 Agent、OpenAI API、Anthropic API | ⭐ |
| **Agent 对象** | 需要状态管理或流式支持 | ⭐⭐ |
| **ClaudeCodeBridge** | 本地安装了 Claude Code CLI | ⭐ |
| **PiAgentBridge** | 本地安装了 Pi Agent CLI | ⭐ |
| **CodexBridge** | 本地安装了 Codex CLI | ⭐ |
| **自定义 Bridge** | 任意 CLI Agent 工具 | ⭐⭐⭐ |

### 4.2 回调函数模式（最简单）

```python
async def agent_callback(
    session_key: str,       # 会话标识 "platform:user:chat[:thread]"
    message: str,           # 用户消息文本
    history: list[dict],    # 对话历史 [{"role":"user","content":"..."}, ...]
    **kwargs,               # system_extra 等额外参数
) -> str:
    """返回 Agent 的回复文本"""
    ...
```

#### 对接 OpenAI API

```python
import openai

client = openai.AsyncOpenAI()

async def openai_agent(session_key, message, history, **kw):
    system_extra = kw.get("system_extra", "")
    messages = [{"role": "system", "content": f"You are a helpful assistant.\n{system_extra}"}]
    messages += history
    messages += [{"role": "user", "content": message}]

    resp = await client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
    )
    return resp.choices[0].message.content

runner = GatewayRunner(config, agent_callback=openai_agent)
```

#### 对接 Anthropic API

```python
import anthropic

client = anthropic.AsyncAnthropic()

async def claude_api_agent(session_key, message, history, **kw):
    system_extra = kw.get("system_extra", "You are a helpful assistant.")
    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=system_extra,
        messages=history + [{"role": "user", "content": message}],
    )
    return resp.content[0].text

runner = GatewayRunner(config, agent_callback=claude_api_agent)
```

#### 对接 LangChain

```python
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

chain = ChatOpenAI(model="gpt-4o")

async def langchain_agent(session_key, message, history, **kw):
    lc_messages = []
    for h in history:
        if h["role"] == "user":
            lc_messages.append(HumanMessage(content=h["content"]))
        elif h["role"] == "assistant":
            lc_messages.append(AIMessage(content=h["content"]))
    lc_messages.append(HumanMessage(content=message))
    result = await chain.ainvoke(lc_messages)
    return result.content
```

### 4.3 Agent 对象模式

当回调函数不够用（需要流式、状态、生命周期管理）时，传入一个带有 `chat()` 和可选 `stream()` 方法的对象：

```python
class MyAgent:
    """Agent 对象接口"""

    async def chat(self, session_key, message, history, system_extra=""):
        """非流式调用——返回完整响应"""
        return "response text"

    async def stream(self, session_key, message, history, system_extra=""):
        """流式调用——yield 文本片段"""
        yield "Hello"
        yield " world"

runner = GatewayRunner(config, agent=MyAgent())
```

`stream()` 方法的签名：

```python
async def stream(self, session_key, message, history, system_extra=""):
    # yield str —— 文本增量
    yield "chunk 1"
    yield "chunk 2"

    # 或 yield ToolCallChunk —— 工具调用进度
    from agent_gateway.core.stream_events import ToolCallChunk
    yield ToolCallChunk(tool_name="search", preview="query...")
```

### 4.4 Claude Code Bridge

将本地安装的 Claude Code CLI 包装为 GatewayRunner 的 Agent 接口。

```python
from agent_gateway.agents import ClaudeCodeBridge

# 基础用法
bridge = ClaudeCodeBridge()
runner = GatewayRunner(config, agent=bridge)

# 自定义参数
bridge = ClaudeCodeBridge(
    model="claude-sonnet-4-6",      # 指定模型
    timeout=60,                     # 超时（秒）
    max_output_bytes=2_000_000,     # 最大输出字节数
    extra_args=["--no-project"],   # 额外 CLI 参数
    command="claude",               # CLI 命令路径
)

# 作为回调函数使用
runner = GatewayRunner(config, agent_callback=bridge.as_callback())
```

**工作原理**：

```
GatewayRunner.chat()
  └─▶ ClaudeCodeBridge.chat()
        └─▶ 构建 prompt（history + system_extra + message）
        └─▶ 执行: claude --print --output-format json --max-turns 1
        └─▶ 解析 JSON 响应: {"type":"result","result":"..."}
```

**流式模式**：

```python
# stream() 自动使用 --output-format stream-json --verbose
# 逐行解析 JSONL 事件，yield 文本增量
async for chunk in bridge.stream("s:1", "hello", []):
    print(chunk)
```

**历史重建**：

Claude Code CLI 每次调用是无状态的。Bridge 将会话历史格式化为多轮对话 prompt：

```
System instructions: ...

Previous conversation:
Human: What is 2+2?
Assistant: 2+2 equals 4.

Current question
```

### 4.5 Pi Agent Bridge

将本地安装的 Pi Agent CLI 包装为 GatewayRunner 的 Agent 接口。支持三种模式：

```python
from agent_gateway.agents import PiAgentBridge

# 模式 1: --print（默认，简单可靠）
bridge = PiAgentBridge()                          # mode="print"
bridge = PiAgentBridge(mode="print", timeout=60)  # 显式指定

# 模式 2: --mode json（结构化 JSONL 流式）
bridge = PiAgentBridge(mode="json", timeout=60)

# 模式 3: --mode rpc（有状态，per-session 子进程）
bridge = PiAgentBridge(mode="rpc", idle_timeout=300, max_concurrent=10)
```

**模式对比**：

| 模式 | `chat()` | `stream()` | 子进程 | 适用场景 |
|------|----------|-----------|--------|---------|
| `print` | ✅ 文本 | ✅ 逐行 stdout | 每次新建 | 通用 |
| `json` | ✅ 解析 JSONL | ✅ text_delta 事件 | 每次新建 | 需要结构化 |
| `rpc` | ✅ JSON-RPC | ✅ RPC 流 | per-session 持久 | 长对话、高吞吐 |

**`json` 模式流式输出**：

```python
bridge = PiAgentBridge(mode="json")
async for chunk in bridge.stream("s:1", "hello", []):
    # 每个 chunk 是 Pi Agent 的 text_delta 增量
    print(chunk, end="", flush=True)
```

**`rpc` 模式**：每个会话自动维护一个持久的 `pi --mode rpc` 子进程，空闲 300 秒后自动清理。

### 4.6 OpenAI Codex Bridge

```python
from agent_gateway.agents import CodexBridge

bridge = CodexBridge(
    model="codex-mini",         # 指定模型
    timeout=120,                # 超时
    extra_args=[],              # 额外参数
    command="codex",            # CLI 命令
)

runner = GatewayRunner(config, agent=bridge)
```

**工作原理**：通过 stdin 传入 prompt，读取 stdout，自动剥离 ANSI 转义码。

### 4.7 自定义 CLI Agent Bridge

对接任意 CLI 工具只需继承 `CLIAgentBridge` 并实现两个方法：

```python
from agent_gateway.agents.base import CLIAgentBridge, SubprocessConfig

class MyCLIBridge(CLIAgentBridge):
    """对接自定义 CLI Agent"""

    def __init__(self, command="my-agent"):
        super().__init__(SubprocessConfig(
            command=[command],
            timeout=60,
            max_output_bytes=1_000_000,
        ))

    def _build_args(self, session_key, message, history, system_extra):
        """构建 CLI 命令参数"""
        return [self.config.command[0], "--prompt", message]

    async def _parse_output(self, raw_stdout, session_key):
        """解析 CLI 输出为响应文本"""
        return raw_stdout.strip()
```

**进阶：支持流式和自定义历史格式**

```python
class AdvancedBridge(CLIAgentBridge):

    def _build_args(self, session_key, message, history, system_extra):
        return ["my-agent", "--stream"]

    async def _parse_output(self, raw_stdout, session_key):
        # 自定义解析逻辑
        import json
        data = json.loads(raw_stdout)
        return data["response"]

    def _format_history(self, history):
        """自定义历史格式"""
        lines = []
        for h in history:
            lines.append(f"{h['role'].upper()}: {h['content']}")
        return "\n".join(lines)

    async def stream(self, session_key, message, history, system_extra=""):
        """流式输出"""
        args = self._build_args(session_key, message, history, system_extra)
        prompt = self._format_prompt(message, history, system_extra)
        async for line in self._run_subprocess_streaming(args, input_text=prompt):
            yield line
```

---

## 5. 消息平台适配器

### 5.1 Telegram

```python
from agent_gateway.adapters.telegram import register_telegram

register_telegram()
```

**配置**：

```yaml
# gateway.yaml
platforms:
  telegram:
    enabled: true
    token: ""                    # 或 TELEGRAM_TOKEN 环境变量
    home_channel: ""             # 默认聊天 ID（用于定时任务）
    dm_policy: allowlist         # open | allowlist | closed
    allow_from: []               # 允许的用户 ID 列表
    group_policy: allowlist
    group_allow_from: []         # 允许的群组 ID 列表
```

**环境变量**：

```bash
export TELEGRAM_TOKEN="123456:ABC-DEF"
export TELEGRAM_ALLOWED_USERS="123456789,987654321"  # 可选
export TELEGRAM_ALLOW_ALL_USERS="1"                  # 开放所有用户
```

**支持的能力**：

| 功能 | 状态 | 说明 |
|------|------|------|
| 文本消息 | ✅ | Markdown 格式 |
| 图片收发 | ✅ | 原生 photo 发送 |
| 语音消息 | ✅ | sendVoice |
| 文件附件 | ✅ | sendDocument |
| 消息编辑 | ✅ | 流式更新 |
| 消息删除 | ✅ | 临时消息 |
| 内联键盘 | ✅ | clarify / confirm |
| 流式草稿 | ✅ | Bot API 9.5+ |
| 话题/线程 | ✅ | Forum Topics |
| 打字指示器 | ✅ | sendChatAction |

### 5.2 Discord

```python
from agent_gateway.adapters.discord import register_discord

register_discord()
```

**配置**：

```yaml
platforms:
  discord:
    enabled: true
    token: ""                    # 或 DISCORD_TOKEN 环境变量
```

**Bot 权限要求**：Message Content Intent（在 Discord Developer Portal 启用）。

### 5.3 Slack

```python
from agent_gateway.adapters.slack import register_slack

register_slack()
```

**配置**：

```yaml
platforms:
  slack:
    enabled: true
    token: "xoxb-..."           # Bot Token 或 SLACK_TOKEN 环境变量
    app_token: "xapp-..."       # Socket Mode Token 或 SLACK_APP_TOKEN 环境变量
```

### 5.4 Webhook

启动一个 HTTP 服务器，接收任意平台的 POST 请求：

```python
from agent_gateway.adapters.webhook import register_webhook

register_webhook()
```

**配置**：

```yaml
platforms:
  webhook:
    enabled: true
    host: "127.0.0.1"
    port: 8080
    path: "/webhook"
    secret: ""                  # 可选：验证 X-Webhook-Secret 头
```

**请求格式**：

```bash
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello!", "user_id": "user1", "chat_id": "chat1"}'
```

**响应格式**：

```json
{"ok": true}
```

Agent 的回复通过 webhook 响应的 HTTP body 返回。

**健康检查**：

```bash
curl http://localhost:8080/health
# → {"status": "ok", "adapter": "Webhook"}
```

### 5.5 自定义平台适配器

只需实现 3 个抽象方法：

```python
from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.message import SendResult
from agent_gateway.core.registry import PlatformEntry, registry
from typing import Any, Optional

class MyPlatformAdapter(BasePlatformAdapter):

    async def connect(self) -> bool:
        """连接平台"""
        # 初始化 SDK、建立长连接、注册回调
        return True

    async def disconnect(self) -> None:
        """断开连接"""
        pass

    async def send(self, chat_id, content, *,
                   reply_to=None, metadata=None) -> SendResult:
        """发送消息"""
        # 调用平台 API 发送
        return SendResult(success=True, message_id="msg-1")

    # -- 可选覆盖 --

    async def edit_message(self, chat_id, message_id, content, **kw):
        """编辑消息（流式更新需要）"""
        return SendResult(success=True)

    async def send_typing(self, chat_id, metadata=None):
        """发送打字指示器"""
        pass

    async def send_image(self, chat_id, url, caption=None, **kw):
        """发送图片"""
        return await self.send(chat_id, f"{caption}\n{url}")


# 注册到全局注册表
registry.register(PlatformEntry(
    name="my_platform",
    label="My Platform",
    adapter_factory=lambda cfg: MyPlatformAdapter(cfg),
    check_fn=lambda: True,           # 依赖检查
    emoji="🚀",
    platform_hint="You are on My Platform.",
    source="plugin",
))
```

**可选覆盖方法速查**：

| 方法 | 用途 | 默认行为 |
|------|------|---------|
| `edit_message()` | 流式更新 | 返回 `success=False` |
| `delete_message()` | 临时消息删除 | 返回 `False` |
| `send_image()` | 原生图片发送 | 发送 URL 文本 |
| `send_voice()` | 语音消息 | 发送路径文本 |
| `send_video()` | 视频消息 | 发送路径文本 |
| `send_document()` | 文件附件 | 发送路径文本 |
| `send_typing()` | 打字指示器 | 空操作 |
| `send_clarify()` | 多选提示 | 编号文本列表 |
| `send_slash_confirm()` | 确认对话框 | 文本回复 |
| `supports_draft_streaming()` | 流式草稿 | 返回 `False` |
| `format_tool_event()` | 工具进度显示 | emoji + 预览 |
| `max_message_length` | 消息长度限制 | `0`（无限制） |

---

## 6. 配置参考

### 6.1 YAML 配置

完整配置文件示例：

```yaml
# gateway.yaml

# ── 平台配置 ──
platforms:
  telegram:
    enabled: true
    token: ""                    # 推荐：用环境变量
    home_channel: ""             # 定时任务默认投递目标
    dm_policy: allowlist         # open | allowlist | closed
    allow_from: []               # 允许的 user_id 列表
    group_policy: allowlist
    group_allow_from: []

  discord:
    enabled: true
    token: ""

  slack:
    enabled: false
    token: ""
    app_token: ""

  webhook:
    enabled: false
    host: "127.0.0.1"
    port: 8080
    path: "/webhook"
    secret: ""

# ── 流式响应 ──
streaming:
  enabled: true                 # 是否启用流式
  min_edit_interval: 0.8        # 编辑最小间隔（秒）
  use_draft: false              # 优先使用原生 draft
  tool_progress: "all"          # all | new | verbose | none
  tool_preview_length: 40       # 工具参数预览最大长度

# ── 会话管理 ──
session:
  max_idle_seconds: 3600        # 空闲超时（秒）
  max_history: 200              # 每会话最大历史条数
  reset_policy: "idle"          # daily | idle | both | none
  cleanup_interval: 300         # 清理检查间隔（秒）

# ── 运维 ──
filter_silence_narration: true  # 过滤静音叙述（防循环）
agent_timeout: 1800             # Agent 处理超时（秒）
```

### 6.2 环境变量覆盖

环境变量优先级 **最高**，会覆盖 YAML 中的值：

```bash
# 平台 Token（自动覆盖 YAML）
export TELEGRAM_TOKEN="bot-token"
export DISCORD_TOKEN="bot-token"
export SLACK_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."

# 访问控制
export TELEGRAM_ALLOWED_USERS="123,456"
export TELEGRAM_ALLOW_ALL_USERS="1"

# Agent Bridge 环境变量（由 CLI 工具自身使用）
export ANTHROPIC_API_KEY="sk-..."
export OPENAI_API_KEY="sk-..."
```

### 6.3 Python 代码配置

```python
from agent_gateway.core.config import GatewayConfig, PlatformConfig

# 方式 1: 从 YAML 加载
config = GatewayConfig.load("gateway.yaml")

# 方式 2: 纯代码构建
config = GatewayConfig()
config.platforms["telegram"] = PlatformConfig(
    enabled=True,
    token="bot-token",
    extra={"dm_policy": "open"},
)
config.streaming.enabled = True
config.session.max_idle_seconds = 7200

# 方式 3: 从字典构建
config = GatewayConfig.from_dict({
    "platforms": {"telegram": {"enabled": True, "token": "..."}},
    "streaming": {"enabled": False},
})
```

---

## 7. 会话管理

### 会话标识

每个会话由四维键唯一标识：

```
platform:user_id:chat_id[:thread_id]
```

示例：

| 场景 | session_key |
|------|------------|
| Telegram 私聊 | `telegram:123456:123456` |
| Telegram 群组 | `telegram:123456:-1001234` |
| Discord 频道 | `discord:789:channel123` |
| Discord 线程 | `discord:789:channel123:thread456` |
| Slack DM | `slack:U123:D123` |
| Slack 线程 | `slack:U123:C123:1234567890.123456` |
| Webhook | `webhook:user1:chat1` |

### 会话生命周期

```
用户首次发消息 → 创建 Session → 活跃处理 → 空闲 → 超时清理
                                  │              │
                                  └─ /new 命令 ──┘→ 重置历史
```

### 重置策略

```python
from agent_gateway.core.session import SessionResetPolicy

# 在配置中设置
config.session.reset_policy = "idle"   # 空闲超时重置
config.session.reset_policy = "daily"  # 每日重置
config.session.reset_policy = "both"   # 两者兼有
config.session.reset_policy = "none"   # 永不自动重置
```

### Slash 命令

GatewayRunner 内置以下命令（用户在聊天中直接输入）：

| 命令 | 功能 |
|------|------|
| `/new` 或 `/reset` | 重置当前会话 |
| `/status` | 查看网关状态 |
| `/sessions` | 查看活跃会话 |
| `/help` | 帮助信息 |

### 编程式会话管理

```python
# 获取 SessionStore
store = runner.session_store

# 查找会话
session = store.get("telegram:123:456")

# 重置会话
store.reset("telegram:123:456")

# 查询用户的所有会话
sessions = store.sessions_for_user("telegram", "123")

# 手动清理空闲会话
removed = store.cleanup_idle()
```

---

## 8. 流式响应

### 启用流式

```yaml
# gateway.yaml
streaming:
  enabled: true
  min_edit_interval: 0.8    # 编辑间隔（秒），控制洪流
  tool_progress: "all"       # 显示工具调用进度
```

### 流式工作流

```
用户消息 → GatewayRunner._call_agent_streaming()
              │
              ├─ Agent.stream() yield "chunk1"
              │   └→ StreamConsumer.on_delta("chunk1")
              │       └→ adapter.send("chunk1")     # 首次发送
              │
              ├─ Agent.stream() yield "chunk2"
              │   └→ StreamConsumer.on_delta("chunk2")
              │       └→ adapter.edit_message(msg, "chunk1chunk2")  # 编辑
              │
              ├─ Agent.stream() yield ToolCallChunk
              │   └→ adapter.send("⚙️ tool: preview")  # 工具进度
              │
              └─ Agent.stream() 完成
                  └→ StreamConsumer.finish(full_text)
                      └→ adapter.send(full_text)    # 最终版本
                      └→ adapter.delete_message(old) # 删除中间版本
```

### 三级流式回退

```
1. Draft Streaming（adapter.send_draft）     ← 最优体验
   ↓ 不支持
2. Edit Streaming（adapter.send + edit_message） ← 通用方案
   ↓ 不支持
3. One-shot（完成后一次发送）                 ← 保底方案
```

### 配置工具进度显示

```yaml
streaming:
  tool_progress: "all"       # 显示所有工具调用
  # tool_progress: "new"     # 仅显示新工具
  # tool_progress: "verbose" # 详细模式（含参数）
  # tool_progress: "none"    # 不显示工具进度
```

---

## 9. 跨平台投递路由

### 投递目标格式

```
"origin"              → 回到消息来源
"local"               → 保存到本地文件
"telegram"            → Telegram 主频道
"telegram:123456"     → 指定 Telegram 会话
"telegram:123:topic"  → 指定话题/线程
"discord"             → Discord 默认频道
"discord:channel_id"  → 指定 Discord 频道
```

### 编程式投递

```python
from agent_gateway.core.delivery import DeliveryTarget, DeliveryRouter

router = runner.delivery_router

# 投递到指定目标
result = await router.deliver(
    content="Hello!",
    target=DeliveryTarget.parse("telegram:123456"),
)

# 投递到多个目标
results = await router.deliver_multi(
    content="Daily report",
    targets=[
        DeliveryTarget.parse("telegram:123456"),
        DeliveryTarget.parse("discord:channel_id"),
    ],
    job_name="Daily Report",
)
```

### 防循环保护

GatewayRouter 自动过滤静音叙述（`"silent"`, `"..."`, `"🔇"` 等），防止 bot-to-bot 乒乓循环。可通过配置关闭：

```yaml
filter_silence_narration: false
```

---

## 10. 媒体处理

### 入站媒体（用户 → Agent）

平台适配器自动将用户发送的媒体下载到本地缓存，通过 `MessageEvent.media_urls` 传递：

```python
async def my_agent(session_key, message, history, **kw):
    # kw 中不直接包含 media，但 history 中的 assistant 消息会包含
    # 图片路径在处理阶段已注入到 prompt
    ...
```

### 出站媒体（Agent → 平台）

Agent 在回复中嵌入特殊标记，Gateway 自动提取并投递：

```
MEDIA:/path/to/image.png            # 发送图片
MEDIA:/path/to/document.pdf         # 发送文件
[[audio_as_voice]]                  # 标记音频为语音消息
[[as_document]]                     # 标记为文件而非图片
```

### 媒体缓存

```python
from agent_gateway.media.cache import MediaCache

cache = MediaCache()

# 保存下载的图片
path = cache.save_image(image_bytes, ext=".jpg")

# 保存音频
path = cache.save_audio(audio_bytes, ext=".ogg")

# 从 URL 下载并缓存
path = await cache.download("https://example.com/image.png", kind="image")

# 清理旧文件
result = cache.cleanup(max_age_hours=24)
# → {"images": 5, "audio": 2, "videos": 0, "documents": 3}
```

### 安全验证

```python
from agent_gateway.utils.safety import validate_media_delivery_path

# 严格模式：只允许缓存目录和近期文件
path = validate_media_delivery_path(
    "/tmp/report.pdf",
    cache_roots=[Path("/path/to/cache")],
    strict=True,
    recency_seconds=600,
)

# 非严格模式：允许任何非敏感路径
path = validate_media_delivery_path("/home/user/report.pdf")

# 自动拒绝敏感路径
validate_media_delivery_path("/etc/passwd")        # → None
validate_media_delivery_path("/root/.ssh/id_rsa")   # → None
```

---

## 11. 安全机制

### 用户授权

```yaml
# 允许列表模式
platforms:
  telegram:
    dm_policy: allowlist
    allow_from: ["123456789"]

# 开放模式（任何人可用）
platforms:
  telegram:
    dm_policy: open

# 关闭模式
platforms:
  telegram:
    dm_policy: closed
```

### Agent Bridge 安全

- **子进程隔离**：CLI Agent 在独立子进程中运行，与主进程隔离
- **超时控制**：默认 120 秒，超时自动 kill 子进程
- **输出限制**：默认 1MB，防止内存溢出
- **输入净化**：移除 null 字节和控制字符
- **无 shell 注入**：使用 `create_subprocess_exec` 而非 `shell=True`

### 媒体安全

- **路径验证**：拒绝 `/etc`、`/proc`、`~/.ssh` 等敏感路径
- **SSRF 防护**：阻止访问内网地址
- **文件类型验证**：通过 magic bytes 验证文件类型

---

## 12. 完整示例

### 示例 1：Claude Code + Telegram 全功能

```python
# examples/claude_code_telegram.py
import asyncio
import logging

from agent_gateway import GatewayRunner, GatewayConfig
from agent_gateway.adapters.telegram import register_telegram
from agent_gateway.agents import ClaudeCodeBridge

logging.basicConfig(level=logging.INFO)


async def main():
    # 1. 注册平台
    register_telegram()

    # 2. 加载配置
    config = GatewayConfig.load("gateway.yaml")

    # 3. 创建 Claude Code 桥接器
    bridge = ClaudeCodeBridge(
        model="claude-sonnet-4-6",
        timeout=120,
    )

    # 4. 创建并启动 Runner
    runner = GatewayRunner(config, agent=bridge)
    runner.install_signal_handlers()  # Ctrl+C 优雅退出

    await runner.start()
    print("🚀 Claude Code Gateway running on Telegram")
    await runner.wait_for_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### 示例 2：Pi Agent + Webhook + 自定义逻辑

```python
# examples/pi_agent_webhook.py
import asyncio
import logging

from agent_gateway import GatewayRunner, GatewayConfig, PlatformConfig
from agent_gateway.adapters.webhook import register_webhook
from agent_gateway.agents import PiAgentBridge

logging.basicConfig(level=logging.INFO)


async def main():
    register_webhook()

    # 纯代码配置（无 YAML 文件）
    config = GatewayConfig()
    config.platforms["webhook"] = PlatformConfig(
        enabled=True,
        token="",
        extra={"host": "0.0.0.0", "port": 9090, "path": "/agent"},
    )

    # Pi Agent JSON 模式（支持流式）
    bridge = PiAgentBridge(mode="json", timeout=60)

    runner = GatewayRunner(config, agent=bridge)
    await runner.start()
    print("🚀 Pi Agent Gateway running at http://0.0.0.0:9090/agent")
    await runner.wait_for_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### 示例 3：多 Agent 路由

```python
# examples/multi_agent.py
import asyncio
import logging

from agent_gateway import GatewayRunner, GatewayConfig
from agent_gateway.adapters.telegram import register_telegram
from agent_gateway.adapters.discord import register_discord

logging.basicConfig(level=logging.INFO)


async def smart_agent(session_key, message, history, **kw):
    """根据平台自动选择 Agent 后端"""
    platform = session_key.split(":")[0]

    if platform == "telegram":
        from agent_gateway.agents import ClaudeCodeBridge
        bridge = ClaudeCodeBridge(timeout=30)
        return await bridge.chat(session_key, message, history)
    elif platform == "discord":
        from agent_gateway.agents import PiAgentBridge
        bridge = PiAgentBridge(timeout=30)
        return await bridge.chat(session_key, message, history)
    else:
        return f"Unknown platform: {platform}"


async def main():
    register_telegram()
    register_discord()

    config = GatewayConfig.load("gateway.yaml")
    runner = GatewayRunner(config, agent_callback=smart_agent)

    await runner.start()
    print("🚀 Multi-Agent Gateway running")
    await runner.wait_for_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

---

## 13. 故障排查

### 常见问题

**Q: `No adapter configured for 'telegram'`**

```bash
# 检查：是否注册了平台
python -c "from agent_gateway import registry; print(registry.summary())"

# 解决：确保调用了 register_telegram()
```

**Q: `TELEGRAM_TOKEN not set`**

```bash
# 确认环境变量
echo $TELEGRAM_TOKEN

# 或在 YAML 中直接设置
```

**Q: Claude Code Bridge 超时**

```python
# 增加超时时间
bridge = ClaudeCodeBridge(timeout=300)  # 5 分钟
```

**Q: `aiohttp_socks not installed` SOCKS 代理错误**

```bash
pip install aiohttp-socks
```

**Q: Pi Agent `--mode rpc` 报错**

```python
# 使用 --print 模式（更稳定）
bridge = PiAgentBridge(mode="print")

# 或使用 --mode json（支持流式）
bridge = PiAgentBridge(mode="json")
```

### 日志调试

```python
import logging

# 开启 DEBUG 日志
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
```

### 查看 Gateway 状态

在任意已连接平台发送 `/status` 命令：

```
📊 Gateway Status

- telegram: ✅ connected
- discord: ❌ disconnected

Sessions: 3
```

---

## 14. API 速查表

### 消息类型

```python
from agent_gateway import MessageEvent, MessageSource, SendResult, EphemeralReply
from agent_gateway.core.message import MessageType, ChatType

# 入站消息
event = MessageEvent(
    text="hello",
    message_type=MessageType.TEXT,     # TEXT/PHOTO/VIDEO/AUDIO/DOCUMENT/...
    source=MessageSource(
        platform="telegram", user_id="123", chat_id="456",
        thread_id=None, chat_type=ChatType.DM, display_name="Alice",
    ),
    media_urls=["/path/to/img.jpg"],
    reply_to_message_id="789",
)

# 出站结果
result = SendResult(success=True, message_id="msg-1", retryable=False)

# 临时回复（自动删除）
reply = EphemeralReply("✅ Done", ttl_seconds=10)
```

### 会话

```python
from agent_gateway import Session, SessionStore

store = SessionStore(max_idle_seconds=3600, max_history=200)
session = store.get_or_create(source)     # 获取或创建
session.add_message("user", "hello")      # 添加消息
session.clear_history()                    # 清空历史
store.reset(session.key)                  # 重置会话
store.cleanup_idle()                      # 清理过期
```

### 投递

```python
from agent_gateway import DeliveryTarget, DeliveryRouter

target = DeliveryTarget.parse("telegram:123456")
router = DeliveryRouter(adapters={"telegram": adapter})
result = await router.deliver("Hello!", target)
```

### 注册表

```python
from agent_gateway import registry, PlatformEntry

registry.register(PlatformEntry(
    name="my_platform",
    label="My Platform",
    adapter_factory=lambda cfg: MyAdapter(cfg),
    check_fn=lambda: True,
    emoji="🚀",
))

adapter = registry.create_adapter("my_platform", config)
print(registry.summary())
```

### Agent Bridge

```python
from agent_gateway.agents import (
    ClaudeCodeBridge, PiAgentBridge, CodexBridge,
    SubprocessConfig, SubprocessPool,
    CLITimeoutError, CLICrashError,
)

# Claude Code
cc = ClaudeCodeBridge(model="claude-sonnet-4-6", timeout=60)
result = await cc.chat("s:1", "hello", [])
async for chunk in cc.stream("s:1", "hello", []): ...

# Pi Agent
pi = PiAgentBridge(mode="json")        # print | json | rpc
result = await pi.chat("s:1", "hello", [])

# 自定义 Bridge
from agent_gateway.agents.base import CLIAgentBridge
class MyBridge(CLIAgentBridge):
    def _build_args(self, *a): return ["my-cli"]
    async def _parse_output(self, raw, sk): return raw.strip()
```

### Runner

```python
from agent_gateway import GatewayRunner, GatewayConfig

config = GatewayConfig.load("gateway.yaml")

# 方式 1: 回调函数
runner = GatewayRunner(config, agent_callback=my_async_function)

# 方式 2: Agent 对象
runner = GatewayRunner(config, agent=my_agent_object)

# 方式 3: Bridge 对象
runner = GatewayRunner(config, agent=claude_code_bridge)

await runner.start()
await runner.wait_for_shutdown()
```

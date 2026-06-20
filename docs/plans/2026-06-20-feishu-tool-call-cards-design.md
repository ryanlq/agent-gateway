# Feishu 实时工具调用卡片 — 设计

- 日期: 2026-06-20
- 范围: `agent-gateway` (Python)。`hermes-desktop` 不改动。
- 目标: 把飞书平台 agent 回合内的 `tool_use` 调用,以一张**实时流式更新的折叠卡片**呈现,替代当前每条工具一条 `⚙️ xxx` 文本消息的杂乱体验。

## 1. 背景与现状

当前 Feishu 流式投递链路 (`core/runner.py:_call_agent_streaming` + `core/stream.py:StreamConsumer`):

- agent 桥接 (claude-code-sdk) yield `AgentEvent`:`text_delta` / `reasoning_delta` / `tool_start` / `tool_complete`。
- `text_delta` → `StreamConsumer.on_delta()` → CardKit 流式卡片 edit。
- `tool_start` → `StreamConsumer.on_tool_call()` → **每条工具单独 `send()` 一条文本消息** `⚙️ name: "preview"`。
- `tool_complete` → **只 emit 给 desktop**(`tool.complete`),**平台侧静默**(`agents/events.py` 文档明示 "silent on chat platforms")。

痛点:
- 每个工具一条独立消息,回合多工具时刷屏。
- 平台永远不知道工具是否完成 / 成功失败 —— 只有 "开始" 没有 "结果"。
- 没有回合级汇总,用户无法一眼看清这轮做了什么、有没有失败。

## 2. 体验目标

一张卡片,随回合流式生长:

```
T+0     你发消息
T+300ms ┌─ 💬 处理中 ─────────────────┐   秒级出现,即时反馈
        └──────────────────────────────┘
T+2s    ┌─ 💬 处理中 ─────────────────┐
        │  🔄 Read   src/app.tsx   …   │   工具逐个冒出
        └──────────────────────────────┘
T+6s    ┌─ ✓ 已完成 ─────────────────┐   header 变绿
        │  ✓ Read   src/app.tsx   0.3s │
        │  ✓ Edit   src/app.tsx   0.1s │
        │  ✗ Bash   pytest -xvs  5.2s │   失败红色
        │  ─────────────────────────── │
        │  🤖 Claude Code · 3 tools · 5.6s │
        └──────────────────────────────┘
```

状态:
| 回合状态 | header 文案 | template |
|---|---|---|
| 进行中 | 💬 处理中 | blue |
| 全部成功 | ✓ 已完成 | green |
| 有失败 | ⚠ 部分失败 | red |
| 中断/异常 | ⏸ 已中断 | orange |

行格式:`**✓ Read**  \`src/app.tsx\`  · 0.3s`;失败行追加一个 `note` 元素显示截断错误(≤1KB)。完整 input/output 不进飞书卡片,footer 提供深链跳转 desktop 对应 session 查看(桌面端已有完整结构化渲染)。

0 tool 回合(纯文本回答)不发卡片 —— 文字答案独立消息,沿用现状。

## 3. 飞书 API 约束(已核实)

- `PATCH /im/v1/messages/:message_id`,`msg_type=interactive`,content 为完整卡片 JSON 字符串 → **支持整卡替换式流式更新**。
- 卡片请求体硬上限 **30KB**。→ summary 卡片只放单行状态(不夹 input/output),结构上规避。
- **每条消息最多 20 次 edit**(`230072`)。→ 节流 + 软停。
- 速率 1000 req/min · 50 req/s · 群 5 QPS。
- text 消息 150KB(降级用)。
- 复用已有权限 `im:message` + `im:message:update_as_bot`,**无需新权限**。不复用 CardKit —— summary 卡片走标准 interactive 卡 + PATCH(更简单、不受 CardKit 元素流式语义约束)。

## 4. 架构

### 4.1 钩子(opt-in,base adapter no-op)

在 `core/adapter.py:BasePlatformAdapter` 新增一个**回合级工具卡片**协议,runner 通过一个**不透明 handle** 驱动,所有状态由 adapter 自己持有。这样 Feishu 的卡片逻辑完全封装在 Feishu 内,其它平台零影响。

```python
def supports_tool_card(self) -> bool:               # 默认 False
    return False

async def begin_tool_round(self, chat_id, *, reply_to=None, metadata=None) -> Any:   # 返回 handle 或 None
    return None

async def tool_round_start(self, handle, tool: dict) -> None:                        # tool: name, tool_id, input
    return None

async def tool_round_complete(self, handle, tool: dict) -> None:                     # tool: name, tool_id, result, is_error, error_message
    return None

async def end_tool_round(self, handle, *, success: bool = True) -> None:
    return None
```

### 4.2 runner 接线(`core/runner.py:_call_agent_streaming`,约 +8 行)

```
循环前:  tool_handle = await adapter.begin_tool_round(chat_id, reply_to, metadata) \
                       if adapter.supports_tool_card() else None
tool_start   分支: if tool_handle: await adapter.tool_round_start(tool_handle, {...})
                   else: await consumer.on_tool_call(...)        # 旧路径,其它平台不变
tool_complete 分支: if tool_handle: await adapter.tool_round_complete(tool_handle, {...})
循环后(成功/异常两条路径): if tool_handle: await adapter.end_tool_round(tool_handle, success=...)
```

`tool_start` 分支:**有 tool card 时不走旧的 `on_tool_call`(避免每工具一条消息)**;无 tool card 时保持旧行为,其它平台零回归。

### 4.3 Feishu 侧

- `begin_tool_round`:返回一个 `ToolCardState`(持有 builder + patcher,**此时不发消息**,惰性)。
- `tool_round_start`(首个工具):此刻才 `create` interactive 卡片消息("💬 处理中");记录每个 tool_id 的 monotonic 开始时间;append running 行;节流 patch。
- `tool_round_complete`:算 elapsed;把该行翻转 ✓/✗;失败时**立即 patch**(高优先级)。
- `end_tool_round`:finalize(header 变色 + footer;若触顶则加 "+N hidden" 提示 + desktop 深链)。0 tool 回合 → 从未建卡,无操作。
- 新增 `patch_tool_card(message_id, card_json)`:`im.v1.message.patch`,`msg_type=interactive`。

### 4.4 cards.py 两个类

- `ToolCardBuilder`:纯卡片 JSON 构造(schema 2.0 interactive)。方法:`initial()` / `append_tool(tool)` / `flip_tool(...)` / `finalize(outcome)` / `to_content_json()`。工具参数摘要器:Read/Edit/Write→`file_path`、Bash→`command`、Grep/Glob→`pattern`、其余→仅工具名。
- `ThrottledCardPatcher`:buffer + flush。flush 触发 = 首个工具 | 失败 | 累积 3 个工具 | 距上次 ≥1.5s | finalize。**软停在 edit #17**(留余量给 finalize)。`asyncio.Lock` 防并发。错误码 `230072`/`230025` → 记 warn、停止后续 patch、finalize 时再试 1 次。

## 5. 已接受的取舍(非 bug)

- **卡片顺序**:卡片在首个 `tool_start` 才创建。若 agent 先输出一段引导文字再调用工具,工具卡片会落在那段文字下方。Claude Code 通常是"工具优先",此情况罕见;保证 0-flicker,不做 create-then-delete。
- **详情折叠**:v1 用 `note` 元素显示失败工具的截断错误;完整 input/output 走 footer desktop 深链。`expandable_note` 作为 v2 增强点(builder 内单方法可换)。

## 6. 落地清单

1. `adapters/feishu/cards.py` (新):`ToolCardBuilder` + `ThrottledCardPatcher`。
2. `adapters/feishu.py`:`supports_tool_card`/`begin_tool_round`/`tool_round_start`/`tool_round_complete`/`end_tool_round` + `patch_tool_card`。
3. `core/adapter.py`:5 个 base no-op 钩子。
4. `core/runner.py`:接 `tool_handle` 生命周期。
5. `tests/test_feishu_cards.py`:builder 各状态 JSON、patcher 节流/软停/flush、0-tool 不建卡。

## 7. 验证

- 单测:mock adapter,跑 (a) 多工具成功、(b) 含失败、(c) 0 工具、(d) >17 edit 触顶 四个场景,断言 patch 次数、header 颜色、footer。
- 手测:真连飞书群发一个需要多工具的问题,确认实时生长 + 失败红色 + 0-tool 不发卡。

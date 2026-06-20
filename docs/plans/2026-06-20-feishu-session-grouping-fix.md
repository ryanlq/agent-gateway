# 飞书会话分组缺失 — 根因分析与修复方案

**日期:** 2026-06-20
**问题:** 飞书群聊话题、机器人私聊在桌面客户端被揉进**同一个聊天窗口/会话列表**,无法区分来源。
**目标:** 桌面端按"平台 × 群/私聊 × 话题"分开呈现飞书会话,与本地会话区分。

---

## 一、根因(已定位)

### 1.1 两条 session 通路,飞书复用了 email 专用的落盘逻辑

gateway 内有**两套 session 模型**:

| 模型 | 位置 | key | 用途 | 是否持久化 |
|---|---|---|---|---|
| `Session` + `SessionStore` | `core/session.py` | `platform:user:chat[:thread]` | 平台会话运行时上下文 | ❌ 仅内存 |
| `DesktopSession` (= `PersistedSession`) + `SessionManager` | `server/session_store.py` | `session_id`(UUID / 派生串) | 桌面端侧边栏 + CLI resume | ✅ `~/.nexus-agent/sessions.json` |

飞书消息进入后:
- `FeishuAdapter` 构造 `MessageSource(platform="feishu", user_id, chat_id, thread_id=root_id)` ✓
- `GatewayRunner.process_message()` 调 `self.session_store.get_or_create(source)` 拿到**内存** Session(这条是对的,key 含 thread_id)
- **但** 完成后调 `self._sync_to_desktop(source, ...)`(`runner.py:353`),把对话写进**持久化** store —— **而 `_sync_to_desktop` 是为 email 写的**

### 1.2 `_sync_to_desktop` 是 email 专用,飞书被硬塞进去

`runner.py:1013` `_sync_to_desktop` 的路由策略**全部是 email 概念**:
- `in_reply_to` / `references` header → 邮件线程
- `subject` → 话题标题
- `desktop_sid = f"email-{sender}-{subject_hash}"` ← **前缀写死 `email-`**

飞书消息进来时 `raw_message = {"event": event}`,**没有** `in_reply_to/references/message_id/subject`,于是:
- `subject = source.thread_id`(飞书话题 root_id)或空
- `sender = source.user_id`
- `desktop_sid = f"email-{user_id}-{hash(thread_id 或空)}"` 或 `f"email-{user_id}"`

**后果:** 同一个飞书用户在不同群、不同话题里的对话,**只要 `thread_id` 为空(非话题回复,即群聊普通消息或私聊),就全塌缩成同一个 `email-{user_id}` session** —— 这正是用户看到的"揉在一个窗口"。即便有话题,也只是按 user+thread 分,丢失了 `chat_id`(群)维度。

### 1.3 持久化 dataclass 根本没有来源字段

即便路由对了,`PersistedSession`(`session_store.py:30`)dataclass **没有** `platform/chat_id/thread_id/source` 字段:
- `store._session_info()`(`session_store.py:295`)输出 `"source": None` 硬编码
- 桌面端 `SessionInfo.source`(`nexus.ts:288`)永远是 null,sidebar 无任何信息分组

### 1.4 桌面端显示层不消费来源

- `chat/index.tsx:206` `threadKey = selectedSessionId || activeSessionId || ...` —— **单选会话模型**
- `chat/sidebar/index.tsx` 的 `workspaceGroupsFor` 按 `session.cwd` 分组,`profileGroups` 按 profile 分组 —— **没有按 platform/chat 分组**
- `session-row.tsx` 只渲染 agent 徽标(`claude-code` → "Claude"),**没有平台徽标**
- `SessionInfo.source` 字段存在但**全代码库无人读取**(grep 确认所有 `.source` 命中都是 `preview.source` / `voicePlayback.source`,无关)

---

## 二、数据流(目标态)

```
飞书消息
  → FeishuAdapter: MessageSource(platform=feishu, chat_id, thread_id)
  → Runner._sync_to_desktop (改造:平台感知路由)
       → store.create(session_id=feishu:{chat}[:{thread}], platform="feishu",
                      chat_id, thread_id, chat_type, source=...)
  → PersistedSession 新字段持久化
  → _session_info() 输出 platform/chat_id/thread_id/source
  → 桌面 /api/profiles/sessions
  → sidebar 按平台/会话分组 + session-row 平台徽标
```

---

## 三、实施计划(分 4 阶段,可独立验证)

### Phase 1 — 后端:持久化来源信息(核心,必做)

**目标:** 让 `sessions.json` 和 API 响应携带 platform/chat_id/thread_id,存量数据平滑兼容。

1. **`server/session_store.py` — `PersistedSession` dataclass 加字段**
   ```python
   # 加在 agent_type 附近
   platform: str | None = None        # "feishu" | "telegram" | None(本地会话)
   chat_id: str | None = None
   thread_id: str | None = None
   chat_type: str | None = None       # "p2p" | "group"
   source: str | None = None          # 显示用来源串,如 "飞书·XX群"
   ```
   - `from_dict` 已有 `filtered` 逻辑(只取 known 字段),**存量 JSON 自动兼容**:旧记录这 5 个字段读为 `None`。
   - 无需 migration。

2. **`store.create()` 接受新参数**
   ```python
   def create(self, *, session_id, agent_type=..., ..., title=None,
              platform=None, chat_id=None, thread_id=None, chat_type=None, source=None):
   ```
   透传给 `PersistedSession(...)`。

3. **`store._session_info()` 输出真实值**(替换 `session_store.py:314` 的 `"source": None`)
   ```python
   "source": session.source,
   "platform": session.platform,
   "chat_id": session.chat_id,
   "thread_id": session.thread_id,
   "chat_type": session.chat_type,
   ```

4. **`store.search()`**(`session_store.py:345`)同步把 `"source": None` 改成实际值。

**验证:** 手动发一条飞书消息,检查 `~/.nexus-agent/sessions.json` 新记录含 `platform/chat_id`;旧记录无该字段但能正常加载(不报错)。

### Phase 2 — 后端:平台感知的 session 路由(把 email 逻辑和飞书解耦)

**目标:** 飞书按 `chat_id + thread_id` 落盘,不再复用 email 的 `email-{user}` 派生逻辑。

1. **`runner.py:353` `_sync_to_desktop` 分支化**

   在方法开头按 `source.platform` 路由:
   ```python
   if source.platform in ("feishu", "telegram", "discord", ...):  # 即时通讯类
       return self._sync_chat_to_desktop(source, user_input, response, event)
   # 原有 email 逻辑保持不变
   ```
   （或更稳妥:保留 `_sync_to_desktop` 签名,内部 `if source.platform == "feishu":` 分流;email 走原路。避免影响 email 回归。)

2. **新增 `_sync_chat_to_desktop`** — IM 平台专用:
   ```python
   def _sync_chat_to_desktop(self, source, user_input, response, event):
       store = self._desktop_store
       if store is None: return
       # 确定性 session_id:平台 + chat + 可选 topic
       parts = [source.platform, source.chat_id]
       if source.thread_id:
           parts.append(source.thread_id)
       desktop_sid = "-".join(parts)   # e.g. feishu-{chat_id}-{root_id}
       existing = store.get(desktop_sid)
       if existing is None:
           title = self._derive_chat_title(source, user_input)
           store.create(
               session_id=desktop_sid,
               agent_type=store.get_config("default_agent", "claude-code-sdk"),
               title=title,
               platform=source.platform,
               chat_id=source.chat_id,
               thread_id=source.thread_id,
               chat_type=getattr(source, "chat_type", None),
               source=self._platform_display(source),
           )
           history = []
       else:
           history = list(existing.history)
       history.append({"role": "user", "content": user_input})
       if response:
           history.append({"role": "assistant", "content": str(response)})
       store.update_history(desktop_sid, history)
       store.update(desktop_sid, last_active=time.time())
   ```

3. **`_derive_chat_title` / `_platform_display`** 辅助:
   - 标题:`source.display_name` 或 `用户消息前 40 字`,前缀加平台 emoji(💬 飞书)。
   - 显示串:`"飞书"` / 私聊 vs 群:`"飞书·群聊"` / `"飞书·私聊"`(用 `chat_type`)。

4. **Email 路径零改动** —— Phase 1 的字段对 email 同样适用(将来 email 也能填 `platform="email"`),但不在本阶段动 email 落盘逻辑,降低回归风险。

**关键决策点(Phase 2):** `desktop_sid` 用 `feishu-{chat_id}-{thread_id}` 还是 `feishu-{chat_id}`(话题归并到群)?
- **推荐 `+thread_id`**(话题独立 session),与后端 `session_key` 语义一致,且符合用户预期"话题即独立会话"。
- 代价:群聊里每个话题一条记录。可用 sidebar 分组消化(Phase 3)。

**验证:** 同一用户在飞书群聊发普通消息、开话题回复、私聊 → 桌面侧边栏出现 3 条独立 session,id 不同。

### Phase 3 — 前端:侧边栏分组 + 来源徽标

**目标:** 桌面端把飞书会话可视化为独立、可辨识的条目。

1. **`types/nexus.ts` — `SessionInfo` 补字段**(已存 `source`,补齐)
   ```ts
   platform?: null | string
   chat_id?: null | string
   thread_id?: null | string
   chat_type?: null | string
   ```

2. **`chat/sidebar/session-row.tsx` — 平台徽标**
   - 复用现成的 `PlatformAvatar`(`messaging/platform-icon.tsx`,已支持 feishu 图标)。
   - `session.platform === 'feishu'` 时在标题左侧渲染小图标 + 弱化的来源标签(如 "飞书·群")。
   - 本地会话(platform=null)不显示徽标,保持现状。

3. **`chat/sidebar/index.tsx` — 分组策略**
   - 新增"来源"分组维度:按 `session.platform` 分(本地 / 飞书 / 其他),或在 workspace 分组之上叠加。
   - **最小改动方案:** 不动现有 workspace/profile 分组,只在 session-row 上加徽标 + 标题前缀(如 "💬 飞书·XX群"),靠标题前缀的视觉区分。**这是 Phase 3 的 MVP。**
   - **进阶方案(可选):** sidebar 增加一个分组开关(类似现有 `agentsGrouped`),把 IM 平台会话单独成组。

4. **会话标题** — Phase 2 后端已写入 `title`,前端 `sessionTitle()`(`lib/chat-runtime.ts`)会自然显示,无需改。

**验证:** 侧边栏飞书会话带图标 + 标题前缀;点击切到不同 session 显示各自历史。

### Phase 4 — 收尾:存量数据 & 边界

1. **存量飞书会话**(`email-{user}` 格式)—— 已有的会停留在旧格式,不迁移(避免破坏);新消息走新逻辑。可选:加个一次性回填脚本,但优先级低。
2. **测试** `tests/test_session_store.py` / 新增 `tests/test_runner_sync.py`:
   - `_sync_chat_to_desktop` 对 feishu/telegram 生成正确 session_id 和字段
   - `from_dict` 对缺字段的旧记录返回 `None`(不抛)
   - email 路径回归(`_sync_to_desktop` email 分支不变)
3. **`/new` `/reset` 命令** 在飞书侧的行为确认 —— 这些命令 reset 内存 SessionStore,但持久化 desktop session 不受影响(符合预期:重置=新内存上下文,历史记录保留)。

---

## 四、风险与权衡

| 风险 | 缓解 |
|---|---|
| Phase 2 改 `_sync_to_desktop` 影响 email 回归 | 分支化,email 路径原样保留 + 专项测试 |
| `desktop_sid` 格式变化导致历史 session "失联" | 存量 email 会话保持旧 id 不动;仅新飞书会话用新 id |
| 话题多 → 侧边栏条目爆炸 | Phase 3 分组 / 标题前缀消化;极端情况后续可加"折叠同群话题" |
| 同一群不同用户的会话 | `desktop_sid` 用 `chat_id` 不含 `user_id` —— 群里多人的消息**归到同一条 session**(合理:群会话是共享的)。若需按用户分,加 user_id 维度 |

**`desktop_sid` 维度决策(需用户确认):**
- 方案 A(推荐):`feishu-{chat_id}[-{thread_id}]` — 群话题独立,群里多人共享一条群 session
- 方案 B:`feishu-{chat_id}-{user_id}` — 按用户隔离(私聊场景合理,群聊割裂)
- 方案 C:混合 — DM 按 user,group 按 chat+thread

---

## 五、推荐落地顺序

1. **Phase 1**(后端字段) — 30 分钟,纯增量,零回归风险
2. **Phase 2**(路由解耦) — 核心,需 + 测试,1-2 小时
3. **Phase 3 MVP**(前端徽标 + 标题前缀) — 30 分钟,视觉立竿见影
4. **Phase 4**(测试 + 边界) — 随各阶段进行

Phase 1+2+3MVP 即可解决用户报告的问题。Phase 3 进阶分组、Phase 4 存量迁移为 nice-to-have。

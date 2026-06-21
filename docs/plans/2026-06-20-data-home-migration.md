# 数据家目录迁移:`~/.hermes` → `~/.nexus-agent`

**日期:** 2026-06-20
**问题:** agent-gateway 是从上游 Hermes 项目抽取出来二次开发的独立模块,但运行时仍把部分数据写到 `~/.hermes` —— 这是上游遗留的目录名,对一个独立产品(Nexus Agent)不合理。当前 gateway/桌面端的数据散落在**三个**家目录(`~/.hermes`、`~/.nexus-agent`、`~/.agent_gateway`),命名混乱、职责不清。
**目标:** 把所有运行时数据收口到 `~/.nexus-agent`(Windows: `%LOCALAPPDATA%\nexus-agent`),彻底清除 `~/.hermes` 残留,并消除 gateway 内部的目录解析不一致。

---

## 一、现状盘点(地面真相)

实测磁盘布局(2026-06-20):

| 数据 | 当前位置 | 归属 | 状态 |
|---|---|---|---|
| 平台配置 | `~/.nexus-agent/gateway.yaml` | gateway | ✅ 已在新家 |
| gateway 配置 JSON | `~/.nexus-agent/gateway-config.json` | gateway | ✅ |
| sessions | `~/.nexus-agent/sessions.json`(438KB) | gateway | ✅ |
| cron | `~/.nexus-agent/cron/`(锁文件 `.tick.lock`) | gateway | ✅ |
| sidecar 下载 | `~/.nexus-agent/gateway/` | desktop | ✅(但 `~/.hermes/gateway/` 也有,重复) |
| **gateway 日志** | `~/.nexus-agent/logs/gateway.log` | gateway | ✅ **本次已迁**(commit `5f2ef6c`) |
| adapter 状态游标 | `~/.agent_gateway/state/` | gateway | ⚠️ 模块名目录,非 `.hermes`,但未统一 |
| 媒体缓存 | `~/.agent_gateway/cache/` | gateway | ⚠️ 同上 |
| 投递输出 | `~/.agent_gateway/output/` | gateway | ⚠️ 同上 |
| **桌面端整个家** | `~/.hermes/`(config.yaml / state.db / .env / skills / memories / ...) | desktop | ❌ **真正的大头,未迁** |

### gateway 侧解析器现状(内部不一致)

gateway 有**多个各自为政**的家目录解析点,无统一函数:

| 解析点 | 文件 | 默认值 | 是否认 `NEXUS_AGENT_HOME` |
|---|---|---|---|
| 日志 | `__main__.py:35` `_resolve_log_dir()` | `~/.nexus-agent/logs` | ✅ |
| config / data 根 | `__main__.py:28` `_NEXUS_AGENT_DIR` | `~/.nexus-agent` | ❌ **硬编码** |
| session 持久化 | `server/session_store.py:27` `_DEFAULT_STORE_DIR` | `~/.nexus-agent` | ❌ 硬编码 |
| adapter 状态 | `utils/state.py:19` `_STATE_DIR` | `~/.agent_gateway/state` | ❌ 硬编码 |
| 媒体缓存 | `media/cache.py:26` `_DEFAULT_CACHE_ROOT` | `~/.agent_gateway/cache` | ❌ 硬编码 |
| 投递输出 | `core/delivery.py:177` `output_dir` | `~/.agent_gateway/output` | ❌ 硬编码 |

**问题:** 日志认 env,config/sessions 不认;state/cache/output 用的是模块名 `.agent_gateway` 而非产品名。三套目录、两套命名。

---

## 二、历史脉络与核心洞察

### 2.1 `.hermes` 是怎么残留的

上游 Hermes 产品的家目录就是 `~/.hermes`。agent-gateway 抽取时,gateway 的日志解析器原样继承了 `~/.hermes/logs` 默认值,而桌面端(`hermes-desktop`,后更名 Nexus Agent)的 `resolveNexusHome()` 也默认 `~/.hermes`。两者通过"碰巧同一个默认值"达成共享日志目录。

但 gateway 在抽取过程中,已经把 config/sessions/cron/sidecar 迁到了 `~/.nexus-agent`,**唯独日志漏网**。本次(2026-06-20)已补齐日志迁移。

### 2.2 核心洞察(为什么比看起来简单)

1. **gateway 侧已基本搬完**:config/sessions/cron/sidecar/日志 都在 `~/.nexus-agent`。剩下的 `~/.agent_gateway`(state/cache/output)是**模块名**,不是 `.hermes` 残留 —— 属于"统一命名"范畴,风险低。
2. **桌面端所有路径收口到一个函数** `resolveNexusHome()`(`electron/main.cjs:152`)。渲染层 `src/` 只在注释和测试 fixture 里提到路径,**不直接读写**(grep 确认)。所以桌面端搬家 = **翻这一个解析器的默认值 + 一次性目录迁移**,不是满地改路径。
3. **应用身份迁移已完成**:app ID(`com.nousresearch.nexus-agent`)、`window.nexusAgent` API、README 文档都已就位。
4. **安装脚本已退役**:`scripts/install.sh`、`scripts/install.ps1` 已不在仓库(`scripts/` 只剩 `assert-root-install.cjs`),`main.cjs` 里引用它们的是**过期注释**。无需迁移安装逻辑。

---

## 三、子任务 A:gateway 统一到 `~/.nexus-agent`(低风险,独立,建议先做)

### A1. 收口目录解析器

新增一个统一解析函数(建议放 `utils/paths.py` 或 `__main__.py`):

```python
def resolve_home() -> Path:
    """Gateway 数据家目录。认 NEXUS_AGENT_HOME,默认 ~/.nexus-agent。"""
    env_home = os.environ.get("NEXUS_AGENT_HOME")
    return Path(env_home) if env_home else Path.home() / ".nexus-agent"
```

将以下全部改为经 `resolve_home()` 派生:

| 文件 | 改动 |
|---|---|
| `__main__.py:28` | `_NEXUS_AGENT_DIR = resolve_home()`(原硬编码 → 认 env) |
| `__main__.py:35` `_resolve_log_dir()` | 复用 `resolve_home() / "logs"`(逻辑不变,去重) |
| `server/session_store.py:27` | `_DEFAULT_STORE_DIR = str(resolve_home())` |
| `utils/state.py:19` | `_STATE_DIR = resolve_home() / "state"`(`~/.agent_gateway` → `~/.nexus-agent`) |
| `media/cache.py:26` | `_DEFAULT_CACHE_ROOT = resolve_home() / "cache"` |
| `core/delivery.py:177` | `output_dir = resolve_home() / "output"` |

### A2. 一次性迁移 `~/.agent_gateway` → `~/.nexus-agent`

启动时(early init)若新子目录不存在但 `~/.agent_gateway` 存在 → 搬过来:

```python
def migrate_legacy_agent_gateway_home() -> None:
    home = resolve_home()
    legacy = Path.home() / ".agent_gateway"
    if not legacy.is_dir():
        return
    for sub in ("state", "cache", "output"):
        src, dst = legacy / sub, home / sub
        if src.exists() and not dst.exists():
            shutil.move(str(src), str(dst))  # 同文件系统原子
```

数据小、可重建,风险低;保留 `~/.agent_gateway` 作 legacy fallback 读取若干版本后再删。

### A3. 收益

gateway 数据完全自洽于 `~/.nexus-agent`,消除三目录/双命名;`NEXUS_AGENT_HOME` 一处覆盖全部。

---

## 四、子任务 B:桌面端 `~/.hermes` → `~/.nexus-agent`(大头,核心)

### B1. 翻解析器默认值(`electron/main.cjs:resolveNexusHome()`)

```js
// macOS / Linux
- return path.join(app.getPath("home"), ".hermes");
+ return path.join(app.getPath("home"), ".nexus-agent");
// Windows
- const localappdata = path.join(process.env.LOCALAPPDATA, "hermes");
+ const localappdata = path.join(process.env.LOCALAPPDATA, "nexus-agent");
- const legacy = path.join(app.getPath("home"), ".hermes");
+ // legacy 探测仍指向 .hermes(迁移用)
```

### B2. 一次性透明迁移(app ready 早期,打开 state.db / spawn gateway **之前**)

```js
function migrateLegacyHermesHome(newHome) {
  const legacy = path.join(app.getPath("home"), ".hermes");
  if (!directoryExists(legacy)) return;
  if (directoryExists(newHome)) {
    mergeLegacyIntoNew(legacy, newHome);  // 两者都在 → 去重合并
  } else {
    fs.renameSync(legacy, newHome);  // 同文件系统原子 rename 整棵树
  }
}
```

**关键约束:**
- 必须在 `state.db` 打开前、gateway spawn 前执行(此时无进程持有 `~/.hermes`)
- 整目录 rename(非文件级),SQLite / 子目录一并搬走
- 迁移后写一个 marker(如 `~/.nexus-agent/.migrated-from-hermes`),避免重复迁移

### B3. 数据分类(迁移 vs 丢弃)

**必须迁移**(用户数据 / 密钥 / 不可重建):
`config.yaml`(待确认,见 §七)、`state.db` + `state.db-shm/wal`、`kanban.db`、`.env`、`skills/`(39 个)、`memories/`、`sessions/`、`pairing/`、`hooks/`、`SOUL.md`、`auth.json`、`channel_directory.json`、`gateway_state.json`、`processes.json`、`.install_method`

**可重建 / 可丢弃**(缓存):`cache/`、`image_cache/`、`audio_cache/`、`bootstrap-cache/`、`models_dev_cache.json`、`ollama_cloud_models_cache.json`、`provider_models_cache.json`、`state-snapshots/`、`bin/`

**去重(新家已有则跳过)**:`cron/`、`gateway/`(sidecar,新家已有活跃副本)

### B4. Windows 三层 legacy 检测

现有逻辑(`%LOCALAPPDATA%\hermes` vs `~/.hermes`)扩展为优先级链:
`新位置 %LOCALAPPDATA%\nexus-agent` → 迁移自 `%LOCALAPPDATA%\hermes` → 迁移自 `~/.hermes`。

---

## 五、加固建议(独立于搬家,建议同步做)

1. **桌面端 spawn gateway 时显式传 `NEXUS_AGENT_HOME`** —— 目前**没传**(见 `main.cjs:resolveGatewayBackend()` 的 `env: {}` / `env: { PYTHONPATH }`)。gateway 靠自己的默认值**碰巧**对上 `~/.nexus-agent`;一旦任一方默认值漂移就静默错位。显式传 `env: { NEXUS_AGENT_HOME: NEXUS_AGENT_HOME }` 杜绝隐患。
2. 清理 `main.cjs` 指向已退役 `install.sh`/`install.ps1` 的过期注释。

---

## 六、风险与回滚

| 风险 | 缓解 |
|---|---|
| **state.db(5.7MB)迁移失败** | 整目录原子 rename;rename 前确保目标不存在;失败可逆向 rename 回 `~/.hermes` |
| **`.env`(密钥)丢失** | 迁到新位置、确认可读后再删旧;迁移用 rename 而非 copy+delete |
| **并发持有 `~/.hermes`** | desktop 启动时 gateway 尚未 spawn,迁移窗口安全;迁移在 app ready 最早阶段 |
| **`~/.hermes` 与 `~/.nexus-agent` 共存(当前机器即此态)** | 走 merge 分支而非 rename;逐项去重,sidecar/cron 以新家为准 |
| **legacy 用户升级后找不到数据** | 保留 legacy fallback 读取若干版本;marker 文件防重复迁移 |

---

## 七、执行顺序

1. **子任务 A**(gateway 统一 `~/.nexus-agent` + `resolve_home()` 收口)+ 加固项 1 → 独立、低风险,先落地
2. **B1 + B2**(翻默认值 + 迁移逻辑)→ 桌面端核心,用 `scripts/test-desktop.mjs` 的 fresh userData 沙箱验证
3. **B3 + B4**(去重 + Windows 三层)→ 清理过期注释
4. **N 个版本后**移除 legacy fallback 与 marker 检测

---

## 八、动手前待确认(2 点)

1. **`~/.hermes/config.yaml` 还有没有人读?** `main.cjs` 和 gateway 全代码库都**无** `config.yaml` 字面量。疑似已被 `~/.nexus-agent/gateway-config.json` 取代、`~/.hermes/config.yaml`(6月8日,14KB)是旧 hermes_cli 死灰烬。**需确认** —— 若已死,迁移时直接丢,不搬。
2. **`~/.hermes/sessions/`(桌面端 session)vs `~/.nexus-agent/sessions.json`(gateway session)** 是两套不同东西,还是其一已废弃?迁移时勿合并错了。

---

## 九、验证清单

- [ ] gateway: `_resolve_log_dir()` / `_NEXUS_AGENT_DIR` / `_STATE_DIR` / `_DEFAULT_CACHE_ROOT` / `output_dir` 全部经 `resolve_home()`,单测覆盖 `NEXUS_AGENT_HOME` 覆盖分支
- [ ] gateway: 启动时 `~/.agent_gateway` → `~/.nexus-agent` 迁移逻辑 + legacy fallback
- [ ] gateway: `pytest -q` 全绿(当前基线 293 passed)
- [ ] desktop: `resolveNexusHome()` 默认值翻转 + 迁移函数,单元测试覆盖 rename / merge / Windows 三层
- [ ] desktop: fresh userData 沙箱跑 `test-desktop.mjs`,验证从空状态 + 从 `~/.hermes` 两种起点都落到 `~/.nexus-agent`
- [ ] desktop: spawn gateway 时传入 `NEXUS_AGENT_HOME`,确认 gateway 日志/sessions 落在预期目录
- [ ] desktop: `tsc --noEmit` 通过
- [ ] 端到端:迁移后 `~/.hermes` 可安全删除(保留 fallback 期内不删)

---

## 附:相关提交

- `5f2ef6c`(agent-gateway)fix(logs): move gateway log home off legacy ~/.hermes to ~/.nexus-agent/logs
- `bfb0d80`(hermes-desktop)fix(status): tail live gateway.log instead of the vestigial gui.log

"""
Agent Cron Tool — structured protocol for agent-initiated cron operations.

Agents embed ``<!--CRON_OPERATION ... -->`` blocks in their text output to
request cron job / script creation.  The gateway post-processes agent
responses, detects these blocks, validates and executes the operations via
:class:`CronManager`, and replaces the blocks with human-readable confirmations.

Supported actions:
    - ``create_job``   — create a scheduled cron job
    - ``create_script`` — write an automation script to ``~/.nexus-agent/scripts/``
    - ``delete_job``   — delete a cron job
    - ``pause_job``    — pause a cron job
    - ``resume_job``   — resume a paused cron job
    - ``list_jobs``    — list all cron jobs (returned as formatted text)
"""

from __future__ import annotations

import json
import logging
import os
import re
import stat
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex for extracting operation blocks
# ---------------------------------------------------------------------------

# Matches: <!--CRON_OPERATION\n```json\n{...}\n```\n-->
# Also tolerates variants without the code fence or with extra whitespace.
_RE_CRON_BLOCK = re.compile(
    r"<!--CRON_OPERATION\s*\n"
    r"(?:```(?:json)?\s*\n)?"
    r"(?P<json>\{.*?\})"
    r"(?:\s*\n```)?"
    r"\s*\n?-->",
    re.DOTALL,
)

# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

_MAX_OPS_PER_WINDOW = 5
_RATE_WINDOW_SECONDS = 600  # 10 minutes

# Per-session operation timestamps: { session_key: [timestamp, ...] }
_op_timestamps: Dict[str, List[float]] = {}


def _check_rate_limit(session_key: str) -> bool:
    """Return True if the session is within rate limits."""
    now = time.monotonic()
    timestamps = _op_timestamps.get(session_key, [])
    # Prune old timestamps
    timestamps = [t for t in timestamps if now - t < _RATE_WINDOW_SECONDS]
    timestamps.append(now)
    _op_timestamps[session_key] = timestamps
    return len(timestamps) <= _MAX_OPS_PER_WINDOW


# ---------------------------------------------------------------------------
# Script content validation
# ---------------------------------------------------------------------------

_DANGEROUS_PATTERNS = [
    re.compile(r"rm\s+-rf\s+/", re.IGNORECASE),
    re.compile(r"\bsudo\b", re.IGNORECASE),
    re.compile(r"\bmkfs\b", re.IGNORECASE),
    re.compile(r"\bdd\s+if=", re.IGNORECASE),
    re.compile(r">\s*/dev/sd", re.IGNORECASE),
    re.compile(r"curl\s+.*\|\s*(ba)?sh", re.IGNORECASE),
    re.compile(r"wget\s+.*\|\s*(ba)?sh", re.IGNORECASE),
    re.compile(r"\bchmod\s+777\b", re.IGNORECASE),
    re.compile(r"\bchown\s+root\b", re.IGNORECASE),
]

_SCRIPT_FILENAME_RE = re.compile(r"^[a-zA-Z0-9_-]+\.(sh|bash|py)$")

_SCRIPTS_DIR = Path.home() / ".nexus-agent" / "scripts"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CronOperation:
    """A parsed cron operation extracted from agent output."""

    action: str
    params: Dict[str, Any]
    raw_block: str  # The original matched text for replacement


@dataclass
class CronOperationResult:
    """The result of executing a cron operation."""

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    raw_block: str = ""  # Link back to the original block for replacement


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class CronToolParser:
    """Extract and replace ``<!--CRON_OPERATION ... -->`` blocks."""

    @staticmethod
    def extract_operations(text: str) -> List[CronOperation]:
        """Extract all cron operation blocks from agent output text."""
        ops: List[CronOperation] = []
        for match in _RE_CRON_BLOCK.finditer(text):
            raw_block = match.group(0)
            json_str = match.group("json")
            try:
                payload = json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.warning("Failed to parse CRON_OPERATION JSON: %s", e)
                continue

            action = payload.get("action", "")
            params = payload.get("params", {})
            if not action or not isinstance(action, str):
                logger.warning("CRON_OPERATION missing 'action' field")
                continue
            if not isinstance(params, dict):
                logger.warning("CRON_OPERATION 'params' must be a dict")
                continue

            ops.append(CronOperation(action=action, params=params, raw_block=raw_block))
        return ops

    @staticmethod
    def replace_operations(
        text: str, results: List[CronOperationResult]
    ) -> str:
        """Replace operation blocks with their result messages."""
        for result in results:
            if result.raw_block and result.raw_block in text:
                icon = "✅" if result.success else "❌"
                replacement = f"{icon} {result.message}"
                text = text.replace(result.raw_block, replacement)
        return text


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class CronToolExecutor:
    """Execute parsed cron operations via :class:`CronManager`."""

    def __init__(self, cron_manager: Any) -> None:
        self._mgr = cron_manager

    async def execute_all(
        self,
        ops: List[CronOperation],
        origin: Optional[Dict[str, Any]] = None,
        session_key: str = "",
    ) -> List[CronOperationResult]:
        """Execute all operations, returning results."""
        results: List[CronOperationResult] = []
        for op in ops:
            # Rate limit check
            if session_key and not _check_rate_limit(session_key):
                results.append(CronOperationResult(
                    success=False,
                    message="操作过于频繁，请稍后再试（每10分钟最多5次操作）",
                    raw_block=op.raw_block,
                ))
                continue

            try:
                result = await self._execute_one(op, origin=origin)
                result.raw_block = op.raw_block
                results.append(result)
            except Exception as exc:
                logger.exception("Cron operation '%s' failed: %s", op.action, exc)
                results.append(CronOperationResult(
                    success=False,
                    message=f"操作失败: {exc}",
                    raw_block=op.raw_block,
                ))
        return results

    async def _execute_one(
        self, op: CronOperation, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """Dispatch a single operation."""
        handler = {
            "create_job": self._create_job,
            "create_script": self._create_script,
            "delete_job": self._delete_job,
            "pause_job": self._pause_job,
            "resume_job": self._resume_job,
            "list_jobs": self._list_jobs,
        }.get(op.action)

        if not handler:
            return CronOperationResult(
                success=False,
                message=f"未知操作: '{op.action}'。支持: create_job, create_script, delete_job, pause_job, resume_job, list_jobs",
            )

        return await handler(op.params, origin=origin)

    # -- Individual operations ------------------------------------------------

    async def _create_job(
        self, params: Dict[str, Any], *, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """Create a cron job via CronManager."""
        prompt = params.get("prompt", "").strip()
        schedule = params.get("schedule", "").strip()
        name = params.get("name", "").strip() or None
        script = params.get("script", "").strip() or None
        deliver = params.get("deliver", "origin").strip()

        if not prompt and not script:
            return CronOperationResult(
                success=False, message="创建任务失败: 需要提供 'prompt' 或 'script'"
            )
        if not schedule:
            return CronOperationResult(
                success=False, message="创建任务失败: 需要提供 'schedule' 参数"
            )

        # Security: constrain deliver to origin or local only
        if deliver not in ("origin", "local", ""):
            deliver = "origin"

        # Resolve deliver target
        if deliver == "origin" and origin:
            parts = [origin.get("platform", "")]
            chat_id = origin.get("chat_id")
            if chat_id:
                parts.append(str(chat_id))
            thread_id = origin.get("thread_id")
            if thread_id:
                parts.append(str(thread_id))
            deliver = ":".join(parts) if len(parts) > 1 else "local"
        elif deliver == "origin":
            deliver = "local"

        try:
            job = self._mgr.create_job(
                prompt=prompt or None,
                schedule=schedule,
                name=name,
                deliver=deliver,
                origin=origin,
                script=script,
            )
        except ValueError as e:
            return CronOperationResult(success=False, message=f"创建任务失败: {e}")
        except Exception as e:
            return CronOperationResult(success=False, message=f"创建任务失败: {e}")

        schedule_display = job.get("schedule_display", schedule)
        next_run = job.get("next_run_at", "?")
        job_id = job.get("id", "?")
        job_name = job.get("name", "cron job")

        return CronOperationResult(
            success=True,
            message=(
                f"已创建定时任务 \"{job_name}\"\n"
                f"• ID: {job_id}\n"
                f"• 计划: {schedule_display}\n"
                f"• 下次执行: {next_run}"
            ),
            data=job,
        )

    async def _create_script(
        self, params: Dict[str, Any], *, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """Write an automation script to the scripts directory."""
        filename = params.get("filename", "").strip()
        content = params.get("content", "").strip()

        if not filename:
            return CronOperationResult(
                success=False, message="创建脚本失败: 需要提供 'filename'"
            )
        if not content:
            return CronOperationResult(
                success=False, message="创建脚本失败: 脚本内容不能为空"
            )

        # Validate filename
        if not _SCRIPT_FILENAME_RE.match(filename):
            return CronOperationResult(
                success=False,
                message=(
                    f"创建脚本失败: 文件名 '{filename}' 不合法。"
                    "只允许字母、数字、下划线、短横线，扩展名为 .sh/.bash/.py"
                ),
            )

        # Validate content for dangerous patterns
        for pattern in _DANGEROUS_PATTERNS:
            if pattern.search(content):
                return CronOperationResult(
                    success=False,
                    message=f"创建脚本失败: 内容包含不允许的危险命令",
                )

        # Write script
        _SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        script_path = _SCRIPTS_DIR / filename

        # Resolve to ensure no path traversal
        try:
            script_path.resolve().relative_to(_SCRIPTS_DIR.resolve())
        except ValueError:
            return CronOperationResult(
                success=False, message="创建脚本失败: 非法的文件路径"
            )

        try:
            script_path.write_text(content, encoding="utf-8")
            # Set permissions: owner read/write/execute
            script_path.chmod(
                stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
            )
        except OSError as e:
            return CronOperationResult(
                success=False, message=f"创建脚本失败: 写入文件出错: {e}"
            )

        logger.info("Created automation script: %s", script_path)

        return CronOperationResult(
            success=True,
            message=(
                f"已创建自动化脚本 `{filename}`\n"
                f"• 路径: {script_path}\n"
                f"• 可在创建定时任务时通过 `\"script\": \"{filename}\"` 引用"
            ),
            data={"filename": filename, "path": str(script_path)},
        )

    async def _delete_job(
        self, params: Dict[str, Any], *, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """Delete a cron job."""
        job_id = params.get("job_id", "").strip()
        if not job_id:
            return CronOperationResult(
                success=False, message="删除失败: 需要提供 'job_id'"
            )

        ok = self._mgr.delete_job(job_id)
        if ok:
            return CronOperationResult(
                success=True, message=f"已删除定时任务 (ID: {job_id})"
            )
        return CronOperationResult(
            success=False, message=f"删除失败: 未找到任务 {job_id}"
        )

    async def _pause_job(
        self, params: Dict[str, Any], *, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """Pause a cron job."""
        job_id = params.get("job_id", "").strip()
        if not job_id:
            return CronOperationResult(
                success=False, message="暂停失败: 需要提供 'job_id'"
            )

        job = self._mgr.pause_job(job_id)
        if job:
            return CronOperationResult(
                success=True,
                message=f"已暂停定时任务 \"{job.get('name', job_id)}\" (ID: {job_id})",
            )
        return CronOperationResult(
            success=False, message=f"暂停失败: 未找到任务 {job_id}"
        )

    async def _resume_job(
        self, params: Dict[str, Any], *, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """Resume a paused cron job."""
        job_id = params.get("job_id", "").strip()
        if not job_id:
            return CronOperationResult(
                success=False, message="恢复失败: 需要提供 'job_id'"
            )

        job = self._mgr.resume_job(job_id)
        if job:
            return CronOperationResult(
                success=True,
                message=(
                    f"已恢复定时任务 \"{job.get('name', job_id)}\" (ID: {job_id})\n"
                    f"• 下次执行: {job.get('next_run_at', '?')}"
                ),
            )
        return CronOperationResult(
            success=False, message=f"恢复失败: 未找到任务 {job_id}"
        )

    async def _list_jobs(
        self, params: Dict[str, Any], *, origin: Optional[Dict[str, Any]] = None
    ) -> CronOperationResult:
        """List all cron jobs as formatted text."""
        jobs = self._mgr.list_jobs()
        if not jobs:
            return CronOperationResult(
                success=True, message="当前没有任何定时任务。"
            )

        lines = [f"📋 **定时任务列表** ({len(jobs)} 个)\n"]
        for j in jobs:
            state_icon = {"scheduled": "🟢", "paused": "⏸️", "completed": "✅", "error": "❌"}.get(
                j.get("state", ""), "❓"
            )
            job_id = j.get("id", "?")
            name = j.get("name", "?")
            schedule = j.get("schedule_display", "?")
            next_run = j.get("next_run_at", "?")
            last_run = j.get("last_run_at", "-")
            lines.append(
                f"{state_icon} **{name}** (ID: `{job_id}`)\n"
                f"   计划: {schedule} | 下次: {next_run} | 上次: {last_run}"
            )

        return CronOperationResult(
            success=True, message="\n".join(lines), data={"count": len(jobs)}
        )

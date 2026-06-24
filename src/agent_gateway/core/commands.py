"""Shared parsing helpers for gateway slash commands.

Both command surfaces — the desktop JSON-RPC path (``server/methods.py``) and
the IM adapter path (``core/runner.py``) — call into these helpers so a command
is parsed identically everywhere. This is the single source of truth that the
"unified in gateway, shared by both ends" design depends on; without it the two
dispatch layers drift (they already have duplicated ``/cron`` handlers).

Currently provides ``/loop`` parsing. ``/clear`` / ``/compact`` / ``/goal`` will
slot in here as they land.
"""

from __future__ import annotations

import re
import shlex
from typing import Optional

# Bare duration token: 10m, 2h, 1d, 45s
_DURATION_RE = re.compile(r"^\d+[smhd]$", re.IGNORECASE)

# A single cron-expr field (digits, *, -, ,, /) — used to detect a 5-field cron
# schedule passed to /loop without quotes splitting it.
_CRON_FIELD_RE = re.compile(r"^[\d\*\-,/]+$")


def normalize_loop_schedule(interval: str) -> str:
    """Force a ``/loop`` interval to be RECURRING.

    ``/loop`` always means "run repeatedly", so a bare duration is promoted to
    ``every <dur>``. This is the meaningful difference from ``/schedule``:
    ``/schedule 10m X`` is one-shot (parse_schedule treats ``10m`` as once),
    while ``/loop 10m X`` is recurring.

    - ``10m``           -> ``every 10m``   (bare duration -> recurring)
    - ``every 2h``      -> ``every 2h``    (already recurring)
    - ``*/10 * * * *``  -> as-is           (cron expr, already recurring)

    Raises ``ValueError`` if the interval is neither a duration, an ``every``
    form, nor a cron expression (natural-language intervals are left to the
    agent-parse fallback in the caller).
    """
    s = interval.strip()
    if not s:
        raise ValueError("缺少循环间隔")
    low = s.lower()

    if low.startswith("every "):
        return s

    # 5+ space-separated cron fields, each a valid cron token -> cron expr
    parts = s.split()
    if len(parts) >= 5 and all(_CRON_FIELD_RE.match(p) for p in parts[:5]):
        return s

    if _DURATION_RE.match(low):
        return f"every {low}"

    raise ValueError(
        f"无法识别的循环间隔 '{interval}'。"
        "示例: /loop 10m <任务>、/loop every 2h <任务>、/loop \"*/10 * * * *\" <任务>"
    )


def parse_loop_args(args: str) -> tuple[str, str, Optional[int]]:
    """Parse ``<interval> [--max N] <prompt>`` for ``/loop``.

    Returns ``(recurring_schedule, prompt, max_runs)``. ``max_runs`` is the
    optional iteration cap (``--max N`` or ``--max=N``); ``None`` means run
    forever. Uses ``shlex.split`` so a quoted cron expression
    (``"*/10 * * * *"``) survives as one interval token.

    Raises ``ValueError`` on missing/empty interval or prompt, or an invalid
    ``--max`` value.
    """
    args = args.strip()
    if not args:
        raise ValueError("用法: /loop <间隔> <任务>  例: /loop 10m 检查部署状态")

    try:
        tokens = shlex.split(args)
    except ValueError:
        # Unbalanced quotes — fall back to a naive split.
        tokens = args.split()
    if not tokens:
        raise ValueError("用法: /loop <间隔> <任务>  例: /loop 10m 检查部署状态")

    # 'every <dur>' consumes two tokens as the interval.
    if tokens[0].lower() == "every" and len(tokens) >= 2:
        interval = f"{tokens[0]} {tokens[1]}"
        rest = tokens[2:]
    else:
        interval = tokens[0]
        rest = tokens[1:]

    # Pull an optional ``--max N`` / ``--max=N`` iteration cap out of the
    # remaining tokens so it never becomes part of the prompt text.
    max_runs: Optional[int] = None
    cleaned: list[str] = []
    i = 0
    while i < len(rest):
        tok = rest[i]
        if tok == "--max":
            if i + 1 >= len(rest):
                raise ValueError("--max 需要一个正整数  例: /loop 10m --max 5 <任务>")
            max_runs = _parse_max_value(rest[i + 1])
            i += 2
            continue
        if tok.startswith("--max="):
            max_runs = _parse_max_value(tok[len("--max="):])
            i += 1
            continue
        cleaned.append(tok)
        i += 1

    prompt = " ".join(cleaned).strip()
    if not prompt:
        raise ValueError("请提供要循环执行的任务描述")

    return normalize_loop_schedule(interval), prompt, max_runs


def _parse_max_value(raw: str) -> int:
    """Coerce a ``--max`` token to a positive int or raise."""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"--max 必须是正整数,收到 '{raw}'")
    if value < 1:
        raise ValueError("--max 必须是 >= 1 的整数")
    return value

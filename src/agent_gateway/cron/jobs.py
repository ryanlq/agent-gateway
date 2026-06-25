"""
Cron job storage and management.

Ported from hermes-agent ``cron/jobs.py`` with hermes-specific dependencies
replaced by agent-gateway equivalents.  Skills, profiles, workdir, and
curator integration have been stripped for the MVP.

Jobs are stored in ``~/.nexus-agent/cron/jobs.json``
Output is saved to ``~/.nexus-agent/cron/output/{job_id}/{timestamp}.md``
"""

import copy
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

try:
    from croniter import croniter

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

# =============================================================================
# Time helper — replaces hermes_time.now()
# =============================================================================


def _now() -> datetime:
    """Return a timezone-aware datetime in the system's local timezone."""
    return datetime.now().astimezone()


# =============================================================================
# Atomic file replace — replaces hermes utils.atomic_replace
# =============================================================================


def _atomic_replace(tmp_path: Union[str, Path], target: Union[str, Path]) -> str:
    """Atomically move *tmp_path* onto *target*, preserving symlinks."""
    target_str = str(target)
    real_path = os.path.realpath(target_str) if os.path.islink(target_str) else target_str
    os.replace(str(tmp_path), real_path)
    return real_path


# =============================================================================
# Configuration / paths
# =============================================================================

NEXUS_DIR = Path(os.path.expanduser("~/.nexus-agent"))
CRON_DIR = NEXUS_DIR / "cron"
JOBS_FILE = CRON_DIR / "jobs.json"

# In-process lock protecting load_jobs→modify→save_jobs cycles.
_jobs_file_lock = threading.Lock()

OUTPUT_DIR = CRON_DIR / "output"
ONESHOT_GRACE_SECONDS = 120


def _job_output_dir(job_id: str) -> Path:
    """Resolve a job's output directory, rejecting any path-escape attempt."""
    text = str(job_id or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text:
        raise ValueError(f"Invalid cron job id for output path: {job_id!r}")
    if Path(text).is_absolute() or Path(text).drive:
        raise ValueError(f"Invalid cron job id for output path: {job_id!r}")
    return OUTPUT_DIR / text


def list_job_outputs(job_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """List a job's saved outputs (one file per tick), newest-first.

    Each ``*.md`` file under ``output/{job_id}/`` is a single run's output,
    written by :func:`save_job_output`. Returns ``[{"run_at", "content"}]``.
    The ``run_at`` is the filename stem (a ``YYYY-MM-DD_HH-MM-SS`` timestamp).
    """
    out_dir = _job_output_dir(job_id)
    if not out_dir.is_dir():
        return []
    files = sorted(out_dir.glob("*.md"), key=lambda f: f.name, reverse=True)
    outputs: List[Dict[str, Any]] = []
    for path in files[:limit]:
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        outputs.append({"run_at": path.stem, "content": content})
    return outputs


# =============================================================================
# Field helpers
# =============================================================================


def _coerce_job_text(value: Any, fallback: str = "") -> str:
    """Coerce nullable cron fields to strings for readers."""
    if value is None:
        return fallback
    return str(value)


def _schedule_display_for_job(job: Dict[str, Any]) -> str:
    display = _coerce_job_text(job.get("schedule_display")).strip()
    if display:
        return display

    schedule = job.get("schedule")
    if isinstance(schedule, dict):
        for key in ("display", "value", "expr", "run_at"):
            text = _coerce_job_text(schedule.get(key)).strip()
            if text:
                return text
    elif schedule is not None:
        return str(schedule)

    return "?"


def _normalize_job_record(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return a read-safe cron job shape for API consumers."""
    normalized = dict(job)
    job_id = _coerce_job_text(normalized.get("id"), "unknown")
    prompt = _coerce_job_text(normalized.get("prompt"))
    normalized["id"] = job_id
    normalized["prompt"] = prompt

    name = _coerce_job_text(normalized.get("name")).strip()
    if not name:
        script = _coerce_job_text(normalized.get("script")).strip()
        label_source = prompt or script or job_id or "cron job"
        name = label_source[:50].strip() or "cron job"
    normalized["name"] = name
    normalized["schedule_display"] = _schedule_display_for_job(normalized)

    state = _coerce_job_text(normalized.get("state")).strip()
    if not state:
        state = "scheduled" if normalized.get("enabled", True) else "paused"
    # Continuous loops idle between iterations: an enabled loop with no pending
    # next_run_at (waiting for the agent to schedule the next iteration) reads
    # as "idle" rather than the misleading "scheduled".
    schedule = normalized.get("schedule") if isinstance(normalized.get("schedule"), dict) else {}
    if (
        schedule.get("kind") == "continuous"
        and state in {"scheduled", ""}
        and normalized.get("enabled", True)
        and not normalized.get("next_run_at")
    ):
        state = "idle"
    normalized["state"] = state

    # Loop termination fields. max_runs is only meaningful for recurring jobs
    # (one-shots run once regardless of any stored repeat.times).
    repeat = normalized.get("repeat") if isinstance(normalized.get("repeat"), dict) else {}
    normalized["completed"] = repeat.get("completed", 0)
    if schedule.get("kind") in {"cron", "interval", "continuous"}:
        normalized["max_runs"] = repeat.get("times")  # None == unlimited
    else:
        normalized["max_runs"] = None
    normalized["stop_condition"] = _coerce_job_text(
        normalized.get("stop_condition")
    ).strip() or None

    return normalized


# =============================================================================
# Secure file / directory permissions
# =============================================================================


def _secure_dir(path: Path) -> None:
    """Set directory to owner-only access (0700).  No-op on Windows."""
    try:
        os.chmod(path, 0o700)
    except (OSError, NotImplementedError):
        pass


def _secure_file(path: Path) -> None:
    """Set file to owner-only read/write (0600).  No-op on Windows."""
    try:
        if path.exists():
            os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass


def ensure_dirs() -> None:
    """Ensure cron directories exist with secure permissions."""
    CRON_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _secure_dir(CRON_DIR)
    _secure_dir(OUTPUT_DIR)


# =============================================================================
# Schedule Parsing
# =============================================================================


def parse_duration(s: str) -> int:
    """
    Parse duration string into minutes.

    Examples:
        "30m" → 30
        "2h"  → 120
        "1d"  → 1440
    """
    s = s.strip().lower()
    match = re.match(r"^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$", s)
    if not match:
        raise ValueError(f"Invalid duration: '{s}'. Use format like '30m', '2h', or '1d'")

    value = int(match.group(1))
    unit = match.group(2)[0]  # First char: m, h, or d

    multipliers = {"m": 1, "h": 60, "d": 1440}
    return value * multipliers[unit]


def parse_schedule(schedule: str) -> Dict[str, Any]:
    """
    Parse schedule string into structured format.

    Returns dict with:
        - kind: "once" | "interval" | "cron"
        - For "once": "run_at" (ISO timestamp)
        - For "interval": "minutes" (int)
        - For "cron": "expr" (cron expression)

    Examples:
        "30m"              → once in 30 minutes
        "every 30m"        → recurring every 30 minutes
        "0 9 * * *"        → cron expression
        "2026-02-03T14:00" → once at timestamp
    """
    schedule = schedule.strip()
    original = schedule
    schedule_lower = schedule.lower()

    # "continuous" / "agent" / "ondemand" → agent-paced loop. The gateway runs
    # the first iteration immediately, then each iteration decides for itself
    # when (or whether) to run the next one via a schedule_next operation. No
    # wall-clock cadence is imposed — the only ceiling is the user's max_runs.
    if schedule_lower in {"continuous", "agent", "ondemand", "agent-paced"}:
        return {"kind": "continuous", "display": "agent-paced"}

    # "every X" pattern → recurring interval
    if schedule_lower.startswith("every "):
        duration_str = schedule[6:].strip()
        minutes = parse_duration(duration_str)
        return {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m",
        }

    # Check for cron expression (5 or 6 space-separated fields)
    parts = schedule.split()
    if len(parts) >= 5 and all(re.match(r"^[\d\*\-,/]+$", p) for p in parts[:5]):
        if not HAS_CRONITER:
            raise ValueError(
                "Cron expressions require 'croniter' package. "
                "Install with: pip install croniter"
            )
        try:
            croniter(schedule)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{schedule}': {e}")
        return {
            "kind": "cron",
            "expr": schedule,
            "display": schedule,
        }

    # ISO timestamp (contains T or looks like date)
    if "T" in schedule or re.match(r"^\d{4}-\d{2}-\d{2}", schedule):
        try:
            dt = datetime.fromisoformat(schedule.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.astimezone()
            return {
                "kind": "once",
                "run_at": dt.isoformat(),
                "display": f"once at {dt.strftime('%Y-%m-%d %H:%M')}",
            }
        except ValueError as e:
            raise ValueError(f"Invalid timestamp '{schedule}': {e}")

    # Duration like "30m", "2h", "1d" → one-shot from now
    try:
        minutes = parse_duration(schedule)
        run_at = _now() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": run_at.isoformat(),
            "display": f"once in {original}",
        }
    except ValueError:
        pass

    raise ValueError(
        f"Invalid schedule '{original}'. Use:\n"
        f"  - Duration: '30m', '2h', '1d' (one-shot)\n"
        f"  - Interval: 'every 30m', 'every 2h' (recurring)\n"
        f"  - Cron: '0 9 * * *' (cron expression)\n"
        f"  - Timestamp: '2026-02-03T14:00:00' (one-shot at time)"
    )


def _ensure_aware(dt: datetime) -> datetime:
    """Return a timezone-aware datetime."""
    if dt.tzinfo is None:
        return dt.astimezone()
    return dt.astimezone()


def _recoverable_oneshot_run_at(
    schedule: Dict[str, Any],
    now: datetime,
    *,
    last_run_at: Optional[str] = None,
) -> Optional[str]:
    """Return a one-shot run time if it is still eligible to fire."""
    if schedule.get("kind") != "once":
        return None
    if last_run_at:
        return None

    run_at = schedule.get("run_at")
    if not run_at:
        return None

    run_at_dt = _ensure_aware(datetime.fromisoformat(run_at))
    if run_at_dt >= now - timedelta(seconds=ONESHOT_GRACE_SECONDS):
        return run_at
    return None


def _compute_grace_seconds(schedule: dict) -> int:
    """Compute how late a job can be and still catch up.

    Uses half the schedule period, clamped between 120 seconds and 2 hours.
    """
    MIN_GRACE = 120
    MAX_GRACE = 7200  # 2 hours

    kind = schedule.get("kind")

    if kind == "interval":
        period_seconds = schedule.get("minutes", 1) * 60
        grace = period_seconds // 2
        return max(MIN_GRACE, min(grace, MAX_GRACE))

    if kind == "cron" and HAS_CRONITER:
        try:
            now = _now()
            cron = croniter(schedule["expr"], now)
            first = cron.get_next(datetime)
            second = cron.get_next(datetime)
            period_seconds = int((second - first).total_seconds())
            grace = period_seconds // 2
            return max(MIN_GRACE, min(grace, MAX_GRACE))
        except Exception:
            pass

    return MIN_GRACE


def compute_next_run(
    schedule: Dict[str, Any], last_run_at: Optional[str] = None
) -> Optional[str]:
    """Compute the next run time for a schedule.  Returns ISO timestamp or None."""
    now = _now()

    if schedule["kind"] == "once":
        return _recoverable_oneshot_run_at(schedule, now, last_run_at=last_run_at)

    elif schedule["kind"] == "interval":
        minutes = schedule["minutes"]
        if last_run_at:
            last = _ensure_aware(datetime.fromisoformat(last_run_at))
            next_run = last + timedelta(minutes=minutes)
        else:
            next_run = now + timedelta(minutes=minutes)
        return next_run.isoformat()

    elif schedule["kind"] == "continuous":
        # Agent-paced: run immediately when first scheduled / resumed /
        # manually triggered (no last_run_at), then go idle after each run.
        # The agent itself re-arms the next run via a schedule_next operation.
        if last_run_at is None:
            return now.isoformat()
        return None

    elif schedule["kind"] == "cron":
        if not HAS_CRONITER:
            logger.warning(
                "Cannot compute next run for cron schedule %r: 'croniter' is "
                "not installed.",
                schedule.get("expr"),
            )
            return None
        base_time = now
        if last_run_at:
            base_time = _ensure_aware(datetime.fromisoformat(last_run_at))
        cron = croniter(schedule["expr"], base_time)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()

    return None


# =============================================================================
# Job CRUD Operations
# =============================================================================


def load_jobs() -> List[Dict[str, Any]]:
    """Load all jobs from storage."""
    ensure_dirs()
    if not JOBS_FILE.exists():
        return []

    _strict_retry = False

    try:
        with open(JOBS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        _strict_retry = True
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                data = json.loads(f.read(), strict=False)
        except Exception as e:
            logger.error("Failed to auto-repair jobs.json: %s", e)
            raise RuntimeError(f"Cron database corrupted and unrepairable: {e}") from e
    except IOError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e

    if isinstance(data, dict):
        jobs = data.get("jobs", [])
        if _strict_retry and jobs:
            save_jobs(jobs)
            logger.warning("Auto-repaired jobs.json (had invalid control characters)")
        return jobs
    if isinstance(data, list):
        if data:
            save_jobs(data)
            logger.warning("Auto-repaired jobs.json (bare list wrapped as dict)")
        return data

    raise RuntimeError(
        f"Cron database corrupted: expected {{'jobs': [...]}}, got {type(data).__name__}"
    )


def save_jobs(jobs: List[Dict[str, Any]]) -> None:
    """Save all jobs to storage (atomic write with fsync)."""
    ensure_dirs()
    fd, tmp_path = tempfile.mkstemp(
        dir=str(JOBS_FILE.parent), suffix=".tmp", prefix=".jobs_"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"jobs": jobs, "updated_at": _now().isoformat()}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        _atomic_replace(tmp_path, JOBS_FILE)
        _secure_file(JOBS_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def create_job(
    prompt: Optional[str],
    schedule: str,
    name: Optional[str] = None,
    deliver: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    script: Optional[str] = None,
    context_from: Optional[Union[str, List[str]]] = None,
    no_agent: bool = False,
    max_runs: Optional[int] = None,
    stop_condition: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new cron job.

    Args:
        prompt: The prompt to run.
        schedule: Schedule string (see parse_schedule).
        name: Optional friendly name.
        deliver: Where to deliver output ("local", etc.).
        origin: Source info where job was created.
        script: Optional pre-run script path.
        context_from: Optional job ID(s) whose output is injected as context.
        no_agent: When True, skip the agent — run script only.
        max_runs: Optional iteration cap for RECURRING jobs (loops). When set,
            the loop auto-terminates after this many successful runs. Ignored
            for one-shots (they run once regardless). None = run forever.
        stop_condition: Optional natural-language condition the loop agent
            evaluates each iteration; when satisfied it self-terminates by
            emitting a CRON_OPERATION pause_job. Only meaningful for loops.

    Returns:
        The created job dict.
    """
    parsed_schedule = parse_schedule(schedule)

    # Default delivery
    if deliver is None:
        deliver = "origin" if origin else "local"

    # Loop iteration cap. One-shots always run once; recurring jobs run forever
    # unless an explicit max_runs termination condition is set.
    if max_runs is not None:
        if not isinstance(max_runs, int) or isinstance(max_runs, bool) or max_runs < 1:
            raise ValueError("max_runs 必须是 >= 1 的整数")
    if parsed_schedule["kind"] == "once":
        repeat_times: Optional[int] = 1
    elif max_runs is not None:
        repeat_times = max_runs
    else:
        repeat_times = None

    normalized_stop_condition = (
        str(stop_condition).strip() if isinstance(stop_condition, str) else None
    )
    normalized_stop_condition = normalized_stop_condition or None

    job_id = uuid.uuid4().hex[:12]
    now = _now().isoformat()

    normalized_script = str(script).strip() if isinstance(script, str) else None
    normalized_script = normalized_script or None
    normalized_no_agent = bool(no_agent)

    if normalized_no_agent and not normalized_script:
        raise ValueError(
            "no_agent=True requires a script — with no agent and no script "
            "there is nothing for the job to run."
        )

    # Normalize context_from
    if isinstance(context_from, str):
        context_from = [context_from.strip()] if context_from.strip() else None
    elif isinstance(context_from, list):
        context_from = [str(j).strip() for j in context_from if str(j).strip()] or None
    else:
        context_from = None

    prompt_text = _coerce_job_text(prompt)
    label_source = prompt_text or normalized_script or "cron job"
    job = {
        "id": job_id,
        "name": name or label_source[:50].strip(),
        "prompt": prompt_text,
        "script": normalized_script,
        "no_agent": normalized_no_agent,
        "context_from": context_from,
        "schedule": parsed_schedule,
        "schedule_display": parsed_schedule.get("display", schedule),
        "repeat": {
            "times": repeat_times,
            "completed": 0,
        },
        "stop_condition": normalized_stop_condition,
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "created_at": now,
        "next_run_at": compute_next_run(parsed_schedule),
        "last_run_at": None,
        "last_status": None,
        "last_error": None,
        "last_delivery_error": None,
        "deliver": deliver,
        "origin": origin,
    }

    jobs = load_jobs()
    jobs.append(job)
    save_jobs(jobs)

    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job by ID."""
    jobs = load_jobs()
    for job in jobs:
        if job["id"] == job_id:
            return _normalize_job_record(job)
    return None


def list_jobs(include_disabled: bool = False) -> List[Dict[str, Any]]:
    """List all jobs, optionally including disabled ones."""
    jobs = [_normalize_job_record(j) for j in load_jobs()]
    if not include_disabled:
        jobs = [j for j in jobs if j.get("enabled", True)]
    return jobs


def update_job(job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update a job by ID, refreshing derived schedule fields when needed."""
    jobs = load_jobs()
    for i, job in enumerate(jobs):
        if job["id"] != job_id:
            continue

        updated = {**job, **updates}
        schedule_changed = "schedule" in updates

        if schedule_changed:
            updated_schedule = updated["schedule"]
            if isinstance(updated_schedule, str):
                updated_schedule = parse_schedule(updated_schedule)
                updated["schedule"] = updated_schedule
            updated["schedule_display"] = updates.get(
                "schedule_display",
                updated_schedule.get("display", updated.get("schedule_display")),
            )
            if updated.get("state") != "paused":
                updated["next_run_at"] = compute_next_run(updated_schedule)

        if updated.get("enabled", True) and updated.get("state") != "paused" and not updated.get("next_run_at"):
            updated["next_run_at"] = compute_next_run(updated["schedule"])

        jobs[i] = updated
        save_jobs(jobs)
        return _normalize_job_record(jobs[i])
    return None


def pause_job(job_id: str, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Pause a job."""
    return update_job(
        job_id,
        {
            "enabled": False,
            "state": "paused",
            "paused_at": _now().isoformat(),
            "paused_reason": reason,
        },
    )


def resume_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Resume a paused job and compute the next future run from now."""
    job = get_job(job_id)
    if not job:
        return None

    next_run_at = compute_next_run(job["schedule"])
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": next_run_at,
        },
    )


def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Schedule a job to run on the next scheduler tick."""
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": _now().isoformat(),
        },
    )


def clear_next_run(job_id: str) -> None:
    """Clear a continuous loop's pending run BEFORE it executes.

    The due job's ``next_run_at`` is the run about to happen. For a continuous
    loop we must forget it now so that, if the running iteration emits NO
    ``schedule_next`` block, the loop lands idle rather than re-firing the same
    stale timestamp forever. If the agent DOES emit ``schedule_next`` (via
    ``set_next_run`` during execution) the loop is correctly re-armed. A no-op
    for non-continuous jobs.
    """
    with _jobs_file_lock:
        jobs = load_jobs()
        for job in jobs:
            if job["id"] != job_id:
                continue
            kind = job.get("schedule", {}).get("kind")
            if kind != "continuous":
                return
            if job.get("next_run_at"):
                job["next_run_at"] = None
                save_jobs(jobs)
            return


def set_next_run(job_id: str, when: Optional[str]) -> Optional[Dict[str, Any]]:
    """Arm (or clear) a job's next run time.

    Used by the agent ``schedule_next`` protocol to re-arm a continuous loop for
    its next iteration. Pass an ISO timestamp to schedule, or ``None`` to idle
    the job (agent declined to schedule another run). Unlike ``trigger_job``
    this does not force the state to "scheduled" — it leaves paused/completed
    jobs alone so the agent can't yank a paused loop back to life on its own.
    """
    jobs = load_jobs()
    with _jobs_file_lock:
        for i, job in enumerate(jobs):
            if job["id"] != job_id:
                continue
            if not job.get("enabled", True):
                # Don't resurrect paused/completed/error loops from within an
                # agent run — that would bypass the user's manual pause.
                logger.info(
                    "set_next_run: job '%s' not enabled (state=%s); ignoring.",
                    job_id, job.get("state"),
                )
                return _normalize_job_record(job)
            job["next_run_at"] = when
            # Only flip idle→scheduled; never downgrade an active terminal state.
            if when and job.get("state") in {"idle", "", None}:
                job["state"] = "scheduled"
            elif not when and job.get("state") == "scheduled":
                job["state"] = "idle"
            save_jobs(jobs)
            return _normalize_job_record(job)
    return None


def remove_job(job_id: str) -> bool:
    """Remove a job by ID."""
    jobs = load_jobs()
    original_len = len(jobs)
    jobs = [j for j in jobs if j["id"] != job_id]
    if len(jobs) < original_len:
        job_output_dir = _job_output_dir(job_id)
        save_jobs(jobs)
        if job_output_dir.exists():
            shutil.rmtree(job_output_dir)
        return True
    return False


def mark_job_run(
    job_id: str,
    success: bool,
    error: Optional[str] = None,
    delivery_error: Optional[str] = None,
) -> None:
    """Mark a job as having been run.

    Updates last_run_at, last_status, increments completed count,
    computes next_run_at, and auto-deletes if repeat limit reached.
    """
    with _jobs_file_lock:
        jobs = load_jobs()
        for i, job in enumerate(jobs):
            if job["id"] == job_id:
                now = _now().isoformat()
                job["last_run_at"] = now
                job["last_status"] = "ok" if success else "error"
                job["last_error"] = error if not success else None
                job["last_delivery_error"] = delivery_error

                # Increment completed count
                if job.get("repeat"):
                    job["repeat"]["completed"] = job["repeat"].get("completed", 0) + 1

                    times = job["repeat"].get("times")
                    completed = job["repeat"]["completed"]
                    if times is not None and times > 0 and completed >= times:
                        kind = job.get("schedule", {}).get("kind")
                        if kind in {"cron", "interval", "continuous"}:
                            # Loop reached its iteration cap — mark it complete
                            # and keep it visible so the user can still review
                            # its outputs in the Loops panel. (One-shots are
                            # removed below, preserving existing behavior.)
                            job["enabled"] = False
                            job["state"] = "completed"
                            job["next_run_at"] = None
                            save_jobs(jobs)
                            return
                        # One-shot finished — remove it.
                        jobs.pop(i)
                        save_jobs(jobs)
                        return

                # Compute next run.
                kind = job.get("schedule", {}).get("kind")
                if kind == "continuous":
                    # Agent-paced: the agent may have pre-armed the next run via
                    # a schedule_next operation during execution — preserve it.
                    # If it didn't, go idle (next_run_at=None, state="idle").
                    if not job.get("next_run_at"):
                        job["next_run_at"] = None
                    if job.get("state") != "paused":
                        job["state"] = "scheduled" if job.get("next_run_at") else "idle"
                else:
                    job["next_run_at"] = compute_next_run(job["schedule"], now)

                    if job["next_run_at"] is None:
                        if kind in {"cron", "interval"}:
                            job["state"] = "error"
                            if not job.get("last_error"):
                                job["last_error"] = (
                                    "Failed to compute next run for recurring "
                                    "schedule (is the 'croniter' package "
                                    "installed?)"
                                )
                            logger.error(
                                "Job '%s' (%s) could not compute next_run_at",
                                job.get("name", job["id"]),
                                kind,
                            )
                        else:
                            job["enabled"] = False
                            job["state"] = "completed"
                    elif job.get("state") != "paused":
                        job["state"] = "scheduled"

                save_jobs(jobs)
                return

        logger.warning("mark_job_run: job_id %s not found, skipping save", job_id)


def advance_next_run(job_id: str) -> bool:
    """Preemptively advance next_run_at for a recurring job before execution.

    Converts the scheduler from at-least-once to at-most-once for recurring
    jobs.  One-shot jobs are left unchanged so they can still retry on restart.

    Returns True if next_run_at was advanced, False otherwise.
    """
    with _jobs_file_lock:
        jobs = load_jobs()
        for job in jobs:
            if job["id"] == job_id:
                kind = job.get("schedule", {}).get("kind")
                if kind not in {"cron", "interval"}:
                    return False
                now = _now().isoformat()
                new_next = compute_next_run(job["schedule"], now)
                if new_next and new_next != job.get("next_run_at"):
                    job["next_run_at"] = new_next
                    save_jobs(jobs)
                    return True
                return False
        return False


def get_due_jobs() -> List[Dict[str, Any]]:
    """Get all jobs that are due to run now.

    For recurring jobs, stale scheduled times are fast-forwarded instead of
    firing a burst of missed runs on gateway restart.
    """
    with _jobs_file_lock:
        return _get_due_jobs_locked()


def _get_due_jobs_locked() -> List[Dict[str, Any]]:
    """Inner implementation of get_due_jobs(); must hold _jobs_file_lock."""
    now = _now()
    raw_jobs = load_jobs()
    jobs = [dict(j) for j in copy.deepcopy(raw_jobs)]
    due = []
    needs_save = False

    for job in jobs:
        if not job.get("enabled", True):
            continue

        next_run = job.get("next_run_at")
        if not next_run:
            schedule = job.get("schedule", {})
            kind = schedule.get("kind")

            recovered_next = _recoverable_oneshot_run_at(
                schedule,
                now,
                last_run_at=job.get("last_run_at"),
            )

            if not recovered_next and kind in {"cron", "interval"}:
                recovered_next = compute_next_run(schedule, now.isoformat())

            if not recovered_next:
                continue

            job["next_run_at"] = recovered_next
            next_run = recovered_next
            logger.info(
                "Job '%s' had no next_run_at; recovering run at %s",
                job.get("name", job["id"]),
                recovered_next,
            )
            for rj in raw_jobs:
                if rj["id"] == job["id"]:
                    rj["next_run_at"] = recovered_next
                    needs_save = True
                    break

        next_run_dt = _ensure_aware(datetime.fromisoformat(next_run))
        if next_run_dt <= now:
            schedule = job.get("schedule", {})
            kind = schedule.get("kind")

            grace = _compute_grace_seconds(schedule)
            if kind in {"cron", "interval"} and (now - next_run_dt).total_seconds() > grace:
                new_next = compute_next_run(schedule, now.isoformat())
                if new_next:
                    logger.info(
                        "Job '%s' missed its scheduled time (%s, grace=%ds). "
                        "Fast-forwarding to next run: %s",
                        job.get("name", job["id"]),
                        next_run,
                        grace,
                        new_next,
                    )
                    for rj in raw_jobs:
                        if rj["id"] == job["id"]:
                            rj["next_run_at"] = new_next
                            needs_save = True
                            break
                    continue  # Skip this stale run

            due.append(job)

    if needs_save:
        save_jobs(raw_jobs)

    return due


def save_job_output(job_id: str, output: str) -> Path:
    """Save job output to file."""
    ensure_dirs()
    job_output_dir = _job_output_dir(job_id)
    job_output_dir.mkdir(parents=True, exist_ok=True)
    _secure_dir(job_output_dir)

    timestamp = _now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = job_output_dir / f"{timestamp}.md"

    fd, tmp_path = tempfile.mkstemp(
        dir=str(job_output_dir), suffix=".tmp", prefix=".output_"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(output)
            f.flush()
            os.fsync(f.fileno())
        _atomic_replace(tmp_path, output_file)
        _secure_file(output_file)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return output_file

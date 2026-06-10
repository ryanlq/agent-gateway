"""
Cron job scheduler — executes due jobs.

Provides :func:`tick` which checks for due jobs and runs them.  The
``CronManager`` calls this every 60 seconds from a background asyncio task.

Uses a file-based lock (``~/.nexus-agent/cron/.tick.lock``) so only one tick
runs at a time if multiple processes overlap.

Ported from hermes-agent ``cron/scheduler.py`` with the following changes:
- Agent execution uses ``create_bridge()`` + ``bridge.chat()`` instead of
  ``AIAgent.run_conversation()``.
- Parallel execution uses ``asyncio.gather()`` instead of ``ThreadPoolExecutor``.
- Delivery is local-only for MVP.
- Skills, profiles, toolsets, prompt injection scanning are stripped.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, List, Optional

# fcntl is Unix-only; on Windows use msvcrt for file locking
try:
    import fcntl
except ImportError:
    fcntl = None
    try:
        import msvcrt
    except ImportError:
        msvcrt = None

logger = logging.getLogger(__name__)

# Sentinel: when a cron agent has nothing new to report, it can respond
# with this marker to suppress delivery.
SILENT_MARKER = "[SILENT]"

# Default inactivity timeout for a single cron job run (seconds).
_DEFAULT_CRON_TIMEOUT = 600

from agent_gateway.cron.jobs import (
    JOBS_FILE,
    OUTPUT_DIR,
    advance_next_run,
    get_due_jobs,
    mark_job_run,
    save_job_output,
    _job_output_dir,
    _now,
)

# =============================================================================
# File locking
# =============================================================================


def _get_lock_paths() -> tuple[Path, Path]:
    """Resolve cron lock paths."""
    lock_dir = JOBS_FILE.parent
    return lock_dir, lock_dir / ".tick.lock"


# =============================================================================
# Script execution
# =============================================================================

_DEFAULT_SCRIPT_TIMEOUT = 120  # seconds


def _run_job_script(script_path: str) -> tuple[bool, str]:
    """Execute a cron job's pre-run script and capture its output.

    Scripts must reside within ``~/.nexus-agent/scripts/``.
    """
    scripts_dir = JOBS_FILE.parent.parent / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir_resolved = scripts_dir.resolve()

    raw = Path(script_path).expanduser()
    if raw.is_absolute():
        path = raw.resolve()
    else:
        path = (scripts_dir / raw).resolve()

    # Guard against path traversal
    try:
        path.relative_to(scripts_dir_resolved)
    except ValueError:
        return False, (
            f"Blocked: script path resolves outside the scripts directory "
            f"({scripts_dir_resolved}): {script_path!r}"
        )

    if not path.exists():
        return False, f"Script not found: {path}"
    if not path.is_file():
        return False, f"Script path is not a file: {path}"

    # Pick interpreter by extension
    suffix = path.suffix.lower()
    if suffix in {".sh", ".bash"}:
        _bash = shutil.which("bash") or (
            "/bin/bash" if os.path.isfile("/bin/bash") else None
        )
        if _bash is None:
            return False, f"Cannot run .sh/.bash script: bash not found on PATH."
        argv = [_bash, str(path)]
    else:
        argv = [sys.executable, str(path)]

    run_env = os.environ.copy()
    run_env["NEXUS_AGENT_HOME"] = str(JOBS_FILE.parent.parent)

    try:
        popen_kwargs = {}
        if sys.platform == "win32":
            # Hide console window on Windows
            popen_kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=_DEFAULT_SCRIPT_TIMEOUT,
            cwd=str(path.parent),
            env=run_env,
            **popen_kwargs,
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if result.returncode != 0:
            parts = [f"Script exited with code {result.returncode}"]
            if stderr:
                parts.append(f"stderr:\n{stderr}")
            if stdout:
                parts.append(f"stdout:\n{stdout}")
            return False, "\n".join(parts)

        return True, stdout

    except subprocess.TimeoutExpired:
        return False, f"Script timed out after {_DEFAULT_SCRIPT_TIMEOUT}s: {path}"
    except Exception as exc:
        return False, f"Script execution failed: {exc}"


def _parse_wake_gate(script_output: str) -> bool:
    """Parse the last stdout line as a wake gate.

    If the last line is JSON like ``{"wakeAgent": false}``, skip the agent.
    Any other output means wake the agent normally.
    """
    if not script_output:
        return True
    stripped_lines = [line for line in script_output.splitlines() if line.strip()]
    if not stripped_lines:
        return True
    last_line = stripped_lines[-1].strip()
    try:
        gate = json.loads(last_line)
    except (json.JSONDecodeError, ValueError):
        return True
    if not isinstance(gate, dict):
        return True
    return gate.get("wakeAgent", True) is not False


# =============================================================================
# Prompt building
# =============================================================================


def _build_job_prompt(job: dict) -> Optional[str]:
    """Build the effective prompt for a cron job.

    Returns None when the script produced empty output (nothing to report).
    """
    prompt = str(job.get("prompt") or "")

    # Run pre-check script if configured
    script_path = job.get("script")
    if script_path:
        success, script_output = _run_job_script(script_path)
        if success:
            if script_output:
                prompt = (
                    "## Script Output\n"
                    "The following data was collected by a pre-run script. "
                    "Use it as context for your analysis.\n\n"
                    f"```\n{script_output}\n```\n\n"
                    f"{prompt}"
                )
            else:
                # Script produced no output — skip AI call
                return None
        else:
            prompt = (
                "## Script Error\n"
                "The data-collection script failed. Report this to the user.\n\n"
                f"```\n{script_output}\n```\n\n"
                f"{prompt}"
            )

    # Inject output from referenced cron jobs
    context_from = job.get("context_from")
    if context_from:
        if isinstance(context_from, str):
            context_from = [context_from]
        for source_job_id in context_from:
            if not source_job_id or not all(
                c in "0123456789abcdef" for c in source_job_id
            ):
                logger.warning(
                    "context_from: skipping invalid job_id %r for job %r",
                    source_job_id,
                    job.get("id"),
                )
                continue
            try:
                job_output_dir = OUTPUT_DIR / source_job_id
                if not job_output_dir.exists():
                    continue
                output_files = sorted(
                    job_output_dir.glob("*.md"),
                    key=lambda f: f.stat().st_mtime,
                    reverse=True,
                )
                if not output_files:
                    continue
                latest_output = output_files[0].read_text(encoding="utf-8").strip()
                _MAX_CONTEXT_CHARS = 8000
                if len(latest_output) > _MAX_CONTEXT_CHARS:
                    latest_output = latest_output[:_MAX_CONTEXT_CHARS] + "\n\n[... output truncated ...]"
                if latest_output:
                    prompt = (
                        f"## Output from job '{source_job_id}'\n"
                        "The following is the most recent output from a preceding "
                        "cron job. Use it as context for your analysis.\n\n"
                        f"```\n{latest_output}\n```\n\n"
                        f"{prompt}"
                    )
            except (OSError, PermissionError) as e:
                logger.warning("context_from: failed to read output for job %r: %s", source_job_id, e)

    # Prepend cron execution guidance
    cron_hint = (
        "[IMPORTANT: You are running as a scheduled cron job. "
        "DELIVERY: Your final response will be automatically delivered "
        "to the user. Just produce your report/output as your "
        "final response and the system handles the rest. "
        "SILENT: If there is genuinely nothing new to report, respond "
        'with exactly "[SILENT]" (nothing else) to suppress delivery.]\n\n'
    )
    prompt = cron_hint + prompt
    return prompt


# =============================================================================
# Job execution
# =============================================================================


async def _execute_agent(store: Any, job: dict, prompt: str) -> str:
    """Run the configured AI agent with the given prompt.

    Uses ``create_bridge()`` from the agent factory to spawn the agent,
    calls ``bridge.chat()``, then shuts down cleanly.
    """
    from agent_gateway.server.agent_factory import create_bridge

    agent_type = store.get_config("default_agent", "claude-code")
    all_params: dict = store.get_config("agent_params") or {}
    agent_params = all_params.get(agent_type, {}) if isinstance(all_params, dict) else {}

    job_id = job.get("id", "unknown")
    bridge = create_bridge(agent_type, **agent_params)
    try:
        result = await bridge.chat(
            session_key=f"cron_{job_id}",
            message=prompt,
            history=[],
        )
        return result or ""
    finally:
        try:
            await asyncio.wait_for(bridge.shutdown(), timeout=5.0)
        except Exception:
            pass


async def run_job(store: Any, job: dict) -> tuple[bool, str, str, Optional[str]]:
    """Execute a single cron job.

    Returns:
        Tuple of (success, full_output_doc, final_response, error_message)
    """
    job_id = job["id"]
    job_name = str(job.get("name") or job.get("prompt") or job_id or "cron job")

    # ---------------------------------------------------------------
    # no_agent short-circuit — script IS the job, no LLM involvement.
    # ---------------------------------------------------------------
    if job.get("no_agent"):
        script_path = job.get("script")
        if not script_path:
            err = "no_agent=True but no script is set"
            logger.error("Job '%s': %s", job_id, err)
            return False, "", "", err

        ok, output = _run_job_script(script_path)
        now_iso = _now().strftime("%Y-%m-%d %H:%M:%S")

        if not ok:
            alert = f"⚠ Cron watchdog '{job_name}' script failed\n\n{output}\n\nTime: {now_iso}"
            doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {now_iso}\n"
                f"**Mode:** no_agent (script)\n"
                f"**Status:** script failed\n\n"
                f"{output}\n"
            )
            return False, doc, alert, output

        if not _parse_wake_gate(output):
            logger.info("Job '%s' (no_agent): wakeAgent=false — silent run", job_id)
            silent_doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {now_iso}\n"
                f"**Mode:** no_agent (script)\n"
                f"**Status:** silent (wakeAgent=false)\n"
            )
            return True, silent_doc, SILENT_MARKER, None

        if not output.strip():
            logger.info("Job '%s' (no_agent): empty stdout — silent run", job_id)
            silent_doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {now_iso}\n"
                f"**Mode:** no_agent (script)\n"
                f"**Status:** silent (empty output)\n"
            )
            return True, silent_doc, SILENT_MARKER, None

        doc = (
            f"# Cron Job: {job_name}\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {now_iso}\n"
            f"**Mode:** no_agent (script)\n\n"
            f"---\n\n{output}\n"
        )
        return True, doc, output, None

    # ---------------------------------------------------------------
    # LLM path — build prompt, execute agent
    # ---------------------------------------------------------------
    # Wake-gate: run pre-check script before building prompt
    script_path = job.get("script")
    if script_path:
        ran_ok, script_output = _run_job_script(script_path)
        if ran_ok and not _parse_wake_gate(script_output):
            logger.info("Job '%s': wakeAgent=false, skipping agent run", job_name)
            silent_doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {_now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                "Script gate returned `wakeAgent=false` — agent skipped.\n"
            )
            return True, silent_doc, SILENT_MARKER, None

    prompt = _build_job_prompt(job)
    if prompt is None:
        logger.info("Job '%s': script produced no output, skipping AI call.", job_name)
        return True, "", SILENT_MARKER, None

    logger.info("Running job '%s' (ID: %s)", job_name, job_id)

    try:
        cron_timeout = _DEFAULT_CRON_TIMEOUT
        env_timeout = os.getenv("NEXUS_AGENT_CRON_TIMEOUT", "").strip()
        if env_timeout:
            try:
                cron_timeout = float(env_timeout)
            except (ValueError, TypeError):
                pass

        final_response = await asyncio.wait_for(
            _execute_agent(store, job, prompt),
            timeout=cron_timeout if cron_timeout > 0 else None,
        )

        if final_response.strip() == "(No response generated)":
            final_response = ""

        logged_response = final_response if final_response else "(No response generated)"

        output = (
            f"# Cron Job: {job_name}\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {_now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"**Schedule:** {job.get('schedule_display', 'N/A')}\n\n"
            f"## Prompt\n\n{prompt}\n\n"
            f"## Response\n\n{logged_response}\n"
        )

        logger.info("Job '%s' completed successfully", job_name)
        return True, output, final_response, None

    except asyncio.TimeoutError:
        error_msg = f"Cron job '{job_name}' timed out after {int(cron_timeout)}s"
        logger.error(error_msg)
        output = (
            f"# Cron Job: {job_name} (TIMEOUT)\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {_now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"## Error\n\n```\n{error_msg}\n```\n"
        )
        return False, output, "", error_msg

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        logger.exception("Job '%s' failed: %s", job_name, error_msg)
        output = (
            f"# Cron Job: {job_name} (FAILED)\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {_now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"**Schedule:** {job.get('schedule_display', 'N/A')}\n\n"
            f"## Prompt\n\n{prompt}\n\n"
            f"## Error\n\n```\n{error_msg}\n```\n"
        )
        return False, output, "", error_msg


# =============================================================================
# Delivery (MVP: local only)
# =============================================================================


def _deliver_result(job: dict, content: str) -> Optional[str]:
    """Deliver job output.  MVP: local only — output is saved by save_job_output().

    Returns None on success, or an error string on failure.
    """
    # Future: route to adapter via runner.adapters for telegram/email/etc.
    return None


# =============================================================================
# Tick — the main scheduler loop entry point
# =============================================================================


async def tick(store: Any, verbose: bool = True) -> int:
    """Check and run all due jobs.

    Uses a file lock so only one tick runs at a time.

    Args:
        store: SessionStore for resolving agent config.
        verbose: Whether to log status messages.

    Returns:
        Number of jobs executed (0 if another tick is already running).
    """
    lock_dir, lock_file = _get_lock_paths()
    lock_dir.mkdir(parents=True, exist_ok=True)

    # Cross-platform file locking
    lock_fd = None
    try:
        lock_fd = open(lock_file, "w", encoding="utf-8")
        if fcntl:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt:
            msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (OSError, IOError):
        logger.debug("Tick skipped — another instance holds the lock")
        if lock_fd is not None:
            lock_fd.close()
        return 0

    try:
        due_jobs = get_due_jobs()

        if verbose and not due_jobs:
            logger.info("%s - No jobs due", _now().strftime("%H:%M:%S"))
            return 0

        if verbose:
            logger.info("%s - %s job(s) due", _now().strftime("%H:%M:%S"), len(due_jobs))

        # Advance next_run_at for all recurring jobs FIRST (at-most-once)
        for job in due_jobs:
            advance_next_run(job["id"])

        # Process each due job
        async def _process_job(job: dict) -> bool:
            """Run one due job end-to-end: execute, save, deliver, mark."""
            try:
                success, output, final_response, error = await run_job(store, job)

                save_job_output(job["id"], output)

                # Deliver the final response
                deliver_content = (
                    final_response
                    if success
                    else f"⚠️ Cron job '{job.get('name', job['id'])}' failed:\n{error}"
                )
                should_deliver = bool(deliver_content.strip())
                if (
                    should_deliver
                    and success
                    and SILENT_MARKER in deliver_content.strip().upper()
                ):
                    should_deliver = False

                delivery_error = None
                if should_deliver:
                    try:
                        delivery_error = _deliver_result(job, deliver_content)
                    except Exception as de:
                        delivery_error = str(de)

                # Treat empty final_response as soft failure
                if success and not final_response.strip():
                    success = False
                    error = "Agent completed but produced empty response"

                mark_job_run(job["id"], success, error, delivery_error=delivery_error)
                return True

            except Exception as e:
                logger.error("Error processing job %s: %s", job["id"], e)
                mark_job_run(job["id"], False, str(e))
                return False

        # Run all due jobs concurrently
        results = await asyncio.gather(
            *[_process_job(job) for job in due_jobs],
            return_exceptions=True,
        )

        executed = sum(1 for r in results if r is True)
        if verbose:
            logger.info("Tick complete: %d/%d jobs executed", executed, len(due_jobs))

        return executed

    finally:
        if fcntl:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except (OSError, IOError):
                pass
        elif msvcrt:
            try:
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
            except (OSError, IOError):
                pass
        lock_fd.close()

"""End-to-end tests for loop self-termination via the SCHEDULER execution path.

``scheduler._build_loop_system_extra`` only *instructs* the agent to emit a
``<!--CRON_OPERATION pause_job -->`` block; something must then *parse and
execute* that block. Historically only the interactive chat paths
(core/runner.py, server/methods.py) did the parsing — the scheduler saved the
block verbatim and never paused the job, so a loop could only end via the
max_runs cap. These tests lock the scheduler-side execution that closes that
gap, plus a regression guard that without a ``cron_manager`` the block is left
untouched (the documented no-op default).
"""

import pytest

from agent_gateway.cron import jobs
from agent_gateway.cron import manager as manager_mod
from agent_gateway.cron import scheduler
from agent_gateway.cron.jobs import create_job, load_jobs


@pytest.fixture
def isolated_cron(tmp_path, monkeypatch):
    # jobs.py holds its paths as hard-coded module globals; redirect them so
    # tests never touch the real ~/.nexus-agent/cron store.
    cron_dir = tmp_path / "cron"
    cron_dir.mkdir(parents=True)
    out_dir = cron_dir / "output"
    out_dir.mkdir(parents=True)
    monkeypatch.setattr(jobs, "NEXUS_DIR", tmp_path)
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "OUTPUT_DIR", out_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    return jobs


def _pause_block(job_id: str) -> str:
    """The exact block an instructed loop agent emits to self-terminate."""
    return (
        "目录不为空,已满足停止条件,终止循环。\n\n"
        "<!--CRON_OPERATION\n"
        "```json\n"
        "{\n"
        f'  "action": "pause_job",\n'
        f'  "params": {{"job_id": "{job_id}"}}\n'
        "}\n"
        "```\n"
        "-->"
    )


def _stored_job(job_id: str) -> dict:
    return next(j for j in load_jobs() if j["id"] == job_id)


async def test_run_job_executes_self_pause_block(isolated_cron, monkeypatch):
    job = create_job(prompt="check dir", schedule="every 2m", name="test loop")
    job_id = job["id"]

    async def fake_execute_agent(store, j, prompt, system_extra=""):
        return _pause_block(job_id)

    monkeypatch.setattr(scheduler, "_execute_agent", fake_execute_agent)

    # A real CronManager (never started) — its pause_job delegates to the
    # patched jobs.pause_job, exercising the same code path as production.
    manager = manager_mod.CronManager(store=None, runner=None)

    success, output, final_response, error = await scheduler.run_job(
        store=None, job=job, cron_manager=manager
    )

    assert success is True
    assert error is None
    # The block was executed and replaced, not left verbatim in the output.
    assert "CRON_OPERATION" not in output
    assert "✅" in output
    assert "pause_job" not in final_response
    # The job is now paused — the loop will not fire again.
    stored = _stored_job(job_id)
    assert stored["enabled"] is False
    assert stored["state"] == "paused"


async def test_run_job_without_cron_manager_leaves_block_verbatim(isolated_cron, monkeypatch):
    """Regression guard: without a cron_manager the block is NOT executed.
    Preserves the documented no-op default for callers that don't opt in, and
    documents the exact pre-fix behavior the scheduler used to have."""
    job = create_job(prompt="check dir", schedule="every 2m")
    job_id = job["id"]

    async def fake_execute_agent(store, j, prompt, system_extra=""):
        return _pause_block(job_id)

    monkeypatch.setattr(scheduler, "_execute_agent", fake_execute_agent)

    success, output, *_ = await scheduler.run_job(store=None, job=job, cron_manager=None)

    assert success is True
    assert "CRON_OPERATION" in output  # block left untouched
    stored = _stored_job(job_id)
    assert stored["enabled"] is True  # not paused


async def test_run_job_injects_loop_system_extra(isolated_cron, monkeypatch):
    """The agent must actually receive the self-termination instructions — the
    prompt-side half of the protocol that tells it its job_id + stop condition
    and how to emit the pause block."""
    job = create_job(prompt="check dir", schedule="every 2m", stop_condition="files != 0")

    async def fake_execute_agent(store, j, prompt, system_extra=""):
        assert system_extra, "loop system_extra must be injected for a recurring job"
        assert "pause_job" in system_extra
        assert "CRON_OPERATION" in system_extra
        assert "files != 0" in system_extra
        return "nothing to report"

    monkeypatch.setattr(scheduler, "_execute_agent", fake_execute_agent)

    await scheduler.run_job(store=None, job=job, cron_manager=None)

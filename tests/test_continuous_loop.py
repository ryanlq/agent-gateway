"""Tests for agent-paced (continuous) loops.

A continuous loop has no fixed cadence: the gateway runs the first iteration
immediately, then each iteration decides — via a ``schedule_next`` CRON_OPERATION
block — when (or whether) to run the next one. The only hard ceiling is the
user's ``max_runs``. These tests lock the schedule parsing, the idle/armed
lifecycle, the ``schedule_next`` executor, and the fact that recurring loops
no longer impose a wall-clock per-iteration timeout.
"""

import pytest

from agent_gateway.core import cron_tool
from agent_gateway.cron import jobs
from agent_gateway.cron import manager as manager_mod
from agent_gateway.cron import scheduler
from agent_gateway.cron.jobs import create_job, load_jobs


@pytest.fixture
def isolated_cron(tmp_path, monkeypatch):
    cron_dir = tmp_path / "cron"
    cron_dir.mkdir(parents=True)
    out_dir = cron_dir / "output"
    out_dir.mkdir(parents=True)
    monkeypatch.setattr(jobs, "NEXUS_DIR", tmp_path)
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "OUTPUT_DIR", out_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    return jobs


def _stored_job(job_id: str) -> dict:
    return next(j for j in load_jobs() if j["id"] == job_id)


def _next_block(job_id: str, delay: str = "5m") -> str:
    """The block an instructed continuous-loop agent emits to re-arm itself."""
    return (
        "Did some work, will check again shortly.\n\n"
        "<!--CRON_OPERATION\n"
        "```json\n"
        "{\n"
        f'  "action": "schedule_next",\n'
        f'  "params": {{"job_id": "{job_id}", "delay": "{delay}"}}\n'
        "}\n"
        "```\n"
        "-->"
    )


# ---------------------------------------------------------------------------
# Schedule parsing
# ---------------------------------------------------------------------------


class TestContinuousParsing:
    def test_continuous_keyword(self):
        parsed = jobs.parse_schedule("continuous")
        assert parsed == {"kind": "continuous", "display": "agent-paced"}

    @pytest.mark.parametrize("alias", ["agent", "ondemand", "agent-paced", "Continuous", "AGENT"])
    def test_continuous_aliases(self, alias):
        assert jobs.parse_schedule(alias)["kind"] == "continuous"

    def test_compute_next_run_first_run_is_now(self):
        sched = jobs.parse_schedule("continuous")
        # No last_run_at → first run is immediate (a real timestamp, not None).
        assert jobs.compute_next_run(sched, None) is not None

    def test_compute_next_run_after_run_is_none(self):
        sched = jobs.parse_schedule("continuous")
        # After a run (last_run_at set) → idle, no automatic next run.
        assert jobs.compute_next_run(sched, "2026-01-01T00:00:00+00:00") is None

    def test_create_job_continuous_runs_immediately(self, isolated_cron):
        job = create_job(prompt="watch the deploy", schedule="continuous")
        # First iteration is armed for now so the next tick fires it.
        assert job["schedule"]["kind"] == "continuous"
        assert job["next_run_at"] is not None
        assert job["repeat"]["times"] is None  # unlimited by default


# ---------------------------------------------------------------------------
# Lifecycle: idle vs armed, mark_job_run preserves agent-set next_run_at
# ---------------------------------------------------------------------------


class TestContinuousLifecycle:
    def test_mark_run_without_schedule_next_goes_idle(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous")
        # tick() clears the due run before execution; simulate that here.
        jobs.clear_next_run(job["id"])
        jobs.mark_job_run(job["id"], success=True)
        stored = _stored_job(job["id"])
        # Agent emitted no schedule_next → loop idles, no pending run.
        assert stored["next_run_at"] is None
        assert stored["state"] == "idle"
        assert stored["repeat"]["completed"] == 1

    def test_mark_run_preserves_agent_set_next_run(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous")
        job_id = job["id"]
        # Agent re-armed the next run mid-iteration.
        jobs.set_next_run(job_id, "2099-01-01T00:00:00+00:00")
        jobs.mark_job_run(job_id, success=True)
        stored = _stored_job(job_id)
        assert stored["next_run_at"] == "2099-01-01T00:00:00+00:00"
        assert stored["state"] == "scheduled"  # armed, not idle

    def test_max_runs_cap_marks_completed(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous", max_runs=2)
        job_id = job["id"]
        jobs.mark_job_run(job_id, success=True)
        jobs.mark_job_run(job_id, success=True)
        stored = _stored_job(job_id)
        assert stored["state"] == "completed"
        assert stored["enabled"] is False
        assert stored["repeat"]["completed"] == 2

    def test_set_next_run_refuses_disabled_job(self, isolated_cron):
        """An agent can't yank a paused loop back to life via schedule_next."""
        job = create_job(prompt="task", schedule="continuous")
        job_id = job["id"]
        jobs.pause_job(job_id)
        result = jobs.set_next_run(job_id, "2099-01-01T00:00:00+00:00")
        # Returned, but the loop stays paused — pause wins over schedule_next.
        assert result is not None
        stored = _stored_job(job_id)
        assert stored["enabled"] is False
        assert stored["state"] == "paused"

    def test_normalize_state_idle(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous")
        jobs.clear_next_run(job["id"])
        jobs.mark_job_run(job["id"], success=True)  # → idle
        normalized = jobs.get_job(job["id"])
        assert normalized["state"] == "idle"


# ---------------------------------------------------------------------------
# schedule_next executor (core/cron_tool.py)
# ---------------------------------------------------------------------------


class TestScheduleNextExecutor:
    @pytest.mark.asyncio
    async def test_schedule_next_arms_future_run(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous")
        job_id = job["id"]
        mgr = manager_mod.CronManager(store=None, runner=None)
        executor = cron_tool.CronToolExecutor(mgr)

        op = cron_tool.CronOperation(
            action="schedule_next",
            params={"job_id": job_id, "delay": "10m"},
            raw_block="<!--CRON_OPERATION-->",
        )
        results = await executor.execute_all([op], session_key="cron_arms")
        assert results[0].success
        assert _stored_job(job_id)["next_run_at"] is not None
        assert _stored_job(job_id)["state"] == "scheduled"

    @pytest.mark.asyncio
    async def test_schedule_next_now_runs_immediately(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous")
        jobs.mark_job_run(job["id"], success=True)  # idle first
        mgr = manager_mod.CronManager(store=None, runner=None)
        executor = cron_tool.CronToolExecutor(mgr)

        for idx, delay in enumerate((None, 0, "now", "")):
            op = cron_tool.CronOperation(
                action="schedule_next",
                params={"job_id": job["id"], "delay": delay},
                raw_block="<!--CRON_OPERATION-->",
            )
            # Unique session per iteration so the rate limiter never trips.
            results = await executor.execute_all([op], session_key=f"cron_now_{idx}")
            assert results[0].success, f"delay={delay!r} should arm a run"
            stored = _stored_job(job["id"])
            assert stored["next_run_at"] is not None

    @pytest.mark.asyncio
    async def test_schedule_next_rejects_fixed_cadence_job(self, isolated_cron):
        """schedule_next is only for continuous loops — refuse interval/cron."""
        job = create_job(prompt="task", schedule="every 10m")
        mgr = manager_mod.CronManager(store=None, runner=None)
        executor = cron_tool.CronToolExecutor(mgr)
        op = cron_tool.CronOperation(
            action="schedule_next",
            params={"job_id": job["id"], "delay": "5m"},
            raw_block="<!--CRON_OPERATION-->",
        )
        results = await executor.execute_all([op], session_key="cron_reject")
        assert not results[0].success
        assert "agent-paced" in results[0].message or "节奏" in results[0].message

    @pytest.mark.asyncio
    async def test_schedule_next_bad_delay_fails_gracefully(self, isolated_cron):
        job = create_job(prompt="task", schedule="continuous")
        mgr = manager_mod.CronManager(store=None, runner=None)
        executor = cron_tool.CronToolExecutor(mgr)
        op = cron_tool.CronOperation(
            action="schedule_next",
            params={"job_id": job["id"], "delay": "banana"},
            raw_block="<!--CRON_OPERATION-->",
        )
        results = await executor.execute_all([op], session_key="cron_baddelay")
        assert not results[0].success
        assert "delay" in results[0].message

    @pytest.mark.asyncio
    async def test_schedule_next_missing_job_id(self, isolated_cron):
        mgr = manager_mod.CronManager(store=None, runner=None)
        executor = cron_tool.CronToolExecutor(mgr)
        op = cron_tool.CronOperation(
            action="schedule_next", params={}, raw_block="<!--CRON_OPERATION-->"
        )
        results = await executor.execute_all([op], session_key="cron_nojobid")
        assert not results[0].success
        assert "job_id" in results[0].message


# ---------------------------------------------------------------------------
# scheduler.run_job — block execution + no per-iteration timeout for loops
# ---------------------------------------------------------------------------


class TestSchedulerContinuousRun:
    @pytest.mark.asyncio
    async def test_run_job_executes_schedule_next_block(self, isolated_cron, monkeypatch):
        job = create_job(prompt="watch deploy", schedule="continuous", name="cl")
        job_id = job["id"]

        async def fake_execute_agent(store, j, prompt, system_extra=""):
            assert "schedule_next" in system_extra, "agent must be taught the protocol"
            return _next_block(job_id, delay="2m")

        monkeypatch.setattr(scheduler, "_execute_agent", fake_execute_agent)
        manager = manager_mod.CronManager(store=None, runner=None)

        success, output, final_response, error = await scheduler.run_job(
            store=None, job=job, cron_manager=manager
        )
        assert success is True
        assert error is None
        # Block replaced with a confirmation, not left verbatim.
        assert "CRON_OPERATION" not in output
        assert "✅" in output
        # The loop is now armed for its next iteration (not idle).
        stored = _stored_job(job_id)
        assert stored["next_run_at"] is not None
        assert stored["state"] == "scheduled"

    @pytest.mark.asyncio
    async def test_recurring_loop_has_no_forced_timeout(self, isolated_cron, monkeypatch):
        """Recurring loops run with timeout=None — the agent's own max_turns/
        timeout govern each iteration; we no longer impose a wall-clock cut."""
        captured = {}
        job = create_job(prompt="long task", schedule="continuous")

        async def fake_wait_for(coro, timeout=None):
            captured["timeout"] = timeout
            return await coro

        async def fake_execute_agent(store, j, prompt, system_extra=""):
            return "done"

        monkeypatch.setattr(scheduler.asyncio, "wait_for", fake_wait_for)
        monkeypatch.setattr(scheduler, "_execute_agent", fake_execute_agent)

        await scheduler.run_job(store=None, job=job, cron_manager=None)
        assert captured["timeout"] is None, "recurring loop must not be force-timed-out"

    @pytest.mark.asyncio
    async def test_continuous_idle_when_agent_emits_nothing(self, isolated_cron, monkeypatch):
        """End-to-end through tick(): an iteration that emits no schedule_next
        and no pause_job leaves the loop idle, ready to be re-armed later."""
        job = create_job(prompt="task", schedule="continuous")
        job_id = job["id"]

        async def fake_execute_agent(store, j, prompt, system_extra=""):
            return "nothing to schedule"  # no schedule_next, no pause_job

        monkeypatch.setattr(scheduler, "_execute_agent", fake_execute_agent)
        # tick() runs the full lifecycle: clear → execute → mark_job_run. The
        # job is due immediately (next_run_at = now at creation).
        await scheduler.tick(store=None, verbose=False)

        stored = _stored_job(job_id)
        assert stored["next_run_at"] is None
        assert stored["state"] == "idle"

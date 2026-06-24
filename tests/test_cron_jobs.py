"""Tests for the cron subsystem that /loop leans on.

These lock the current behavior of the schedule parser, job CRUD/persistence,
next-run advancement, and due-job detection BEFORE building /loop on top of it.
The cron machinery has near-zero prior coverage (only test_methods_run_prompt.py
touched it tangentially, via a fake manager), so these are the first real
regression guards for jobs.py.

Path isolation: jobs.py holds module-level globals (JOBS_FILE / OUTPUT_DIR /
CRON_DIR) hard-coded to ~/.nexus-agent/cron/ — it neither honors resolve_home()
nor NEXUS_AGENT_HOME. The ``isolated_cron`` fixture monkeypatches those globals
to a tmp dir so tests never touch the real cron store.
"""

from datetime import timedelta

import pytest

from agent_gateway.cron import jobs
from agent_gateway.cron.jobs import (
    advance_next_run,
    create_job,
    get_due_jobs,
    get_job,
    list_job_outputs,
    list_jobs,
    load_jobs,
    parse_schedule,
    save_job_output,
    save_jobs,
    _job_output_dir,
)
from agent_gateway.cron.jobs import _now


# ---------------------------------------------------------------------------
# Fixture — redirect the hard-coded cron paths to tmp_path
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# parse_schedule — pure, no I/O. The core of /loop's interval parsing.
# ---------------------------------------------------------------------------


def test_parse_every_minutes():
    assert parse_schedule("every 30m") == {
        "kind": "interval",
        "minutes": 30,
        "display": "every 30m",
    }


def test_parse_every_hours_converts_to_minutes():
    r = parse_schedule("every 2h")
    assert r["kind"] == "interval"
    assert r["minutes"] == 120


def test_parse_every_days():
    r = parse_schedule("every 1d")
    assert r["kind"] == "interval"
    assert r["minutes"] == 1440


@pytest.mark.parametrize("dur", ["30m", "2h", "1d"])
def test_parse_bare_duration_is_oneshot(dur):
    """A bare duration is ONE-SHOT — this is exactly why /loop must normalize
    `10m` -> `every 10m` to get recurring semantics."""
    r = parse_schedule(dur)
    assert r["kind"] == "once"
    assert "run_at" in r


def test_parse_cron_expression():
    r = parse_schedule("0 9 * * *")
    assert r["kind"] == "cron"
    assert r["expr"] == "0 9 * * *"


def test_parse_invalid_cron_field_raises():
    with pytest.raises(ValueError):
        parse_schedule("0 99 * * *")  # hour 99 is invalid -> croniter rejects


def test_parse_short_token_is_not_cron():
    # 3 fields -> not a cron expr -> falls through -> no duration/timestamp -> error
    with pytest.raises(ValueError):
        parse_schedule("0 9 *")


def test_parse_garbage_raises():
    with pytest.raises(ValueError):
        parse_schedule("not a schedule!!!")


def test_parse_iso_timestamp():
    r = parse_schedule("2030-01-01T10:00:00")
    assert r["kind"] == "once"
    assert r["run_at"].startswith("2030-01-01T10:00")


def test_parse_strips_whitespace():
    assert parse_schedule("  every 5m  ")["kind"] == "interval"


# ---------------------------------------------------------------------------
# create_job / list / get / persistence — isolated to tmp
# ---------------------------------------------------------------------------


def test_create_job_returns_full_record(isolated_cron):
    job = create_job(prompt="check deploy", schedule="every 10m", name="deploy-check")
    assert job["id"]
    assert len(job["id"]) == 12
    assert job["schedule"]["kind"] == "interval"
    assert job["schedule"]["minutes"] == 10
    assert job["prompt"] == "check deploy"
    assert job["name"] == "deploy-check"
    assert "next_run_at" in job
    assert "schedule_display" in job


def test_create_job_persists_and_lists(isolated_cron):
    job = create_job(prompt="hi", schedule="every 10m")
    assert isolated_cron.JOBS_FILE.exists()

    listed = list_jobs()
    assert any(j["id"] == job["id"] for j in listed)

    got = get_job(job["id"])
    assert got is not None
    assert got["prompt"] == "hi"


def test_create_job_default_deliver_is_local_without_origin(isolated_cron):
    job = create_job(prompt="x", schedule="every 10m")
    assert job["deliver"] == "local"


def test_create_job_invalid_schedule_raises(isolated_cron):
    with pytest.raises(ValueError):
        create_job(prompt="x", schedule="garbage")


# ---------------------------------------------------------------------------
# Loop termination: max_runs cap + stop_condition
# ---------------------------------------------------------------------------


def test_create_loop_with_max_runs_sets_repeat_times(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m", max_runs=5)
    assert job["repeat"] == {"times": 5, "completed": 0}
    assert job["stop_condition"] is None


def test_create_loop_without_max_runs_is_unlimited(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m")
    assert job["repeat"]["times"] is None  # unlimited


def test_create_loop_persists_stop_condition(isolated_cron):
    job = create_job(
        prompt="x", schedule="every 1m", stop_condition="deploy is green"
    )
    assert job["stop_condition"] == "deploy is green"
    reloaded = [j for j in load_jobs() if j["id"] == job["id"]][0]
    assert reloaded["stop_condition"] == "deploy is green"


def test_create_loop_strips_empty_stop_condition(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m", stop_condition="   ")
    assert job["stop_condition"] is None


def test_create_loop_rejects_bad_max_runs(isolated_cron):
    for bad in (0, -1, 1.5):
        with pytest.raises(ValueError):
            create_job(prompt="x", schedule="every 1m", max_runs=bad)


def test_max_runs_ignored_for_oneshot(isolated_cron):
    # One-shots run once regardless; max_runs must not flip them to recurring.
    job = create_job(prompt="x", schedule="10m", max_runs=5)
    assert job["schedule"]["kind"] == "once"
    assert job["repeat"]["times"] == 1


def test_normalize_surfaces_loop_termination_fields(isolated_cron):
    job = create_job(
        prompt="x", schedule="every 1m", max_runs=7, stop_condition="done"
    )
    view = get_job(job["id"])
    assert view["max_runs"] == 7
    assert view["completed"] == 0
    assert view["stop_condition"] == "done"


def test_normalize_max_runs_none_for_unlimited_loop(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m")
    assert get_job(job["id"])["max_runs"] is None


def test_mark_run_completes_loop_at_cap_not_removes(isolated_cron):
    from agent_gateway.cron.jobs import mark_job_run

    job = create_job(prompt="x", schedule="every 1m", max_runs=2)
    # Two successful runs reach the cap.
    mark_job_run(job["id"], success=True)
    mark_job_run(job["id"], success=True)

    surviving = [j for j in load_jobs() if j["id"] == job["id"]]
    assert surviving, "capped loop must stay visible for output review"
    view = get_job(job["id"])
    assert view["state"] == "completed"
    assert view["enabled"] is False
    assert view["completed"] == 2
    assert view["next_run_at"] is None


def test_mark_run_keeps_oneshot_remove_behavior(isolated_cron):
    from agent_gateway.cron.jobs import mark_job_run

    job = create_job(prompt="x", schedule="10m")  # one-shot
    mark_job_run(job["id"], success=True)
    assert not [j for j in load_jobs() if j["id"] == job["id"]]


def test_mark_run_below_cap_stays_scheduled(isolated_cron):
    from agent_gateway.cron.jobs import mark_job_run

    job = create_job(prompt="x", schedule="every 1m", max_runs=5)
    mark_job_run(job["id"], success=True)
    view = get_job(job["id"])
    assert view["state"] == "scheduled"
    assert view["enabled"] is True
    assert view["completed"] == 1


# ---------------------------------------------------------------------------
# advance_next_run — recurring jobs advance; one-shot does not
# ---------------------------------------------------------------------------


def test_advance_next_run_moves_interval_forward(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m")
    original_next = get_job(job["id"])["next_run_at"]

    moved = advance_next_run(job["id"])
    assert moved is True

    new_next = get_job(job["id"])["next_run_at"]
    assert new_next != original_next


def test_advance_next_run_returns_false_for_oneshot(isolated_cron):
    job = create_job(prompt="x", schedule="30m")  # once
    assert advance_next_run(job["id"]) is False


def test_advance_next_run_unknown_job(isolated_cron):
    assert advance_next_run("nonexistent") is False


# ---------------------------------------------------------------------------
# get_due_jobs — future job not due; one-shot just-past is due
# (one-shots are NOT subject to the recurring fast-forward, so this is robust
#  regardless of the grace window value.)
# ---------------------------------------------------------------------------


def test_future_job_is_not_due(isolated_cron):
    create_job(prompt="x", schedule="every 1m")  # next_run ~1m in the future
    due = get_due_jobs()
    assert due == []


def test_past_oneshot_is_due(isolated_cron):
    past = (_now() - timedelta(seconds=1)).isoformat()
    job = create_job(prompt="x", schedule=past)  # once, run_at 1s ago

    # Force the created job's next_run_at to the past (create_job may compute
    # next_run from run_at; make it explicit so the assertion is deterministic).
    all_jobs = load_jobs()
    for j in all_jobs:
        if j["id"] == job["id"]:
            j["next_run_at"] = past
    save_jobs(all_jobs)

    due = get_due_jobs()
    assert any(d["id"] == job["id"] for d in due)


# ---------------------------------------------------------------------------
# list_job_outputs — read per-tick outputs (powers the Loops panel)
# ---------------------------------------------------------------------------


def test_list_job_outputs_reads_saved_output(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m")
    save_job_output(job["id"], "# run\nthe result")

    outputs = list_job_outputs(job["id"])
    assert len(outputs) == 1
    assert "the result" in outputs[0]["content"]
    assert outputs[0]["run_at"]  # timestamp stem


def test_list_job_outputs_newest_first(isolated_cron):
    job = create_job(prompt="x", schedule="every 1m")
    out_dir = _job_output_dir(job["id"])
    out_dir.mkdir(parents=True)
    (out_dir / "2026-06-01_10-00-00.md").write_text("older", encoding="utf-8")
    (out_dir / "2026-06-02_10-00-00.md").write_text("newer", encoding="utf-8")

    outputs = list_job_outputs(job["id"])
    assert [o["content"] for o in outputs] == ["newer", "older"]


def test_list_job_outputs_empty_for_unknown_job(isolated_cron):
    assert list_job_outputs("nonexistent") == []

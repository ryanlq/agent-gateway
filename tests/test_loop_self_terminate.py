"""Tests for the loop self-awareness system-prompt injection.

``scheduler._build_loop_system_extra`` is what gives a recurring loop agent
the ability to terminate itself: it tells the agent its job_id, the iteration
it's on, any stop condition, and the exact ``<!--CRON_OPERATION-->`` block to
emit to pause the loop. Without these invariants the agent cannot stop a loop
it is running inside.
"""

from agent_gateway.cron.scheduler import _build_loop_system_extra


def _loop_job(job_id="abc123", *, completed=0, times=None, stop_condition=None):
    return {
        "id": job_id,
        "schedule": {"kind": "interval", "minutes": 10},
        "repeat": {"times": times, "completed": completed},
        "stop_condition": stop_condition,
    }


def test_self_terminate_empty_for_oneshot():
    assert _build_loop_system_extra({"schedule": {"kind": "once"}}) == ""


def test_self_terminate_includes_job_id():
    extra = _build_loop_system_extra(_loop_job(job_id="deadbeef"))
    assert "deadbeef" in extra
    assert '"job_id": "deadbeef"' in extra


def test_self_terminate_includes_pause_action():
    extra = _build_loop_system_extra(_loop_job())
    assert "pause_job" in extra
    assert "CRON_OPERATION" in extra


def test_self_terminate_reports_iteration_number():
    extra = _build_loop_system_extra(_loop_job(completed=3, times=10))
    # The run about to happen is iteration completed+1 = 4.
    assert "iteration 4 of 10" in extra


def test_self_terminate_uncapped_omits_total():
    extra = _build_loop_system_extra(_loop_job(completed=2))
    assert "iteration 3 of" not in extra
    assert "iteration 3." in extra


def test_self_terminate_embeds_stop_condition():
    extra = _build_loop_system_extra(
        _loop_job(stop_condition="all tests pass")
    )
    assert "all tests pass" in extra


def test_self_terminate_guides_when_no_stop_condition():
    extra = _build_loop_system_extra(_loop_job())
    # Still told it MAY self-terminate once the goal is achieved.
    assert "terminate" in extra.lower()


def test_self_terminate_works_for_cron_kind():
    extra = _build_loop_system_extra(
        {"id": "x", "schedule": {"kind": "cron", "expr": "*/10 * * * *"}, "repeat": {"times": None, "completed": 0}}
    )
    assert "x" in extra
    assert "pause_job" in extra

"""Tests for the ``/loop`` command.

Covers the shared parser (``core/commands.py``), the desktop wiring
(``server/methods.py::handle_slash_exec``), and that the command surfaces in
``commands.catalog`` (the desktop ``/`` menu source). The IM path
(``core/runner.py::_cmd_loop``) reuses the same parser, so the parsing tests
guard both surfaces.
"""

import pytest

from agent_gateway.core.commands import normalize_loop_schedule, parse_loop_args
from agent_gateway.server import methods


# ---------------------------------------------------------------------------
# normalize_loop_schedule / parse_loop_args — pure
# ---------------------------------------------------------------------------


def test_normalize_bare_duration_becomes_recurring():
    assert normalize_loop_schedule("10m") == "every 10m"


def test_normalize_every_passthrough():
    assert normalize_loop_schedule("every 2h") == "every 2h"


def test_normalize_cron_passthrough():
    assert normalize_loop_schedule("*/10 * * * *") == "*/10 * * * *"


def test_normalize_bad_interval_raises():
    with pytest.raises(ValueError):
        normalize_loop_schedule("daily")


def test_parse_loop_bare_duration():
    # The key /loop behavior: bare 10m -> recurring every 10m.
    assert parse_loop_args("10m check deploy") == ("every 10m", "check deploy", None)


def test_parse_loop_every_form():
    assert parse_loop_args("every 2h check status") == ("every 2h", "check status", None)


def test_parse_loop_quoted_cron():
    assert parse_loop_args('"*/10 * * * *" check deploy') == (
        "*/10 * * * *",
        "check deploy",
        None,
    )


def test_parse_loop_missing_prompt_raises():
    with pytest.raises(ValueError):
        parse_loop_args("10m")


def test_parse_loop_empty_raises():
    with pytest.raises(ValueError):
        parse_loop_args("")


def test_parse_loop_bad_interval_raises():
    with pytest.raises(ValueError):
        parse_loop_args("10x check")


# --max N iteration cap ------------------------------------------------------


def test_parse_loop_max_space_form():
    assert parse_loop_args("10m --max 5 check deploy") == (
        "every 10m",
        "check deploy",
        5,
    )


def test_parse_loop_max_equals_form():
    assert parse_loop_args("every 2h check status --max=20") == (
        "every 2h",
        "check status",
        20,
    )


def test_parse_loop_max_leading_position():
    # --max may appear before the prompt body.
    assert parse_loop_args("10m --max 3 check status") == (
        "every 10m",
        "check status",
        3,
    )


def test_parse_loop_max_not_in_prompt():
    # The flag must be stripped, never folded into the prompt text.
    schedule, prompt, _ = parse_loop_args("10m report --max 7 status")
    assert "--max" not in prompt
    assert prompt == "report status"


def test_parse_loop_max_bad_value_raises():
    with pytest.raises(ValueError):
        parse_loop_args("10m --max soon check")


def test_parse_loop_max_zero_raises():
    with pytest.raises(ValueError):
        parse_loop_args("10m --max 0 check")


def test_parse_loop_max_missing_value_raises():
    with pytest.raises(ValueError):
        parse_loop_args("10m --max")


# ---------------------------------------------------------------------------
# Desktop wiring — handle_slash_exec /loop -> CronManager.create_job
# ---------------------------------------------------------------------------


class _FakeCron:
    """Records create_job kwargs; stands in for CronManager."""

    def __init__(self):
        self.created: list[dict] = []

    def create_job(self, **kwargs):
        self.created.append(kwargs)
        return {
            "id": "fakeid123456",
            "name": kwargs.get("name") or "cron job",
            "schedule_display": kwargs.get("schedule"),
            "next_run_at": "soon",
        }


async def _noop_emit(*_args, **_kwargs):
    return None


async def test_loop_slash_exec_creates_recurring_local_job():
    fake = _FakeCron()
    methods._cron_manager = fake
    try:
        result = await methods.handle_slash_exec(
            {"session_id": "s1", "command": "loop 10m check deploy"},
            emit=_noop_emit,
            sessions=None,
        )
    finally:
        methods._cron_manager = None

    assert fake.created, "create_job was not called"
    job_kwargs = fake.created[0]
    assert job_kwargs["schedule"] == "every 10m"  # recurring, not one-shot
    assert job_kwargs["prompt"] == "check deploy"
    assert job_kwargs["deliver"] == "local"  # desktop default
    assert job_kwargs["max_runs"] is None  # default: unlimited
    assert "output" in result
    assert "fakeid123456" in result["output"]


async def test_loop_slash_exec_max_threads_into_create_job():
    fake = _FakeCron()
    methods._cron_manager = fake
    try:
        await methods.handle_slash_exec(
            {"session_id": "s1", "command": "loop 10m --max 5 check deploy"},
            emit=_noop_emit,
            sessions=None,
        )
    finally:
        methods._cron_manager = None

    assert fake.created, "create_job was not called"
    assert fake.created[0]["max_runs"] == 5
    assert fake.created[0]["prompt"] == "check deploy"  # flag stripped


async def test_loop_slash_exec_bad_interval_returns_warning():
    methods._cron_manager = _FakeCron()
    try:
        result = await methods.handle_slash_exec(
            {"session_id": "s1", "command": "loop 10x check"},
            emit=_noop_emit,
            sessions=None,
        )
    finally:
        methods._cron_manager = None

    assert "warning" in result
    assert "output" not in result


async def test_loop_slash_exec_no_cron_manager_returns_warning():
    methods._cron_manager = None
    result = await methods.handle_slash_exec(
        {"session_id": "s1", "command": "loop 10m check"},
        emit=_noop_emit,
        sessions=None,
    )
    assert "warning" in result


# ---------------------------------------------------------------------------
# commands.catalog — /loop + previously-hidden /cron surface in the menu
# ---------------------------------------------------------------------------


async def test_catalog_includes_loop_and_cron():
    result = await methods.handle_commands_catalog({}, emit=_noop_emit, sessions=None)
    names = [pair[0] for pair in result["pairs"]]
    assert "/loop" in names
    assert "/cron" in names
    assert "/schedule" in names

"""The IQM server's clock may differ from ours. Reported `started_at`/`finished_at` and the
charged exec time come from IQM's clock, while the running-time estimate that enforces time
limits never exceeds the true time — so a task is never failed early because of skew."""

from datetime import UTC, datetime, timedelta

import pytest
import utils_iqm
from utils import TEST_PROJECT
from utils import TestTask as TT


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


# IQM an hour ahead of us, and IQM slightly behind (within the default --max-clock-skew of 4s).
@pytest.mark.parametrize("offset", [3600, -3])
def test_iqm_clock_skew_never_fails_task_early(qscheduler_iqm, monkeypatch, offset):
    monkeypatch.setattr(utils_iqm, "CLOCK_OFFSET", offset)
    qs = qscheduler_iqm
    qs.limit_check_interval = 1
    qs.start()

    task_id = qs.submit(TT().compute_time(30), time_limit_ms=2_000)
    qs.wait_for_failed(task_id)
    info = qs.get_task(task_id)
    assert info["error"] == "Task time limit exceeded"

    # Never early on IQM's own clock; late by at most the check interval plus the estimate's
    # underestimation.
    exec_ms = info["exectime_ms"]
    assert 2_000 <= exec_ms < 5_000

    # started_at/finished_at are IQM's timestamps and the charge is exactly their difference.
    started, finished = _parse_ts(info["started_at"]), _parse_ts(info["finished_at"])
    assert abs(started - (datetime.now(UTC) + timedelta(seconds=offset))) < timedelta(
        seconds=60
    )
    assert abs((finished - started) / timedelta(milliseconds=1) - exec_ms) <= 1
    assert qs.get_project(TEST_PROJECT)["consumed_ms"] == exec_ms

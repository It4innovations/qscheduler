"""IQM server outages: polling never gives up (it backs off), cancels are retried, and the
project is charged the job's exact exec time once IQM answers again."""

import time

from utils import TEST_PROJECT
from utils import TestTask as TT


def _assert_charged_exec_time(qs, task_id):
    exec_ms = qs.get_task(task_id).get("exectime_ms", 0)
    assert abs(qs.get_project(TEST_PROJECT)["consumed_ms"] - exec_ms) < 100
    return exec_ms


def test_iqm_outage_polling_recovers(qscheduler_iqm):
    qs = qscheduler_iqm
    qs.start()
    task_id = qs.submit(TT().compute_time(3))
    qs.wait_for_running(task_id)

    # The job finishes on IQM during the outage; the scheduler learns it afterwards.
    qs.backend.unavailable = True
    time.sleep(5)
    assert qs.get_task_state(task_id) == "running"
    qs.backend.unavailable = False

    qs.wait_for_finished(task_id)
    assert _assert_charged_exec_time(qs, task_id) > 2_500


def test_iqm_cancel_retried_during_outage(qscheduler_iqm):
    qs = qscheduler_iqm
    qs.start()
    task_id = qs.submit(TT().compute_time(60))
    qs.wait_for_running(task_id)

    qs.backend.unavailable = True
    qs.cancel_task(task_id)
    time.sleep(3)
    assert qs.get_task_state(task_id) == "running"
    qs.backend.unavailable = False

    qs.wait_for_cancelled(task_id)
    _assert_charged_exec_time(qs, task_id)


def test_iqm_time_limit_during_outage_charge_capped(qscheduler_iqm):
    qs = qscheduler_iqm
    qs.limit_check_interval = 1
    qs.start(add_test_project=False)
    qs.add_project("p", limit_ms=3_000)
    task_id = qs.submit(TT().compute_time(60), project="p", time_limit_ms=1_500)
    qs.wait_for_running(task_id)

    # The limit fires while IQM is down, so the cancel can't be delivered. The task's
    # in-flight estimate stops growing at its own limit (1.5s): without the cap it would pass
    # the project's 3s budget during the outage and new submits would be rejected (402).
    qs.backend.unavailable = True
    time.sleep(4)
    other = qs.submit(TT(), project="p")
    assert qs.get_project("p")["consumed_ms"] == 0
    qs.backend.unavailable = False

    qs.wait_for_failed(task_id)
    assert qs.get_task(task_id)["error"] == "Task time limit exceeded"
    exec_ms = qs.get_task(task_id)["exectime_ms"]
    qs.wait_for_finished(other)
    other_ms = qs.get_task(other).get("exectime_ms", 0)
    assert abs(qs.get_project("p")["consumed_ms"] - exec_ms - other_ms) < 100

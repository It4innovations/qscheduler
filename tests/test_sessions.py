import os
import time

from utils import TEST_MACHINE_NAME, TEST_PROJECT
from utils import TestTask as TT


def test_session_owner_too_long(qscheduler_test):
    qscheduler_test.start()
    qscheduler_test.new_session(time_limit=1, owner="x" * 251, expect_error=422)
    qscheduler_test.new_session(time_limit=1, owner="x" * 250)


def test_session_info_fields(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=1, owner="dave")
    qscheduler.wait_for_session_open(s)

    info = qscheduler.get_session(s)
    assert info["id"] == s
    assert info["state"] == "open"
    assert info["machine"] == TEST_MACHINE_NAME
    assert info["project"] == TEST_PROJECT
    assert info["owner"] == "dave"
    assert info["time_limit_ms"] == 1_000
    assert info["created_at"] is not None
    assert info["opened_at"] is not None
    assert "closed_at" not in info
    # May be a small nonzero value if a checkpoint has already landed (tokio's interval
    # fires an immediate first tick), but shouldn't yet reflect the full session length.
    assert info.get("exectime_ms", 0) < 900

    qscheduler.wait_for_session_closed(s)
    closed = qscheduler.get_session(s)
    assert closed["state"] == "closed"
    assert closed["closed_at"] is not None
    # time_limit=1s, so the session was open for roughly that long.
    assert closed["exectime_ms"] >= 900


def test_session_exectime_ms_updates_periodically(qscheduler_test):
    """exectime_ms is refreshed by the periodic checkpoint while a session is still
    open, not just once it closes."""
    qs = qscheduler_test
    qs.config(limit_check_interval=1)
    qs.start()

    s = qs.new_session(time_limit=30)
    qs.wait_for_session_open(s)

    first = qs.get_session(s).get("exectime_ms", 0)
    time.sleep(3.5)
    second = qs.get_session(s)

    assert second["state"] == "open", (
        "session closed before we could observe a checkpoint"
    )
    assert second["exectime_ms"] > first
    # ~3.5s elapsed since open, across several 1s checkpoints (loose bound: only the
    # periodic-update behavior matters here, not exact timing precision).
    assert second["exectime_ms"] >= 2_000


def test_session_empty(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)
    qscheduler.wait_for_session_closed(s)
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)
    qscheduler.wait_for_session_closed(s)


def test_session_fail_submit(qscheduler):
    qscheduler.start()
    qscheduler.submit(TT(), session_id=123, expect_error=422)


def test_session_submit_and_timeout(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=3)
    qscheduler.wait_for_session_open(s)
    t1 = qscheduler.submit(TT())
    time.sleep(0.5)
    t2 = qscheduler.submit(TT(), session_id=s)
    qscheduler.wait_for_finished(t2)
    qscheduler.assert_task_waiting(t1)
    qscheduler.assert_session_open(s)
    qscheduler.wait_for_session_closed(s)
    qscheduler.wait_for_finished(t1)


def test_session_submit_and_cancel(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=2)
    qscheduler.wait_for_session_open(s)
    time.sleep(0.2)
    ts = [qscheduler.submit(TT().compute_time(1), session_id=s) for _ in range(10)]
    qscheduler.wait_for_session_closed(s)
    time.sleep(0.5)
    qscheduler.assert_task_finished(ts[0])
    qscheduler.assert_task_finished(ts[1])
    for t in ts[2:]:
        qscheduler.assert_task_canceled(t)
        qscheduler.assert_task_canceled(t)


def test_session_max_duration_rejected(qscheduler):
    qscheduler.config(max_session_time=2)
    qscheduler.start()
    qscheduler.new_session(time_limit=2)
    qscheduler.new_session(time_limit=3, expect_error=422)


def test_session_no_overlap(qscheduler):
    qscheduler.config(queue_size=2)
    qscheduler.start()
    s1 = qscheduler.new_session(time_limit=1)
    s2 = qscheduler.new_session(time_limit=2)
    qscheduler.wait_for_session_open(s1)
    assert qscheduler.get_session_state(s2) == "waiting"
    qscheduler.wait_for_session_open(s2)
    assert qscheduler.get_session_state(s1) == "closed"
    qscheduler.wait_for_session_closed(s2)


def test_session_cancel_open(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=60)
    qscheduler.wait_for_session_open(s)
    time.sleep(0.2)
    ts = [qscheduler.submit(TT().compute_time(1), session_id=s) for _ in range(10)]
    qscheduler.cancel_session(s)
    qscheduler.wait_for_session_closed(s)
    for t in ts:
        qscheduler.wait_for_finished_or_canceled(t)


def test_session_cancel_waiting(qscheduler):
    qscheduler.config(queue_size=2)
    qscheduler.start()
    s1 = qscheduler.new_session(time_limit=60)
    s2 = qscheduler.new_session(time_limit=60)
    qscheduler.wait_for_session_open(s1)
    assert qscheduler.get_session_state(s2) == "waiting"
    qscheduler.cancel_session(s2)
    qscheduler.wait_for_session_closed(s2)
    assert qscheduler.get_session_state(s1) == "open"


def test_session_cancel_already_closed(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)
    qscheduler.wait_for_session_closed(s)
    qscheduler.cancel_session(s, expect_error=409)


def _wait_for_log(qs, text, log_name="qscheduler.log"):
    log_path = os.path.join(qs.working_dir, log_name)
    deadline = time.monotonic() + 5.0
    while True:
        with open(log_path) as f:
            if text in f.read():
                return
        assert time.monotonic() < deadline, f"not logged: {text}"
        time.sleep(0.2)


def test_closed_session_evicted_from_memory(qscheduler_test):
    qs = qscheduler_test
    qs.config(task_retention="1s")
    qs.start()

    session_id = qs.new_session(time_limit=60, owner="alice")
    qs.wait_for_session_open(session_id)
    time.sleep(0.1)
    qs.cancel_session(session_id)
    qs.wait_for_session_closed(session_id)
    info = qs.get_session(session_id)
    assert info["owner"] == "alice"

    # With the owner gone too, so nothing in memory could have answered.
    _wait_for_log(qs, "Evicted closed sessions from memory")
    _wait_for_log(qs, "Evicted idle users from memory")

    # Served from the DB now, and it makes no difference.
    assert qs.get_session(session_id) == info
    qs.cancel_session(session_id, expect_error=409)
    qs.new_session(time_limit=60, owner="alice")


def test_session_not_evicted_before_closing(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=1, task_retention="1s")
    qs.start()

    opened = qs.new_session(time_limit=60)
    qs.wait_for_session_open(opened)
    waiting = qs.new_session(time_limit=60)
    time.sleep(2.5)

    assert qs.get_session_state(opened) == "open"
    assert qs.get_session_state(waiting) == "waiting"
    t = qs.submit(TT().compute_time(60), session_id=opened)
    qs.wait_for_running(t)

    # A waiting session opens fine once the one before it is gone.
    qs.cancel_session(opened)
    qs.wait_for_session_closed(opened)
    qs.wait_for_session_open(waiting)


def test_session_closed_by_restart_evicted_from_memory(qscheduler_test):
    qs = qscheduler_test
    qs.config(task_retention="1s")
    qs.start()

    session_id = qs.new_session(time_limit=60, owner="alice")
    qs.wait_for_session_open(session_id)
    qs.restart()
    qs.wait_for_session_closed(session_id)
    assert qs.get_session(session_id)["owner"] == "alice"

    _wait_for_log(qs, "Evicted closed sessions from memory", "qscheduler-restart.log")
    assert qs.get_session(session_id)["owner"] == "alice"
    qs.cancel_session(session_id, expect_error=409)


def test_cancel_nonexistent_session(qscheduler):
    qscheduler.start()
    qscheduler.cancel_session(999_999, expect_error=404)


def test_session_task_time_charged_only_via_session(qscheduler_test):
    """A task in a session isn't charged to the project itself (the session is): the
    project's consumption equals the session's exec time, not session + task time."""
    qs = qscheduler_test
    qs.config(limit_check_interval=1)
    qs.start()
    s = qs.new_session(time_limit=30)
    qs.wait_for_session_open(s)
    t = qs.submit(TT().compute_time(2.5), session_id=s)
    qs.wait_for_finished(t)
    qs.cancel_session(s)
    qs.wait_for_session_closed(s)

    session_ms = qs.get_session(s)["exectime_ms"]
    assert abs(qs.get_project(TEST_PROJECT)["consumed_ms"] - session_ms) < 100

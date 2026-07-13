from utils import TEST_MACHINE_NAME, TEST_PROJECT
from utils import TestTask as TT
import time


def test_session_info_fields(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)

    info = qscheduler.get_session(s)
    assert info["id"] == s
    assert info["state"] == "open"
    assert info["machine"] == TEST_MACHINE_NAME
    assert info["project"] == TEST_PROJECT
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
    qs.session_check_interval = 1
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
    qscheduler.max_session_time = 2
    qscheduler.start()
    qscheduler.new_session(time_limit=2)
    qscheduler.new_session(time_limit=3, expect_error=422)


def test_session_no_overlap(qscheduler):
    qscheduler.queue_size = 2
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
    qscheduler.queue_size = 2
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


def test_cancel_nonexistent_session(qscheduler):
    qscheduler.start()
    qscheduler.cancel_session(999_999, expect_error=404)

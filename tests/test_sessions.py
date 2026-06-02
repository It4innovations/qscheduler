from utils import TestTask as TT
import time


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

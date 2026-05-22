from utils import TestTask as TT
import time


def test_session_empty(qscheduler):
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)
    qscheduler.wait_for_session_closed(s)
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)
    qscheduler.wait_for_session_closed(s)


def test_session_fail_submit(qscheduler):
    qscheduler.submit(TT(), session_id=123, expect_error=422)


def test_session_submit_and_timeout(qscheduler):
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
    s = qscheduler.new_session(time_limit=1)
    qscheduler.wait_for_session_open(s)
    time.sleep(0.2)
    t1 = qscheduler.submit(TT().wait(1), session_id=s)
    t2 = qscheduler.submit(TT().wait(1), session_id=s)
    t3 = qscheduler.submit(TT().wait(1), session_id=s)
    qscheduler.wait_for_session_closed(s)
    qscheduler.assert_task_finished(t1)
    qscheduler.assert_task_canceled(t2)
    qscheduler.assert_task_canceled(t3)

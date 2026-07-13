from utils import TEST_MACHINE_NAME, TEST_PROJECT, TEST_USER
from utils import TestTask as TT


def test_task_info_fields_project(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)

    info = qscheduler.get_task(t)
    assert info["id"] == t
    assert info["state"] == "finished"
    assert info["machine"] == TEST_MACHINE_NAME
    assert info["user"] == TEST_USER
    assert info["project"] == TEST_PROJECT
    assert "session" not in info
    assert "error" not in info
    assert info["created_at"] is not None
    assert info["finished_at"] is not None


def test_task_info_fields_session(qscheduler):
    qscheduler.start()
    s = qscheduler.new_session(time_limit=10)
    qscheduler.wait_for_session_open(s)
    t = qscheduler.submit(TT(), session_id=s)
    qscheduler.wait_for_finished(t)

    info = qscheduler.get_task(t)
    assert info["session"] == s
    assert "project" not in info


def test_task_info_fields_failed(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().error("boom"))
    qscheduler.wait_for_failed(t)

    info = qscheduler.get_task(t)
    assert info["state"] == "failed"
    assert info["error"] == "boom"


def test_submit_simple(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().compute_time(2))
    qscheduler.wait_for_running(t)
    qscheduler.wait_for_finished(t)
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)


def test_submit_parallel1(qscheduler):
    qscheduler.queue_size = 1
    qscheduler.start()
    t = qscheduler.submit(TT().compute_time(2))
    t1 = qscheduler.submit(TT())
    t2 = qscheduler.submit(TT())
    qscheduler.wait_for_running(t)
    assert qscheduler.get_task_state(t1) == "waiting"
    assert qscheduler.get_task_state(t2) == "waiting"
    qscheduler.wait_for_finished(t)
    qscheduler.wait_for_finished(t1)
    qscheduler.wait_for_finished(t2)


def test_submit_parallel2(qscheduler):
    qscheduler.queue_size = 2
    qscheduler.start()
    t0 = qscheduler.submit(TT().compute_time(2))
    t1 = qscheduler.submit(TT().compute_time(2))
    t2 = qscheduler.submit(TT())
    qscheduler.wait_for_running(t0)
    qscheduler.wait_for_running(t1)
    assert qscheduler.get_task_state(t0) == "running"
    assert qscheduler.get_task_state(t1) == "running"
    assert qscheduler.get_task_state(t2) == "waiting"
    qscheduler.wait_for_finished(t0)
    qscheduler.wait_for_finished(t1)
    qscheduler.wait_for_finished(t2)


def test_submit_fails1(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().error("My error"))
    assert qscheduler.wait_for_failed(t) == "failed"
    assert qscheduler.get_task(t)["error"] == "My error"
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)


def test_submit_fails2(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().error("My error"))
    t2 = qscheduler.submit(TT())
    assert qscheduler.wait_for_failed(t) == "failed"
    assert qscheduler.get_task(t)["error"] == "My error"
    qscheduler.wait_for_finished(t2)


def test_session_closed_but_task_may_finished(qscheduler_test):
    qscheduler_test.start()
    session_id = qscheduler_test.new_session(time_limit=1)
    qscheduler_test.wait_for_session_open(session_id)
    task_id = qscheduler_test.submit(TT().submit_time(3), session_id=session_id)
    qscheduler_test.wait_for_session_closed(session_id)
    qscheduler_test.wait_for_finished_or_canceled(task_id)


def test_cancel_running_task(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().compute_time(5))
    qscheduler.wait_for_running(t)
    qscheduler.cancel_task(t)
    qscheduler.wait_for_cancelled(t)


def test_cancel_queued_task(qscheduler_test):
    qscheduler_test.queue_size = 1
    qscheduler_test.start()
    t = qscheduler_test.submit(TT().compute_time(2))
    t1 = qscheduler_test.submit(TT())
    qscheduler_test.wait_for_running(t)
    assert qscheduler_test.get_task_state(t1) == "waiting"
    qscheduler_test.cancel_task(t1)
    qscheduler_test.wait_for_cancelled(t1)
    qscheduler_test.wait_for_finished(t)


def test_cancel_finished_task(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)
    qscheduler.cancel_task(t, expect_error=409)


def test_cancel_nonexistent_task(qscheduler):
    qscheduler.start()
    qscheduler.cancel_task(999_999, expect_error=404)

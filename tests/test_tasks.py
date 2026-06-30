from utils import TestTask as TT


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
    assert qscheduler.get_task_status(t1)["state"] == "waiting"
    assert qscheduler.get_task_status(t2)["state"] == "waiting"
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
    assert qscheduler.get_task_status(t0)["state"] == "running"
    assert qscheduler.get_task_status(t1)["state"] == "running"
    assert qscheduler.get_task_status(t2)["state"] == "waiting"
    qscheduler.wait_for_finished(t0)
    qscheduler.wait_for_finished(t1)
    qscheduler.wait_for_finished(t2)


def test_submit_fails1(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().error("My error"))
    assert qscheduler.wait_for_failed(t) == {
        "error": "My error",
        "state": "failed",
    }
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)


def test_submit_fails2(qscheduler):
    qscheduler.start()
    t = qscheduler.submit(TT().error("My error"))
    t2 = qscheduler.submit(TT())
    assert qscheduler.wait_for_failed(t) == {
        "error": "My error",
        "state": "failed",
    }
    qscheduler.wait_for_finished(t2)


def test_session_closed_but_task_may_finished(qscheduler_test):
    qscheduler_test.start()
    session_id = qscheduler_test.new_session(time_limit=1)
    qscheduler_test.wait_for_session_open(session_id)
    task_id = qscheduler_test.submit(TT().submit_time(3), session_id=session_id)
    qscheduler_test.wait_for_session_closed(session_id)
    qscheduler_test.wait_for_finished_or_canceled(task_id)

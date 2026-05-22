from utils import TestTask as TT


def test_submit_simple(qscheduler):
    t = qscheduler.submit(TT().wait(2))
    qscheduler.wait_for_running(t)
    qscheduler.wait_for_finished(t)
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)


def test_submit_parallel(qscheduler):
    t = qscheduler.submit(TT().wait(2))
    t1 = qscheduler.submit(TT())
    t2 = qscheduler.submit(TT())
    qscheduler.wait_for_running(t)
    assert qscheduler.get_task_status(t1)["state"] == "waiting"
    assert qscheduler.get_task_status(t2)["state"] == "waiting"
    qscheduler.wait_for_finished(t)
    qscheduler.wait_for_finished(t1)
    qscheduler.wait_for_finished(t2)


def test_submit_fails1(qscheduler):
    t = qscheduler.submit(TT().error("My error"))
    assert qscheduler.wait_for_failed(t) == {
        "error": "Task fails with: My error",
        "state": "failed",
    }
    t = qscheduler.submit(TT())
    qscheduler.wait_for_finished(t)


def test_submit_fails2(qscheduler):
    t = qscheduler.submit(TT().error("My error"))
    t2 = qscheduler.submit(TT())
    assert qscheduler.wait_for_failed(t) == {
        "error": "Task fails with: My error",
        "state": "failed",
    }
    qscheduler.wait_for_finished(t2)

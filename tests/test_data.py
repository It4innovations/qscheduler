import json

from utils import TestTask as TT


def test_task_result_and_artifact(qscheduler_iqm):
    qscheduler_iqm.start()
    t = qscheduler_iqm.submit(TT())
    qscheduler_iqm.wait_for_finished(t)

    result = json.loads(qscheduler_iqm.get_task_result(t))
    assert result["status"] == "completed"

    artifact = json.loads(qscheduler_iqm.get_task_artifact(t, "measurements"))
    assert artifact == {"artifact": "measurements", "values": [0, 1, 0, 1]}


def test_task_unknown_artifact_iqm(qscheduler_iqm):
    qscheduler_iqm.start()
    t = qscheduler_iqm.submit(TT())
    qscheduler_iqm.wait_for_finished(t)
    qscheduler_iqm.get_task_artifact(t, "nonexistent", expect_error=404)


def test_task_result_not_submitted(qscheduler):
    qscheduler.queue_size = 1
    qscheduler.start()
    t = qscheduler.submit(TT().compute_time(2))
    t1 = qscheduler.submit(TT())
    qscheduler.wait_for_running(t)
    assert qscheduler.get_task_state(t1) == "waiting"
    qscheduler.get_task_result(t1, expect_error=409)
    qscheduler.get_task_artifact(t1, "measurements", expect_error=409)
    qscheduler.wait_for_finished(t)
    qscheduler.wait_for_finished(t1)


def test_task_result_nonexistent_task(qscheduler):
    qscheduler.start()
    qscheduler.get_task_result(999_999, expect_error=404)
    qscheduler.get_task_artifact(999_999, "measurements", expect_error=404)


def test_task_result_test_backend_default(qscheduler_test):
    qscheduler_test.start()
    t = qscheduler_test.submit(TT())
    qscheduler_test.wait_for_finished(t)
    assert qscheduler_test.get_task_result(t) == ""


def test_task_result_test_backend_value(qscheduler_test):
    qscheduler_test.start()
    t = qscheduler_test.submit(
        TT()
        .result("test-result")
        .artifact("foo", "test-output-for-foo")
        .artifact("bar", "test-output-for-bar")
    )
    qscheduler_test.wait_for_finished(t)

    assert qscheduler_test.get_task_result(t) == "test-result"
    assert qscheduler_test.get_task_artifact(t, "foo") == "test-output-for-foo"
    assert qscheduler_test.get_task_artifact(t, "bar") == "test-output-for-bar"
    qscheduler_test.get_task_artifact(t, "baz", expect_error=404)


def test_task_result_test_backend_failed(qscheduler_test):
    qscheduler_test.start()
    t = qscheduler_test.submit(
        TT().error("boom").result("failure-detail").artifact("log", "some log")
    )
    qscheduler_test.wait_for_failed(t)

    assert qscheduler_test.get_task_result(t) == "failure-detail"
    assert qscheduler_test.get_task_artifact(t, "log") == "some log"


def test_task_result_test_backend_not_completed(qscheduler_test):
    qscheduler_test.start()
    t = qscheduler_test.submit(TT().compute_time(2).result("test-result"))
    qscheduler_test.wait_for_running(t)
    qscheduler_test.get_task_result(t, expect_error=500)
    qscheduler_test.get_task_artifact(t, "foo", expect_error=500)
    qscheduler_test.wait_for_finished(t)

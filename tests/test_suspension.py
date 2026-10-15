import time

from utils import TEST_MACHINE_NAME
from utils import TestTask as TT

SUSPENDED_ERROR = f"Machine '{TEST_MACHINE_NAME}' is suspended: "


def _task_error(qs, task_id):
    return qs.get_task(task_id)["error"]


def test_suspend_rejects_new_work(qscheduler):
    qscheduler.start()
    s = qscheduler.suspend_machine("Calibration in progress")
    assert s["message"] == "Calibration in progress"
    assert s["cancel"] == "none"
    assert "suspended_at" in s
    assert "resumed_at" not in s

    r = qscheduler.submit(TT(), expect_error=409)
    assert r.text == SUSPENDED_ERROR + "Calibration in progress"
    r = qscheduler.new_session(10, expect_error=409)
    assert r.text == SUSPENDED_ERROR + "Calibration in progress"

    resumed = qscheduler.resume_machine()
    assert resumed["message"] == "Calibration in progress"
    assert resumed["resumed_at"] >= resumed["suspended_at"]

    qscheduler.wait_for_finished(qscheduler.submit(TT()))
    qscheduler.wait_for_session_open(qscheduler.new_session(1))


def test_suspension_history(qscheduler_test):
    qscheduler_test.start()
    assert qscheduler_test.list_suspensions() == []
    qscheduler_test.suspend_machine("first", cancel="waiting")
    qscheduler_test.resume_machine()
    qscheduler_test.suspend_machine("second", cancel="all")

    history = qscheduler_test.list_suspensions()
    assert [(s["message"], s["cancel"]) for s in history] == [
        ("second", "all"),
        ("first", "waiting"),
    ]
    assert "resumed_at" not in history[0]
    assert history[1]["resumed_at"] >= history[1]["suspended_at"]


def test_suspend_rejects_task_in_open_session(qscheduler_test):
    qscheduler_test.start()
    s = qscheduler_test.new_session(10)
    qscheduler_test.wait_for_session_open(s)
    qscheduler_test.suspend_machine("maintenance")
    r = qscheduler_test.submit(TT(), session_id=s, expect_error=409)
    assert r.text == SUSPENDED_ERROR + "maintenance"
    # Policy "none" leaves the session itself open
    qscheduler_test.assert_session_open(s)


def test_suspend_none_keeps_existing_work(qscheduler_test):
    qscheduler_test.queue_size = 1
    qscheduler_test.start()
    t1 = qscheduler_test.submit(TT().compute_time(1))
    t2 = qscheduler_test.submit(TT())
    s = qscheduler_test.new_session(1)
    qscheduler_test.wait_for_running(t1)
    assert qscheduler_test.get_task_state(t2) == "waiting"

    qscheduler_test.suspend_machine("maintenance", cancel="none")
    qscheduler_test.wait_for_finished(t1)
    qscheduler_test.wait_for_finished(t2)
    qscheduler_test.wait_for_session_open(s)


def test_suspend_waiting(qscheduler):
    qscheduler.queue_size = 1
    qscheduler.start()
    t1 = qscheduler.submit(TT().compute_time(2))
    t2 = qscheduler.submit(TT())
    s = qscheduler.new_session(10)
    qscheduler.wait_for_running(t1)

    qscheduler.suspend_machine("broken coupler", cancel="waiting")
    qscheduler.wait_for_failed(t2)
    assert _task_error(qscheduler, t2) == "Machine suspended: broken coupler"
    qscheduler.wait_for_session_closed(s)
    assert "opened_at" not in qscheduler.get_session(s)
    # The submitted task is computed as normal
    qscheduler.wait_for_finished(t1)


def test_suspend_waiting_closes_open_session(qscheduler_test):
    qscheduler_test.queue_size = 1
    qscheduler_test.start()
    s = qscheduler_test.new_session(30)
    qscheduler_test.wait_for_session_open(s)
    t1 = qscheduler_test.submit(TT().compute_time(10), session_id=s)
    t2 = qscheduler_test.submit(TT(), session_id=s)
    qscheduler_test.wait_for_running(t1)
    assert qscheduler_test.get_task_state(t2) == "waiting"

    qscheduler_test.suspend_machine("maintenance", cancel="waiting")
    qscheduler_test.wait_for_session_closed(s)
    for t in (t1, t2):
        qscheduler_test.wait_for_failed(t)
        assert _task_error(qscheduler_test, t) == "Machine suspended: maintenance"


def test_suspend_all(qscheduler):
    qscheduler.queue_size = 1
    qscheduler.start()
    t1 = qscheduler.submit(TT().compute_time(30))
    t2 = qscheduler.submit(TT())
    qscheduler.wait_for_running(t1)

    qscheduler.suspend_machine("emergency", cancel="all")
    for t in (t1, t2):
        qscheduler.wait_for_failed(t)
        assert _task_error(qscheduler, t) == "Machine suspended: emergency"
    # The backend's later report of the cancellation doesn't change the outcome
    time.sleep(1)
    assert qscheduler.get_task_state(t1) == "failed"

    # The machine works again once resumed
    qscheduler.resume_machine()
    qscheduler.wait_for_finished(qscheduler.submit(TT()))


def test_suspend_all_charges_running_project_task(qscheduler_test):
    qscheduler_test.start()
    t = qscheduler_test.submit(TT().compute_time(30))
    qscheduler_test.wait_for_running(t)
    time.sleep(1)
    qscheduler_test.suspend_machine("emergency", cancel="all")
    qscheduler_test.wait_for_failed(t)
    # The estimated running time so far is charged to the project
    assert qscheduler_test.get_task(t)["exectime_ms"] >= 500
    assert qscheduler_test.get_project("test-project")["consumed_ms"] >= 500


def test_suspend_message_validation(qscheduler_test):
    qscheduler_test.start()
    r = qscheduler_test.suspend_machine("x" * 513, expect_error=422)
    assert "513 characters" in r.text
    qscheduler_test.suspend_machine("", expect_error=422)
    qscheduler_test.suspend_machine("   ", expect_error=422)
    qscheduler_test.suspend_machine("m", cancel="everything", expect_error=422)
    assert qscheduler_test.list_suspensions() == []

    # The limit counts characters, not bytes
    qscheduler_test.suspend_machine("č" * 512)


def test_suspend_conflicts(qscheduler_test):
    qscheduler_test.start()
    qscheduler_test.resume_machine(expect_error=409)
    qscheduler_test.suspend_machine("first")
    qscheduler_test.suspend_machine("second", expect_error=409)
    assert [s["message"] for s in qscheduler_test.list_suspensions()] == ["first"]
    qscheduler_test.resume_machine()
    qscheduler_test.resume_machine(expect_error=409)


def test_suspend_unknown_machine(qscheduler_test):
    qscheduler_test.start()
    qscheduler_test.suspend_machine("m", machine="no-such-machine", expect_error=404)
    qscheduler_test.resume_machine(machine="no-such-machine", expect_error=404)
    qscheduler_test.list_suspensions(machine="no-such-machine", expect_error=404)


def test_suspension_survives_restart(qscheduler_test):
    qscheduler_test.start()
    qscheduler_test.suspend_machine("maintenance")
    qscheduler_test.restart()

    r = qscheduler_test.submit(TT(), expect_error=409)
    assert r.text == SUSPENDED_ERROR + "maintenance"
    qscheduler_test.resume_machine()
    qscheduler_test.wait_for_finished(qscheduler_test.submit(TT()))

    qscheduler_test.restart()
    qscheduler_test.wait_for_finished(qscheduler_test.submit(TT()))
    assert len(qscheduler_test.list_suspensions()) == 1


def test_remove_machine_with_suspension_history(qscheduler_test, db_connect):
    qscheduler_test.start()
    qscheduler_test.suspend_machine("retired")
    qscheduler_test.stop(kill=False)
    r = qscheduler_test.remove_machine()
    assert r.returncode == 0, r.stderr
    with db_connect.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM machine_suspensions")
        assert cur.fetchone()[0] == 0

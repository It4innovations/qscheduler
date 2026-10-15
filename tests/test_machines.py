import time

from utils import TestTask as TT
from utils_iqm import ABOUT


def test_machine_about_test_backend(qscheduler_test):
    qscheduler_test.start()
    assert qscheduler_test.get_machine_about() == {"type": "test", "version": "1.0"}


def test_machine_about_iqm(qscheduler_iqm):
    qscheduler_iqm.start()
    assert qscheduler_iqm.get_machine_about() == {
        "type": "iqm",
        "version": {
            "qccsw_version": ABOUT["qccsw_version"],
            "server_version": ABOUT["server_version"],
            "station_control_version": ABOUT["station_control_version"],
        },
    }


def test_machine_about_unknown_machine(qscheduler):
    qscheduler.start()
    qscheduler.get_machine_about("no-such-machine", expect_error=404)


def test_machine_about_iqm_backend_error(qscheduler_iqm, threaded_httpserver):
    qscheduler_iqm.start()
    # Handlers are matched in registration order, so drop the fake's /about handler
    # and replace it with a failing one.
    threaded_httpserver.clear_all_handlers()
    threaded_httpserver.expect_request("/about").respond_with_data("boom", status=503)
    qscheduler_iqm.get_machine_about(expect_error=500)


def _count(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def test_machine_remove_empty(qscheduler_test, db_connect):
    qscheduler_test.start()
    qscheduler_test.stop()
    r = qscheduler_test.remove_machine()
    assert r.returncode == 0, r.stderr
    assert "removed" in r.stdout
    with db_connect.cursor() as cur:
        assert _count(cur, "machines") == 0


def test_machine_remove_unknown(qscheduler_test):
    qscheduler_test.start()
    qscheduler_test.stop()
    r = qscheduler_test.remove_machine("no-such-machine")
    assert r.returncode == 1
    assert "not found" in r.stderr


def test_machine_remove_with_dependents(qscheduler_test, db_connect):
    qscheduler_test.start()
    task_id = qscheduler_test.submit(TT().compute_time(0.3))
    qscheduler_test.wait_for_finished(task_id)
    session_id = qscheduler_test.new_session(10)
    qscheduler_test.wait_for_session_state(session_id, "open")
    qscheduler_test.submit(TT(), session_id=session_id)
    time.sleep(0.3)
    qscheduler_test.cancel_session(session_id)
    qscheduler_test.wait_for_session_state(session_id, "closed")
    qscheduler_test.stop(kill=False)

    with db_connect.cursor() as cur:
        cur.execute("SELECT consumed_ms FROM projects")
        consumed_before = cur.fetchone()[0]
    # Both the task and the session charged the project
    assert consumed_before >= 600

    r = qscheduler_test.remove_machine()
    assert r.returncode == 1
    assert "2 task(s) and 1 session(s)" in r.stderr
    assert "--force" in r.stderr
    assert "not be returned to their projects" in r.stderr
    with db_connect.cursor() as cur:
        assert _count(cur, "machines") == 1
        assert _count(cur, "tasks") == 2
        assert _count(cur, "sessions") == 1

    r = qscheduler_test.remove_machine(force=True)
    assert r.returncode == 0, r.stderr
    assert "2 task(s) and 1 session(s)" in r.stdout
    assert "not returned to their projects" in r.stdout
    with db_connect.cursor() as cur:
        assert _count(cur, "machines") == 0
        assert _count(cur, "tasks") == 0
        assert _count(cur, "sessions") == 0
        # Consumed time is not returned to the project
        cur.execute("SELECT consumed_ms FROM projects")
        assert cur.fetchone()[0] == consumed_before

import json
import time

import requests
from utils import TEST_MACHINE_NAME
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


def _machine_row(cur, name=TEST_MACHINE_NAME):
    cur.execute("SELECT type::text, config::text FROM machines WHERE name = %s", [name])
    machine_type, config = cur.fetchone()
    return machine_type, json.loads(config)


def test_machine_update(qscheduler_test, db_connect):
    qscheduler_test.max_session_time = 60
    qscheduler_test.start()
    qscheduler_test.new_session(time_limit=30)
    qscheduler_test.stop(kill=False)

    r = qscheduler_test.update_machine(
        "--queue-size=7",
        "--max-session-time=10s",
    )
    assert r.returncode == 0, r.stderr
    assert "updated" in r.stdout

    with db_connect.cursor() as cur:
        machine_type, config = _machine_row(cur)
    assert machine_type == "test"
    assert config["name"] == TEST_MACHINE_NAME
    assert config["queue_size"] == 7
    assert config["max_session_time_ms"] == 10_000

    # The service loads the updated configuration on restart
    qscheduler_test.restart()
    qscheduler_test.new_session(time_limit=10)
    qscheduler_test.new_session(time_limit=30, expect_error=422)


def test_machine_update_backend_type(qscheduler_test, db_connect):
    qscheduler_test.start()
    qscheduler_test.stop(kill=False)

    r = qscheduler_test.update_machine(
        backend_args=[
            "iqm",
            "--url=http://127.0.0.1:1",
            "--token=tok",
            "--machine-name=garnet",
            "--poll-interval=1s",
        ]
    )
    assert r.returncode == 0, r.stderr

    with db_connect.cursor() as cur:
        machine_type, config = _machine_row(cur)
    assert machine_type == "iqm"
    assert config["backend"]["type"] == "iqm"
    assert config["backend"]["machine_name"] == "garnet"


def test_machine_update_unknown(qscheduler_test, db_connect):
    qscheduler_test.start()
    qscheduler_test.stop(kill=False)
    with db_connect.cursor() as cur:
        before = _machine_row(cur)

    r = qscheduler_test.update_machine("--queue-size=7", name="no-such-machine")
    assert r.returncode == 1
    assert "not found" in r.stderr
    with db_connect.cursor() as cur:
        assert _machine_row(cur) == before


def test_api_add_machine(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.start()
    info = qs.api_add_machine("second", queue_size=3, max_session_time_ms=5_000).json()
    assert info == {
        "name": "second",
        "queue_size": 3,
        "limit_check_interval_ms": 2_000,
        "max_session_time_ms": 5_000,
        "default_task_time_limit_ms": 3_600_000,
        "max_clock_skew_ms": 4_000,
        "backend": {"type": "test"},
        "suspension": None,
    }
    assert qs.api_get_machine("second") == info
    assert [m["name"] for m in qs.api_list_machines()] == ["TestMachine", "second"]
    with db_connect.cursor() as cur:
        machine_type, config = _machine_row(cur, "second")
    assert machine_type == "test"
    assert config["queue_size"] == 3

    # Usable without a restart
    task_id = qs.submit(TT(), machine="second")
    qs.wait_for_finished(task_id)
    qs.new_session(time_limit=10, machine="second", expect_error=422)

    qs.api_add_machine("second", expect_status=409)
    qs.api_add_machine("third", queue_size=0, expect_status=422)
    qs.api_add_machine("", expect_status=422)

    qs.restart()
    assert qs.api_get_machine("second") == info
    task_id = qs.submit(TT(), machine="second")
    qs.wait_for_finished(task_id)


def test_api_update_machine(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.max_session_time = 60
    qs.start()
    tasks = [qs.submit(TT().compute_time(0.5)) for _ in range(5)]
    qs.wait_for_running(tasks[0])

    info = qs.api_update_machine(queue_size=1, max_session_time_ms=10_000).json()
    assert info["queue_size"] == 1
    assert info["max_session_time_ms"] == 10_000
    # Omitted fields get their defaults
    assert info["default_task_time_limit_ms"] == 3_600_000
    with db_connect.cursor() as cur:
        _, config = _machine_row(cur)
    assert config["queue_size"] == 1
    assert config["max_session_time_ms"] == 10_000

    # Running and queued tasks survive the reconfiguration
    for task_id in tasks:
        qs.wait_for_finished(task_id)
    # The new limits apply right away
    qs.new_session(time_limit=30, expect_error=422)
    session_id = qs.new_session(time_limit=5)
    qs.wait_for_session_open(session_id)

    qs.api_update_machine("no-such-machine", expect_status=404)
    qs.api_update_machine(queue_size=0, expect_status=422)
    qs.api_update_machine(limit_check_interval_ms=0, expect_status=422)


def test_api_update_machine_closes_open_session(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    session_id = qs.new_session(time_limit=30)
    qs.wait_for_session_open(session_id)
    task_id = qs.submit(TT().compute_time(10), session_id=session_id)
    qs.wait_for_running(task_id)

    qs.api_update_machine(queue_size=3)
    qs.wait_for_session_closed(session_id)
    qs.wait_for_cancelled(task_id)
    # The machine keeps working afterwards
    task_id = qs.submit(TT())
    qs.wait_for_finished(task_id)


def test_api_update_machine_backend(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    iqm = {
        "type": "iqm",
        "url": "http://127.0.0.1:1",
        "token": "secret",
        "machine_name": "garnet",
        "poll_interval_ms": 1_000,
    }
    info = qs.api_update_machine(backend=iqm).json()
    assert info["backend"] == {
        "type": "iqm",
        "url": "http://127.0.0.1:1",
        "machine_name": "garnet",
        "poll_interval_ms": 1_000,
    }
    assert "secret" not in json.dumps(qs.api_list_machines())
    qs.get_machine_about(expect_error=500)

    qs.api_update_machine(backend={**iqm, "poll_interval_ms": 0}, expect_status=422)
    qs.api_update_machine()
    assert qs.get_machine_about() == {"type": "test", "version": "1.0"}


def test_api_update_suspended_machine(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.suspend_machine("maintenance")
    info = qs.api_update_machine(queue_size=3).json()
    assert info["suspension"]["message"] == "maintenance"
    qs.submit(TT(), expect_error=409)
    qs.resume_machine()
    task_id = qs.submit(TT())
    qs.wait_for_finished(task_id)


def test_api_remove_machine(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.start()
    qs.api_add_machine("second")
    assert qs.api_remove_machine("second").json() == {"tasks": 0, "sessions": 0}
    qs.api_remove_machine("second", expect_status=404)
    qs.submit(TT(), machine="second", expect_error=422)
    with db_connect.cursor() as cur:
        assert _count(cur, "machines") == 1

    finished = qs.submit(TT())
    qs.wait_for_finished(finished)
    running = qs.submit(TT().compute_time(10))
    qs.wait_for_running(running)
    r = qs.api_remove_machine(expect_status=409)
    assert "2 task(s) and 0 session(s)" in r.text
    # Not removed: the machine still works
    task_id = qs.submit(TT())
    qs.wait_for_finished(task_id)

    assert qs.api_remove_machine(force=True).json() == {"tasks": 3, "sessions": 0}
    with db_connect.cursor() as cur:
        assert _count(cur, "machines") == 0
        assert _count(cur, "tasks") == 0
    qs.get_machine_about(expect_error=404)
    for task_id in (finished, running):
        r = requests.get(qs.url(f"tasks/{task_id}"), timeout=5)
        assert r.status_code == 404, r.text
    assert qs.api_list_machines() == []

    qs.restart()
    assert qs.api_list_machines() == []


def test_api_admin_auth(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.admin_request("GET", "machines", token="wrong", expect_status=401)
    qs.admin_request("GET", "notifiers", token="", expect_status=401)
    qs.admin_request(
        "DELETE", f"machine/{TEST_MACHINE_NAME}", token="wrong", expect_status=401
    )
    assert len(qs.api_list_machines()) == 1

    qs.stop()
    qs.admin_token = None
    qs.restart()
    qs.admin_request("GET", "machines", token="anything", expect_status=403)

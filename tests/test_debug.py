"""The debug endpoint. Its format isn't stable, so these tests only cover what's useful to rely on
while debugging; update them freely when the format changes."""

import time

from utils import TEST_MACHINE_NAME
from utils import TestTask as TT


def _machine(qs, name=TEST_MACHINE_NAME):
    machines = qs.api_debug_scheduler()["machines"]
    return next(m for m in machines if m["name"] == name)


def _user(qs, user):
    users = _machine(qs)["users"]
    return next(u for u in users if u["user"] == user)


def test_debug_scheduler_requires_admin_token(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.admin_request("GET", "debug/scheduler")
    qs.admin_request("GET", "debug/scheduler", token="wrong", expect_status=401)
    qs.stop()
    qs.config(admin_token=None)
    qs.restart()
    qs.admin_request("GET", "debug/scheduler", token="anything", expect_status=403)


def test_debug_scheduler_on_idle_machine(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=3)
    qs.start()
    machine = _machine(qs)
    assert machine["queue_size"] == 3
    assert machine["users"] == []
    assert machine["running_session"] is None
    assert not machine["suspended"]
    assert not machine["unavailable"]


def test_debug_scheduler_reports_usage_and_average(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.wait_for_finished(qs.submit(TT().compute_time(0.2), user="alice"))
    alice = _user(qs, "alice")
    assert alice["usage_s"] >= 0.2
    assert alice["average_task_time_s"] == alice["usage_s"]
    assert alice["tasks_in_backend"] == 0
    assert alice["effective_usage_s"] == alice["usage_s"]
    assert alice["queue"] == []


def test_debug_scheduler_reports_queue_and_tasks_in_backend(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=1)
    qs.start()
    running = qs.submit(TT().compute_time(60), user="alice")
    qs.wait_for_running(running)
    task = qs.submit(TT().compute_time(0.05), user="alice")
    session = qs.new_session(30, owner="alice")
    alice = _user(qs, "alice")
    assert alice["tasks_in_backend"] == 1
    assert alice["average_task_time_s"] is None
    # Priced at the placeholder, as there's no average yet.
    assert alice["effective_usage_s"] > alice["usage_s"]
    assert alice["queue"] == [
        {"type": "task", "id": task},
        {"type": "session", "id": session},
    ]


def test_debug_scheduler_reports_running_session(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    assert _machine(qs)["running_session"] == {"id": session, "queue": []}
    qs.cancel_session(session)
    qs.wait_for_session_closed(session)
    assert _machine(qs)["running_session"] is None
    assert _user(qs, "alice")["usage_s"] > 0


def test_debug_scheduler_reports_suspension(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.suspend_machine("maintenance")
    assert _machine(qs)["suspended"]
    qs.resume_machine()
    assert not _machine(qs)["suspended"]


def test_debug_scheduler_does_not_change_state(qscheduler_test):
    qs = qscheduler_test
    qs.config(decay_half_time="1s")
    qs.start()
    qs.wait_for_finished(qs.submit(TT().compute_time(0.2), user="alice"))
    before = _user(qs, "alice")
    time.sleep(0.2)
    after = _user(qs, "alice")
    assert after["usage_s"] == before["usage_s"]


def test_debug_scheduler_names_user_evicted_from_memory(qscheduler_test):
    qs = qscheduler_test
    qs.config(task_retention="1s")
    qs.start()
    qs.wait_for_finished(qs.submit(TT().compute_time(0.2), user="alice"))
    # Long enough for an eviction pass to drop the idle user from memory, not its usage.
    time.sleep(2.5)
    assert _user(qs, "alice")["usage_s"] > 0

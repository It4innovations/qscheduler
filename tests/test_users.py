import time
from urllib.parse import quote

import requests
from utils import TEST_MACHINE_NAME, TEST_PROJECT
from utils import TestTask as TT


def _wait_consumption(qs, user, expected, timeout=5.0):
    """Consumption is written to the DB asynchronously after a task or a session ends."""
    deadline = time.monotonic() + timeout
    while True:
        consumption = qs.get_user_consumption(user)
        if consumption == expected or time.monotonic() >= deadline:
            assert consumption == expected
            return
        time.sleep(0.1)


def test_list_users(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    assert qs.list_users() == []

    qs.submit(TT(), user="bob")
    qs.submit(TT(), user="alice")
    qs.submit(TT(), user="bob")
    assert qs.list_users() == ["alice", "bob"]

    # A session owner is a user too, unless the session was rejected.
    qs.new_session(time_limit=1, owner="carol")
    qs.new_session(time_limit=1, owner="ghost", project="unknown", expect_error=422)
    qs.new_session(time_limit=1_000_000, owner="ghost", expect_error=422)
    assert qs.list_users() == ["alice", "bob", "carol"]


def test_user_consumption(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.add_project("p2", limit_ms=1_000_000)
    qs.api_add_machine("second")

    tasks = [
        qs.submit(TT().compute_time(0.2), user="alice"),
        qs.submit(TT().compute_time(0.2), user="alice"),
        qs.submit(TT().compute_time(0.2), project="p2", user="alice"),
        qs.submit(TT().compute_time(0.2), machine="second", user="alice"),
    ]
    for t in tasks:
        qs.wait_for_finished(t)
    times = [qs.get_task(t)["exectime_ms"] for t in tasks]

    session_id = qs.new_session(
        time_limit=30, machine="second", project="p2", owner="alice"
    )
    qs.wait_for_session_open(session_id)
    # The task's user is known, but not charged: the session's owner is.
    t_session = qs.submit(
        TT().compute_time(0.2), machine="second", session_id=session_id, user="bob"
    )
    qs.wait_for_finished(t_session)
    qs.cancel_session(session_id)
    qs.wait_for_session_closed(session_id)
    session_ms = qs.get_session(session_id)["exectime_ms"]

    expected = [
        {
            "project": TEST_PROJECT,
            "machine": TEST_MACHINE_NAME,
            "consumed_ms": times[0] + times[1],
        },
        {"project": "p2", "machine": TEST_MACHINE_NAME, "consumed_ms": times[2]},
        {"project": TEST_PROJECT, "machine": "second", "consumed_ms": times[3]},
        {"project": "p2", "machine": "second", "consumed_ms": session_ms},
    ]
    expected.sort(key=lambda e: (e["project"], e["machine"]))
    _wait_consumption(qs, "alice", expected)
    assert qs.get_user_consumption("bob") == []

    # It stays available after a restart.
    qs.restart()
    assert qs.get_user_consumption("alice") == expected
    assert qs.list_users() == ["alice", "bob"]


def test_user_consumption_unknown_user(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    qs.get_user_consumption("nobody", expect_error=404)


def test_user_consumption_name_in_path_is_decoded(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    name = "a b/c"
    task_id = qs.submit(TT().compute_time(0.2), user=name)
    qs.wait_for_finished(task_id)
    assert qs.list_users() == [name]
    _wait_consumption(
        qs,
        name,
        [
            {
                "project": TEST_PROJECT,
                "machine": TEST_MACHINE_NAME,
                "consumed_ms": qs.get_task(task_id)["exectime_ms"],
            }
        ],
    )
    # The encoded form is a different name, so it can't match by accident.
    r = requests.get(qs.url(f"users/{quote(quote(name, safe=''))}/consumption"))
    assert r.status_code == 404


def test_user_active_task_limit(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_tasks=3)
    qs.start()

    tasks = [qs.submit(TT().compute_time(60), user="alice") for _ in range(3)]
    r = qs.submit(TT(), user="alice", expect_error=429)
    assert "alice" in r.text and "3" in r.text

    # The limit is per user.
    other = qs.submit(TT().compute_time(60), user="bob")

    # A task that has ended no longer counts; the rejected submit never did.
    qs.cancel_task(tasks[0])
    qs.wait_for_cancelled(tasks[0])
    last = qs.submit(TT().compute_time(60), user="alice")
    assert last == other + 1
    qs.submit(TT(), user="alice", expect_error=429)


def test_user_active_task_limit_counts_session_tasks(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_tasks=2)
    qs.start()

    session_id = qs.new_session(time_limit=30, owner="alice")
    qs.wait_for_session_open(session_id)
    qs.submit(TT().compute_time(60), session_id=session_id, user="alice")
    qs.submit(TT().compute_time(60), session_id=session_id, user="alice")
    qs.submit(TT(), session_id=session_id, user="alice", expect_error=429)


def test_user_active_task_limit_after_restart(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=1, max_user_active_tasks=3)
    qs.start()

    tasks = [qs.submit(TT().compute_time(60), user="alice") for _ in range(3)]
    qs.restart()

    # The running task may not survive the restart with the test backend, the waiting ones do.
    active = sum(
        qs.get_task_state(t) not in ("finished", "failed", "cancelled") for t in tasks
    )
    assert active >= 2
    for _ in range(3 - active):
        qs.submit(TT().compute_time(60), user="alice")
    qs.submit(TT(), user="alice", expect_error=429)

import time
from urllib.parse import quote

import pytest
import requests
from utils import (
    KIB,
    MIB,
    TEST_MACHINE_NAME,
    TEST_PROJECT,
    padded_payload,
    read_http_status,
)
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


def test_user_active_item_limit(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_items=3)
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


def test_user_active_item_limit_counts_session_tasks(qscheduler_test):
    """The session and the tasks in it are separate items."""
    qs = qscheduler_test
    qs.config(max_user_active_items=3)
    qs.start()

    session_id = qs.new_session(time_limit=30, owner="alice")
    qs.wait_for_session_open(session_id)
    qs.submit(TT().compute_time(60), session_id=session_id, user="alice")
    qs.submit(TT().compute_time(60), session_id=session_id, user="alice")
    qs.submit(TT(), session_id=session_id, user="alice", expect_error=429)


def test_user_active_item_limit_after_restart(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=1, max_user_active_items=3)
    qs.start()

    tasks = [qs.submit(TT().compute_time(60), user="alice") for _ in range(3)]
    qs.restart()

    # The test backend re-runs a resumed task from its payload, so all of them are still active.
    for t in tasks:
        assert qs.get_task_state(t) not in ("finished", "failed", "cancelled")
    qs.submit(TT(), user="alice", expect_error=429)


def _long_task(size=400 * KIB):
    return padded_payload(size, TT().compute_time(60))


def test_user_payload_limit(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_payload="1M")
    qs.start()

    first = qs.submit(None, payload=_long_task(), user="alice")
    qs.submit(None, payload=_long_task(), user="alice")
    r = qs.submit(None, payload=_long_task(), user="alice", expect_error=429)
    assert "alice" in r.text and str(MIB) in r.text
    # What still fits is accepted.
    qs.submit(None, payload=_long_task(100 * KIB), user="alice")

    # The limit is per user.
    qs.submit(None, payload=_long_task(), user="bob")

    # A cancelled task gives its bytes back.
    qs.cancel_task(first)
    qs.wait_for_cancelled(first)
    qs.submit(None, payload=_long_task(), user="alice")
    qs.submit(None, payload=_long_task(), user="alice", expect_error=429)


@pytest.mark.parametrize(
    "task", [TT(), TT().error("fails")], ids=["finished", "failed"]
)
def test_user_payload_limit_released_by_final_state(qscheduler_test, task):
    qs = qscheduler_test
    qs.config(max_user_active_payload="1M")
    qs.start()

    # A payload filling the limit exactly fits, again and again once the previous one ended.
    for _ in range(2):
        t = qs.submit(None, payload=padded_payload(MIB, task))
        qs.wait_for_task_state(t, ("finished", "failed"))


def test_user_payload_over_limit_is_too_large(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_payload="1M")
    qs.start()

    r = qs.submit(None, payload=padded_payload(MIB + 1), expect_error=413)
    assert str(MIB) in r.text
    # A rejected request doesn't create the user.
    assert qs.list_users() == []


def test_user_limits_reject_upload_before_reading_it(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_payload="1M", max_user_active_items=3)
    qs.start()

    # None of these sends a body: without the rejection they would wait for it.
    with qs.submit_headers_only(MIB + 1, user="alice") as sock:
        assert read_http_status(sock) == 413
    assert qs.list_users() == []

    qs.submit(None, payload=_long_task(), user="alice")
    qs.submit(None, payload=_long_task(), user="alice")
    with qs.submit_headers_only(400 * KIB, user="alice") as sock:
        assert read_http_status(sock) == 429

    # An upload that fits waits for its body, and is accepted.
    with qs.submit_headers_only(100 * KIB, user="alice") as sock:
        with pytest.raises(TimeoutError):
            read_http_status(sock, timeout=0.5)
        sock.sendall(_long_task(100 * KIB))
        assert read_http_status(sock) == 201

    # The limit of active tasks is checked up front as well.
    with qs.submit_headers_only(1, user="alice") as sock:
        assert read_http_status(sock) == 429


def test_user_payload_limit_without_content_length(qscheduler_test):
    qs = qscheduler_test
    qs.config(max_user_active_payload="1M")
    qs.start()

    # An iterator is sent chunked, so the size is only known once the body is read.
    qs.submit(None, payload=iter([padded_payload(MIB + 1)]), expect_error=413)
    assert qs.list_users() == []

    qs.submit(None, payload=iter([_long_task()]), user="alice")
    qs.submit(None, payload=iter([_long_task()]), user="alice")
    qs.submit(None, payload=iter([_long_task()]), user="alice", expect_error=429)


def test_user_active_item_limit_counts_sessions(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=1, max_user_active_items=3)
    qs.start()

    # Waiting and open sessions both count.
    opened = qs.new_session(time_limit=60, owner="alice")
    qs.wait_for_session_open(opened)
    waiting = qs.new_session(time_limit=60, owner="alice")
    assert qs.get_session_state(waiting) == "waiting"
    qs.new_session(time_limit=60, owner="alice")

    r = qs.new_session(time_limit=60, owner="alice", expect_error=429)
    assert "alice" in r.text and "3" in r.text
    qs.new_session(time_limit=60, owner="bob")


def test_user_active_items_are_shared_by_tasks_and_sessions(qscheduler_test):
    qs = qscheduler_test
    qs.config(queue_size=1, max_user_active_items=2)
    qs.start()

    # A session doesn't leave room for more tasks than the limit, and vice versa.
    session_id = qs.new_session(time_limit=60, owner="alice")
    qs.submit(TT().compute_time(60), user="alice")
    qs.submit(TT(), user="alice", expect_error=429)
    qs.new_session(time_limit=60, owner="alice", expect_error=429)

    # It is the owner who is counted for the session, and the user for a task in it.
    qs.cancel_session(session_id)
    qs.wait_for_session_closed(session_id)
    qs.new_session(time_limit=60, owner="alice")
    qs.new_session(time_limit=60, owner="alice", expect_error=429)


def _session_cancelled_open(qs):
    session_id = qs.new_session(time_limit=60, owner="alice")
    qs.wait_for_session_open(session_id)
    qs.cancel_session(session_id)
    return session_id


def _session_cancelled_waiting(qs):
    qs.wait_for_running(qs.submit(TT().compute_time(60), user="bob"))
    session_id = qs.new_session(time_limit=60, owner="alice")
    assert qs.get_session_state(session_id) == "waiting"
    qs.cancel_session(session_id)
    return session_id


def _session_expired(qs):
    return qs.new_session(time_limit=1, owner="alice")


def _session_suspended_open(qs):
    session_id = qs.new_session(time_limit=60, owner="alice")
    qs.wait_for_session_open(session_id)
    qs.suspend_machine("maintenance", cancel="all")
    return session_id


def _session_suspended_waiting(qs):
    qs.wait_for_running(qs.submit(TT().compute_time(60), user="bob"))
    session_id = qs.new_session(time_limit=60, owner="alice")
    assert qs.get_session_state(session_id) == "waiting"
    qs.suspend_machine("maintenance", cancel="waiting")
    return session_id


@pytest.mark.parametrize(
    "close",
    [
        _session_cancelled_open,
        _session_cancelled_waiting,
        _session_expired,
        _session_suspended_open,
        _session_suspended_waiting,
    ],
    ids=[
        "cancelled_open",
        "cancelled_waiting",
        "expired",
        "suspended_open",
        "suspended_waiting",
    ],
)
def test_closed_session_releases_its_owner_item(qscheduler_test, close):
    qs = qscheduler_test
    qs.config(queue_size=1, max_user_active_items=1)
    qs.start()

    session_id = close(qs)
    qs.wait_for_session_closed(session_id)
    if close in (_session_suspended_open, _session_suspended_waiting):
        qs.resume_machine()

    # Exactly the one item is free again.
    qs.new_session(time_limit=60, owner="alice")
    qs.new_session(time_limit=60, owner="alice", expect_error=429)


def test_session_owner_stays_counted_past_user_eviction(qscheduler_test):
    """An owner whose only active item is a session stays in memory: if it were evicted, the count
    would be lost, and closing the session would have no user to uncount it from."""
    qs = qscheduler_test
    qs.config(task_retention="1s", max_user_active_items=1)
    qs.start()

    session_id = qs.new_session(time_limit=60, owner="alice")
    qs.wait_for_session_open(session_id)
    time.sleep(3)
    qs.new_session(time_limit=60, owner="alice", expect_error=429)

    qs.cancel_session(session_id)
    qs.wait_for_session_closed(session_id)
    qs.new_session(time_limit=60, owner="alice")

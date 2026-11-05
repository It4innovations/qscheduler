import os
import time

from utils import KIB, TEST_MACHINE_NAME, TEST_PROJECT, padded_payload
from utils import TestTask as TT


def _wait_db_task(cur, task_id, expected_state, timeout=5.0):
    deadline = time.monotonic() + timeout
    row = None
    while time.monotonic() < deadline:
        cur.execute(
            "SELECT state, finished_at, error FROM tasks WHERE id = %s", [task_id]
        )
        row = cur.fetchone()
        if row and row[0] == expected_state:
            return row
        time.sleep(0.1)
    raise TimeoutError(
        f"Task {task_id} not in DB state '{expected_state}' after {timeout}s (last entry: {row})"
    )


def _wait_db_session_opened(cur, session_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur.execute(
            "SELECT opened_at, closed_at FROM sessions WHERE id = %s", [session_id]
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return row
        time.sleep(0.1)
    raise TimeoutError(
        f"Session {session_id} 'opened_at' not set in DB after {timeout}s"
    )


def test_task_inserted_in_db(qscheduler_test, db_connect):
    qscheduler_test.config(queue_size=1)
    qscheduler_test.start()
    task_id = qscheduler_test.submit(TT().compute_time(5))

    with db_connect.cursor() as cur:
        cur.execute("SELECT id, machine_id FROM tasks WHERE id = %s", [task_id])
        row = cur.fetchone()

    assert row is not None
    assert row[0] == task_id
    assert row[1] == 1


def test_task_finished_in_db(qscheduler_test, db_connect):
    qscheduler_test.start()
    task_id = qscheduler_test.submit(TT())
    qscheduler_test.wait_for_finished(task_id)

    with db_connect.cursor() as cur:
        row = _wait_db_task(cur, task_id, "finished")

    assert row[1] is not None  # finished_at
    assert row[2] is None  # no error


def test_task_failed_in_db(qscheduler_test, db_connect):
    qscheduler_test.start()
    task_id = qscheduler_test.submit(TT().error("something went wrong"))
    qscheduler_test.wait_for_failed(task_id)

    with db_connect.cursor() as cur:
        row = _wait_db_task(cur, task_id, "failed")

    assert row[1] is not None  # finished_at
    assert "something went wrong" in row[2]  # error message


def test_session_inserted_in_db(qscheduler_test, db_connect):
    qscheduler_test.start()
    session_id = qscheduler_test.new_session(time_limit=10)

    with db_connect.cursor() as cur:
        cur.execute(
            "SELECT id, machine_id, time_limit_ms FROM sessions WHERE id = %s",
            [session_id],
        )
        row = cur.fetchone()

    assert row is not None
    assert row[0] == session_id
    assert row[1] == 1
    assert row[2] == 10000


def test_session_opened_in_db(qscheduler_test, db_connect):
    qscheduler_test.start()
    session_id = qscheduler_test.new_session(time_limit=10)
    qscheduler_test.wait_for_session_open(session_id)

    with db_connect.cursor() as cur:
        row = _wait_db_session_opened(cur, session_id)

    assert row[0] is not None  # opened_at set
    assert row[1] is None  # closed_at still null


def test_session_closed_with_cancelled_tasks_in_db(qscheduler_test, db_connect):
    # submit_time=3 > session time_limit=1: the backend confirms submission only after
    # the session has already expired, triggering the late-cancellation path in the launcher.
    qscheduler_test.start()
    session_id = qscheduler_test.new_session(time_limit=1)
    qscheduler_test.wait_for_session_open(session_id)
    task_id = qscheduler_test.submit(
        TT().submit_time(3).compute_time(1), session_id=session_id
    )
    qscheduler_test.wait_for_session_closed(session_id)
    qscheduler_test.wait_for_cancelled(task_id)

    with db_connect.cursor() as cur:
        cur.execute("SELECT closed_at FROM sessions WHERE id = %s", [session_id])
        session_row = cur.fetchone()
        task_row = _wait_db_task(cur, task_id, "cancelled")

    assert session_row[0] is not None  # closed_at set
    assert task_row[1] is not None  # finished_at set for cancelled task


def _db_payload_is_null(cur, task_id) -> bool:
    cur.execute("SELECT payload IS NULL FROM tasks WHERE id = %s", [task_id])
    return cur.fetchone()[0]


def test_payload_cleared_when_task_terminal(qscheduler_test, db_connect):
    qscheduler_test.config(queue_size=1)
    qscheduler_test.start()
    t_running = qscheduler_test.submit(TT().compute_time(2))
    t_queued = qscheduler_test.submit(TT())
    t_failed = qscheduler_test.submit(TT().error("boom"))
    qscheduler_test.wait_for_running(t_running)

    with db_connect.cursor() as cur:
        assert not _db_payload_is_null(cur, t_running)
        assert not _db_payload_is_null(cur, t_queued)

    qscheduler_test.cancel_task(t_queued)
    qscheduler_test.wait_for_finished(t_running)
    qscheduler_test.wait_for_failed(t_failed)

    with db_connect.cursor() as cur:
        _wait_db_task(cur, t_running, "finished")
        _wait_db_task(cur, t_queued, "cancelled")
        _wait_db_task(cur, t_failed, "failed")
        assert _db_payload_is_null(cur, t_running)
        assert _db_payload_is_null(cur, t_queued)
        assert _db_payload_is_null(cur, t_failed)


def test_payload_size_kept_after_payload_cleared(qscheduler_test, db_connect):
    qscheduler_test.start()
    task = qscheduler_test.submit(None, payload=padded_payload(3 * KIB))
    qscheduler_test.wait_for_finished(task)

    with db_connect.cursor() as cur:
        _wait_db_task(cur, task, "finished")
        assert _db_payload_is_null(cur, task)
        cur.execute("SELECT payload_size FROM tasks WHERE id = %s", [task])
        assert cur.fetchone()[0] == 3 * KIB


def _db_task_user(cur, task_id):
    cur.execute(
        "SELECT u.id, u.name FROM tasks t JOIN users u ON u.id = t.user_id "
        "WHERE t.id = %s",
        [task_id],
    )
    return cur.fetchone()


def test_task_users_stored_once(qscheduler_test, db_connect):
    qscheduler_test.start()
    t1 = qscheduler_test.submit(TT(), user="alice")
    t2 = qscheduler_test.submit(TT(), user="bob")
    t3 = qscheduler_test.submit(TT(), user="alice")
    for t in (t1, t2, t3):
        qscheduler_test.wait_for_finished(t)

    with db_connect.cursor() as cur:
        u1, u2, u3 = (_db_task_user(cur, t) for t in (t1, t2, t3))
        cur.execute("SELECT count(*) FROM users")
        user_count = cur.fetchone()[0]

    assert u1 == u3
    assert u1[1] == "alice"
    assert u2[1] == "bob"
    assert u1[0] != u2[0]
    assert user_count == 2
    assert qscheduler_test.get_task(t1)["user"] == "alice"
    assert qscheduler_test.get_task(t2)["user"] == "bob"


def test_idle_user_evicted_and_reloaded(qscheduler_test, db_connect):
    qscheduler_test.config(task_retention="1s")
    qscheduler_test.start()
    t1 = qscheduler_test.submit(TT(), user="alice")
    qscheduler_test.wait_for_finished(t1)

    log_path = os.path.join(qscheduler_test.working_dir, "qscheduler.log")
    deadline = time.monotonic() + 5.0
    while True:
        with open(log_path) as f:
            if "Evicted idle users from memory" in f.read():
                break
        assert time.monotonic() < deadline, "user was not evicted"
        time.sleep(0.2)

    assert qscheduler_test.get_task(t1)["user"] == "alice"
    t2 = qscheduler_test.submit(TT(), user="alice")
    qscheduler_test.wait_for_finished(t2)
    assert qscheduler_test.get_task(t2)["user"] == "alice"
    with db_connect.cursor() as cur:
        assert _db_task_user(cur, t1) == _db_task_user(cur, t2)


def _db_user_consumption(cur, name):
    """The user's consumed time as `{(project, machine): ms}`."""
    cur.execute(
        "SELECT p.name, m.name, c.consumed_ms FROM user_consumption c "
        "JOIN users u ON u.id = c.user_id JOIN projects p ON p.id = c.project_id "
        "JOIN machines m ON m.id = c.machine_id WHERE u.name = %s",
        [name],
    )
    return {(project, machine): ms for project, machine, ms in cur.fetchall()}


def _db_user_consumed(cur, name):
    return sum(_db_user_consumption(cur, name).values())


def _db_task_exec_time(cur, task_id):
    cur.execute("SELECT exec_time_ms FROM tasks WHERE id = %s", [task_id])
    return cur.fetchone()[0]


def test_user_consumed_time(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.start()
    t_finished = qs.submit(TT().compute_time(0.3), user="alice")
    t_other_user = qs.submit(TT().compute_time(0.3), user="bob")
    t_failed = qs.submit(TT().compute_time(0.3).error("boom"), user="alice")
    t_no_time = qs.submit(TT(), user="carol")
    qs.wait_for_finished(t_finished)
    qs.wait_for_finished(t_other_user)
    qs.wait_for_failed(t_failed)
    qs.wait_for_finished(t_no_time)

    t_cancelled = qs.submit(TT().compute_time(30), user="alice")
    qs.wait_for_running(t_cancelled)
    time.sleep(0.3)
    qs.cancel_task(t_cancelled)
    qs.wait_for_cancelled(t_cancelled)

    with db_connect.cursor() as cur:
        _wait_db_task(cur, t_finished, "finished")
        _wait_db_task(cur, t_other_user, "finished")
        _wait_db_task(cur, t_failed, "failed")
        _wait_db_task(cur, t_no_time, "finished")
        _wait_db_task(cur, t_cancelled, "cancelled")

        alice_tasks = [t_finished, t_failed, t_cancelled]
        alice_times = [_db_task_exec_time(cur, t) for t in alice_tasks]
        assert all(ms > 0 for ms in alice_times)
        assert _db_user_consumed(cur, "alice") == sum(alice_times)
        assert _db_user_consumed(cur, "bob") == _db_task_exec_time(cur, t_other_user)
        assert _db_user_consumed(cur, "carol") == (
            _db_task_exec_time(cur, t_no_time) or 0
        )


def _wait_db_session_closed(cur, session_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur.execute("SELECT closed_at FROM sessions WHERE id = %s", [session_id])
        if cur.fetchone()[0] is not None:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Session {session_id} not closed in DB after {timeout}s")


def _db_session_and_owner_time(cur, session_id):
    """In one statement, as a checkpoint may land between two queries of an open session."""
    cur.execute(
        "SELECT s.exec_time_ms, c.consumed_ms FROM sessions s "
        "LEFT JOIN user_consumption c ON c.user_id = s.user_id "
        "AND c.project_id = s.project_id AND c.machine_id = s.machine_id "
        "WHERE s.id = %s",
        [session_id],
    )
    return cur.fetchone()


def test_session_time_charged_to_owner_only(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.start()
    session_id = qs.new_session(time_limit=30, owner="dave")
    qs.wait_for_session_open(session_id)
    # Tasks of the owner and of another user: neither is charged on top of the session.
    t_alice = qs.submit(TT().compute_time(0.3), session_id=session_id, user="alice")
    t_dave = qs.submit(TT().compute_time(0.3), session_id=session_id, user="dave")
    qs.wait_for_finished(t_alice)
    qs.wait_for_finished(t_dave)
    qs.cancel_session(session_id)
    qs.wait_for_session_closed(session_id)

    with db_connect.cursor() as cur:
        _wait_db_task(cur, t_alice, "finished")
        _wait_db_task(cur, t_dave, "finished")
        _wait_db_session_closed(cur, session_id)

        assert _db_task_exec_time(cur, t_alice) > 0
        assert _db_task_exec_time(cur, t_dave) > 0
        assert _db_user_consumed(cur, "alice") == 0
        session_ms, owner_ms = _db_session_and_owner_time(cur, session_id)
        assert session_ms > 0
        assert owner_ms == session_ms


def test_session_owner_charged_while_open_and_across_restart(
    qscheduler_test, db_connect
):
    qs = qscheduler_test
    qs.config(limit_check_interval=1)
    qs.start()
    session_id = qs.new_session(time_limit=30, owner="dave")
    qs.wait_for_session_open(session_id)

    with db_connect.cursor() as cur:
        deadline = time.monotonic() + 5.0
        while True:
            session_ms, owner_ms = _db_session_and_owner_time(cur, session_id)
            if session_ms:
                break
            assert time.monotonic() < deadline, "no session checkpoint"
            time.sleep(0.1)
        assert owner_ms == session_ms

    qs.restart()

    with db_connect.cursor() as cur:
        _wait_db_session_closed(cur, session_id)
        session_ms, owner_ms = _db_session_and_owner_time(cur, session_id)
        assert session_ms > 0
        assert owner_ms == session_ms
    assert qs.get_session(session_id)["owner"] == "dave"


def test_user_consumed_time_across_restart(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.start()
    task_id = qs.submit(TT().compute_time(2), user="alice")
    qs.wait_for_running(task_id)

    qs.restart()
    with db_connect.cursor() as cur:
        assert _db_user_consumed(cur, "alice") == 0

    qs.wait_for_finished(task_id)
    with db_connect.cursor() as cur:
        _wait_db_task(cur, task_id, "finished")
        exec_ms = _db_task_exec_time(cur, task_id)
        assert exec_ms > 0
        assert _db_user_consumed(cur, "alice") == exec_ms


def test_user_consumption_per_project_and_machine(qscheduler_test, db_connect):
    qs = qscheduler_test
    qs.start()
    qs.add_project("p2", limit_ms=1_000_000)
    qs.api_add_machine("second")

    # Run one after another on each machine, so the session on "second" opens after t_second.
    t_default = qs.submit(TT().compute_time(0.2), user="alice")
    t_p2 = qs.submit(TT().compute_time(0.2), project="p2", user="alice")
    t_second = qs.submit(TT().compute_time(0.2), machine="second", user="alice")
    t_bob = qs.submit(TT().compute_time(0.2), machine="second", user="bob")
    for t in (t_default, t_p2, t_second, t_bob):
        qs.wait_for_finished(t)

    session_id = qs.new_session(
        time_limit=30, machine="second", project="p2", owner="alice"
    )
    qs.wait_for_session_open(session_id)
    time.sleep(0.2)
    qs.cancel_session(session_id)
    qs.wait_for_session_closed(session_id)

    with db_connect.cursor() as cur:
        for t in (t_default, t_p2, t_second, t_bob):
            _wait_db_task(cur, t, "finished")
        _wait_db_session_closed(cur, session_id)
        session_ms, _ = _db_session_and_owner_time(cur, session_id)
        expected_alice = {
            (TEST_PROJECT, TEST_MACHINE_NAME): _db_task_exec_time(cur, t_default),
            ("p2", TEST_MACHINE_NAME): _db_task_exec_time(cur, t_p2),
            (TEST_PROJECT, "second"): _db_task_exec_time(cur, t_second),
            ("p2", "second"): session_ms,
        }
        expected_bob = {(TEST_PROJECT, "second"): _db_task_exec_time(cur, t_bob)}
        assert all(ms > 0 for ms in expected_alice.values())
        assert _db_user_consumption(cur, "alice") == expected_alice
        assert _db_user_consumption(cur, "bob") == expected_bob

    # Removing the machine removes only its entries, and they don't come back with a new
    # machine of the same name.
    qs.api_remove_machine("second", force=True)
    with db_connect.cursor() as cur:
        cur.execute("SELECT count(*) FROM user_consumption")
        assert cur.fetchone()[0] == 2
        assert _db_user_consumption(cur, "alice") == {
            (TEST_PROJECT, TEST_MACHINE_NAME): expected_alice[
                (TEST_PROJECT, TEST_MACHINE_NAME)
            ],
            ("p2", TEST_MACHINE_NAME): expected_alice[("p2", TEST_MACHINE_NAME)],
        }
        assert _db_user_consumption(cur, "bob") == {}

    qs.api_add_machine("second")
    t_again = qs.submit(TT().compute_time(0.2), machine="second", user="bob")
    qs.wait_for_finished(t_again)
    with db_connect.cursor() as cur:
        _wait_db_task(cur, t_again, "finished")
        assert _db_user_consumption(cur, "bob") == {
            (TEST_PROJECT, "second"): _db_task_exec_time(cur, t_again)
        }

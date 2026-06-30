import time

from utils import TestTask as TT


def _wait_db_task(cur, task_id, expected_state, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur.execute(
            "SELECT state, finished_at, error FROM tasks WHERE id = %s", [task_id]
        )
        row = cur.fetchone()
        if row and row[0] == expected_state:
            return row
        time.sleep(0.1)
    raise TimeoutError(
        f"Task {task_id} not in DB state '{expected_state}' after {timeout}s"
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
    qscheduler_test.queue_size = 1
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

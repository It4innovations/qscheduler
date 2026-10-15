"""Graceful shutdown on SIGTERM.

Each test stops the service with SIGTERM right after changing some state and asserts
the change reached the database before the process exited.
"""

import time

from utils import TEST_PROJECT
from utils import TestTask as TT


def test_shutdown_exits_cleanly(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    assert qs.stop(kill=False) == 0


def test_shutdown_flushes_session_close(qscheduler_test, db_connect):
    """A session reported closed by the API is closed and charged in the DB."""
    qs = qscheduler_test
    qs.start()
    session_id = qs.new_session(10)
    qs.wait_for_session_state(session_id, "open")
    qs.submit(TT(), session_id=session_id)
    time.sleep(0.3)
    qs.cancel_session(session_id)
    qs.wait_for_session_state(session_id, "closed")
    assert qs.stop(kill=False) == 0

    with db_connect.cursor() as cur:
        cur.execute(
            "SELECT closed_at, exec_time_ms FROM sessions WHERE id = %s", [session_id]
        )
        closed_at, exec_time_ms = cur.fetchone()
        cur.execute("SELECT consumed_ms FROM projects WHERE name = %s", [TEST_PROJECT])
        consumed_ms = cur.fetchone()[0]
    assert closed_at is not None
    assert exec_time_ms >= 300
    assert consumed_ms == exec_time_ms


def test_shutdown_checkpoints_open_session(qscheduler_test, db_connect):
    """An open session's time since the last periodic checkpoint is stored on shutdown,
    so after a restart it is closed at shutdown time and fully charged."""
    qs = qscheduler_test
    # No periodic checkpoint happens during the test
    qs.limit_check_interval = 60
    qs.start()
    session_id = qs.new_session(30)
    qs.wait_for_session_state(session_id, "open")
    time.sleep(0.5)
    assert qs.stop(kill=False) == 0

    with db_connect.cursor() as cur:
        cur.execute(
            "SELECT closed_at, updated_at, exec_time_ms FROM sessions WHERE id = %s",
            [session_id],
        )
        closed_at, updated_at, exec_time_ms = cur.fetchone()
    assert closed_at is None
    assert updated_at is not None
    assert exec_time_ms >= 500

    qs.restart()
    assert qs.get_session_state(session_id) == "closed"
    assert qs.get_project(TEST_PROJECT)["consumed_ms"] == exec_time_ms


def test_shutdown_applies_accepted_task_cancel(qscheduler_test):
    """A cancel accepted by the API just before shutdown is not lost: the queued
    task stays cancelled after a restart instead of being run."""
    qs = qscheduler_test
    qs.queue_size = 1
    qs.start()
    running = qs.submit(TT().compute_time(30))
    qs.wait_for_running(running)
    queued = qs.submit(TT())
    qs.cancel_task(queued)
    assert qs.stop(kill=False) == 0

    qs.restart()
    qs.assert_task_canceled(queued)

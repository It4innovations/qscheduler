"""State restoration after a crash/restart.

Each test sets up some state, abruptly kills the service (SIGKILL, so nothing is
flushed on shutdown), then starts a fresh process against the same database and
asserts the state was rebuilt from the database.
"""

import time
from datetime import datetime

from utils import TestTask as TT


def test_restore_running_task(qscheduler_test):
    """A task already submitted to the backend is re-attached and finishes."""
    qs = qscheduler_test
    qs.start()

    task_id = qs.submit(TT().compute_time(3))
    qs.wait_for_running(task_id)

    qs.restart()

    # On restart the task has a backend_id, so it is re-attached to the backend;
    # the test backend re-runs it from its payload and it finishes.
    qs.wait_for_finished(task_id)


def test_restore_waiting_task(qscheduler_test):
    """A task still waiting in the queue is re-queued and runs after a restart."""
    qs = qscheduler_test
    qs.queue_size = 1
    qs.start()

    # t1 occupies the only slot; t2 stays queued and is never sent to the backend.
    t1 = qs.submit(TT().compute_time(3))
    qs.wait_for_running(t1)
    t2 = qs.submit(TT())
    assert qs.get_task_state(t2) == "waiting"

    qs.restart()

    # t1 (running) is re-attached, t2 (waiting) is re-queued; both complete.
    qs.wait_for_finished(t2)
    qs.wait_for_finished(t1)


def test_restore_closes_running_session(qscheduler_test):
    """An open session is closed and all of its tasks are cancelled on restart."""
    qs = qscheduler_test
    qs.start()

    session_id = qs.new_session(time_limit=30)
    qs.wait_for_session_open(session_id)
    task_id = qs.submit(TT().compute_time(30), session_id=session_id)
    qs.wait_for_running(task_id)

    qs.restart()

    qs.wait_for_session_closed(session_id)
    qs.wait_for_cancelled(task_id)


def test_restore_waiting_session(qscheduler_test):
    """A session queued but never opened is re-queued and opens after a restart."""
    qs = qscheduler_test
    qs.queue_size = 1
    qs.start()

    # Keep the machine busy so the session stays queued (waiting), not opened.
    t1 = qs.submit(TT().compute_time(2))
    qs.wait_for_running(t1)
    session_id = qs.new_session(time_limit=30)
    assert qs.get_session_state(session_id) == "waiting"

    qs.restart()

    # Once the restored task frees the slot, the restored session opens.
    qs.wait_for_session_open(session_id)


def test_restore_finished_task_reachable_via_db(qscheduler_test):
    """A finished task is evicted from Core's in-memory state across a restart
    (Core::restore only reloads non-terminal rows), but GET /tasks/{id} still returns
    it via the DB fallback."""
    qs = qscheduler_test
    qs.start()

    task_id = qs.submit(TT())
    qs.wait_for_finished(task_id)
    before = qs.get_task(task_id)

    qs.restart()

    after = qs.get_task(task_id)
    assert after["state"] == "finished"
    assert after["id"] == before["id"]
    assert after["user"] == before["user"]
    assert after["machine"] == before["machine"]
    assert after["project"] == before["project"]
    assert after["finished_at"] is not None


def test_restore_dangling_session_closed_at_last_checkpoint(qscheduler_test):
    """A session that crashes while open, after several periodic checkpoints have
    already landed in the DB, is force-closed on restart using the *last checkpoint's*
    timestamp — not its original opened_at (proving the checkpoint's updated_at is
    actually used, not just a fallback that happens to coincide with it), and not the
    restart time (which would overstate how long it was open by however long the
    process was down)."""
    qs = qscheduler_test
    qs.session_check_interval = 1
    qs.start()

    session_id = qs.new_session(time_limit=60)
    qs.wait_for_session_open(session_id)

    # Let several 1s checkpoints land so the DB's last update is well past opened_at.
    time.sleep(3.5)
    before_kill = time.time()

    # Simulate a crash, then wait a couple of seconds before restarting so the last
    # checkpoint and the restart time are clearly distinguishable.
    qs.stop(kill=True)
    time.sleep(2)
    qs._start_binary(log_name="qscheduler-restart.log")

    info = qs.get_session(session_id)
    assert info["state"] == "closed"
    opened_at = datetime.fromisoformat(info["opened_at"]).timestamp()
    closed_at = datetime.fromisoformat(info["closed_at"]).timestamp()

    assert closed_at - opened_at > 2.5, (
        f"closed_at ({closed_at}) should reflect a later checkpoint, not just fall "
        f"back to opened_at ({opened_at})"
    )
    assert abs(closed_at - before_kill) < 1.5, (
        f"closed_at ({closed_at}) should be near the last checkpoint / kill time "
        f"({before_kill}), not inflated by the delay before restart"
    )


def test_restore_closed_session_reachable_via_db(qscheduler_test):
    """A closed session not referenced by any active task is evicted from Core's
    in-memory state across a restart, but GET /sessions/{id} still returns it via the
    DB fallback."""
    qs = qscheduler_test
    qs.start()

    session_id = qs.new_session(time_limit=1)
    qs.wait_for_session_open(session_id)
    qs.wait_for_session_closed(session_id)
    before = qs.get_session(session_id)

    qs.restart()

    after = qs.get_session(session_id)
    assert after["state"] == "closed"
    assert after["machine"] == before["machine"]
    assert after["project"] == before["project"]
    assert after["closed_at"] is not None

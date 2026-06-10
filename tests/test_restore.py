"""State restoration after a crash/restart.

Each test sets up some state, abruptly kills the service (SIGKILL, so nothing is
flushed on shutdown), then starts a fresh process against the same database and
asserts the state was rebuilt from the database.
"""

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
    assert qs.get_task_status(t2)["state"] == "waiting"

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

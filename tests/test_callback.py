import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils import TestTask as TT


class CallbackHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        data = json.loads(self.rfile.read(length))
        self.server.callbacks.append(data)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


def start_callback_server() -> HTTPServer:
    server = HTTPServer(("127.0.0.1", 0), CallbackHandler)
    server.callbacks = []
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()
    return server


@pytest.fixture
def make_callback_server():
    """Factory for callback servers; all of them are shut down at teardown."""
    servers = []

    def make() -> HTTPServer:
        server = start_callback_server()
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.shutdown()


@pytest.fixture
def callback_server(make_callback_server):
    return make_callback_server()


NOTIFY_TOKEN = "test-secret-token"
NOTIFIER = "cb"


def callback_url(server) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}/callback"


def notifier_rows(r) -> list[list[str]]:
    """Rows of `notifier list` output, header included."""
    assert r.returncode == 0, r.stderr
    return [line.split() for line in r.stdout.splitlines()]


def wait_for_callbacks(server, count=1, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len(server.callbacks) < count:
        time.sleep(0.05)
    assert len(server.callbacks) >= count, (
        f"expected {count} callbacks, got {len(server.callbacks)} within {timeout}s"
    )


@pytest.fixture
def notify_qs(qscheduler, callback_server):
    qscheduler.notifiers = [(NOTIFIER, callback_url(callback_server), NOTIFY_TOKEN)]
    return qscheduler


def test_callback_fires_on_task_finish(notify_qs, callback_server):
    notify_qs.start()
    task_id = notify_qs.submit(TT(), notify=NOTIFIER)
    notify_qs.wait_for_finished(task_id)

    wait_for_callbacks(callback_server)
    cb = callback_server.callbacks[0]
    assert cb["task"]["id"] == task_id
    assert cb["task"]["state"] == "finished"
    assert cb["token"] == NOTIFY_TOKEN


def test_callback_fires_on_task_fail(notify_qs, callback_server):
    notify_qs.start()
    task_id = notify_qs.submit(TT().error("something went wrong"), notify=NOTIFIER)
    notify_qs.wait_for_failed(task_id)

    wait_for_callbacks(callback_server)
    cb = callback_server.callbacks[0]
    assert cb["task"]["id"] == task_id
    assert cb["task"]["state"] == "failed"
    assert cb["task"]["error"] == "something went wrong"
    assert cb["token"] == NOTIFY_TOKEN


def test_callback_fires_on_task_cancel(notify_qs, callback_server):
    notify_qs.queue_size = 1
    notify_qs.start()
    session_id = notify_qs.new_session(time_limit=2, notify=NOTIFIER)
    notify_qs.wait_for_session_open(session_id)
    time.sleep(0.3)
    t1 = notify_qs.submit(TT().compute_time(1), session_id=session_id, notify=NOTIFIER)
    t2 = notify_qs.submit(TT().compute_time(1), session_id=session_id, notify=NOTIFIER)
    notify_qs.wait_for_finished(t1)
    notify_qs.wait_for_cancelled(t2)
    time.sleep(0.3)
    wait_for_callbacks(callback_server, count=3)
    states = {
        cb["task"]["id"]: cb["task"]["state"]
        for cb in callback_server.callbacks
        if "task" in cb
    }
    assert states[t1] == "finished"
    assert states[t2] == "cancelled"


def test_callback_task_matches_get_task(notify_qs, callback_server):
    """The `task` object embedded in the callback is identical to what GET /tasks/{id}
    returns right after — both build a TaskInfo from the same (by then frozen,
    terminal) task state."""
    notify_qs.start()
    task_id = notify_qs.submit(TT(), notify=NOTIFIER)
    notify_qs.wait_for_finished(task_id)

    wait_for_callbacks(callback_server)
    cb_task = callback_server.callbacks[0]["task"]
    assert cb_task == notify_qs.get_task(task_id)
    assert cb_task["notify"] == NOTIFIER


def test_get_task_notify(qscheduler_test, callback_server):
    """`notify` is reported by GET /tasks/{id}, both from memory and (after a restart evicts
    the terminal task) from the DB."""
    qs = qscheduler_test
    qs.notifiers = [(NOTIFIER, callback_url(callback_server), None)]
    qs.start()
    t1 = qs.submit(TT(), notify=NOTIFIER)
    t2 = qs.submit(TT())
    qs.wait_for_finished(t1)
    qs.wait_for_finished(t2)
    assert qs.get_task(t1)["notify"] == NOTIFIER
    assert "notify" not in qs.get_task(t2)
    qs.restart()
    assert qs.get_task(t1)["notify"] == NOTIFIER
    assert "notify" not in qs.get_task(t2)


def test_callback_fires_on_session_open_and_close(notify_qs, callback_server):
    notify_qs.start()
    session_id = notify_qs.new_session(time_limit=1, notify=NOTIFIER)
    notify_qs.wait_for_session_open(session_id)
    notify_qs.wait_for_session_closed(session_id)

    wait_for_callbacks(callback_server, count=2)
    session_cbs = [cb for cb in callback_server.callbacks if "session" in cb]
    assert len(session_cbs) == 2
    for cb in session_cbs:
        assert cb["session"]["id"] == session_id
        assert cb["token"] == NOTIFY_TOKEN
    states = [cb["session"]["state"] for cb in session_cbs]
    assert states == ["open", "closed"]


def test_callback_fires_on_queued_session_cancel(notify_qs, callback_server):
    notify_qs.queue_size = 1
    notify_qs.start()
    session1 = notify_qs.new_session(time_limit=2, notify=NOTIFIER)
    notify_qs.wait_for_session_open(session1)
    session2 = notify_qs.new_session(time_limit=2, notify=NOTIFIER)

    notify_qs.cancel_session(session2)
    notify_qs.wait_for_session_state(session2, "closed")
    notify_qs.cancel_session(session1)
    notify_qs.wait_for_session_state(session1, "closed")

    wait_for_callbacks(callback_server, count=3)
    session_cbs = {
        (cb["session"]["id"], cb["session"]["state"])
        for cb in callback_server.callbacks
        if "session" in cb
    }
    assert (session1, "open") in session_cbs
    assert (session1, "closed") in session_cbs
    assert (session2, "open") not in session_cbs
    assert (session2, "closed") in session_cbs


def test_callback_session_matches_get_session(notify_qs, callback_server):
    """The `session` object embedded in each callback is identical to what
    GET /sessions/{id} returns right after.

    For the "open" event, `exectime_ms` is excluded from the comparison: it's
    checkpointed periodically while the session stays open, so a GET issued after the
    callback was received can legitimately show a larger value. The "closed" event has
    no such caveat — a closed session's fields are frozen — so it's compared in full.
    """
    notify_qs.start()
    session_id = notify_qs.new_session(time_limit=5, notify=NOTIFIER)
    notify_qs.wait_for_session_open(session_id)

    wait_for_callbacks(callback_server, count=1)
    open_cb = callback_server.callbacks[0]["session"]
    fetched_open = notify_qs.get_session(session_id)
    assert open_cb["state"] == "open"
    assert {k: v for k, v in open_cb.items() if k != "exectime_ms"} == {
        k: v for k, v in fetched_open.items() if k != "exectime_ms"
    }

    notify_qs.wait_for_session_closed(session_id)
    wait_for_callbacks(callback_server, count=2)
    closed_cb = callback_server.callbacks[1]["session"]
    assert closed_cb["state"] == "closed"
    assert closed_cb == notify_qs.get_session(session_id)
    assert open_cb["notify"] == NOTIFIER
    assert closed_cb["notify"] == NOTIFIER


def test_no_callback_without_notify(notify_qs, callback_server):
    notify_qs.start()
    session_id = notify_qs.new_session(time_limit=1)
    task_id = notify_qs.submit(TT())
    notify_qs.wait_for_finished(task_id)
    notify_qs.wait_for_session_closed(session_id)
    # A task with a notifier, submitted last, proves the worker was running meanwhile
    t2 = notify_qs.submit(TT(), notify=NOTIFIER)
    notify_qs.wait_for_finished(t2)

    wait_for_callbacks(callback_server)
    time.sleep(0.3)
    assert [cb["task"]["id"] for cb in callback_server.callbacks] == [t2]


def test_unknown_notifier_rejected(notify_qs, callback_server):
    notify_qs.start()
    notify_qs.submit(TT(), notify="nope", expect_error=422)
    notify_qs.new_session(time_limit=1, notify="nope", expect_error=422)


def test_callbacks_routed_per_notifier(
    qscheduler_test, callback_server, make_callback_server
):
    callback_server2 = make_callback_server()
    qs = qscheduler_test
    qs.notifiers = [
        ("first", callback_url(callback_server), "t1"),
        ("second", callback_url(callback_server2), None),
    ]
    qs.start()
    t1 = qs.submit(TT(), notify="first")
    t2 = qs.submit(TT(), notify="second")
    session_id = qs.new_session(time_limit=1, notify="second")
    qs.wait_for_finished(t1)
    qs.wait_for_finished(t2)
    qs.wait_for_session_closed(session_id)

    wait_for_callbacks(callback_server, count=1)
    wait_for_callbacks(callback_server2, count=3)
    time.sleep(0.3)
    assert [cb["task"]["id"] for cb in callback_server.callbacks] == [t1]
    assert callback_server.callbacks[0]["token"] == "t1"
    assert [cb["task"]["id"] for cb in callback_server2.callbacks if "task" in cb] == [
        t2
    ]
    assert [
        cb["session"]["state"] for cb in callback_server2.callbacks if "session" in cb
    ] == ["open", "closed"]
    assert all("token" not in cb for cb in callback_server2.callbacks)


def test_callback_after_restart(qscheduler_test, callback_server):
    """The task's notifier is persisted: a task running across a restart still notifies."""
    qs = qscheduler_test
    qs.notifiers = [(NOTIFIER, callback_url(callback_server), NOTIFY_TOKEN)]
    qs.start()
    task_id = qs.submit(TT().compute_time(1.5), notify=NOTIFIER)
    qs.wait_for_running(task_id)
    qs.restart()
    qs.wait_for_finished(task_id)

    wait_for_callbacks(callback_server)
    assert callback_server.callbacks[0]["task"]["id"] == task_id


def test_notifier_cli(qscheduler_test, callback_server, db_connect):
    qs = qscheduler_test
    url = callback_url(callback_server)
    qs.notifiers = [(NOTIFIER, url, None)]
    qs.start()

    r = qs.add_notifier(NOTIFIER, url)
    assert r.returncode != 0
    assert "already exists" in r.stderr
    r = qs.add_notifier("x" * 13, url)
    assert r.returncode != 0
    assert "at most" in r.stderr or "1 to 12" in r.stderr
    assert qs.add_notifier("x" * 12, url).returncode == 0
    assert notifier_rows(qs.list_notifiers()) == [
        ["NAME", "TOKEN", "URL"],
        [NOTIFIER, "no", url],
        ["x" * 12, "no", url],
    ]
    r = qs.remove_notifier("missing")
    assert r.returncode != 0
    assert "not found" in r.stderr

    # A notifier added while running is unknown until a restart
    qs.submit(TT(), notify="x" * 12, expect_error=422)

    task_id = qs.submit(TT(), notify=NOTIFIER)
    qs.wait_for_finished(task_id)
    r = qs.remove_notifier(NOTIFIER)
    assert r.returncode == 0, r.stderr
    with db_connect.cursor() as cur:
        cur.execute("SELECT notifier_id FROM tasks WHERE id = %s", [task_id])
        assert cur.fetchone()[0] is None

    assert [row[0] for row in notifier_rows(qs.list_notifiers())] == ["NAME", "x" * 12]

    qs.restart()
    # The removed notifier is no longer reported for the task
    assert "notify" not in qs.get_task(task_id)
    qs.submit(TT(), notify=NOTIFIER, expect_error=422)
    t2 = qs.submit(TT(), notify="x" * 12)
    qs.wait_for_finished(t2)


def test_get_session_notify(qscheduler_test, callback_server):
    """`notify` is reported by GET /sessions/{id}, both from memory and (after a restart
    evicts the closed session) from the DB."""
    qs = qscheduler_test
    qs.notifiers = [(NOTIFIER, callback_url(callback_server), None)]
    qs.start()
    s1 = qs.new_session(time_limit=1, notify=NOTIFIER)
    s2 = qs.new_session(time_limit=1)
    qs.wait_for_session_closed(s1)
    qs.wait_for_session_closed(s2)
    assert qs.get_session(s1)["notify"] == NOTIFIER
    assert "notify" not in qs.get_session(s2)
    qs.restart()
    assert qs.get_session(s1)["notify"] == NOTIFIER
    assert "notify" not in qs.get_session(s2)

    # Removing the notifier clears it (once the service restarts)
    assert qs.remove_notifier(NOTIFIER).returncode == 0
    qs.restart()
    assert "notify" not in qs.get_session(s1)

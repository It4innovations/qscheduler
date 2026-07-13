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


@pytest.fixture
def callback_server():
    server = HTTPServer(("127.0.0.1", 0), CallbackHandler)
    server.callbacks = []
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()
    yield server
    server.shutdown()


NOTIFY_TOKEN = "test-secret-token"


def wait_for_callbacks(server, count=1, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len(server.callbacks) < count:
        time.sleep(0.05)
    assert len(server.callbacks) >= count, (
        f"expected {count} callbacks, got {len(server.callbacks)} within {timeout}s"
    )


@pytest.fixture
def notify_qs(qscheduler, callback_server):
    cb_port = callback_server.server_address[1]
    qscheduler.notify_url = f"http://127.0.0.1:{cb_port}/callback"
    qscheduler.notify_token = NOTIFY_TOKEN
    return qscheduler


def test_callback_fires_on_task_finish(notify_qs, callback_server):
    notify_qs.start()
    task_id = notify_qs.submit(TT())
    notify_qs.wait_for_finished(task_id)

    wait_for_callbacks(callback_server)
    cb = callback_server.callbacks[0]
    assert cb["task"]["id"] == task_id
    assert cb["task"]["state"] == "finished"
    assert cb["token"] == NOTIFY_TOKEN


def test_callback_fires_on_task_fail(notify_qs, callback_server):
    notify_qs.start()
    task_id = notify_qs.submit(TT().error("something went wrong"))
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
    session_id = notify_qs.new_session(time_limit=2)
    notify_qs.wait_for_session_open(session_id)
    time.sleep(0.3)
    t1 = notify_qs.submit(TT().compute_time(1), session_id=session_id)
    t2 = notify_qs.submit(TT().compute_time(1), session_id=session_id)
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


def test_callback_fires_on_session_open_and_close(notify_qs, callback_server):
    notify_qs.start()
    session_id = notify_qs.new_session(time_limit=1)
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
    session1 = notify_qs.new_session(time_limit=2)
    notify_qs.wait_for_session_open(session1)
    session2 = notify_qs.new_session(time_limit=2)

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

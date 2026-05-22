import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils import QScheduler, TestTask as TT


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
def qscheduler_notify(tmp_path, free_port, callback_server):
    cb_port = callback_server.server_address[1]
    qs = QScheduler(
        str(tmp_path),
        free_port,
        notify_url=f"http://127.0.0.1:{cb_port}/callback",
        notify_token=NOTIFY_TOKEN,
    )
    qs.start()
    yield qs
    qs.cleanup()


def test_callback_fires_on_task_finish(qscheduler_notify, callback_server):
    task_id = qscheduler_notify.submit(TT())
    qscheduler_notify.wait_for_finished(task_id)

    wait_for_callbacks(callback_server)
    cb = callback_server.callbacks[0]
    assert cb["event"]["task_id"] == task_id
    assert cb["event"]["state"] == "finished"
    assert cb["token"] == NOTIFY_TOKEN


def test_callback_fires_on_task_fail(qscheduler_notify, callback_server):
    task_id = qscheduler_notify.submit(TT().error("something went wrong"))
    qscheduler_notify.wait_for_failed(task_id)

    wait_for_callbacks(callback_server)
    cb = callback_server.callbacks[0]
    assert cb["event"]["task_id"] == task_id
    assert cb["event"]["state"] == "failed"
    assert cb["token"] == NOTIFY_TOKEN


def test_callback_fires_on_task_cancel(qscheduler_notify, callback_server):
    session_id = qscheduler_notify.new_session(time_limit=1)
    qscheduler_notify.wait_for_session_open(session_id)
    time.sleep(0.2)
    t1 = qscheduler_notify.submit(TT().wait(1), session_id=session_id)
    t2 = qscheduler_notify.submit(TT(), session_id=session_id)

    qscheduler_notify.wait_for_finished(t1)
    qscheduler_notify.assert_task_canceled(t2)

    wait_for_callbacks(callback_server, count=2)
    states = {
        cb["event"]["task_id"]: cb["event"]["state"] for cb in callback_server.callbacks
    }
    assert states[t1] == "finished"
    assert states[t2] == "cancelled"

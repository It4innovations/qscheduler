#!/usr/bin/env python3
"""Mock backend server: spawns per-task worker servers on demand."""

import logging
import threading

from flask import Flask, request
from werkzeug.serving import make_server

logging.getLogger("werkzeug").setLevel(logging.ERROR)


class Worker:

    def __init__(self):
        pass

    def set_task(self, body: bytes):
        print(f"[worker] task received: {len(body)} bytes")
        print("[worker] compiling ...")
        print("[worker] ... compiled")

    def start_task(self):
        print("[worker] task started")

    def delete_task(self):
        print("[worker] task deleted")


def make_worker_app(worker: Worker) -> Flask:
    app = Flask(__name__)

    @app.post("/task")
    def accept_task():
        body = request.get_data()
        worker.set_task(body)
        return "", 200

    @app.post("/task/start")
    def start_task():
        worker.start_task()
        return "", 200

    @app.delete("/task")
    def delete_task():
        worker.delete_task()
        return "", 200

    return app


main_app = Flask(__name__)


@main_app.post("/worker")
def new_worker():
    worker = Worker()
    app = make_worker_app(worker)
    server = make_server("localhost", 0, app)
    port = server.socket.getsockname()[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://localhost:{port}"
    print(f"[main] worker started at {url}")
    return url, 200


if __name__ == "__main__":
    print("listening on http://localhost:8080")
    main_app.run(port=8080, use_reloader=False)

import uuid
import json
import time
from datetime import datetime, timedelta
from werkzeug.wrappers import Response


def make_result(obj):
    return Response(
        json.dumps(obj),
        content_type="application/json",
    )


class Task:
    def __init__(self, config, submit_time):
        self.config = config
        self.submit_time = submit_time
        self.cancelled = False

    def cancel(self):
        self.cancelled = True

    def status(self):
        if self.cancelled:
            return {"status": "cancelled"}
        compute_time = self.config.get("compute_time")
        if compute_time is not None:
            now = datetime.now()
            if now < self.submit_time + timedelta(seconds=compute_time):
                return {"status": "processing"}
        r = self.config["result"]
        if r["type"] == "Ok":
            return {"status": "completed"}
        else:
            return {"status": "failed", "errors": [{"message": r["message"]}]}


class IqmFakeBackend:
    def __init__(self, httpserver):
        self.httpserver = httpserver
        self.machine_name = "start24"
        self.token = "test-token-1234"

    def _check_auth(self, request):
        expected = f"Bearer {self.token}"
        return request.headers.get("Authorization") == expected

    def _auth_error(self):
        return Response("Unauthorized", status=401)

    def start(self):
        self.httpserver.expect_request(
            f"/api/v1/jobs/{self.machine_name}/circuit",
            method="POST",
        ).respond_with_handler(self._handle_submit)

    def _handle_submit(self, request):
        if not self._check_auth(request):
            return self._auth_error()
        config = request.json
        submit_time = config.get("submit_time")
        if submit_time is not None:
            time.sleep(submit_time)
        task_id = str(uuid.uuid4())
        task = Task(config, datetime.now())

        self.httpserver.expect_request(
            f"/api/v1/jobs/{task_id}",
            method="GET",
        ).respond_with_handler(
            lambda request: self._auth_error() if not self._check_auth(request) else make_result(task.status())
        )

        self.httpserver.expect_request(
            f"/api/v1/jobs/{task_id}/cancel",
            method="POST",
        ).respond_with_handler(
            lambda request: self._auth_error() if not self._check_auth(request) else make_result(task.cancel() or {})
        )

        return make_result({"id": task_id})

    def build_config(self):
        return f"""
type = "iqm"
url = "http://127.0.0.1:{self.httpserver.port}"
machine_name = "{self.machine_name}"
token = "{self.token}"
check_interval_ms = 200
        """.strip()

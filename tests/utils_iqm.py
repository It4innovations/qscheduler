import re
import uuid
import json
import time
from datetime import datetime, timedelta
from werkzeug.wrappers import Response

_QC_ID = "00000000-0000-0000-0000-000000000000"
_CALIBRATION_SET_ID = "00000000-0000-0000-0000-000000000001"

# Cumulative seconds for each compilation step (total ~0.5 s).
_COMPILE_STEPS = [
    ("iqm-server", "created", 0),
    ("iqm-station-control", "received", 0.14),
    ("iqm-station-control", "validation_started", 0.027),
    ("iqm-station-control", "validation_ended", 0.001),
    ("iqm-station-control", "compilation_started", 0.013),
    ("iqm-station-control", "compilation_ended", 0.316),
]
_COMPILE_DURATION = sum(d for _, _, d in _COMPILE_STEPS)


def make_result(obj):
    return Response(
        json.dumps(obj),
        content_type="application/json",
    )


def _fmt_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _build_timeline(start, steps):
    """Build IQM timeline entries from (source, status, delta_seconds) tuples.
    Timestamps are accumulated from start; delta=0 means same instant as start."""
    entries = []
    t = start
    for source, status, delta in steps:
        t += timedelta(seconds=delta)
        entries.append({"source": source, "status": status, "timestamp": _fmt_ts(t)})
    return entries


class Task:
    def __init__(self, config, created_at, task_id):
        self.config = config
        self.created_at = created_at
        # submit_time in config = compilation phase before execution starts.
        self.pre_exec_secs = config.get("submit_time", 0)
        self.compute_time = config.get("compute_time")
        self.cancelled = False
        self.cancelled_at = None
        self.task_id = task_id

    def _exec_start(self):
        return self.created_at + timedelta(seconds=self.pre_exec_secs)

    def cancel(self):
        self.cancelled = True
        self.cancelled_at = datetime.now()

    def status(self):
        if self.cancelled:
            exec_start = self._exec_start()
            if self.cancelled_at <= exec_start:
                # Cancelled during compilation — execution never started.
                timeline = [
                    {
                        "source": "iqm-server",
                        "status": "created",
                        "timestamp": _fmt_ts(self.created_at),
                    },
                    {
                        "source": "iqm-server",
                        "status": "cancelled",
                        "timestamp": _fmt_ts(self.cancelled_at),
                    },
                ]
            else:
                # Cancelled after execution started — include full compile pipeline.
                if self.pre_exec_secs > _COMPILE_DURATION:
                    timeline = _build_timeline(self.created_at, _COMPILE_STEPS)
                else:
                    timeline = [
                        {
                            "source": "iqm-server",
                            "status": "created",
                            "timestamp": _fmt_ts(self.created_at),
                        }
                    ]
                timeline += [
                    {
                        "source": "iqm-station-control",
                        "status": "execution_started",
                        "timestamp": _fmt_ts(exec_start),
                    },
                    {
                        "source": "iqm-station-control",
                        "status": "cancelled",
                        "timestamp": _fmt_ts(self.cancelled_at),
                    },
                ]
            return {
                "id": self.task_id,
                "type": "circuit",
                "qc": {"id": _QC_ID},
                "status": "cancelled",
                "timeline": timeline,
                "artifacts": [],
                "messages": [],
            }

        now = datetime.now()
        exec_start = self._exec_start()

        if now < exec_start:
            return {"status": "waiting"}

        if self.compute_time is not None and now < exec_start + timedelta(
            seconds=self.compute_time
        ):
            return {"status": "processing"}

        r = self.config["outcome"]
        if r["type"] == "Ok":
            exec_s = self.compute_time if self.compute_time is not None else 0.5
            exec_ms = int(exec_s * 1000)
            timeline = _build_timeline(
                self.created_at,
                [
                    ("iqm-server", "created", 0),
                    ("iqm-station-control", "received", 0.14),
                    ("iqm-station-control", "validation_started", 0.027),
                    ("iqm-station-control", "validation_ended", 0.001),
                    ("iqm-station-control", "compilation_started", 0.013),
                    ("iqm-station-control", "compilation_ended", 0.316),
                    ("iqm-station-control", "execution_started", 4.82),
                    ("iqm-station-control", "execution_ended", exec_s),
                    ("iqm-station-control", "post_processing_started", 0.001),
                    ("iqm-station-control", "post_processing_ended", 0.05),
                    ("iqm-server", "ready", 0.01),
                    ("iqm-server", "completed", 0.001),
                ],
            )
            doc = {
                "id": self.task_id,
                "type": "circuit",
                "qc": {"id": _QC_ID},
                "status": "completed",
                "compilation": {
                    "execution_runtime_estimate_ms": exec_ms * 3,
                    "calibration_set_id": _CALIBRATION_SET_ID,
                },
                "execution": {"progress": {}},
                "runtime_ms": exec_ms,
                "timeline": timeline,
                "artifacts": [
                    {"type": "runtime_estimates"},
                    {"type": "measurements"},
                    {"type": "measurement_counts"},
                ],
                "messages": [],
            }
        else:
            doc = {"status": "failed", "errors": [{"message": r["message"]}]}
        if "result" in self.config:
            doc["result"] = self.config["result"]
        return doc


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
        task_id = str(uuid.uuid4())
        # Record created_at before the submit delay so the compilation window
        # is anchored to when the job arrived, not when it was accepted.
        task = Task(config, datetime.now(), task_id)

        self.httpserver.expect_request(
            f"/api/v1/jobs/{task_id}",
            method="GET",
        ).respond_with_handler(
            lambda request: (
                self._auth_error()
                if not self._check_auth(request)
                else make_result(task.status())
            )
        )

        self.httpserver.expect_request(
            f"/api/v1/jobs/{task_id}/cancel",
            method="POST",
        ).respond_with_handler(
            lambda request: (
                self._auth_error()
                if not self._check_auth(request)
                else make_result(task.cancel() or {})
            )
        )

        artifacts = config.get("artifacts", {})
        # "measurements" has a default payload so tests that don't set custom
        # artifacts still get a fetchable one.
        for name in {"measurements", *artifacts}:

            def handle_artifact(request, name=name):
                if not self._check_auth(request):
                    return self._auth_error()
                if name in artifacts:
                    return Response(artifacts[name], content_type="text/plain")
                return make_result({"artifact": "measurements", "values": [0, 1, 0, 1]})

            self.httpserver.expect_request(
                f"/api/v1/jobs/{task_id}/artifacts/{name}",
                method="GET",
            ).respond_with_handler(handle_artifact)

        # Fallback for any artifact name not registered above, so requesting an
        # unknown artifact gets a real 404 rather than the httpserver default of 500
        # for "no handler found".
        self.httpserver.expect_request(
            re.compile(rf"^/api/v1/jobs/{re.escape(task_id)}/artifacts/.+$"),
            method="GET",
        ).respond_with_handler(
            lambda request: (
                self._auth_error()
                if not self._check_auth(request)
                else Response("Unknown artifact", status=404)
            )
        )

        submit_delay = config.get("submit_time")
        if submit_delay is not None:
            time.sleep(submit_delay)

        return make_result({"id": task_id})

    def machine_args(self):
        return [
            "iqm",
            f"--url=http://127.0.0.1:{self.httpserver.port}",
            f"--machine-name={self.machine_name}",
            f"--token={self.token}",
            "--check-interval=200ms",
        ]

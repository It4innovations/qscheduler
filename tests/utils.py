from typing import Sequence
import requests
import os
import subprocess
import time
import json

OK_RESULT = {"type": "Ok"}
TEST_PROJECT = "test-project"


class TestTask:
    def __init__(self):
        self._submit_time = 0
        self._compute_time = 0
        self._result = OK_RESULT

    def submit_time(self, time: float):
        self._submit_time = time
        return self

    def compute_time(self, time: float):
        self._compute_time = time
        return self

    def error(self, message):
        self._result = {"type": "Fail", "message": message}
        return self

    def create_payload(self) -> dict:
        msg = {"result": self._result}
        if self._compute_time > 0:
            msg["compute_time"] = self._compute_time
        if self._submit_time > 0:
            msg["submit_time"] = self._submit_time
        data = json.dumps(msg)
        return data.encode()


MACHINE_NAME = "TestMachine"


class QScheduler:
    def __init__(
        self,
        working_dir: str,
        port: int,
        backend,
        database_url: str,
    ):
        self.working_dir = working_dir
        self.port = port
        self.backend = backend
        self.database_url = database_url
        self.notify_url = None
        self.notify_token = None
        self.queue_size = 2
        self._process = None
        self._log_file = None

    def url(self, path: str):
        return f"http://127.0.0.1:{self.port}/{path}"

    @property
    def _binary(self):
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "target",
            "debug",
            "qscheduler",
        )

    def _add_machine(self):
        args = [
            self._binary,
            "machine",
            "add",
            MACHINE_NAME,
            f"--queue-size={self.queue_size}",
        ]
        if self.notify_url:
            args.append(f"--notify-url={self.notify_url}")
        if self.notify_token:
            args.append(f"--notify-token={self.notify_token}")
        args += self.backend.machine_args()
        subprocess.run(
            args,
            check=True,
            env={**os.environ, "DATABASE_URL": self.database_url},
        )

    def _start_binary(self, log_name="qscheduler.log"):
        log_path = os.path.join(self.working_dir, log_name)
        self._log_file = open(log_path, "w")
        self._process = subprocess.Popen(
            [self._binary, "serve", "--port", str(self.port)],
            stdout=self._log_file,
            stderr=self._log_file,
            env={"RUST_LOG": "debug", "DATABASE_URL": self.database_url},
            cwd=self.working_dir,
        )

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"qscheduler exited early with code {self._process.returncode}"
                )
            try:
                r = requests.get(self.url("version"), timeout=1)
                if r.status_code == 200:
                    return
            except requests.ConnectionError:
                pass
            time.sleep(0.1)

        raise RuntimeError("qscheduler did not start within 10 seconds")

    def start(self, add_test_project=True):
        self.backend.start()
        self._add_machine()
        self._start_binary()
        if add_test_project:
            self.add_project(TEST_PROJECT, 100_000)

    def cleanup(self):
        if self._process is None:
            return
        if self._process.poll() is not None:
            if self._log_file:
                self._log_file.close()
            raise RuntimeError(
                f"qscheduler exited unexpectedly with code {self._process.returncode}"
            )
        self._process.terminate()
        try:
            self._process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()
        finally:
            if self._log_file:
                self._log_file.close()

    def stop(self, kill=True):
        """Stop the running service process without tearing down the DB.

        With kill=True the process is SIGKILLed to simulate an abrupt crash, so no
        graceful shutdown logic runs and recovery happens purely from the database.
        """
        if self._process is None:
            return
        if self._process.poll() is None:
            if kill:
                self._process.kill()
            else:
                self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        if self._log_file:
            self._log_file.close()
            self._log_file = None
        self._process = None

    def restart(self, kill=True):
        """Simulate a crash and restart: stop the process and start a fresh one
        against the same database and port."""
        self.stop(kill=kill)
        self._start_binary(log_name="qscheduler-restart.log")

    def add_project(
        self, name: str, limit_ms: int, active: bool = True, expect_status_code=None
    ):
        r = requests.post(
            self.url("projects"),
            json={"name": name, "limit_ms": limit_ms, "active": active},
            timeout=5,
        )
        if expect_status_code is None:
            r.raise_for_status()
        else:
            assert r.status_code == expect_status_code, (
                f"Expected HTTP {expect_status_code}, got {r.status_code}: {r.text}"
            )

    def list_projects(self) -> list[dict]:
        r = requests.get(self.url("projects"), timeout=5)
        r.raise_for_status()
        return r.json()

    def get_project(self, name: str) -> dict:
        r = requests.get(self.url(f"projects/{name}"), timeout=5)
        r.raise_for_status()
        return r.json()

    def submit(
        self,
        task: TestTask,
        session_id: int | None = None,
        machine_id: int = 1,
        project: str | None = None,
        expect_error: int | None = None,
    ):
        if session_id is not None and project is not None:
            raise Exception("Cannot set both session_id and project")
        if session_id is None and project is None:
            # Attach to default project
            project = TEST_PROJECT
        payload = task.create_payload()
        msg = {"session_id": session_id, "machine_id": machine_id, "project": project}
        if session_id is not None:
            msg["session_id"] = session_id

        r = requests.post(
            self.url("tasks"),
            params=msg,
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            timeout=5,
        )
        if expect_error is not None:
            assert r.status_code == expect_error, (
                f"Expected HTTP {expect_error}, got {r.status_code}: {r.text}"
            )
            return r
        if not r.ok:
            raise Exception(f"HTTP {r.status_code} submitting task: {r.text}")
        return r.json()

    def new_session(
        self, time_limit: int, machine_id: int = 1, project: str = TEST_PROJECT
    ):
        msg = {
            "machine_id": machine_id,
            "time_limit_secs": time_limit,
            "project": TEST_PROJECT,
        }
        r = requests.post(self.url("sessions"), params=msg, timeout=5)
        r.raise_for_status()
        return r.json()

    def get_session_state(self, session_id: int) -> dict:
        r = requests.get(self.url(f"sessions/{session_id}"), timeout=5)
        r.raise_for_status()
        return r.json()

    def wait_for_session_state(self, session_id: int, target: str):
        TIMEOUT = 10
        deadline = time.monotonic() + TIMEOUT
        wait_time = 0.1
        state = None
        while time.monotonic() < deadline:
            state = self.get_session_state(session_id)
            if state == target:
                return state
            if state == "closed":
                raise Exception(
                    f"Session {session_id}: expected state {target} but got {state}"
                )
            time.sleep(wait_time)
            wait_time += 0.1
        raise Exception(
            f"Session {session_id}: expected state {target} but got {state} after {TIMEOUT}s"
        )

    def wait_for_task_state(self, task_id: int, target: str | Sequence[str]):
        if isinstance(target, str):
            target = (target,)
        TIMEOUT = 10
        deadline = time.monotonic() + TIMEOUT
        wait_time = 0.1
        while time.monotonic() < deadline:
            state = self.get_task_status(task_id)
            tp = state["state"]
            if tp in target:
                return state
            if tp in ("failed", "finished", "cancelled"):
                raise Exception(
                    f"Task {task_id}: expects state {target} but got {tp} (full state: {state})"
                )
            time.sleep(wait_time)
            wait_time += 0.1
        raise Exception(
            f"Task {task_id}: expects state {target} but got {tp} (full state: {state}) after {TIMEOUT}s"
        )

    def assert_task_finished(self, task_id: int):
        state = self.get_task_status(task_id)
        assert state["state"] == "finished", (
            f"Expected task {task_id} to be finished, got {state!r}"
        )

    def assert_task_canceled(self, task_id: int):
        state = self.get_task_status(task_id)
        assert state["state"] == "cancelled", (
            f"Expected task {task_id} to be cancelled, got {state!r}"
        )

    def wait_for_running(self, task_id: int):
        self.wait_for_task_state(task_id, "running")

    def wait_for_finished(self, task_id: int):
        self.wait_for_task_state(task_id, "finished")

    def wait_for_cancelled(self, task_id: int):
        self.wait_for_task_state(task_id, "cancelled")

    def wait_for_finished_or_canceled(self, task_id: int):
        self.wait_for_task_state(task_id, ("cancelled", "finished"))

    def wait_for_failed(self, task_id: int):
        return self.wait_for_task_state(task_id, "failed")

    def assert_session_open(self, session_id: int):
        state = self.get_session_state(session_id)
        assert state == "open", (
            f"Expected session {session_id} to be open, got {state!r}"
        )

    def wait_for_session_open(self, task_id: int):
        self.wait_for_session_state(task_id, "open")

    def wait_for_session_closed(self, task_id: int):
        self.wait_for_session_state(task_id, "closed")

    def assert_task_waiting(self, task_id: int):
        return self.get_task_status(task_id) == {"state": "waiting"}

    def get_task_status(self, task_id: int) -> str:
        r = requests.get(self.url(f"tasks/{task_id}"), timeout=5)
        r.raise_for_status()
        return r.json()

    def version(self) -> str:
        r = requests.get(self.url("version"), timeout=5)
        r.raise_for_status()
        return r.text

import requests
import os
import subprocess
import time
import json

OK_RESULT = {"type": "Ok"}


class TestTask:
    def __init__(self):
        self._wait = 0
        self._result = OK_RESULT

    def wait(self, time: float):
        self._wait = time
        return self

    def error(self, message):
        self._result = {"type": "Fail", "message": message}
        return self

    def create_payload(self) -> dict:
        msg = {"result": self._result}
        if self._wait > 0:
            msg["wait"] = self._wait
        data = json.dumps(msg)
        return data.encode()


class QScheduler:
    def __init__(
        self,
        working_dir: str,
        port: int,
        notify_url: str | None = None,
        notify_token: str | None = None,
    ):
        self.working_dir = working_dir
        self.port = port
        self.notify_url = notify_url
        self.notify_token = notify_token
        self._process = None
        self._log_file = None

    def url(self, path: str):
        return f"http://127.0.0.1:{self.port}/{path}"

    def start(self):
        config_path = os.path.join(self.working_dir, "config.toml")
        if self.notify_url and self.notify_token:
            notify_line = f'\nnotify = {{url = "{self.notify_url}", token = "{self.notify_token}"}}'
        elif self.notify_url:
            notify_line = f'\nnotify = {{url = "{self.notify_url}"}}'
        else:
            notify_line = ""
        with open(config_path, "w") as f:
            f.write(f"""[service]
port = {self.port}

[[machines]]
id = 1
name = "TestMachine"
backend = "Test"{notify_line}
""")

        log_path = os.path.join(self.working_dir, "qscheduler.log")
        self._log_file = open(log_path, "w")

        binary = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "target",
            "debug",
            "qscheduler",
        )
        self._process = subprocess.Popen(
            [binary, config_path],
            stdout=self._log_file,
            stderr=self._log_file,
            env={"RUST_LOG": "debug"},
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

    def submit(
        self,
        task: TestTask,
        session_id: int | None = None,
        machine_id: int = 1,
        repeats: int = 1,
        max_compute_time: int = 1,
        max_waiting_time: int | None = None,
        expect_error: int | None = None,
    ):
        payload = task.create_payload()
        msg = {
            "session_id": session_id,
            "machine_id": machine_id,
            "repeats": repeats,
            "max_compute_time_secs": max_compute_time,
        }
        if max_waiting_time is not None:
            msg["max_waiting_time"] = max_waiting_time
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
        r.raise_for_status()
        return r.json()

    def new_session(self, time_limit: int, machine_id: int = 1):
        msg = {
            "machine_id": machine_id,
            "time_limit_secs": time_limit,
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

    def wait_for_task_state(self, task_id: int, target: str):
        TIMEOUT = 10
        deadline = time.monotonic() + TIMEOUT
        wait_time = 0.1
        while time.monotonic() < deadline:
            state = self.get_task_status(task_id)
            tp = state["state"]
            if tp == target:
                return state
            if tp in ("failed", "finished"):
                raise Exception(
                    "Task {task_id}: expects state {target} but got {target} (full state: {state})"
                )
            time.sleep(wait_time)
            wait_time += 0.1
        if tp in ("failed", "finished"):
            raise Exception(
                "Task {task_id}: expects state {target} but got {target} (full state: {state}) after {TIMEOUT}s"
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

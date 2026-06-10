# Q-Scheduler

A task scheduling service that dispatches work to remote quantum computing backends.

## Quick Start

### Build the Docker image

```bash
docker build -t qscheduler .
```

### Configure and start the service

The service is configured via a TOML file.

```bash
docker run -it --network host -v ./test.toml:/config.toml qscheduler /config.toml
```

Once running, the OpenAPI spec is served at `GET /api-docs/openapi.json`.

## Configuration TOML

The service is configured with a single TOML file passed as a command-line argument.

### Top-level fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `log` | string | no | Path to a log file. If omitted, logs are written to stdout. |
| `[service]` | table | yes | HTTP service settings (see below). |
| `[[machines]]` | array of tables | yes | One or more execution targets (see below). |

### `[service]`

| Field | Type | Description |
|-------|------|-------------|
| `port` | integer | TCP port the HTTP API listens on. |

### `[[machines]]`

Each machine represents one execution target. Multiple `[[machines]]` entries are allowed.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | integer | yes | Unique machine ID referenced when submitting tasks. |
| `name` | string | yes | Human-readable label (used in logs). |
| `queue_size` | integer | yes | Maximum number of tasks that can be queued on this machine. |
| `[machines.backend]` | table | yes | Backend configuration. See [Backends](#backends). |
| `[machines.notify]` | table | no | Callback configuration for task completion events. See [Notifications](#notifications). |

### Backends

The backend is configured as a TOML table with a `type` field.

**`test`** — in-process test backend. Useful for development and integration testing.

```toml
[machines.backend]
type = "test"
```

The task payload must be a JSON object:

```json
{"result": {"type": "Ok"}}
{"result": {"type": "Fail", "message": "error text"}, "submit_time": 0.5, "compute_time": 1.5}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `result` | object | yes | Either `{"type": "Ok"}` or `{"type": "Fail", "message": "..."}`. |
| `submit_time` | float | no | Seconds to wait before acknowledging submission (default: 0). |
| `compute_time` | float | no | Seconds to wait before reporting the final result (default: 0). |

---

**`iqm`** — IQM quantum computing backend.

```toml
[machines.backend]
type = "iqm"
url = "https://example.iqm.fi"
token = "your-bearer-token"
machine_name = "star24"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | yes | Base URL of the IQM service. |
| `token` | string | yes | Bearer token for authentication. |
| `machine_name` | string | yes | Name of the target quantum device. |
| `check_interval_ms` | integer | no | Polling interval in milliseconds for job status (default: 1000). |

The task payload must be a JSON circuit description accepted by the IQM API (`POST /api/v1/jobs/{machine_name}/circuit`).


### Notifications

When `notify` is configured on a machine, the service sends an HTTP `POST` request to the given URL after every task on that machine reaches a terminal state (`finished`, `failed`, or `cancelled`). Delivery is retried with exponential backoff up to 16 times.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | yes | Endpoint that receives task completion events. |
| `token` | string | no | Arbitrary token included in the request body for authentication. |

The request body is JSON:

```json
{
  "task_id": 42,
  "state": "finished"
  "token": "my-secret-token"
}
```

`state` is one of `"finished"`, `"failed"`, or `"cancelled"`. The `token` field is omitted when not configured.

### Complete example

```toml
log = "/var/log/qscheduler.log"

[service]
port = 3000

[[machines]]
id = 1
name = "SimulatorMachine"
queue_size = 4

[machines.backend]
type = "test"

[machines.notify]
url = "https://my-app.example.com/callbacks/qscheduler"
token = "supersecret"

[[machines]]
id = 2
name = "QuantumDevice"
queue_size = 2

[machines.backend]
type = "iqm"
url = "https://example.iqm.fi"
token = "your-bearer-token"
machine_name = "star24"
```

## API

The service exposes a JSON REST API. All responses use `Content-Type: application/json`.

An OpenAPI spec is available at `GET /api-docs/openapi.json`.

---

### `GET /version`

Returns the service version string.

**Response `200`** — plain text, e.g. `qscheduler v1.0.0`.

---

### `POST /tasks`

Submit a task for execution.

**Query parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `session_id` | integer | no | Session to associate the task with. If omitted, the task runs as a standalone task. If provided, the session must be in the `"open"` state or the task is rejected. |
| `machine_id` | integer | yes | ID of the target machine. |
| `repeats` | integer | yes | Number of times to run the task. |
| `max_compute_time_secs` | integer | yes | Maximum execution time in seconds. |
| `max_waiting_time_secs` | integer | no | Seconds to wait in queue before cancelling. |

**Request body** — `application/octet-stream` — raw task payload forwarded to the backend.

**Response `201`** — task ID as a JSON integer.

**Response `422`** — invalid or unknown `machine_id` / `session_id`, or session is not running.

---

### `GET /tasks/{id}`

Get the current state of a task.

**Response `200`** — JSON object with a `state` field:

| `state` | Additional fields | Description |
|---------|-------------------|-------------|
| `"waiting"` | — | Queued, not yet started. |
| `"running"` | — | Currently executing. |
| `"finished"` | — | Completed successfully. |
| `"failed"` | `"error"` (string) | Execution failed. |
| `"cancelled"` | — | Cancelled before or during execution. |

```json
{"state": "finished"}
{"state": "failed", "error": "out of memory"}
```

**Response `404`** — task not found.

---

### `POST /sessions`

Create a session — a time-bounded group of tasks on one machine. Tasks in the session are
cancelled when the session closes.

**Query parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `machine_id` | integer | yes | ID of the target machine. |
| `time_limit_secs` | integer | yes | Session lifetime in seconds. |

**Response `201`** — session ID as a JSON integer.

---

### `GET /sessions/{id}`

Get the current state of a session.

**Response `200`** — one of `"waiting"`, `"open"`, or `"closed"`.

**Response `404`** — session not found.

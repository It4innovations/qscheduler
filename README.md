# Q-Scheduler

A task scheduling service that dispatches work to remote quantum computing backends.

## Quick Start

### Build

```bash
cargo build --release -p qscheduler
```

or build the Docker image:

```bash
docker build -t qscheduler .
```

### Database

QScheduler is stateless except for PostgreSQL, which is the source of truth for machines,
projects, sessions, and tasks. Every invocation of the `qscheduler` binary (both `serve` and
`machine`) requires the `DATABASE_URL` environment variable:

```
DATABASE_URL=postgres://user:password@host/dbname
```

Migrations (in `migrations/`) run automatically whenever the binary connects to the database;
no manual migration step is needed.

> **Local development:** A `compose.yml` is included. Run `docker compose up -d` to start a
> local PostgreSQL instance. A `.env` file is also picked up automatically (via `dotenvy`), so
> you can put `DATABASE_URL=...` there instead of exporting it in your shell.

### Register a machine

Machines are no longer configured via a TOML file — they are registered directly in the
database using the `qscheduler machine` subcommand, then loaded by the service at startup.

```bash
qscheduler machine add <NAME> [OPTIONS] <BACKEND> [BACKEND OPTIONS]
```

| Argument / option | Description |
|---|---|
| `<NAME>` | Unique machine name, referenced when submitting tasks and creating sessions. |
| `--queue-size <N>` | Maximum number of tasks that can be queued on this machine (default: `4`). |
| `--notify-url <URL>` | If set, POST a callback here whenever a task on this machine reaches a terminal state, or a session on this machine opens or closes. See [Notifications](#notifications). |
| `--notify-token <TOKEN>` | Arbitrary token included in notification payloads. |
| `--session-check-interval <DURATION>` | How often session time-consumption is persisted to the database, e.g. `5s` (default: `5s`). |
| `--max-session-time <DURATION>` | Maximum duration a session on this machine may request, e.g. `2h`. Sessions requesting longer are rejected at submit time (default: `2h`). |

`<BACKEND>` is one of:

**`test`** — in-process test backend, useful for development and integration testing. Takes no
extra options.

```bash
qscheduler machine add SimulatorMachine test
```

**`iqm`** — IQM quantum computing backend.

```bash
qscheduler machine add QuantumDevice \
  --queue-size 2 \
  iqm --url https://example.iqm.fi --token your-bearer-token \
      --machine-name star24 --check-interval 1s
```

| IQM option | Description |
|---|---|
| `--url <URL>` | Base URL of the IQM service. |
| `--token <TOKEN>` | Bearer token for authentication. |
| `--machine-name <NAME>` | Name of the target quantum device on the IQM server. |
| `--check-interval <DURATION>` | Polling interval for job status, as a humantime string, e.g. `1s`/`500ms` (the CLI's own `--help` text says "in milliseconds", but a bare integer is rejected — a unit suffix is required). |

`qscheduler machine update <NAME> ...` is intended to change an existing machine's configuration
using the same arguments as `add`, but it currently fails with a database error (`column
"queue_size" does not exist`) — the `machines` table only has `id`/`name`/`type`/`config`
columns, while `update_machine` queries `queue_size`/`notify_url`/`notify_token` as if they were
separate columns. Until this is fixed, machine configuration can only be set at creation time
with `machine add`; to change it, drop and re-add the machine (which changes its ID —
existing sessions/tasks referencing it are unaffected since those tables reference machines by
ID, not name).

Machines are only loaded once at service startup, so **the service must be restarted** after
`machine add` for the change to take effect.

### Register a project

Projects are created through the HTTP API rather than the CLI — see
[`POST /projects`](#post-projects) below.

```bash
curl -X POST http://localhost:4300/projects \
  -H 'Content-Type: application/json' \
  -d '{"name": "my-project", "limit_ms": 3600000}'
```

### Start the service

```bash
qscheduler serve --port 4300
```

```bash
docker run -it --network host -e DATABASE_URL=postgres://user:password@host/dbname \
  qscheduler serve --port 4300
```

Once running, the OpenAPI spec is served at `GET /api-docs/openapi.json`.

## Backends

**`test`** — the task payload is a JSON object:

```json
{"result": {"type": "Ok"}}
{"result": {"type": "Fail", "message": "error text"}, "submit_time": 0.5, "compute_time": 1.5}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `result` | object | yes | Either `{"type": "Ok"}` or `{"type": "Fail", "message": "..."}`. |
| `submit_time` | float | no | Seconds to wait before acknowledging submission (default: 0). |
| `compute_time` | float | no | Seconds to wait before reporting the final result (default: 0). |

**`iqm`** — the task payload must be a JSON circuit description accepted by the IQM API
(`POST /api/v1/jobs/{machine_name}/circuit`).

### Notifications

When `--notify-url` is configured on a machine, the service sends an HTTP `POST` request to
that URL:

- after every task on the machine reaches a terminal state (`finished`, `failed`, or
  `cancelled`);
- whenever a session on the machine opens or closes.

Delivery is retried with exponential backoff up to 16 times.

Task events have this request body:

```json
{
  "task_id": 42,
  "state": "finished",
  "token": "my-secret-token"
}
```

`state` is one of `"finished"`, `"failed"`, or `"cancelled"`.

Session events have this request body:

```json
{
  "session_id": 7,
  "state": "opened",
  "token": "my-secret-token"
}
```

`state` is one of `"opened"` or `"closed"`. A session that is cancelled before ever opening
only fires `"closed"`.

The `token` field is omitted when `--notify-token` was not configured.

## API

The service exposes a JSON REST API. All responses use `Content-Type: application/json` unless
noted otherwise. An OpenAPI spec is available at `GET /api-docs/openapi.json`.

Tasks belong to either a **project** (a time-accounted budget shared across tasks) or a
**session** (an exclusive, time-limited reservation of a machine for a project) — never both.

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
| `machine` | string | yes | Name of the target machine. |
| `project` | string | exactly one of `project` / `session_id` | Name of the project to charge the task's time to. |
| `session_id` | integer | exactly one of `project` / `session_id` | Session to associate the task with. The session must be in the `"open"` state or the task is rejected. |

**Request body** — `application/octet-stream` — raw task payload forwarded to the backend.

**Response `201`** — task ID as a JSON integer.

**Response `402`** — the project has exceeded its time limit, or the project is not active.

**Response `422`** — neither or both of `project`/`session_id` given, unknown `machine`/`project`, or the session is invalid or not open.

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

### `DELETE /tasks/{id}`

Request cancellation of a task. Cancellation is asynchronous: a queued task is removed from the
queue, while a running task is cancelled on the backend; in both cases the task eventually
reaches the `"cancelled"` state (poll `GET /tasks/{id}` to observe it).

**Response `202`** — cancellation requested.

**Response `404`** — task not found.

**Response `409`** — task already in a terminal state (`finished`, `failed`, or `cancelled`).

---

### `POST /sessions`

Create a session — a time-bounded, exclusive reservation of one machine for one project. Tasks
in the session are cancelled when the session closes.

**Query parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `machine` | string | yes | Name of the target machine. |
| `project` | string | yes | Name of the project the session's time is charged to. |
| `time_limit_ms` | integer | yes | Session lifetime in milliseconds. |

**Response `201`** — session ID as a JSON integer.

**Response `402`** — the project has exceeded its time limit, or the project is not active.

**Response `404`** — unknown `machine`.

**Response `422`** — unknown `project`, or `time_limit_ms` exceeds the machine's `--max-session-time`.

---

### `GET /sessions/{id}`

Get the current state of a session.

**Response `200`** — one of `"waiting"`, `"open"`, or `"closed"`.

**Response `404`** — session not found.

---

### `DELETE /sessions/{id}`

Request cancellation of a session. Cancellation is asynchronous: a queued (`"waiting"`) session
is removed from the queue, while an `"open"` session is closed and its tasks are cancelled (both
those already submitted to the backend and those still queued); in both cases the session
eventually reaches the `"closed"` state.

**Response `202`** — cancellation requested.

**Response `404`** — session not found.

**Response `409`** — session already closed.

---

### `POST /projects`

Register a project — a named, time-accounted budget that tasks and sessions are charged
against.

**Request body**

```json
{"name": "my-project", "limit_ms": 3600000, "active": true}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique project name. |
| `limit_ms` | integer | yes | Time budget, in milliseconds. |
| `active` | boolean | no | Whether the project can accept new tasks/sessions (default: `true`). |

**Response `201`** — empty body.

**Response `409`** — a project with this name already exists.

---

### `GET /projects`

List all projects.

**Response `200`** — JSON array of project objects (see below).

---

### `GET /projects/{name}`

Get a single project by name.

**Response `200`**

```json
{"name": "my-project", "consumed_ms": 120000, "limit_ms": 3600000, "active": true}
```

**Response `404`** — project not found.

---

### `GET /machine/{machine}/arch`

Fetch the backend's architecture description.

**Response `200`** — plain-text/JSON string, backend-specific.

**Response `404`** — unknown machine.

**Response `500`** — backend error.

---

### `GET /machine/{machine}/calibration/{calibration}/{endpoint}`

Fetch a calibration data set from the backend.

**Response `200`** — plain-text/JSON string, backend-specific.

**Response `404`** — unknown machine.

**Response `500`** — backend error.

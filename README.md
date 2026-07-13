# Q-Scheduler

A task scheduling service that dispatches work to remote quantum computing backends.

## Features

- **Task scheduling** — accepts tasks and queues them per machine, dispatching to a pluggable
  backend (a real quantum backend or an in-process test backend).
- **Project accounting** — tasks can be charged against a project, a named, time-limited budget
  that tracks accumulated execution time and rejects new tasks once its limit is exceeded or it
  is deactivated.
- **Sessions** — an exclusive, time-limited reservation of a machine for a single project, so a
  sequence of tasks can run back-to-back without interleaving with other projects' work.
- **Notifications** — optional HTTP callbacks on task completion and session open/close.
- **REST API** — a JSON HTTP API for submitting and monitoring tasks, sessions, and projects,
  with an OpenAPI spec served at runtime.

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
projects, sessions, and tasks. Every invocation of the `qscheduler` binary requires the `DATABASE_URL` environment variable:

```
DATABASE_URL=postgres://user:password@host/dbname
```

Migrations (in `migrations/`) run automatically whenever the binary connects to the database;
no manual migration step is needed.

> **Local development:** A `compose.yml` is included. Run `docker compose up -d` to start a
> local PostgreSQL instance. A `.env` file is also picked up automatically (via `dotenvy`), so
> you can put `DATABASE_URL=...` there instead of exporting it in your shell.

### Register a machine

Quantum machines are registered directly in the
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
qscheduler machine add QuantumDevice iqm --url https://iqm.machine.com --token your-token --machine-name star24
```

| IQM option | Description |
|---|---|
| `--url <URL>` | Base URL of the IQM service. |
| `--token <TOKEN>` | Bearer token for authentication. |
| `--machine-name <NAME>` | Name of the target quantum device on the IQM server. |
| `--check-interval <DURATION>` | Polling interval for job status, as a humantime string, e.g. `1s`/`500ms` (the CLI's own `--help` text says "in milliseconds", but a bare integer is rejected — a unit suffix is required). |

`qscheduler machine update <NAME> ...` is intended to change an existing machine's configuration
using the same arguments as `add`.
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

## Local Testing

To try out the full flow locally with no external dependencies, use the in-process `test`
backend (see [Backends](#backends)) instead of a real quantum backend. `compose.yml` publishes
PostgreSQL on the host's `5432` port, so the service container can just reach it at `localhost`
by running with `--network host` — no custom Docker network needed.

1. Start PostgreSQL:

   ```bash
   docker compose up -d
   ```

2. Build the image:

   ```bash
   docker build -t qscheduler .
   ```

3. Register a machine using the `test` backend:

   ```bash
   docker run --rm --network host \
     -e DATABASE_URL=postgres://postgres:xpass11@localhost/postgres \
     qscheduler machine add TestMachine test
   ```

4. Start the service (machines are only loaded at startup, so this must run after step 3):

   ```bash
   docker run -d --name qscheduler --network host \
     -e DATABASE_URL=postgres://postgres:xpass11@localhost/postgres \
     qscheduler serve --port 4300
   ```

5. Exercise it — register a project, then submit a task against `TestMachine`:

   ```bash
   curl -X POST http://localhost:4300/projects \
     -H 'Content-Type: application/json' \
     -d '{"name": "test-project", "limit_ms": 3600000}'

   curl -X POST 'http://localhost:4300/tasks?machine=TestMachine&project=test-project&user=me' \
     -H 'Content-Type: application/octet-stream' \
     --data-raw '{"outcome": {"type": "Ok"}}'
   ```

   Poll `GET /tasks/{id}` with the returned task ID to see it move from `"waiting"` to
   `"finished"`.

## Backends

**`test`** — the task payload is a JSON object:

```json
{"outcome": {"type": "Ok"}, "result": "42", "artifacts": {"log": "some log"}}
{"outcome": {"type": "Fail", "message": "error text"}, "submit_time": 0.5, "compute_time": 1.5}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `outcome` | object | yes | Either `{"type": "Ok"}` or `{"type": "Fail", "message": "..."}`. |
| `result` | string | no | Value returned by `GET /tasks/{id}/result` once the task completes (default: `""`). |
| `artifacts` | object | no | Map of artifact name to value, served by `GET /tasks/{id}/artifacts/{name}` once the task completes (default: `{}`). |
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

Task events have this request body, with `task` in the same shape returned by
[`GET /tasks/{id}`](#get-tasksid):

```json
{
  "event": "task",
  "task": {
    "id": 42,
    "project": "my-project",
    "user": "me",
    "backend_id": "abc123",
    "machine": "QuantumDevice",
    "state": "finished",
    "created_at": "2026-07-13T10:00:00Z",
    "finished_at": "2026-07-13T10:00:05Z",
    "exectime_ms": 1234
  },
  "token": "my-secret-token"
}
```

The task fires once it reaches a terminal `state`: `"finished"`, `"failed"`, or `"cancelled"`.

Session events have this request body, with `session` in the same shape returned by
[`GET /sessions/{id}`](#get-sessionsid):

```json
{
  "event": "session",
  "session": {
    "id": 7,
    "state": "open",
    "machine": "QuantumDevice",
    "project": "my-project",
    "time_limit_ms": 3600000,
    "created_at": "2026-07-13T10:00:00Z",
    "opened_at": "2026-07-13T10:00:01Z"
  },
  "token": "my-secret-token"
}
```

The session fires once when it opens (`session.state` is `"open"`) and once when it closes
(`session.state` is `"closed"`). A session that is cancelled before ever opening only fires the
`"closed"` event.

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

### `GET /health`

Health check. Reports whether the service is up and able to reach the database.

**Response `200`** — the database is reachable:

```json
{ "status": "ok" }
```

**Response `503`** — the database is unreachable:

```json
{ "status": "unhealthy" }
```

---

### `POST /tasks`

Submit a task for execution.

**Query parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `machine` | string | yes | Name of the target machine. |
| `project` | string | exactly one of `project` / `session_id` | Name of the project to charge the task's time to. |
| `session_id` | integer | exactly one of `project` / `session_id` | Session to associate the task with. The session must be in the `"open"` state or the task is rejected. |
| `user` | string | yes | Free-form identifier of the user submitting the task, stored alongside the task. |

**Request body** — `application/octet-stream` — raw task payload forwarded to the backend.

**Response `201`** — task ID as a JSON integer.

**Response `402`** — the project has exceeded its time limit, or the project is not active.

**Response `422`** — neither or both of `project`/`session_id` given, unknown `machine`/`project`, or the session is invalid or not open.

---

### `GET /tasks/{id}`

Get a task's current info. Works for tasks no longer held in memory too (e.g. after a service
restart) — the service falls back to the database for terminal tasks.

**Response `200`**

| Field | Type | Present | Description |
|-------|------|---------|-------------|
| `id` | integer | always | Task ID. |
| `session` | integer | if the task belongs to a session | Session the task ran in. Mutually exclusive with `project`. |
| `project` | string | if the task belongs to a project directly | Name of the project the task's time is charged to. Mutually exclusive with `session`. |
| `user` | string | always | Free-form identifier of the submitting user. |
| `backend_id` | string | once submitted to the backend | Backend-assigned job ID. |
| `machine` | string | always | Name of the target machine. |
| `state` | string | always | One of `"waiting"`, `"running"`, `"finished"`, `"failed"`, `"cancelled"`. |
| `created_at` | timestamp | always | When the task was submitted. |
| `finished_at` | timestamp | once in a terminal state | When the task reached its final state. |
| `exectime_ms` | integer | if any execution time was consumed | Milliseconds of execution time. |
| `error` | string | only when `state` is `"failed"` | Failure reason. |

```json
{
  "id": 42,
  "project": "my-project",
  "user": "me",
  "backend_id": "abc123",
  "machine": "QuantumDevice",
  "state": "failed",
  "created_at": "2026-07-13T10:00:00Z",
  "finished_at": "2026-07-13T10:00:05Z",
  "exectime_ms": 1234,
  "error": "out of memory"
}
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

### `GET /tasks/{id}/result`

Fetch a task's raw result from the backend that ran it.

**Response `200`** — plain text, backend-specific (e.g. the full job-status JSON document for
the `iqm` backend, or the raw `result` string for the `test` backend).

**Response `404`** — task not found.

**Response `409`** — task has not been submitted to a backend yet.

**Response `500`** — backend request failed.

---

### `GET /tasks/{id}/artifacts/{name}`

Fetch a named artifact produced for a task from the backend that ran it.

**Response `200`** — plain text, backend-specific.

**Response `404`** — task not found, or the named artifact doesn't exist on the backend.

**Response `409`** — task has not been submitted to a backend yet.

**Response `500`** — backend request failed.

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

Get a session's current info. Works for closed sessions no longer held in memory too (e.g.
after a service restart) — the service falls back to the database.

**Response `200`**

| Field | Type | Present | Description |
|-------|------|---------|-------------|
| `id` | integer | always | Session ID. |
| `state` | string | always | One of `"waiting"`, `"open"`, or `"closed"`. |
| `machine` | string | always | Name of the reserved machine. |
| `project` | string | always | Name of the project the session's time is charged to. |
| `time_limit_ms` | integer | always | Session lifetime in milliseconds. |
| `created_at` | timestamp | always | When the session was created. |
| `opened_at` | timestamp | once opened | When the session became active. |
| `closed_at` | timestamp | once closed | When the session ended. |
| `exectime_ms` | integer | if any execution time was consumed | Milliseconds the session was open and accruing time (may appear before `closed_at`, as it's checkpointed periodically while open). |

```json
{
  "id": 7,
  "state": "closed",
  "machine": "QuantumDevice",
  "project": "my-project",
  "time_limit_ms": 3600000,
  "created_at": "2026-07-13T10:00:00Z",
  "opened_at": "2026-07-13T10:00:01Z",
  "closed_at": "2026-07-13T11:00:01Z",
  "exectime_ms": 3600000
}
```

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

### `PATCH /projects/{name}`

Update a project's `active` flag and/or time `limit_ms`. Fields omitted from the request body
are left unchanged. Does not affect `consumed_ms`.

**Request body**

```json
{"active": false, "limit_ms": 7200000}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `active` | boolean | no | Whether the project can accept new tasks/sessions. Omit to leave unchanged. |
| `limit_ms` | integer | no | Time budget, in milliseconds. Omit to leave unchanged. |

**Response `200`** — the updated project.

```json
{"name": "my-project", "consumed_ms": 120000, "limit_ms": 7200000, "active": false}
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

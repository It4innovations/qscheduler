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
- **Machine suspension** — take a machine out of service with a message: new tasks and sessions
  are rejected, and existing work is optionally failed. Every suspension is kept in a history.
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
| `--limit-check-interval <DURATION>` | How often session time-consumption is persisted to the database and task and project time limits are checked for running work, e.g. `5s` (default: `2s`). |
| `--max-session-time <DURATION>` | Maximum duration a session on this machine may request, e.g. `2h`. Sessions requesting longer are rejected at submit time (default: `2h`). |
| `--default-task-time-limit <DURATION>` | Execution time limit for tasks submitted without `time_limit_ms`, e.g. `30m` (default: `1h`). |
| `--max-clock-skew <DURATION>` | Assumed maximum amount by which the backend's clock lags this host's (default: `4s`). Running tasks' time is estimated so it never exceeds the true time while the skew stays within this bound, so time limits never fail a task early; the estimate may lag the true time by up to this amount (plus one poll interval), letting a task overrun its limit or its project's budget slightly. |

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
| `--poll-interval <DURATION>` | Polling interval for job status, as a humantime string, e.g. `1s`/`500ms` (a unit suffix is required). |

If the IQM server is unreachable or returns errors, a job is never given up on: its polls back
off exponentially (starting at `--poll-interval`, capped at 60s) and return to the normal
interval after the first successful poll. A cancel that can't be delivered is retried on the
job's later polls. Each poll/cancel request times out after 10s.

`qscheduler machine update <NAME> ...` changes an existing machine's configuration using the
same arguments as `add`. It replaces the whole configuration: an option left out is reset to its
default, not kept at its current value.
Machines are only loaded once at service startup, so **the service must be restarted** after
`machine add` or `machine update` for the change to take effect.

### Remove a machine

```bash
qscheduler machine remove <NAME> [--force]
```

A machine with no tasks and no sessions is removed directly. If it still has tasks or
sessions, the command prints how many and refuses to remove it. With `--force`, the machine is
removed together with all its tasks and sessions, including their consumption records.

Time already consumed by the removed tasks and sessions is **not** returned to their projects:
each project's consumed time stays as it was before the removal.

As with `add`/`update`, restart the service afterwards; do not remove a machine while the
service is running.

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

On `SIGTERM` (e.g. `docker stop`) or `Ctrl+C` the service shuts down gracefully: it stops
accepting requests, finishes in-flight ones, applies cancel requests it has already accepted,
writes all pending state to the database (including the time of a running session), and exits.
No new tasks are submitted to backends during shutdown. Tasks still running on a backend are
re-attached on the next start. A crash or `SIGKILL` skips this, so up to one
`--limit-check-interval` of session time may go uncharged.

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

Notifiers are named HTTP callback targets, independent of machines. They are registered in the
database with the `qscheduler notifier` subcommand and loaded by the service at startup, so
adding or removing one takes effect after a restart:

```bash
qscheduler notifier add <NAME> --url <URL> [--token <TOKEN>]
qscheduler notifier remove <NAME>
qscheduler notifier list
```

| Argument / option | Description |
|---|---|
| `<NAME>` | Unique notifier name, at most 512 characters; tasks and sessions select the notifier by it. |
| `--url <URL>` | URL to `POST` events to. |
| `--token <TOKEN>` | Arbitrary token included in event payloads. |

Removing a notifier turns off notifications for every task and session that used it. `notifier list` prints each notifier's name, URL and whether it has a token (the token itself is
not shown).

A task or session opts in with the `notify` parameter of [`POST /tasks`](#post-tasks) or
[`POST /sessions`](#post-sessions); without it, no notification is sent. An unknown notifier name
is rejected with `422`. The service then sends an HTTP `POST` request to that notifier's URL:

- after the task reaches a terminal state (`finished`, `failed`, or `cancelled`);
- whenever the session opens or closes.

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

The `token` field is omitted when the notifier has no `--token`.

## API

The service exposes a JSON REST API. All responses use `Content-Type: application/json` unless
noted otherwise. An OpenAPI spec is available at `GET /api-docs/openapi.json`.

### Versioning

QScheduler follows [Semantic Versioning](https://semver.org); one version (`[workspace.package]`
in `Cargo.toml`) covers the binary and the API (`info.version` of the OpenAPI spec). Compared
to the last release, a breaking API change bumps the major version (the minor version while
on `0.y.z`), any other API change bumps the minor version (the patch version on `0.y.z`).

- `openapi.json` in the repository root is a snapshot of the spec (also printed by
  `qscheduler openapi`). `cargo test` fails when it is out of date; regenerate it with
  `UPDATE_OPENAPI=1 cargo test -p service --test openapi_snapshot`.
- `scripts/check_api_version.py` (run in CI, needs [oasdiff](https://github.com/oasdiff/oasdiff))
  compares the snapshot with `openapi.json` at the latest `vX.Y.Z` tag and fails when the
  version bump is too small for the API changes.
- Release: set the version in `Cargo.toml`, regenerate the snapshot, update `CHANGELOG.md`, and
  tag the release commit `vX.Y.Z`.

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
| `time_limit_ms` | integer | no | Maximum execution time in milliseconds, counted from when the backend starts running the task (queue time doesn't count). When exceeded, the task is cancelled on the backend and ends `"failed"` with error `"Task time limit exceeded"`. Enforced with a running-time estimate that never exceeds the true time (see `--max-clock-skew`) and checked every machine `--limit-check-interval`, so a task is never stopped early but may overrun slightly. Defaults to the machine's `--default-task-time-limit`. |
| `notify` | string | no | Name of the notifier to send the task's completion event to (see [Notifications](#notifications)). No notification is sent when omitted. |

**Request body** — `application/octet-stream` — raw task payload forwarded to the backend.

**Response `201`** — task ID as a JSON integer.

**Response `402`** — the project has exceeded its time limit, or the project is not active.

**Response `409`** — the machine is suspended (also for a task submitted into an open session).
The body is the error text with the suspension message, e.g.
`Machine 'garnet' is suspended: Calibration in progress`.

**Response `422`** — neither or both of `project`/`session_id` given, unknown `machine`/`project`/`notify`, `time_limit_ms` is zero, or the session is invalid or not open.

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
| `time_limit_ms` | integer | always (except tasks created before time limits existed) | Execution time limit in milliseconds, counted from `started_at`. |
| `notify` | string | if the task was submitted with `notify` | Name of the notifier that receives the task's completion event. Absent once the notifier is removed (after a restart). |
| `started_at` | timestamp | once the backend started running the task | When the backend started executing the task, on the **backend's clock**. |
| `finished_at` | timestamp | once in a terminal state | When the task reached its final state. For a task that ran on the backend this is the backend's timestamp (**backend's clock**; `finished_at − started_at` equals `exectime_ms`); for a task ended by the scheduler itself (e.g. cancelled while queued) it's the scheduler's time. |
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
| `notify` | string | no | Name of the notifier to send the session's open/close events to (see [Notifications](#notifications)). No notification is sent when omitted. |

**Response `201`** — session ID as a JSON integer.

**Response `402`** — the project has exceeded its time limit, or the project is not active.

**Response `404`** — unknown `machine`.

**Response `409`** — the machine is suspended; the body contains the suspension message.

**Response `422`** — unknown `project` or `notify`, or `time_limit_ms` exceeds the machine's `--max-session-time`.

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
| `notify` | string | if the session was created with `notify` | Name of the notifier that receives the session's open/close events. Absent once the notifier is removed (after a restart). |
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

A task submitted with `project` is charged its execution time; a task in a session is not
charged itself — the session's open time is. Running work counts against the budget while it
runs (checked every machine `--limit-check-interval`): once the budget is exhausted, the
project's open sessions are closed and its submitted tasks are cancelled and end `"failed"` with
error `"Project time limit exceeded"`, as do its tasks still waiting in the queue.

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

Get a single project by name. `consumed_ms` is confirmed consumption only — finished tasks and
checkpointed session time — so it never decreases. Running tasks' time counts toward the limit
checks but is not included until the task finishes (then with its exact execution time).

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

### `GET /machine/{machine}/about`

Describe the machine's backend: its type and version information.

**Response `200`**

| Field | Type | Description |
|---|---|---|
| `type` | string | Backend type: `iqm` or `test`. |
| `version` | object \| string | Backend-specific version info (see below). |

For the `iqm` backend, `version` holds the version fields reported by the IQM server's `/about`
endpoint (other fields of that response are omitted):

```json
{
  "type": "iqm",
  "version": {
    "qccsw_version": "4.6.3-00027338-127701ef",
    "server_version": "2.204.9.20260728110546",
    "station_control_version": "49.0.3"
  }
}
```

For the `test` backend, `version` is a plain string:

```json
{ "type": "test", "version": "1.0" }
```

**Response `404`** — unknown machine.

**Response `500`** — backend error (including an IQM `/about` response missing any of the
version fields above).

---

### `GET /machine/{machine}/calibration/{calibration}/{endpoint}`

Fetch a calibration data set from the backend.

**Response `200`** — plain-text/JSON string, backend-specific.

**Response `404`** — unknown machine.

**Response `500`** — backend error.

---

### `POST /machine/{machine}/suspension`

Suspend a machine. While it is suspended, every new task and session for it is rejected with
`409` and the suspension message, including tasks submitted into a session that is already
open. A machine has at most one active suspension.

**Request body** — `application/json`

| Field | Type | Required | Description |
|---|---|---|---|
| `message` | string | yes | Why the machine is suspended; 1–512 characters. |
| `cancel` | string | no | What happens to existing work (default `"none"`, see below). |

| `cancel` | Effect on existing work |
|---|---|
| `"none"` | Nothing: queued and running tasks and queued and open sessions continue as normal. |
| `"waiting"` | Every task not yet submitted to the backend ends `"failed"`, and every session is closed: queued sessions never open, and the tasks of an open session fail, including the ones already submitted to the backend. Other submitted tasks are computed as normal. |
| `"all"` | Like `"waiting"`, and every task already submitted to the backend ends `"failed"` as well. |

A task failed by a suspension has the error `"Machine suspended: <message>"`. A submitted task is
failed right away, without waiting for the backend: its cancellation on the backend is requested
(and retried while the backend is unreachable), and whatever the backend reports about it
afterwards is ignored. A project task is charged its estimated running time so far, which never
exceeds the true time.

The `cancel` policy is applied once, when the suspension starts. After a restart, a suspended
machine is still suspended.

**Response `201`** — the new suspension:

```json
{
  "message": "Calibration in progress",
  "cancel": "waiting",
  "suspended_at": "2026-10-15T12:00:00.000000Z"
}
```

**Response `404`** — unknown machine.

**Response `409`** — the machine is already suspended.

**Response `422`** — `message` is empty or longer than 512 characters, or `cancel` is invalid.

---

### `DELETE /machine/{machine}/suspension`

End the machine's suspension, so it accepts new tasks and sessions again. Work failed or closed
by the suspension stays failed or closed.

**Response `200`** — the ended suspension, now with `resumed_at` set.

**Response `404`** — unknown machine.

**Response `409`** — the machine is not suspended.

---

### `GET /machine/{machine}/suspensions`

List the machine's suspensions, newest first. The active suspension, if any, comes first and
has no `resumed_at`.

**Response `200`**

```json
[
  {
    "message": "Calibration in progress",
    "cancel": "waiting",
    "suspended_at": "2026-10-15T12:00:00.000000Z",
    "resumed_at": "2026-10-15T13:30:00.000000Z"
  }
]
```

**Response `404`** — unknown machine.

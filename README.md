# Q-Scheduler

A scheduling service that accepts tasks and dispatches them to remote quantum computing
backends (currently IQM, plus an in-process `test` backend for development).

## How it works

- **Machines** are named execution targets, each backed by a backend. Every machine has its own
  queue and hands a limited number of tasks to its backend at once.
- **Projects** are named, time-limited budgets. Work is charged against a project, and new work
  is rejected once its budget is exhausted or it is deactivated.
- **Tasks** are payloads forwarded to a machine's backend. A task belongs either to a project
  (charged its execution time) or to an open session. A task's `kind` is `circuit` (the
  default; gate-level circuits as JSON) or `pulse` (a compiled pulse-level job as protobuf, see
  [Pulse-level tasks](#pulse-level-tasks)).
- **Sessions** are exclusive, time-limited reservations of a machine for one project; the
  project is charged the time the session is open.
- **Suspensions** take a machine out of service with a message, optionally failing its existing
  work.
- **Notifiers** are HTTP callback targets that tasks and sessions can opt into, to be told
  when a task finishes or a session opens or closes.

Everything is driven through a JSON REST API that refers to machines, projects and notifiers by
name. PostgreSQL is the only state: tasks and sessions survive a restart, and tasks still
running on a backend are re-attached.

**The API is documented in its OpenAPI spec**, served by the running service at
`GET /api-docs/openapi.json` and kept in the repository as [`openapi.json`](openapi.json) (also
printed by `qscheduler openapi`). It describes every endpoint, field, default and error, and
the task payload format of each backend.

## Running

### Build

```bash
cargo build --release -p qscheduler    # or: docker build -t qscheduler .
```

### Database

Every invocation of `qscheduler` needs `DATABASE_URL`, e.g.
`DATABASE_URL=postgres://user:password@host/dbname`. A `.env` file in the working directory is
picked up automatically. Migrations run automatically on connect.

For local development, `docker compose up -d` starts PostgreSQL from the included
`compose.yml` (`postgres://postgres:xpass11@localhost/postgres`).

### Start the service

```bash
ADMIN_TOKEN=my-secret qscheduler serve --port 4300
```

Request bodies are limited to 32 MiB by default; change the limit with `--max-payload-mb <N>`.

On `SIGTERM` (e.g. `docker stop`) or `Ctrl+C`, the service shuts down gracefully: it finishes
in-flight requests, applies accepted cancel requests, writes all pending state (including the
time of a running session) to the database, and exits without submitting new tasks. A crash or
`SIGKILL` skips this, so up to one limit-check interval of session time may go uncharged.

## Admin API

Machines and notifiers are managed through the admin endpoints `/machines`,
`/machine/{name}` and `/notifiers`. Machines can be added, reconfigured and removed, and
notifiers added and removed, while the service runs; changes apply immediately, with no
restart. Reconfiguring a machine restarts only that machine's scheduling: queued work is
kept, and an open session on it is closed.

The admin endpoints are protected by a single shared secret, the `ADMIN_TOKEN` environment
variable of the service. Requests must send it as a bearer token:

```bash
curl -X POST http://localhost:4300/machines \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name": "TestMachine", "backend": {"type": "test"}}'
```

If `ADMIN_TOKEN` is unset or empty, the admin endpoints are disabled and return `403`; a missing
or wrong token gets `401`. Backend and notifier tokens can be set but are never returned.
See the OpenAPI spec for the request bodies and the exact behavior.

### CLI alternative

The same management is available from the command line. These commands write directly to the
database, so a running service only picks up their changes **after a restart**:

```bash
qscheduler machine add <NAME> [OPTIONS] test
qscheduler machine add <NAME> [OPTIONS] iqm --url <URL> --token <TOKEN> --machine-name <NAME> --poll-interval 1s
qscheduler machine update <NAME> ...        # replaces the whole configuration
qscheduler machine remove <NAME> [--force]
qscheduler notifier add <NAME> --url <URL> [--token <TOKEN>]
qscheduler notifier remove <NAME>
qscheduler notifier list
```

The options match the fields of the admin API's machine configuration, but take durations as
strings (`--limit-check-interval 5s`, `--max-session-time 2h`) instead of milliseconds; see
`qscheduler machine add --help`.

## Hello world

This example uses the in-process `test` backend, so it needs nothing but PostgreSQL. It
creates a machine and a project, runs a task, and reads the task's status and result.

**1. Start PostgreSQL and the service:**

```bash
docker compose up -d
export DATABASE_URL=postgres://postgres:xpass11@localhost/postgres ADMIN_TOKEN=my-secret
cargo run -p qscheduler -- serve --port 4300
```

**2. In another terminal, register a test machine** (admin API, so it needs the token):

```bash
export ADMIN_TOKEN=my-secret
curl -X POST http://localhost:4300/machines \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name": "TestMachine", "backend": {"type": "test"}}'
```

```json
{"name":"TestMachine","queue_size":4,"limit_check_interval_ms":2000,"max_session_time_ms":7200000,"default_task_time_limit_ms":3600000,"max_clock_skew_ms":4000,"backend":{"type":"test"},"suspension":null}
```

**3. Create a project** with a one-hour budget to charge the task to:

```bash
curl -X POST http://localhost:4300/projects -H 'Content-Type: application/json' \
  -d '{"name": "hello", "limit_ms": 3600000}'
```

**4. Submit a task.** The `test` backend takes a JSON payload that says how the task should
end. This one succeeds after 2 seconds with the result `Hello, world!`:

```bash
curl -X POST 'http://localhost:4300/tasks?machine=TestMachine&project=hello&user=me' \
  -H 'Content-Type: application/octet-stream' \
  --data-raw '{"outcome": {"type": "Ok"}, "result": "Hello, world!", "compute_time": 2}'
```

The response is the new task's ID: `1`.

**5. Check its status.** Right after submission the task is `running`; after about 2 seconds
it is `finished`:

```bash
curl http://localhost:4300/tasks/1
```

```json
{"id":1,"project":"hello","user":"me","backend_id":"test-1","machine":"TestMachine","state":"finished","created_at":"2026-10-23T09:57:10.012982Z","time_limit_ms":3600000,"started_at":"2026-10-23T09:57:10.017417Z","finished_at":"2026-10-23T09:57:12.018375647Z","exectime_ms":2000}
```

**6. Read the result and the project's consumed time:**

```bash
curl http://localhost:4300/tasks/1/result
# Hello, world!

curl http://localhost:4300/projects/hello
# {"name":"hello","consumed_ms":2000,"limit_ms":3600000,"active":true}
```

## Pulse-level tasks

Besides gate-level circuits, a machine can run compiled pulse-level jobs, e.g. an IQM
`RunDefinition` produced by [IQM Pulla](https://pypi.org/project/iqm-pulla/). Pulse access is
off by default and is enabled per machine: `"pulse_access": true` in the admin API

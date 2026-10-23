Scheduling service dispatching tasks to quantum computing backends.

The API refers to machines, projects and notifiers by **name**. Request and response bodies are
JSON unless an endpoint says otherwise.

## Concepts

- **Machine** — a named execution target backed by a backend (`iqm` or the in-process `test`
  backend). Each machine has its own task queue and hands at most `queue_size` tasks to its
  backend at once.
- **Project** — a named, time-limited budget. Work charged to a project is rejected with
  `402` once the budget is exhausted or the project is deactivated.
- **Session** — an exclusive, time-limited reservation of one machine for one project. While a
  session is open, only its tasks run on the machine.
- **Task** — a payload forwarded to a machine's backend. A task belongs to exactly one of a
  project (it is charged its execution time) or an open session (the session's open time is
  charged instead).
- **Suspension** — a machine taken out of service with a message; new work for it is rejected
  with `409`.
- **Notifier** — a named HTTP callback target that tasks and sessions can opt into with
  `notify`.

The database is the source of truth: tasks and sessions survive a service restart, and tasks
still running on a backend are re-attached.

## Admin API

Machines and notifiers are managed through the admin endpoints (`/machines`,
`/machine/{machine}` and `/notifiers`). Changes apply to the running service at once. These
endpoints require the header `Authorization: Bearer <token>`, where the token is the value of
the `ADMIN_TOKEN` environment variable of the service. When `ADMIN_TOKEN` is unset or empty,
the admin endpoints are disabled and return `403`; a missing or wrong token gets `401`. Backend
and notifier tokens are accepted but never returned.

## Notifications

A task or session with `notify` makes the service `POST` a JSON event to that notifier's URL:
for a task once it reaches a terminal state (`finished`, `failed` or `cancelled`), for a
session when it opens (`session.state` is `open`) and when it closes (`closed`). A session
cancelled before it opened only sends the `closed` event. A failed delivery is retried with
exponential backoff, up to 16 attempts.

The event body carries the same object as `GET /tasks/{id}` (`TaskInfo`) or
`GET /sessions/{id}` (`SessionInfo`), plus the notifier's token when it has one:

```json
{"event": "task", "task": {"id": 42, "state": "finished", "...": "..."}, "token": "secret"}
{"event": "session", "session": {"id": 7, "state": "open", "...": "..."}, "token": "secret"}
```

## Versioning

`info.version` is the service version and follows Semantic Versioning: a breaking API change
bumps the major version (the minor version while on `0.y.z`), any other API change bumps the
minor version (the patch version on `0.y.z`).

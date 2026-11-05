# Changelog

## v0.3.0 - 2026-11-05

### Changed

- **Breaking:** `POST /sessions` requires the new `owner` parameter, the name of the user owning
  the session. Sessions created before this version get the user of their first task as the
  owner, or a user named `unknown` if they have no tasks.
- Session info (`GET /sessions/{id}` and session notifications) contains the session's `owner`.
- **Breaking:** service option `--max-payload-mb <N>` replaced by `--max-payload <SIZE>`: a whole
  number with an optional `K`, `M`, `G` or `T` suffix, at most `4G`. The default stays 32 MiB.
- **Breaking:** the default task time limit (`--default-task-time-limit`) changed from `1h` to
  `15m`, and a task asking for more than the new per-machine maximum (see Added) gets `422`.

### Added

- Machines schedule users by fair share instead of in submission order.
- `--task-retention` also applies to closed sessions: they are evicted from memory afterwards and
  served from the database.
- Machine option `--decay-half-time` / `decay_half_time_ms` (default `90m`): the time after which a
  user's fair share usage drops to half.
- Machine option `--max-task-time-limit` / `max_task_time_limit_ms` (default `15m`): the maximum
  time limit a task may ask for. Tasks already accepted keep their limit when it is lowered.
- Per-user limits on active work: `--max-user-active-items` (default 2000) on tasks not yet
  finished, failed or cancelled plus owned sessions not yet closed, and `--max-user-active-payload`
  (default `200M`) on the total payload size of the active tasks. `POST /tasks` and `POST /sessions`
  answer `429` over a limit, `POST /tasks` answers `413` if the payload alone is larger than the
  limit. A task submit with a `Content-Length` is rejected before its body is read.
- `GET /debug/scheduler` (admin token) returns the scheduler's state per machine, for internal
  debugging only. Its format is not guaranteed to be stable across versions.
- Consumed time is tracked per user, project and machine: a user is charged the time of the
  sessions it owns and of its tasks outside a session, but not of its tasks inside a session.
  Existing tasks and sessions are included. Removing a machine removes its entries.
- `GET /users` lists the names of all known users.
- `GET /users/{name}/consumption` returns a user's consumed time per project and machine.

## v0.2.2 - 2026-10-27

### Added

- Pulse level access
- Task payload clear + Task memory retention

## v0.2.1 - 2026-10-23

### Added

- Admin API

## v0.2.0

### Changed

- Machine option `--session-check-interval` renamed to `--limit-check-interval` (it now also
  drives task time limit checks); its default changed from `5s` to `2s`.
- IQM backend option `--check-interval` renamed to `--poll-interval`.
- Existing machine configurations stored under the old names are still read.
- IQM: job polling never gives up (previously failed after 120 failed polls); unreachable
  jobs are polled with exponential backoff, and failed cancels are retried instead of dropped.
- **Breaking:** notifications are no longer configured per machine. The machine options
  `--notify-url`/`--notify-token` are removed, and a `notify` block in an existing machine
  configuration is ignored. Instead, register notifiers (see Added) and select one per task or
  session; without `notify`, nothing is sent.
- Notification delivery logs name the notifier and a short failure reason (e.g. `HTTP 500`,
  `cannot connect`), including the "discarding notification after max failures" warning.

### Added

- Semantic versioning of the API: one workspace version is reported in the OpenAPI spec
  (`info.version`, title `qscheduler`); a CI check fails when the version is not bumped enough
  for API changes since the last release (see "Versioning" in the README)
- `qscheduler openapi [--output FILE]` prints the OpenAPI spec without needing a database

- `GET /machine/{machine}/about` endpoint describing a machine's backend
- Task time limit: optional `time_limit_ms` on `POST /tasks`
- Project time limit is enforced on running project tasks (not only before submit)
- Machine option `--max-clock-skew` (default `4s`): running-time estimates never exceed the
  true time despite backend clock skew, so tasks are never stopped early
- Machine suspension: `POST /machine/{machine}/suspension` (with a message of up to 512
  characters) and `DELETE /machine/{machine}/suspension`. A suspended machine rejects new tasks
  and sessions, including tasks submitted into an open session, with HTTP 409 and the message.
  The `cancel` option sets what happens to existing work: `none` (it continues), `waiting`
  (tasks not yet submitted to the backend fail and all sessions close) or `all` (running tasks
  fail too, and their cancellation on the backend is requested).
- `GET /machine/{machine}/suspensions`: a machine's suspension history
- `machine remove` CLI command (`--force` also deletes the machine's tasks and sessions)
- Graceful shutdown on `SIGTERM`/`Ctrl+C`: pending state, including running session time, is
  written to the database before exit
- Notifiers: named HTTP callback targets independent of machines, managed with the
  `notifier add <NAME> --url <URL> [--token <TOKEN>]`, `notifier remove <NAME>` and
  `notifier list` CLI commands (names up to 512 characters; changes take effect after a restart).
  Removing a notifier turns off notifications for the tasks and sessions that used it.
- Optional `notify` parameter on `POST /tasks` and `POST /sessions` selecting the notifier for
  the task's completion event or the session's open/close events; an unknown notifier is
  rejected with HTTP 422
- `notify` field in `GET /tasks/{id}`, `GET /sessions/{id}` and the callback payloads

### Fixed

- `machine update` failed with a database error (`column "queue_size" does not exist`). It now
  replaces the machine's whole configuration (options left out are reset to their defaults) and
  its backend type.


## v0.1.0

Initial release

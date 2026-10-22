# Changelog

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
  `notifier list` CLI commands (names up to 12 characters; changes take effect after a restart).
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

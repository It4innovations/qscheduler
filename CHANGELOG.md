# Changelog

## [Unreleased]

### Changed

- Machine option `--session-check-interval` renamed to `--limit-check-interval` (it now also
  drives task time limit checks); its default changed from `5s` to `2s`.
- IQM backend option `--check-interval` renamed to `--poll-interval`.
- Existing machine configurations stored under the old names are still read.
- IQM: job polling never gives up (previously failed after 120 failed polls); unreachable
  jobs are polled with exponential backoff, and failed cancels are retried instead of dropped.

### Added

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

### Fixed

- `machine update` failed with a database error (`column "queue_size" does not exist`). It now
  replaces the machine's whole configuration (options left out are reset to their defaults) and
  its backend type.


## v0.1.0

Initial release

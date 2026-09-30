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


## v0.1.0

Initial release

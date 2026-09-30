-- Per-task execution time limit (effective value, resolved at submit time) and the moment the
-- backend first reported the task as running, from which the limit is counted.
ALTER TABLE tasks
    ADD COLUMN time_limit_ms BIGINT,
    ADD COLUMN started_at TIMESTAMPTZ;

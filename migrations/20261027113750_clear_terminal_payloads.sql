-- Payloads are only needed until a task reaches a terminal state; newer versions clear them
-- at that point, this releases those of tasks that ended before.
UPDATE tasks SET payload = NULL WHERE state <> 'waiting' AND payload IS NOT NULL;

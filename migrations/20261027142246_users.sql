-- Users get their own table; tasks reference it by id instead of repeating the name.
CREATE TABLE users (
    id SERIAL PRIMARY KEY CHECK (id <> 0),
    name VARCHAR(250) NOT NULL UNIQUE
);

INSERT INTO users (name) SELECT DISTINCT "user" FROM tasks;

ALTER TABLE tasks ADD COLUMN user_id INTEGER REFERENCES users(id);
UPDATE tasks t SET user_id = u.id FROM users u WHERE u.name = t."user";
ALTER TABLE tasks ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE tasks DROP COLUMN "user";

-- Every session has an owner. Sessions predating owners belong to the user of their first task;
-- a session without tasks has no known user, so it goes to a placeholder user.
ALTER TABLE sessions ADD COLUMN user_id INTEGER REFERENCES users(id);
UPDATE sessions s
SET user_id = (SELECT t.user_id FROM tasks t WHERE t.session_id = s.id ORDER BY t.id LIMIT 1);
INSERT INTO users (name)
SELECT 'unknown' WHERE EXISTS (SELECT 1 FROM sessions WHERE user_id IS NULL)
ON CONFLICT (name) DO NOTHING;
UPDATE sessions SET user_id = (SELECT id FROM users WHERE name = 'unknown') WHERE user_id IS NULL;
ALTER TABLE sessions ALTER COLUMN user_id SET NOT NULL;

-- Time consumed by a user per project and machine. Removing a machine removes its entries.
CREATE TABLE user_consumption (
    user_id INTEGER NOT NULL REFERENCES users(id),
    project_id INTEGER NOT NULL REFERENCES projects(id),
    machine_id INTEGER NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    consumed_ms BIGINT NOT NULL,
    PRIMARY KEY (user_id, project_id, machine_id)
);

INSERT INTO user_consumption (user_id, project_id, machine_id, consumed_ms)
SELECT c.user_id, c.project_id, c.machine_id, SUM(c.exec_time_ms)
FROM (
    SELECT user_id, project_id, machine_id, exec_time_ms FROM tasks WHERE session_id IS NULL
    UNION ALL
    SELECT user_id, project_id, machine_id, exec_time_ms FROM sessions
) c
WHERE c.exec_time_ms <> 0
GROUP BY c.user_id, c.project_id, c.machine_id;

-- A user is charged the time of the sessions it owns and of its tasks outside a session. Tasks
-- inside a session are not charged separately, as the session's owner already pays for its time.
CREATE FUNCTION update_user_consumption() RETURNS TRIGGER AS $$
DECLARE
    delta BIGINT;
BEGIN
    delta := COALESCE(NEW.exec_time_ms, 0) - COALESCE(OLD.exec_time_ms, 0);
    IF delta <> 0 THEN
        INSERT INTO user_consumption (user_id, project_id, machine_id, consumed_ms)
        VALUES (NEW.user_id, NEW.project_id, NEW.machine_id, delta)
        ON CONFLICT (user_id, project_id, machine_id)
        DO UPDATE SET consumed_ms = user_consumption.consumed_ms + EXCLUDED.consumed_ms;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER task_exec_time_changed_user
    AFTER UPDATE OF exec_time_ms ON tasks
    FOR EACH ROW WHEN (NEW.session_id IS NULL)
    EXECUTE FUNCTION update_user_consumption();

CREATE TRIGGER session_exec_time_changed_user
    AFTER UPDATE OF exec_time_ms ON sessions
    FOR EACH ROW EXECUTE FUNCTION update_user_consumption();

-- Size of the task's payload in bytes; it outlives the payload, which is cleared once the task ends.
-- Payloads that are already cleared have an unknown size, recorded as 0.
ALTER TABLE tasks ADD COLUMN payload_size BIGINT NOT NULL DEFAULT 0;
UPDATE tasks SET payload_size = octet_length(payload) WHERE payload IS NOT NULL;
ALTER TABLE tasks ALTER COLUMN payload_size DROP DEFAULT;

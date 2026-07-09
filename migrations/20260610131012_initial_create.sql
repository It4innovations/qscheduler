CREATE TYPE task_state AS ENUM ('waiting', 'finished', 'failed', 'cancelled');
CREATE TYPE machine_type AS ENUM ('iqm', 'test');

CREATE TABLE machines (
    id SERIAL PRIMARY KEY CHECK (id <> 0),
    name VARCHAR(250) NOT NULL UNIQUE,
    type machine_type NOT NULL,
    config JSON NOT NULL
);

CREATE TABLE projects (
    id SERIAL PRIMARY KEY CHECK (id <> 0),
    name VARCHAR(250) NOT NULL UNIQUE,
    consumed_ms BIGINT NOT NULL DEFAULT 0,
    limit_ms BIGINT NOT NULL,
    active BOOLEAN NOT NULL
);

CREATE TABLE sessions (
    id BIGSERIAL PRIMARY KEY CHECK (id <> 0),
    machine_id INTEGER NOT NULL REFERENCES machines(id),
    project_id INTEGER NOT NULL REFERENCES projects(id),
    time_limit_ms BIGINT NOT NULL,
    opened_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    exec_time_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tasks (
    id BIGSERIAL PRIMARY KEY CHECK (id <> 0),
    machine_id INTEGER NOT NULL REFERENCES machines(id),
    session_id BIGINT REFERENCES sessions(id),
    project_id INTEGER REFERENCES projects(id),
    backend_id TEXT,
    state task_state NOT NULL DEFAULT 'waiting',
    error TEXT,
    finished_at TIMESTAMPTZ,
    "user" VARCHAR(250) NOT NULL,
    payload BYTEA,
    result BYTEA,
    exec_time_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((session_id IS NULL) <> (project_id IS NULL))
);

CREATE FUNCTION update_project_consumed_ms() RETURNS TRIGGER AS $$
DECLARE
    delta BIGINT;
BEGIN
    delta := COALESCE(NEW.exec_time_ms, 0) - COALESCE(OLD.exec_time_ms, 0);
    IF delta <> 0 THEN
        UPDATE projects SET consumed_ms = consumed_ms + delta WHERE id = NEW.project_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER task_exec_time_changed
    AFTER UPDATE OF exec_time_ms ON tasks
    FOR EACH ROW EXECUTE FUNCTION update_project_consumed_ms();

CREATE TRIGGER session_exec_time_changed
    AFTER UPDATE OF exec_time_ms ON sessions
    FOR EACH ROW EXECUTE FUNCTION update_project_consumed_ms();

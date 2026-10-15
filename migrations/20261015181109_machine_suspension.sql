-- Machine suspensions: the full history, plus the active one (`resumed_at IS NULL`). A suspended
-- machine rejects new tasks and sessions; `cancel` records what was done to its existing work.
CREATE TYPE suspension_cancel AS ENUM ('none', 'waiting', 'all');

CREATE TABLE machine_suspensions (
    id BIGSERIAL PRIMARY KEY CHECK (id <> 0),
    machine_id INTEGER NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    message VARCHAR(512) NOT NULL,
    cancel suspension_cancel NOT NULL,
    suspended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resumed_at TIMESTAMPTZ,
    CHECK (resumed_at IS NULL OR resumed_at >= suspended_at)
);

-- At most one active suspension per machine
CREATE UNIQUE INDEX machine_suspensions_active
    ON machine_suspensions (machine_id) WHERE resumed_at IS NULL;

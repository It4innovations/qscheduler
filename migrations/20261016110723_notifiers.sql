-- Notifiers: named HTTP callback targets, independent of machines. Tasks and sessions opt in by
-- referencing one; removing a notifier turns off notifications for everything that used it.
CREATE TABLE notifiers (
    id SERIAL PRIMARY KEY CHECK (id <> 0),
    name VARCHAR(12) NOT NULL UNIQUE,
    url TEXT NOT NULL,
    token TEXT
);

ALTER TABLE tasks
    ADD COLUMN notifier_id INTEGER REFERENCES notifiers(id) ON DELETE SET NULL;

ALTER TABLE sessions
    ADD COLUMN notifier_id INTEGER REFERENCES notifiers(id) ON DELETE SET NULL;

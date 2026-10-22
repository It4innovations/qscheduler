CREATE TYPE task_kind AS ENUM ('circuit', 'pulse');

ALTER TABLE tasks ADD COLUMN kind task_kind NOT NULL DEFAULT 'circuit';

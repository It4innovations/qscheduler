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

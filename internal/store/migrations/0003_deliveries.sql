-- message_id is nullable from the start: spec 008's schedule-source firing
-- records are deliveries with no message, and SQLite cannot relax a NOT NULL
-- later without a table rebuild. UNIQUE treats NULLs as distinct, which is
-- exactly what several firing records per action need.
CREATE TABLE deliveries (
    id             TEXT PRIMARY KEY,           -- UUIDv7
    message_id     TEXT REFERENCES messages(id),
    action_name    TEXT NOT NULL,
    action_version INTEGER,                    -- NULL until claimed
    state          TEXT NOT NULL,              -- pending|claimed|done|failed|filtered|dead|cancelled
    attempt        INTEGER NOT NULL DEFAULT 0,
    not_before     TEXT,                       -- backoff gate, UTC
    pgid           INTEGER,                    -- live subprocess group
    stderr_tail    TEXT,                       -- last 8KiB, terminal states
    error          TEXT,
    claimed_at     TEXT,
    completed_at   TEXT,
    UNIQUE (message_id, action_name)
);

CREATE INDEX deliveries_ready ON deliveries(state, not_before);
CREATE INDEX deliveries_action ON deliveries(action_name, state);

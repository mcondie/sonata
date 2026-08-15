CREATE TABLE messages (
    id                    TEXT PRIMARY KEY,           -- UUIDv7
    queue                 TEXT NOT NULL,
    payload               TEXT NOT NULL,              -- JSON
    headers               TEXT NOT NULL DEFAULT '{}', -- JSON, includes hops
    trace_id              TEXT NOT NULL,
    origin_action         TEXT,
    origin_action_version INTEGER,
    origin_message_id     TEXT REFERENCES messages(id),
    created_at            TEXT NOT NULL               -- RFC3339 UTC
);

CREATE INDEX messages_queue ON messages(queue, id);
CREATE INDEX messages_trace ON messages(trace_id, id);

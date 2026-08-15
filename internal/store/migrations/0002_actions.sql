CREATE TABLE actions (
    name       TEXT NOT NULL,
    version    INTEGER NOT NULL,
    definition TEXT NOT NULL,              -- canonical JSON
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,              -- RFC3339 UTC
    PRIMARY KEY (name, version)
);

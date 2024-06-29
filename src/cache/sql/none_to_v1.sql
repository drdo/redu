CREATE TABLE metadata_integer (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID;
INSERT INTO metadata_integer (key, value) VALUES ('version', 1);

CREATE TABLE paths (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    component TEXT NOT NULL
);
CREATE UNIQUE INDEX paths_parent_component ON paths (parent_id, component);

-- The entries tables are sharded per snapshot and created dynamically

CREATE TABLE marks (path TEXT PRIMARY KEY) WITHOUT ROWID;

CREATE TABLE metadata_integer (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID;
INSERT INTO metadata_integer (key, value) VALUES ('version', 1);

CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY,
    hash TEXT UNIQUE NOT NULL
);
CREATE INDEX snapshots_hash ON snapshots (hash);

CREATE TABLE paths (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    component TEXT NOT NULL
);
CREATE UNIQUE INDEX paths_parent_component ON paths (parent_id, component);

CREATE TABLE entries (
    snapshot_id INTEGER NOT NULL,
    path_id INTEGER NOT NULL,
    size INTEGER NOT NULL,
    is_dir INTEGER NOT NULL,
    PRIMARY KEY (path_id, snapshot_id)
) WITHOUT ROWID;
CREATE INDEX entries_snapshot_id ON entries (snapshot_id); -- for deletion

CREATE TABLE marks (
    path TEXT PRIMARY KEY
) WITHOUT ROWID;

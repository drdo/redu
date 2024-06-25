CREATE TABLE metadata_integer (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
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
    PRIMARY KEY (snapshot_id, path_id)
);
CREATE INDEX entries_path_id ON entries (path_id);

CREATE TABLE marks (
    path TEXT PRIMARY KEY
);

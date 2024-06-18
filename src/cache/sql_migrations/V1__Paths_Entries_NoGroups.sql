DROP TABLE snapshots;
DROP TABLE files;
DROP TABLE directories;

CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY,
    hash TEXT UNIQUE NOT NULL
);
CREATE INDEX snapshots_hash ON snapshots (hash);

CREATE TABLE paths (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    parent TEXT GENERATED ALWAYS AS (path_parent(path))
);
CREATE INDEX paths_path on paths (path);
CREATE INDEX paths_parent ON paths (parent);

CREATE TABLE entries (
    snapshot_id INTEGER,
    path_id INTEGER,
    size INTEGER,
    is_dir INTEGER,
    PRIMARY KEY (snapshot_id, path_id)
);

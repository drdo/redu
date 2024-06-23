DROP INDEX files_path_parent;
DROP INDEX directories_path_parent;
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

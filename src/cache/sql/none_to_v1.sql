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

CREATE TABLE snapshots (
    hash TEXT PRIMARY KEY,
	time INTEGER,
    parent TEXT,
    tree TEXT NOT NULL,
    hostname TEXT,
    username TEXT,
    uid INTEGER,
    gid INTEGER,
    original_id TEXT,
    program_version TEXT
) WITHOUT ROWID;
CREATE TABLE snapshot_paths (
    hash TEXT,
    path TEXT,
    PRIMARY KEY (hash, path)
) WITHOUT ROWID;
CREATE TABLE snapshot_excludes (
    hash TEXT,
    path TEXT,
    PRIMARY KEY (hash, path)
) WITHOUT ROWID;
CREATE TABLE snapshot_tags (
    hash TEXT,
    tag TEXT,
    PRIMARY KEY (hash, tag)
) WITHOUT ROWID;

-- The entries tables are sharded per snapshot and created dynamically

CREATE TABLE marks (path TEXT PRIMARY KEY) WITHOUT ROWID;

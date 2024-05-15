PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

-- snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    "group" INTEGER NOT NULL
);

-- files
CREATE TABLE IF NOT EXISTS files (
    snapshot_group INTEGER,
    path TEXT,
    size INTEGER,
    parent TEXT GENERATED ALWAYS AS (path_parent(path)),
    PRIMARY KEY (snapshot_group, path)
);

CREATE INDEX IF NOT EXISTS files_path_parent
ON files (parent);

-- directories
CREATE TABLE IF NOT EXISTS directories (
    snapshot_group INTEGER,
    path TEXT,
    size INTEGER,
    parent TEXT GENERATED ALWAYS AS (path_parent(path)),
    PRIMARY KEY (snapshot_group, path)
);

CREATE INDEX IF NOT EXISTS directories_path_parent
ON directories (parent);

-- marks
CREATE TABLE IF NOT EXISTS marks (
    path TEXT PRIMARY KEY
);

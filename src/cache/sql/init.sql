-- files
CREATE TABLE IF NOT EXISTS files (
    snapshot TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER,
    parent TEXT GENERATED ALWAYS AS (path_parent(path)),
    PRIMARY KEY (snapshot, path)
);

CREATE INDEX IF NOT EXISTS files_path_parent
ON files (parent);

-- directories
CREATE TABLE IF NOT EXISTS directories (
    snapshot TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER,
    parent TEXT GENERATED ALWAYS AS (path_parent(path)),
    PRIMARY KEY (snapshot, path)
);

CREATE INDEX IF NOT EXISTS directories_path_parent
ON directories (parent);

-- snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY
);

-- marks
CREATE TABLE IF NOT EXISTS marks (
    path TEXT NOT NULL PRIMARY KEY
);
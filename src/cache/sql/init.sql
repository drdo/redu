PRAGMA foreign_keys = ON;

-- files
CREATE TABLE IF NOT EXISTS files (
    snapshot TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER,
    parent TEXT GENERATED ALWAYS AS (path_parent(path)),
    PRIMARY KEY (snapshot, path),
    FOREIGN KEY (snapshot) REFERENCES snapshots (id),
    FOREIGN KEY (snapshot, parent) REFERENCES directories (snapshot, path) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS files_path_parent
ON files (path_parent(path));

-- directories
CREATE TABLE IF NOT EXISTS directories (
    snapshot TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER,
    parent TEXT GENERATED ALWAYS AS (path_parent(path)),
    PRIMARY KEY (snapshot, path),
    FOREIGN KEY (snapshot) REFERENCES snapshots (id) DEFERRABLE INITIALLY DEFERRED
);

-- snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY
);

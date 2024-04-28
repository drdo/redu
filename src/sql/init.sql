PRAGMA foreign_keys = ON;

-- files
CREATE TABLE IF NOT EXISTS files (
    snapshot TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER,
    PRIMARY KEY (snapshot, path),
    FOREIGN KEY (snapshot) REFERENCES snapshots (id)
);

CREATE INDEX IF NOT EXISTS files_path_parent
ON files (path_parent(path));

-- snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY
);

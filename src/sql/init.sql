PRAGMA recursive_triggers = true;

-- files
CREATE TABLE IF NOT EXISTS files (
    snapshot TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER,
    PRIMARY KEY(snapshot, path)
);

CREATE TRIGGER IF NOT EXISTS files_insert_maintain_parent_size
AFTER INSERT ON files
FOR EACH ROW
BEGIN
    UPDATE files
    SET size = size + new.size
    WHERE snapshot = new.snapshot AND path = PATH_PARENT(new.path);
END;

CREATE TRIGGER IF NOT EXISTS files_update_maintain_parent_size
AFTER UPDATE ON files
FOR EACH ROW
BEGIN
    UPDATE files
    SET size = size - old.size
    WHERE snapshot = new.snapshot AND path = PATH_PARENT(old.path);

    UPDATE files
    SET size = size + new.size
    WHERE snapshot = new.snapshot AND path = PATH_PARENT(new.path);
END;

CREATE TRIGGER IF NOT EXISTS files_delete_maintain_parent_size
AFTER DELETE ON files
FOR EACH ROW
BEGIN
    UPDATE files
    SET size = size - old.size
    WHERE snapshot = old.snapshot AND path = PATH_PARENT(old.path);
END;

-- snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY
);
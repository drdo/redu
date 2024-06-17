use std::cell::Cell;
use std::path::Path;

use camino::{Utf8Path, Utf8PathBuf};
use log::trace;
use refinery::embed_migrations;
use rusqlite::{Connection, params, Row};
use rusqlite::functions::FunctionFlags;
use thiserror::Error;

use crate::cache::filetree::FileTree;
use crate::types::{Directory, Entry, File};

pub mod filetree;

embed_migrations!("src/cache/sql_migrations");

pub fn is_corruption_error(error: &OpenError) -> bool {
    const CORRUPTION_CODES: [rusqlite::ErrorCode; 2] = [
        rusqlite::ErrorCode::DatabaseCorrupt,
        rusqlite::ErrorCode::NotADatabase,
    ];
    match error {
        OpenError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error { code, .. },
            _,
        )) => CORRUPTION_CODES.contains(code),
        _ => false,
    }
}

#[derive(Debug)]
pub struct Cache {
    conn: Connection,
}

#[derive(Error, Debug)]
pub enum OpenError {
    #[error("Sqlite error")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Error running migrations")]
    Migration(#[from] refinery::Error),
}

impl Cache {
    pub fn open(file: &Path) -> Result<Self, OpenError> {
        let mut conn = Connection::open(&file)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        conn.create_scalar_function(
            "path_parent",
            1,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_INNOCUOUS,
            |ctx| {
                let path = Utf8Path::new(ctx.get_raw(0).as_str()?);
                let parent = path.parent().map(ToOwned::to_owned);
                Ok(parent.map(|p| p.to_string()))
            },
        )?;
        conn.profile(Some(|stmt, duration| {
            trace!("SQL {stmt} (took {duration:#?})")
        }));
        migrations::runner().run(&mut conn)?;
        Ok(Cache { conn })
    }

    pub fn get_snapshots(&self) -> Result<Vec<Box<str>>, rusqlite::Error> {
        self.conn
            .prepare("SELECT id FROM snapshots")?
            .query_map([], |row| row.get("id"))?
            .collect()
    }

    /// This returns the children files/directories of the given path.
    /// Each entry's size is the largest size of that file/directory across
    /// all snapshots.
    pub fn get_max_file_sizes(
        &self,
        path: Option<impl AsRef<Utf8Path>>,
    ) -> Result<Vec<Entry>, rusqlite::Error> {
        let aux = |row: &Row| {
            let child_path = {
                let child_path: Utf8PathBuf =
                    row.get::<&str, String>("path")?.into();
                path.as_ref()
                    .map(AsRef::as_ref)
                    .clone()
                    .map(|p| {
                        child_path.strip_prefix(p.as_std_path()).unwrap().into()
                    })
                    .unwrap_or(child_path)
            };
            let size = row.get("size")?;
            Ok(if row.get("is_dir")? {
                Entry::Directory(Directory { path: child_path, size })
            } else {
                Entry::File(File { path: child_path, size })
            })
        };

        match path {
            None => {
                let mut stmt = self.conn.prepare("\
                    WITH
                        fs AS (SELECT path, size, 0 as is_dir
                               FROM files
                               WHERE parent IS NULL),
                        dirs AS (SELECT path, size, 1 as is_dir
                                 FROM directories
                                 WHERE parent IS NULL)
                    SELECT path, max(size) as size, max(is_dir) as is_dir
                    FROM (SELECT * FROM fs UNION ALL SELECT * FROM dirs) AS entries
                    GROUP BY path
                    ORDER BY size DESC
                ")?;
                let rows = stmt.query_map([], aux)?;
                rows.collect()
            }
            Some(ref path) => {
                let mut stmt = self.conn.prepare("\
                    WITH
                        fs AS (SELECT path, size, 0 as is_dir
                               FROM files
                               WHERE parent = ?),
                        dirs AS (SELECT path, size, 1 as is_dir
                                 FROM directories
                                 WHERE parent = ?)
                    SELECT path, max(size) as size, max(is_dir) as is_dir
                    FROM (SELECT * FROM fs UNION ALL SELECT * FROM dirs) AS entries
                    GROUP BY path
                    ORDER BY size DESC
                ")?;
                let path_str = path.as_ref().as_str();
                let rows = stmt.query_map([path_str, path_str], aux)?;
                rows.collect()
            }
        }
    }

    pub fn save_snapshot_group(
        &mut self,
        group: SnapshotGroup,
    ) -> Result<(), rusqlite::Error> {
        let tx = self.conn.transaction()?;
        {
            let group_id: u64 = tx
                .query_row_and_then(
                    r#"SELECT max("group")+1 FROM snapshots"#,
                    [],
                    |row| row.get::<usize, Option<u64>>(0),
                )?
                .unwrap_or(0);
            let mut snapshot_stmt = tx.prepare(
                r#"
                INSERT INTO snapshots (id, "group")
                VALUES (?, ?)"#,
            )?;
            let mut file_stmt = tx.prepare(
                "
                INSERT INTO files (snapshot_group, path, size)
                VALUES (?, ?, ?)",
            )?;
            let mut dir_stmt = tx.prepare(
                "\
                INSERT INTO directories (snapshot_group, path, size)
                VALUES (?, ?, ?)",
            )?;

            for id in group.snapshots.iter() {
                snapshot_stmt.execute(params![id, group_id])?;
            }

            for entry in group.filetree.take().iter() {
                match entry {
                    Entry::File(File { path, size }) => file_stmt
                        .execute(params![group_id, path.into_string(), size])?,
                    Entry::Directory(Directory { path, size }) => dir_stmt
                        .execute(params![group_id, path.into_string(), size])?,
                };
            }
        }
        tx.commit()
    }

    pub fn get_snapshot_group(
        &self,
        id: impl AsRef<str>,
    ) -> Result<u64, rusqlite::Error> {
        self.conn.query_row_and_then(
            r#"SELECT "group" FROM snapshots WHERE id = ?"#,
            [id.as_ref()],
            |row| row.get(0),
        )
    }

    pub fn delete_group(&mut self, id: u64) -> Result<(), rusqlite::Error> {
        let tx = self.conn.transaction()?;
        tx.execute(r#"DELETE FROM snapshots WHERE "group" = ?"#, [id])?;
        tx.execute(r#"DELETE FROM files WHERE snapshot_group = ?"#, [id])?;
        tx.execute(r#"DELETE FROM directories WHERE snapshot_group = ?"#, [
            id,
        ])?;
        tx.commit()
    }

    // Marks ////////////////////////////////////////////////
    pub fn get_marks(&self) -> Result<Vec<Utf8PathBuf>, rusqlite::Error> {
        let mut stmt = self.conn.prepare("SELECT path FROM marks")?;
        let result = stmt
            .query_map([], |row| Ok(row.get::<&str, String>("path")?.into()))?
            .collect();
        result
    }

    pub fn upsert_mark(
        &mut self,
        path: &Utf8Path,
    ) -> Result<usize, rusqlite::Error> {
        self.conn.execute(
            "INSERT INTO marks (path) VALUES (?) \
             ON CONFLICT (path) DO NOTHING",
            [path.as_str()],
        )
    }

    pub fn delete_mark(
        &mut self,
        path: &Utf8Path,
    ) -> Result<usize, rusqlite::Error> {
        self.conn.execute("DELETE FROM marks WHERE path = ?", [path.as_str()])
    }

    pub fn delete_all_marks(&mut self) -> Result<usize, rusqlite::Error> {
        self.conn.execute("DELETE FROM marks", [])
    }
}

pub struct SnapshotGroup {
    snapshots: Vec<Box<str>>,
    filetree: Cell<FileTree>,
}

impl SnapshotGroup {
    pub fn new() -> Self {
        SnapshotGroup {
            snapshots: Vec::new(),
            filetree: Cell::new(FileTree::new()),
        }
    }

    pub fn add_snapshot(&mut self, id: Box<str>, filetree: FileTree) {
        self.snapshots.push(id);
        self.filetree.replace(self.filetree.take().merge(filetree));
    }

    pub fn count(&self) -> usize {
        self.snapshots.len()
    }
}

#[cfg(any(test, feature = "bench"))]
pub mod tests {
    use camino::Utf8PathBuf;

    use super::filetree::FileTree;

    pub struct PathGenerator {
        branching_factor: usize,
        state: Vec<(usize, Utf8PathBuf, usize)>,
    }

    impl PathGenerator {
        pub fn new(depth: usize, branching_factor: usize) -> Self {
            let mut state = Vec::with_capacity(depth);
            state.push((depth, Utf8PathBuf::new(), 0));
            PathGenerator { branching_factor, state }
        }
    }

    impl Iterator for PathGenerator {
        type Item = Utf8PathBuf;

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let (depth, prefix, child) = self.state.pop()?;
                if child < self.branching_factor {
                    let mut new_prefix = prefix.clone();
                    new_prefix.push(Utf8PathBuf::from(child.to_string()));
                    self.state.push((depth, prefix, child + 1));
                    if depth == 1 {
                        break (Some(new_prefix));
                    } else {
                        self.state.push((depth - 1, new_prefix, 0));
                    }
                }
            }
        }
    }

    pub fn generate_filetree(
        depth: usize,
        branching_factor: usize,
    ) -> FileTree {
        let mut filetree = FileTree::new();
        for path in PathGenerator::new(depth, branching_factor) {
            filetree.insert(&path, 1).unwrap();
        }
        filetree
    }
}

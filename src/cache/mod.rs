use std::path::Path;

use camino::{Utf8Path, Utf8PathBuf};
use log::trace;
use refinery::embed_migrations;
use rusqlite::{Connection, OptionalExtension, params, Row};
use rusqlite::functions::FunctionFlags;
use thiserror::Error;

use crate::cache::filetree::FileTree;
use crate::types::{Directory, Entry, File};

pub mod filetree;
#[cfg(any(test, feature = "bench"))] pub mod tests;

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
        Self::open_(file, refinery::Target::Latest)
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn open_with_target(
        file: &Path,
        migration_target: refinery::Target,
    ) -> Result<Self, OpenError> {
        Self::open_(file, migration_target)
    }

    fn open_(
        file: &Path,
        migration_target: refinery::Target,
    ) -> Result<Self, OpenError> {
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
                Ok(parent.and_then(|p| {
                    let s = p.to_string();
                    if s.is_empty() { None }
                    else { Some(s) }
                }))
            },
        )?;
        conn.profile(Some(|stmt, duration| {
            trace!("SQL {stmt} (took {duration:#?})")
        }));
        migrations::runner()
            .set_target(migration_target)
            .run(&mut conn)?;
        Ok(Cache { conn })
    }

    pub fn get_snapshots(&self) -> Result<Vec<Box<str>>, rusqlite::Error> {
        self.conn
            .prepare("SELECT hash FROM snapshots")?
            .query_map([], |row| row.get("hash"))?
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
                    SELECT path, max(size) as size, max(is_dir) as is_dir
                    FROM entries JOIN paths ON path_id = paths.id
                    WHERE parent IS NULL
                    GROUP BY path
                    ORDER BY size DESC
                ")?;
                let rows = stmt.query_map([], aux)?;
                rows.collect()
            }
            Some(ref path) => {
                let mut stmt = self.conn.prepare("\
                    SELECT path, max(size) as size, max(is_dir) as is_dir
                    FROM entries JOIN paths ON path_id = paths.id
                    WHERE parent = ?
                    GROUP BY path
                    ORDER BY size DESC
                ")?;
                let rows = stmt.query_map([path.as_ref().as_str()], aux)?;
                rows.collect()
            }
        }
    }

    pub fn save_snapshot(
        &mut self,
        hash: impl AsRef<str>,
        filetree: FileTree,
    ) -> Result<(), rusqlite::Error> {
        let id = hash.as_ref();
        let tx = self.conn.transaction()?;
        {
            let snapshot_id = tx.query_row(
                "INSERT INTO snapshots (hash) VALUES (?) RETURNING (id)",
                [hash.as_ref()],
                |row| row.get::<usize, u64>(0),
            )?;
            let mut paths_stmt = tx.prepare("\
                INSERT INTO paths (path)
                VALUES (?)
                ON CONFLICT (path) DO NOTHING
            ")?;
            let mut entries_stmt = tx.prepare("\
                INSERT INTO entries (snapshot_id, path_id, size, is_dir)
                VALUES (?, (SELECT id FROM paths WHERE path = ?), ?, ?)
            ")?;
            for entry in filetree.iter() {
                let (path, size, is_dir) = match entry {
                    Entry::File(File { path, size }) => (path, size, false),
                    Entry::Directory(Directory { path, size }) => (path, size, true),
                };
                paths_stmt.execute([path.as_str()])?;
                entries_stmt.execute(params![snapshot_id, path.as_str(), size, is_dir])?;
            }
        }
        tx.commit()
    }

    pub fn delete_snapshot(&mut self, hash: impl AsRef<str>) -> Result<(), rusqlite::Error> {
        let hash = hash.as_ref();
        let tx = self.conn.transaction()?;
        if let Some(snapshot_id) = tx
            .query_row(
                "DELETE FROM snapshots WHERE hash = ? RETURNING (id)",
                [hash],
                |row| row.get::<usize, u64>(0),
            )
            .optional()?
        {
            tx.execute("DELETE FROM entries WHERE snapshot_id = ?", [snapshot_id])?;
        }
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

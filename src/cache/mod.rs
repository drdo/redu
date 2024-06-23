use std::path::Path;

use camino::{Utf8Path, Utf8PathBuf};
use log::trace;
use refinery::{embed_migrations, Migration, Runner, Target};
use rusqlite::{
    functions::FunctionFlags, params, Connection, OptionalExtension, Row,
};
use thiserror::Error;

use crate::cache::filetree::SizeTree;

pub mod filetree;
#[cfg(any(test, feature = "bench"))]
pub mod tests;

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

pub struct Migrator {
    conn: Connection,
    runner: Runner,
    need_to_migrate: bool,
}

impl Migrator {
    pub fn migrate(mut self) -> Result<Cache, refinery::Error> {
        self.runner.run(&mut self.conn)?;
        Ok(Cache { conn: self.conn })
    }

    pub fn need_to_migrate(&self) -> bool {
        self.need_to_migrate
    }
}

impl Cache {
    pub fn open(file: &Path) -> Result<Migrator, OpenError> {
        Self::open_(file, Target::Latest)
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn open_with_target(
        file: &Path,
        migration_target: Target,
    ) -> Result<Migrator, OpenError> {
        Self::open_(file, migration_target)
    }

    fn open_(
        file: &Path,
        migration_target: Target,
    ) -> Result<Migrator, OpenError> {
        let mut conn = Connection::open(file)?;
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
                    if s.is_empty() {
                        None
                    } else {
                        Some(s)
                    }
                }))
            },
        )?;
        conn.profile(Some(|stmt, duration| {
            trace!("SQL {stmt} (took {duration:#?})")
        }));
        let runner = migrations::runner().set_target(migration_target);
        let need_to_migrate = {
            let all_migrations =
                runner.get_migrations().iter().map(Migration::version);
            let target_version = match migration_target {
                Target::Latest => all_migrations.max(),
                Target::Fake => all_migrations.max(),
                Target::Version(v) => Some(v),
                Target::FakeVersion(v) => Some(v),
            };
            let applied_migrations = {
                let stmt = "\
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='refinery_schema_history'";
                let refinery_schema_history_exists =
                    conn.query_row(stmt, [], |_| Ok(())).optional()?.is_some();
                if refinery_schema_history_exists {
                    runner.get_applied_migrations(&mut conn)?
                } else {
                    vec![]
                }
            };
            target_version
                .map(|t| !applied_migrations.iter().any(|m| t == m.version()))
                .unwrap_or(false)
        };
        Ok(Migrator { conn, runner, need_to_migrate })
    }

    pub fn get_snapshots(&self) -> Result<Vec<Box<str>>, rusqlite::Error> {
        self.conn
            .prepare("SELECT hash FROM snapshots")?
            .query_map([], |row| row.get("hash"))?
            .collect()
    }

    pub fn get_parent_id(
        &self,
        path_id: PathId,
    ) -> Result<Option<Option<PathId>>, rusqlite::Error> {
        self.conn
            .query_row(
                "SELECT parent_id FROM paths WHERE id = ?",
                [path_id.0],
                |row| row.get("parent_id").map(raw_u64_to_o_path_id),
            )
            .optional()
    }

    /// This is not very efficient, it does one query per path component.
    /// Mainly used for testing convenience.
    pub fn get_path_id_by_path(
        &self,
        path: &Utf8Path,
    ) -> Result<Option<PathId>, rusqlite::Error> {
        let mut path_id = None;
        for component in path {
            path_id = self
                .conn
                .query_row(
                    "SELECT id FROM paths
                     WHERE parent_id = ? AND component = ?",
                    params![o_path_id_to_raw_u64(path_id), component],
                    |row| row.get(0).map(PathId),
                )
                .optional()?;
            if path_id.is_none() {
                return Ok(None);
            }
        }
        Ok(path_id)
    }

    /// This returns the children files/directories of the given path.
    /// Each entry's size is the largest size of that file/directory across
    /// all snapshots.
    pub fn get_max_file_sizes(
        &self,
        path_id: Option<PathId>,
    ) -> Result<Vec<Entry>, rusqlite::Error> {
        let aux = |row: &Row| {
            Ok(Entry {
                path_id: PathId(row.get("path_id")?),
                parent_id: raw_u64_to_o_path_id(row.get("parent_id")?),
                component: row.get("component")?,
                size: row.get("size")?,
                is_dir: row.get("is_dir")?,
            })
        };
        let mut stmt = self.conn.prepare(
            "SELECT
                 path_id,
                 parent_id,
                 component,
                 max(size) as size,
                 max(is_dir) as is_dir
             FROM entries JOIN paths ON path_id = paths.id
             WHERE parent_id = ?
             GROUP BY path_id
             ORDER BY size DESC",
        )?;
        let rows = stmt.query_map([o_path_id_to_raw_u64(path_id)], aux)?;
        rows.collect()
    }

    pub fn save_snapshot(
        &mut self,
        hash: impl AsRef<str>,
        tree: SizeTree,
    ) -> Result<(), rusqlite::Error> {
        let tx = self.conn.transaction()?;
        {
            let snapshot_id = tx.query_row(
                "INSERT INTO snapshots (hash) VALUES (?) RETURNING (id)",
                [hash.as_ref()],
                |row| row.get::<usize, u64>(0),
            )?;
            let mut paths_stmt = tx.prepare(
                "INSERT INTO paths (parent_id, component)
                 VALUES (?, ?)
                 ON CONFLICT (parent_id, component) DO NOTHING",
            )?;
            let mut paths_query = tx.prepare(
                "SELECT id FROM paths WHERE parent_id = ? AND component = ?",
            )?;
            let mut entries_stmt = tx.prepare(
                "INSERT INTO entries (snapshot_id, path_id, size, is_dir)
                 VALUES (?, ?, ?, ?)",
            )?;

            tree.0.traverse_with_context(
                |id_stack, component, size, is_dir| {
                    let parent_id = id_stack.last().copied();
                    paths_stmt.execute(params![
                        o_path_id_to_raw_u64(parent_id),
                        component,
                    ])?;
                    let path_id = paths_query.query_row(
                        params![o_path_id_to_raw_u64(parent_id), component],
                        |row| row.get(0).map(PathId),
                    )?;
                    entries_stmt.execute(params![
                        snapshot_id,
                        path_id.0,
                        size,
                        is_dir
                    ])?;
                    Ok::<PathId, rusqlite::Error>(path_id)
                },
            )?;
        }
        tx.commit()
    }

    pub fn delete_snapshot(
        &mut self,
        hash: impl AsRef<str>,
    ) -> Result<(), rusqlite::Error> {
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
            tx.execute("DELETE FROM entries WHERE snapshot_id = ?", [
                snapshot_id,
            ])?;
        }
        tx.commit()
    }

    // Marks ////////////////////////////////////////////////
    pub fn get_marks(&self) -> Result<Vec<Utf8PathBuf>, rusqlite::Error> {
        let mut stmt = self.conn.prepare("SELECT path FROM marks")?;
        #[allow(clippy::let_and_return)]
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

// A PathId should never be 0.
// This is reserved for the absolute root and should match None
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct PathId(u64);

fn raw_u64_to_o_path_id(id: u64) -> Option<PathId> {
    if id == 0 {
        None
    } else {
        Some(PathId(id))
    }
}

fn o_path_id_to_raw_u64(path_id: Option<PathId>) -> u64 {
    path_id.map(|path_id| path_id.0).unwrap_or(0)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub path_id: PathId,
    pub parent_id: Option<PathId>,
    pub component: String,
    pub size: usize,
    pub is_dir: bool,
}

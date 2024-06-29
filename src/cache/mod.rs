use std::{collections::HashSet, path::Path};

use camino::{Utf8Path, Utf8PathBuf};
use log::trace;
use rusqlite::{
    functions::FunctionFlags, params, Connection, OptionalExtension, Row,
};
use thiserror::Error;

use crate::cache::filetree::SizeTree;

pub mod filetree;
#[cfg(any(test, feature = "bench"))]
pub mod tests;

#[derive(Debug)]
pub struct Cache {
    conn: Connection,
}

#[derive(Error, Debug)]
pub enum OpenError {
    #[error("Sqlite error")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Error running migrations")]
    Migration(#[from] MigrationError),
}

impl Cache {
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
    #[cfg(any(test, feature = "bench"))]
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
    ) -> Result<usize, rusqlite::Error> {
        let mut file_count = 0;
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
                    file_count += 1;
                    Ok::<PathId, rusqlite::Error>(path_id)
                },
            )?;
        }
        tx.commit()?;
        Ok(file_count)
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

////////// Migrations //////////////////////////////////////////////////////////
type VersionId = u64;

struct Migration {
    old: Option<VersionId>,
    new: VersionId,
    resync_necessary: bool,
    migration_fun: fn(&mut Connection) -> Result<(), rusqlite::Error>,
}

const INTEGER_METADATA_TABLE: &str = "metadata_integer";

pub const LATEST_VERSION: VersionId = 1;

const MIGRATIONS: [Migration; 3] = [
    Migration {
        old: None,
        new: 0,
        resync_necessary: false,
        migration_fun: migrate_none_to_v0,
    },
    Migration {
        old: None,
        new: 1,
        resync_necessary: false,
        migration_fun: migrate_none_to_v1,
    },
    Migration {
        old: Some(0),
        new: 1,
        resync_necessary: true,
        migration_fun: migrate_v0_to_v1,
    },
];

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("Invalid state, unable to determine version")]
    UnableToDetermineVersion,
    #[error("Do not know how to migrate from the current version")]
    NoMigrationPath { old: Option<VersionId>, new: VersionId },
    #[error("Sqlite error")]
    Sql(#[from] rusqlite::Error),
}

pub struct Migrator<'a> {
    conn: Connection,
    migration: Option<&'a Migration>,
}

impl<'a> Migrator<'a> {
    pub fn open(file: &Path) -> Result<Self, MigrationError> {
        Self::open_(file, LATEST_VERSION)
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn open_with_target(
        file: &Path,
        target: VersionId,
    ) -> Result<Self, MigrationError> {
        Self::open_(file, target)
    }

    // We don't try to find multi step migrations.
    fn open_(file: &Path, target: VersionId) -> Result<Self, MigrationError> {
        let mut conn = Connection::open(file)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        // This is only used in V0
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
        let current = determine_version(&conn)?;
        if current == Some(target) {
            return Ok(Migrator { conn, migration: None });
        }
        if let Some(migration) =
            MIGRATIONS.iter().find(|m| m.old == current && m.new == target)
        {
            Ok(Migrator { conn, migration: Some(migration) })
        } else {
            Err(MigrationError::NoMigrationPath { old: current, new: target })
        }
    }

    pub fn migrate(mut self) -> Result<Cache, rusqlite::Error> {
        if let Some(migration) = self.migration {
            (migration.migration_fun)(&mut self.conn)?;
        }
        Ok(Cache { conn: self.conn })
    }

    pub fn need_to_migrate(&self) -> Option<(Option<VersionId>, VersionId)> {
        self.migration.map(|m| (m.old, m.new))
    }

    pub fn resync_necessary(&self) -> bool {
        self.migration.map(|m| m.resync_necessary).unwrap_or(false)
    }
}

fn migrate_none_to_v0(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute_batch(include_str!("sql/none_to_v0.sql"))?;
    tx.commit()
}

fn migrate_none_to_v1(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute_batch(include_str!("sql/none_to_v1.sql"))?;
    tx.commit()
}

fn migrate_v0_to_v1(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute_batch(include_str!("sql/v0_to_v1.sql"))?;
    tx.commit()
}

fn determine_version(
    conn: &Connection,
) -> Result<Option<VersionId>, MigrationError> {
    const V0_TABLES: [&str; 4] = ["snapshots", "files", "directories", "marks"];

    let tables = get_tables(conn)?;
    if tables.contains(INTEGER_METADATA_TABLE) {
        conn.query_row(
            &format!(
                "SELECT value FROM {INTEGER_METADATA_TABLE}
                 WHERE key = 'version'"
            ),
            [],
            |row| row.get::<usize, VersionId>(0),
        )
        .optional()?
        .map(|v| Ok(Some(v)))
        .unwrap_or(Err(MigrationError::UnableToDetermineVersion))
    } else if V0_TABLES.iter().all(|t| tables.contains(*t)) {
        // The V0 tables are present but without a metadata table
        // Assume V0 (pre-versioning schema).
        Ok(Some(0))
    } else {
        // No metadata table and no V0 tables, assume a fresh db.
        Ok(None)
    }
}

fn get_tables(conn: &Connection) -> Result<HashSet<String>, rusqlite::Error> {
    let mut stmt =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let names = stmt.query_map([], |row| row.get(0))?;
    names.collect()
}

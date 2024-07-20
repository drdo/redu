use std::{collections::HashSet, path::Path};

use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use log::trace;
use rusqlite::{
    functions::FunctionFlags, params, types::FromSqlError, Connection,
    OptionalExtension, Row,
};
use thiserror::Error;

use crate::{cache::filetree::SizeTree, restic::Snapshot};

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

#[derive(Error, Debug)]
pub enum Error {
    #[error("SQL error")]
    Sql(#[from] rusqlite::Error),
    #[error("Unexpected SQL datatype")]
    FromSqlError(#[from] FromSqlError),
    #[error("Error parsing JSON")]
    Json(#[from] serde_json::Error),
    #[error("Exhausted timestamp precision (a couple hundred thousand years after the epoch).")]
    ExhaustedTimestampPrecision,
}

impl Cache {
    pub fn get_snapshots(&self) -> Result<Vec<Snapshot>, Error> {
        self.conn
            .prepare(
                "SELECT \
                     hash, \
                     time, \
                     parent, \
                     tree, \
                     hostname, \
                     username, \
                     uid, \
                     gid, \
                     original_id, \
                     program_version, \
                     coalesce((SELECT json_group_array(path) FROM snapshot_paths WHERE hash = snapshots.hash), json_array()) as paths, \
                     coalesce((SELECT json_group_array(path) FROM snapshot_excludes WHERE hash = snapshots.hash), json_array()) as excludes, \
                     coalesce((SELECT json_group_array(tag) FROM snapshot_tags WHERE hash = snapshots.hash), json_array()) as tags \
                 FROM snapshots")?
            .query_and_then([], |row|
                Ok(Snapshot {
                    id: row.get("hash")?,
                    time: timestamp_to_datetime(row.get("time")?)?,
                    parent: row.get("parent")?,
                    tree: row.get("tree")?,
                    paths: serde_json::from_str(row.get_ref("paths")?.as_str()?)?,
                    hostname: row.get("hostname")?,
                    username: row.get("username")?,
                    uid: row.get("uid")?,
                    gid: row.get("gid")?,
                    excludes: serde_json::from_str(row.get_ref("excludes")?.as_str()?)?,
                    tags: serde_json::from_str(row.get_ref("tags")?.as_str()?)?,
                    original_id: row.get("original_id")?,
                    program_version: row.get("program_version")?,
                })
            )?
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
                    "SELECT id FROM paths \
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
    pub fn get_entries(
        &self,
        path_id: Option<PathId>,
    ) -> Result<Vec<Entry>, rusqlite::Error> {
        let aux = |row: &Row| {
            Ok(Entry {
                path_id: PathId(row.get("path_id")?),
                component: row.get("component")?,
                size: row.get("size")?,
                is_dir: row.get("is_dir")?,
            })
        };
        let raw_path_id = o_path_id_to_raw_u64(path_id);
        let cte_stmt_string = get_tables(&self.conn)?
            .into_iter()
            .filter(|name| name.starts_with("entries_"))
            .map(|table| {
                format!(
                    "SELECT \
                         path_id, \
                         component, \
                         size, \
                         is_dir \
                     FROM \"{table}\" JOIN paths ON path_id = paths.id \
                     WHERE parent_id = {raw_path_id}\n"
                )
            })
            .intersperse(String::from(" UNION ALL "))
            .collect::<String>();
        if cte_stmt_string.is_empty() {
            return Ok(vec![]);
        }
        let mut stmt = self.conn.prepare(&format!(
            "WITH rich_entries AS ({cte_stmt_string}) \
             SELECT \
                 path_id, \
                 component, \
                 max(size) as size, \
                 max(is_dir) as is_dir \
             FROM rich_entries \
             GROUP BY path_id \
             ORDER BY size DESC",
        ))?;
        let rows = stmt.query_map([], aux)?;
        rows.collect()
    }

    pub fn get_entry_details(
        &self,
        path_id: PathId,
    ) -> Result<EntryDetails, Error> {
        let aux = |row: &Row| -> Result<EntryDetails, Error> {
            Ok(EntryDetails {
                max_size: row.get("max_size")?,
                max_size_snapshot_hash: row.get("max_size_snapshot_hash")?,
                first_seen: timestamp_to_datetime(row.get("first_seen")?)?,
                first_seen_snapshot_hash: row
                    .get("first_seen_snapshot_hash")?,
                last_seen: timestamp_to_datetime(row.get("last_seen")?)?,
                last_seen_snapshot_hash: row.get("last_seen_snapshot_hash")?,
            })
        };
        let raw_path_id = path_id.0;
        let rich_entries_cte = get_tables(&self.conn)?
            .iter()
            .filter_map(|name| name.strip_prefix("entries_"))
            .map(|snapshot_hash| {
                format!(
                    "SELECT \
                         hash, \
                         size, \
                         time \
                     FROM \"entries_{snapshot_hash}\" \
                         JOIN paths ON path_id = paths.id \
                         JOIN snapshots ON hash = '{snapshot_hash}' \
                     WHERE path_id = {raw_path_id}\n"
                )
            })
            .intersperse(String::from(" UNION ALL "))
            .collect::<String>();
        let query = format!(
            "WITH \
                rich_entries AS ({rich_entries_cte}), \
                first_seen AS (
                    SELECT hash, time
                    FROM rich_entries
                    ORDER BY time ASC
                    LIMIT 1), \
                last_seen AS (
                    SELECT hash, time
                    FROM rich_entries
                    ORDER BY time DESC
                    LIMIT 1), \
                max_size AS (
                    SELECT hash, size
                    FROM rich_entries
                    ORDER BY size DESC, time DESC
                    LIMIT 1) \
             SELECT \
                 max_size.size AS max_size, \
                 max_size.hash AS max_size_snapshot_hash, \
                 first_seen.time AS first_seen, \
                 first_seen.hash as first_seen_snapshot_hash, \
                 last_seen.time AS last_seen, \
                 last_seen.hash as last_seen_snapshot_hash \
             FROM max_size
             JOIN first_seen ON 1=1
             JOIN last_seen ON 1=1"
        );
        self.conn.query_row_and_then(&query, [], aux)
    }

    pub fn save_snapshot(
        &mut self,
        snapshot: &Snapshot,
        tree: SizeTree,
    ) -> Result<usize, rusqlite::Error> {
        let mut file_count = 0;
        let tx = self.conn.transaction()?;
        {
            tx.execute(
                "INSERT INTO snapshots ( \
                     hash, \
                     time, \
                     parent, \
                     tree, \
                     hostname, \
                     username, \
                     uid, \
                     gid, \
                     original_id, \
                     program_version \
                 ) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    snapshot.id,
                    datetime_to_timestamp(snapshot.time),
                    snapshot.parent,
                    snapshot.tree,
                    snapshot.hostname,
                    snapshot.username,
                    snapshot.uid,
                    snapshot.gid,
                    snapshot.original_id,
                    snapshot.program_version
                ],
            )?;
            let mut snapshot_paths_stmt = tx.prepare(
                "INSERT INTO snapshot_paths (hash, path) VALUES (?, ?)",
            )?;
            for path in snapshot.paths.iter() {
                snapshot_paths_stmt.execute([&snapshot.id, path])?;
            }
            let mut snapshot_excludes_stmt = tx.prepare(
                "INSERT INTO snapshot_excludes (hash, path) VALUES (?, ?)",
            )?;
            for path in snapshot.excludes.iter() {
                snapshot_excludes_stmt.execute([&snapshot.id, path])?;
            }
            let mut snapshot_tags_stmt = tx.prepare(
                "INSERT INTO snapshot_tags (hash, tag) VALUES (?, ?)",
            )?;
            for path in snapshot.tags.iter() {
                snapshot_tags_stmt.execute([&snapshot.id, path])?;
            }
        }
        {
            let entries_table = format!("entries_{}", &snapshot.id);
            tx.execute(
                &format!(
                    "CREATE TABLE \"{entries_table}\" (
                         path_id INTEGER PRIMARY KEY,
                         size INTEGER NOT NULL,
                         is_dir INTEGER NOT NULL,
                         FOREIGN KEY (path_id) REFERENCES paths (id)
                     )"
                ),
                [],
            )?;
            let mut entries_stmt = tx.prepare(&format!(
                "INSERT INTO \"{entries_table}\" (path_id, size, is_dir) \
                 VALUES (?, ?, ?)",
            ))?;

            let mut paths_stmt = tx.prepare(
                "INSERT INTO paths (parent_id, component)
                 VALUES (?, ?)
                 ON CONFLICT (parent_id, component) DO NOTHING",
            )?;
            let mut paths_query = tx.prepare(
                "SELECT id FROM paths WHERE parent_id = ? AND component = ?",
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
                    entries_stmt.execute(params![path_id.0, size, is_dir])?;
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
        tx.execute("DELETE FROM snapshots WHERE hash = ?", [hash])?;
        tx.execute("DELETE FROM snapshot_paths WHERE hash = ?", [hash])?;
        tx.execute("DELETE FROM snapshot_excludes WHERE hash = ?", [hash])?;
        tx.execute("DELETE FROM snapshot_tags WHERE hash = ?", [hash])?;
        tx.execute(&format!("DROP TABLE IF EXISTS \"entries_{}\"", hash), [])?;
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
    pub component: String,
    pub size: usize,
    pub is_dir: bool,
}

#[derive(Clone, Debug)]
pub struct EntryDetails {
    pub max_size: usize,
    pub max_size_snapshot_hash: String,
    pub first_seen: DateTime<Utc>,
    pub first_seen_snapshot_hash: String,
    pub last_seen: DateTime<Utc>,
    pub last_seen_snapshot_hash: String,
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

////////// Misc ////////////////////////////////////////////////////////////////
fn timestamp_to_datetime(timestamp: i64) -> Result<DateTime<Utc>, Error> {
    DateTime::from_timestamp_micros(timestamp)
        .map(Ok)
        .unwrap_or(Err(Error::ExhaustedTimestampPrecision))
}

fn datetime_to_timestamp(datetime: DateTime<Utc>) -> i64 {
    datetime.timestamp_micros()
}

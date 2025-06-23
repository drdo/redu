use std::{
    cmp::{max, Reverse},
    collections::{HashMap, HashSet},
    path::Path,
};

use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use log::trace;
use rusqlite::{
    functions::FunctionFlags,
    params,
    trace::{TraceEvent, TraceEventCodes},
    types::FromSqlError,
    Connection, OptionalExtension,
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

    fn entries_tables(
        &self,
    ) -> Result<impl Iterator<Item = String>, rusqlite::Error> {
        Ok(get_tables(&self.conn)?
            .into_iter()
            .filter(|name| name.starts_with("entries_")))
    }

    /// This returns the children files/directories of the given path.
    /// Each entry's size is the largest size of that file/directory across
    /// all snapshots.
    pub fn get_entries(
        &self,
        path_id: Option<PathId>,
    ) -> Result<Vec<Entry>, rusqlite::Error> {
        let raw_path_id = o_path_id_to_raw_u64(path_id);
        let mut entries: Vec<Entry> = Vec::new();
        let mut index: HashMap<PathId, usize> = HashMap::new();
        for table in self.entries_tables()? {
            let stmt_str = format!(
                "SELECT \
                     path_id, \
                     component, \
                     size, \
                     is_dir \
                 FROM \"{table}\" JOIN paths ON path_id = paths.id \
                 WHERE parent_id = {raw_path_id}\n",
            );
            let mut stmt = self.conn.prepare(&stmt_str)?;
            let rows = stmt.query_map([], |row| {
                Ok(Entry {
                    path_id: PathId(row.get("path_id")?),
                    component: row.get("component")?,
                    size: row.get("size")?,
                    is_dir: row.get("is_dir")?,
                })
            })?;
            for row in rows {
                let row = row?;
                let path_id = row.path_id;
                match index.get(&path_id) {
                    None => {
                        entries.push(row);
                        index.insert(path_id, entries.len() - 1);
                    }
                    Some(i) => {
                        let entry = &mut entries[*i];
                        entry.size = max(entry.size, row.size);
                        entry.is_dir = entry.is_dir || row.is_dir;
                    }
                }
            }
        }
        entries.sort_by_key(|e| Reverse(e.size));
        Ok(entries)
    }

    pub fn get_entry_details(
        &self,
        path_id: PathId,
    ) -> Result<Option<EntryDetails>, Error> {
        let raw_path_id = path_id.0;
        let run_query = |table: &str| -> Result<
            Option<(String, usize, DateTime<Utc>)>,
            Error,
        > {
            let snapshot_hash = table.strip_prefix("entries_").unwrap();
            let stmt_str = format!(
                "SELECT \
                     hash, \
                     size, \
                     time \
                 FROM \"{table}\" \
                     JOIN paths ON path_id = paths.id \
                     JOIN snapshots ON hash = '{snapshot_hash}' \
                 WHERE path_id = {raw_path_id}\n"
            );
            let mut stmt = self.conn.prepare(&stmt_str)?;
            stmt.query_row([], |row| {
                Ok((row.get("hash")?, row.get("size")?, row.get("time")?))
            })
            .optional()?
            .map(|(hash, size, timestamp)| {
                Ok((hash, size, timestamp_to_datetime(timestamp)?))
            })
            .transpose()
        };

        let mut entries_tables = self.entries_tables()?;
        let mut details = loop {
            match entries_tables.next() {
                None => return Ok(None),
                Some(table) => {
                    if let Some((hash, size, time)) = run_query(&table)? {
                        break EntryDetails {
                            max_size: size,
                            max_size_snapshot_hash: hash.clone(),
                            first_seen: time,
                            first_seen_snapshot_hash: hash.clone(),
                            last_seen: time,
                            last_seen_snapshot_hash: hash,
                        };
                    }
                }
            }
        };
        let mut max_size_time = details.first_seen; // Time of the max_size snapshot
        for table in entries_tables {
            if let Some((hash, size, time)) = run_query(&table)? {
                if size > details.max_size
                    || (size == details.max_size && time > max_size_time)
                {
                    details.max_size = size;
                    details.max_size_snapshot_hash = hash.clone();
                    max_size_time = time;
                }
                if time < details.first_seen {
                    details.first_seen = time;
                    details.first_seen_snapshot_hash = hash.clone();
                }
                if time > details.last_seen {
                    details.last_seen = time;
                    details.last_seen_snapshot_hash = hash;
                }
            }
        }
        Ok(Some(details))
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
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
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
        let conn = Connection::open(file)?;
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
        conn.trace_v2(
            TraceEventCodes::SQLITE_TRACE_PROFILE,
            Some(|e| {
                if let TraceEvent::Profile(stmt, duration) = e {
                    trace!("SQL {} (took {:#?})", stmt.sql(), duration);
                }
            }),
        );
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
    tx.execute_batch(include_str!("cache/sql/none_to_v0.sql"))?;
    tx.commit()
}

fn migrate_none_to_v1(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute_batch(include_str!("cache/sql/none_to_v1.sql"))?;
    tx.commit()
}

fn migrate_v0_to_v1(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute_batch(include_str!("cache/sql/v0_to_v1.sql"))?;
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

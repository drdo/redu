use camino::{Utf8Path, Utf8PathBuf};
use directories::ProjectDirs;
use rusqlite::{Connection, params, Row, Transaction};
use rusqlite::functions::FunctionFlags;
use uuid::Uuid;

use crate::cache::filetree::FileTree;
use crate::types::{Directory, Entry, File};

mod filetree;

// TODO: Some queries are vulnerable to SQL injection
// if the restic binary attacks us

#[derive(Debug)]
pub enum OpenError {
    DetermineDirectory(String),
    CreateDirectory(std::io::Error),
    Sql(rusqlite::Error),
}

impl From<rusqlite::Error> for OpenError {
    fn from(value: rusqlite::Error) -> Self { OpenError::Sql(value) }
}

#[derive(Debug)]
pub struct Cache {
    filename: Box<str>,
    conn: Connection,
}

impl Cache {
    pub fn open(name: &str) -> Result<Self, OpenError> {
        let dir = ProjectDirs::from("eu", "drdo", "dorestic")
            .ok_or_else(|| OpenError::DetermineDirectory(
                "could not determine appropriate cache location".to_owned()
            ))?
            .cache_dir()
            .to_string_lossy()
            .into_owned();
        let filename = format!("{dir}/{name}.db");
        std::fs::create_dir_all(&dir).map_err(OpenError::CreateDirectory)?;
        let conn = Connection::open(&filename)?;
        conn.create_scalar_function(
            "path_parent",
            1,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_INNOCUOUS,
            |ctx| {
                let path = Utf8Path::new(ctx.get_raw(0).as_str()?);
                let parent = path
                    .parent()
                    .map(ToOwned::to_owned);
                Ok(parent.map(|p| p.to_string()))
            }
        )?;
        conn.execute_batch(include_str!("sql/init.sql"))?;
        Ok(Cache { filename: filename.into(), conn })
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn get_snapshots(
        &self,
    ) -> Result<Vec<Box<str>>, rusqlite::Error>
    {
        self.conn
            .prepare("SELECT id FROM snapshots")?
            .query_and_then([], |row| Ok(row.get("id")?))?
            .collect()
    }

    /// This returns the children files/directories of the given path.
    /// Each entry's size is the largest size of that file/directory across
    /// all snapshots.
    pub fn get_max_file_sizes(
        &self,
        path: Option<impl AsRef<Utf8Path>>,
    ) -> Result<Vec<Entry>, rusqlite::Error>
    {
        let aux = |row: &Row| {
            let child_path = {
                let child_path: Utf8PathBuf = row.get::<&str, String>("path")?.into();
                path.as_ref()
                    .map(AsRef::as_ref)
                    .clone()
                    .map(|p| child_path.strip_prefix(p.as_std_path()).unwrap().into())
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
                let rows = stmt.query_and_then([], aux)?;
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
                let rows = stmt.query_and_then([path_str, path_str], aux)?;
                rows.collect()
            }
        }
    }

    pub fn start_snapshot(
        &mut self,
        id: &str
        ) -> Result<SnapshotHandle, rusqlite::Error>
    {
        let table = Box::from(format!("finish-{}-{}", id, Uuid::new_v4()));
        let tx = self.conn.transaction()?;
        let stmt = format!("\
            CREATE TEMP TABLE \"{}\" ( \
                path TEXT NOT NULL PRIMARY KEY, \
                size INTEGER NOT NULL \
            )",
                           table);
        tx.execute(&stmt, [])?;
        Ok(SnapshotHandle {
            snapshot: Box::from(id),
            table,
            tx,
        })
    }

    pub fn delete_snapshot(&mut self, id: &str) -> Result<(), rusqlite::Error> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM files WHERE snapshot = ?", params![id])?;
        tx.execute("DELETE FROM snapshots WHERE id = ?", params![id])?;
        tx.commit()
    }
}

////////////////////////////////////////////////////////////////////////////////
pub struct SnapshotHandle<'cache> {
    snapshot: Box<str>,
    table: Box<str>,
    tx: Transaction<'cache>,
}

impl<'cache> SnapshotHandle<'cache> {
    pub fn insert_file(
        &self,
        path: &Utf8Path,
        size: usize
    ) -> Result<(), rusqlite::Error>
    {
        let stmt = format!(
            "INSERT INTO \"{}\" (path, size) VALUES (?, ?)",
            &self.table);
        self.tx
            .execute(&stmt, params![path.as_str(), size])
            .map(|_| ())
    }

    pub fn finish(self) -> Result<(), rusqlite::Error> {
        let tree = { // Build tree
            let mut tree = FileTree::new();
            let mut stmt = self.tx.prepare(&format!(
                "SELECT path, size FROM \"{}\"",
                self.table))?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let path = row.get_ref("path")?.as_str()?;
                let size = row.get("size")?;
                tree.insert(Utf8Path::new(path), size);
            }
            tree
        };
        self.tx.execute(&format!("DROP TABLE \"{}\"", &self.table), [])?;
        self.tx.execute(
            "INSERT INTO snapshots (id) VALUES (?)",
            [&self.snapshot])?;
        {
            let mut file_stmt = self.tx.prepare("INSERT INTO files VALUES (?, ?, ?)")?;
            let mut dir_stmt = self.tx.prepare("INSERT INTO directories VALUES (?, ?, ?)")?;
            for entry in tree.iter() { match entry {
                Entry::File(File{ path, size}) =>
                    file_stmt.execute(params![&self.snapshot, path.into_string(), size])?,
                Entry::Directory(Directory{ path, size}) =>
                    dir_stmt.execute(params![&self.snapshot, path.into_string(), size])?,
            };}
        }
        self.tx.commit()
    }
}

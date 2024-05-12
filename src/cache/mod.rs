use camino::{Utf8Path, Utf8PathBuf};
use directories::ProjectDirs;
use log::trace;
use rusqlite::{Connection, params, Row};
use rusqlite::functions::FunctionFlags;

use crate::cache::filetree::FileTree;
use crate::types::{Directory, Entry, File};

pub mod filetree;

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
        let mut conn = Connection::open(&filename)?;
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
        conn.profile(Some(|stmt, duration| {
            trace!("SQL {stmt} (took {duration:#?})")
        }));
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

    pub fn save_snapshot(
        &mut self,
        snapshot: &str,
        filetree: &FileTree
    ) -> Result<(), rusqlite::Error>
    {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO snapshots (id) VALUES (?)",
            [&snapshot])?;
        {
            let mut file_stmt = tx.prepare("INSERT INTO files VALUES (?, ?, ?)")?;
            let mut dir_stmt = tx.prepare("INSERT INTO directories VALUES (?, ?, ?)")?;
            for entry in filetree.iter() {
                match entry {
                    Entry::File(File{ path, size}) =>
                        file_stmt.execute(
                            params![&snapshot, path.into_string(), size])?,
                    Entry::Directory(Directory{ path, size}) =>
                        dir_stmt.execute(
                            params![&snapshot, path.into_string(), size])?,
                };
            }
        }
        tx.commit()
    }

    pub fn delete_snapshot(&mut self, id: &str) -> Result<(), rusqlite::Error> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM files WHERE snapshot = ?", params![id])?;
        tx.execute("DELETE FROM directories WHERE snapshot = ?", params![id])?;
        tx.execute("DELETE FROM snapshots WHERE id = ?", params![id])?;
        tx.commit()
    }

    pub fn get_marks(&self) -> Result<Vec<Utf8PathBuf>, rusqlite::Error> {
        let mut stmt = self.conn.prepare("SELECT path FROM marks")?;
        let result = stmt
            .query_map([], |row| Ok(row.get::<&str, String>("path")?.into()))?
            .collect();
        result
    }

    pub fn upsert_mark(&mut self, path: &Utf8Path) -> Result<usize, rusqlite::Error> {
        self.conn.execute(
            "INSERT INTO marks (path) VALUES (?) \
             ON CONFLICT (path) DO NOTHING",
            [path.as_str()]
        )
    }

    pub fn delete_mark(&mut self, path: &Utf8Path) -> Result<usize, rusqlite::Error> {
        self.conn.execute(
            "DELETE FROM marks WHERE path = ?",
            [path.as_str()]
        )
    }

    pub fn delete_all_marks(&mut self) -> Result<usize, rusqlite::Error> {
        self.conn.execute("DELETE FROM marks", [])
    }
}

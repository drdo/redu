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

#[cfg(any(test, feature = "bench"))]
pub mod tests {
    use camino::Utf8PathBuf;

    use super::filetree::FileTree;

    fn string_range(n: usize) -> impl Iterator<Item=String> {
        (0..n).map(|i| i.to_string())
    }

    pub struct PathGenerator {
        branching_factor: usize,
        state: Vec<Box<dyn Iterator<Item=String>>>,
    }

    impl PathGenerator {
        pub fn new(depth: usize, branching_factor: usize) -> Self {
            let mut state = Vec::with_capacity(depth);
            for _ in 0..depth {
                let it: Box<dyn Iterator<Item=_>> =
                    Box::new(string_range(branching_factor));
                state.push(it);
            }
            PathGenerator { branching_factor, state }
        }

        /// Reset all items before and including this index
        fn reset(&mut self, index: usize) {
            for i in 0..=index {
                self.state[i] = Box::new(string_range(self.branching_factor));
            }
        }
    }

    impl Iterator for PathGenerator {
        type Item = Utf8PathBuf;

        fn next(&mut self) -> Option<Self::Item> {
            let mut path: Utf8PathBuf = Utf8PathBuf::new();
            let mut i = 0;
            loop {
                if i == self.state.len() {
                    // This iteration is done, produce value.
                    break Some(path);
                } else if let Some(component) = self.state[i].next() {
                    path.push(component);
                    i += 1;
                } else if i+1 == self.state.len() {
                    // It's None in the last component. The iterator is finished.
                    break None;
                } else {
                    // It's None in some intermediate component.
                    self.reset(i);
                }
            }
        }
    }

    pub fn generate_filetree(depth: usize, branching_factor: usize) -> FileTree {
        let mut filetree = FileTree::new();
        for path in PathGenerator::new(depth, branching_factor) {
            filetree.insert(&path, 1);
        }
        filetree
    }
}


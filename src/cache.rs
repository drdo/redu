use std::collections::{hash_map, HashMap};

use camino::{Utf8Path, Utf8PathBuf};
use directories::ProjectDirs;
use rusqlite::{Connection, params, Transaction};
use rusqlite::functions::FunctionFlags;
use uuid::Uuid;

use crate::types::Snapshot;

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
    ) -> Result<Vec<Snapshot>, rusqlite::Error>
    {
        self.conn
            .prepare("SELECT id FROM snapshots")?
            .query_and_then([], |row| Ok(Snapshot {
                id: row.get("id")?
            }))?
            .collect()
    }

    /// This returns the children files/directories of the given path.
    /// Each entry's size is the largest size of that file/directory across
    /// all snapshots.
    pub fn get_max_file_sizes(
        &self,
        path: Option<&Utf8Path>,
    ) -> Result<Vec<(Box<str>, usize)>, rusqlite::Error>
    {
        let (prefix, mut stmt, params) = match path {
            None => {
                let stmt = self.conn.prepare(
                    "SELECT path, max(size) as size \
                     FROM files \
                     WHERE path_parent(path) IS NULL \
                     GROUP BY path")?;
                ("".to_owned(), stmt, params![])
            }
            Some(path) => {
                let stmt = self.conn.prepare(
                    "SELECT path, max(size) as size \
                     FROM files \
                     WHERE path_parent(path) = ? \
                     GROUP BY path")?;
                (path.to_string(), stmt, params![path.as_str()])
            }
        };
        let rows = stmt
            .query_and_then(params, |row| {
                let path = row.get::<&str, String>("path")?
                    .strip_prefix(&prefix)
                    .unwrap()
                    .into();
                let size = row.get("size")?;
                Ok((path, size)) })?;
        rows.collect()
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
            let mut stmt = self.tx.prepare("INSERT INTO files VALUES (?, ?, ?)")?;
            for (path, size) in tree.iter() {
                stmt.execute(params![&self.snapshot, path.into_string(), size])?;
            }
        }
        self.tx.commit()
    }
}

////////////////////////////////////////////////////////////////////////////////
struct FileTree {
    size: usize,
    children: HashMap<Box<str>, FileTree>,
}

impl FileTree {
    fn new() -> FileTree {
        FileTree {
            size: 0,
            children: HashMap::new(),
        }
    }

    fn insert(&mut self, path: &Utf8Path, size: usize) {
        let mut current = self;
        for c in path.iter() {
            current.size += size;
            current = current.children.entry(Box::from(c)).or_insert(FileTree::new());
        }
        current.size = size;
    }

    fn iter<'a>(&'a self) -> FileTreeIter<'a> {
        FileTreeIter {
            stack: vec![FileTreeIterNode {
                path: None,
                size: self.size,
                children: self.children.iter(),
            }]
        }
    }
}

struct FileTreeIter<'a> {
    stack: Vec<FileTreeIterNode<'a>>,
}

struct FileTreeIterNode<'a> {
    path: Option<Utf8PathBuf>,
    size: usize,
    children: hash_map::Iter<'a, Box<str>, FileTree>,
}

impl<'a> FileTreeIterNode<'a> {
    fn path_extend(&self, path: &(impl AsRef<str> + ?Sized)) -> Utf8PathBuf {
        let mut extended_path = self.path.clone().unwrap_or_default();
        extended_path.push(Utf8Path::new(path));
        extended_path
    }
}

impl<'a> Iterator for FileTreeIter<'a> {
    type Item = (Utf8PathBuf, usize);

    fn next(&mut self) -> Option<Self::Item> {
        // Depth first traversal
        loop {
            match self.stack.pop() {
                None => break None,
                Some(mut node) => {
                    match node.children.next() {
                        None =>
                            break node.path.map(|p| (p, node.size)),
                        Some((name, tree)) => {
                            let new_node = {
                                FileTreeIterNode {
                                    path: Some(node.path_extend(name)),
                                    size: tree.size,
                                    children: tree.children.iter(),
                                }
                            };
                            self.stack.push(node);
                            self.stack.push(new_node);
                        }
                    }
                }
            }
        }
    }
}


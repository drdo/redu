use std::path::PathBuf;

use directories::ProjectDirs;
use rusqlite::{Connection, params};
use rusqlite::functions::FunctionFlags;

use crate::types::{File, Snapshot};

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
    filename: String,
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
        conn.execute_batch(include_str!("sql/init.sql"))?;
        conn.create_scalar_function(
            "PATH_PARENT",
            1,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_INNOCUOUS,
            |ctx| {
                let path = PathBuf::from(ctx.get::<String>(0)?);
                let parent = path
                    .parent()
                    .map(ToOwned::to_owned);
                Ok(parent.map(|p| p.to_string_lossy().into_owned()))
            }
        )?;
        Ok(Cache { filename, conn })
    }

    pub fn filename(&self) -> &str {
        self.filename.as_str()
    }

    pub fn get_snapshots(
        &self
    ) -> Result<Vec<Snapshot>, rusqlite::Error>
    {
        self.conn
            .prepare("SELECT id FROM snapshots")?
            .query_and_then([], |row| Ok(Snapshot {
                id: row.get("id")?
            }))?
            .collect()
    }

    pub fn upsert_file(&self, file: &File) -> Result<(), rusqlite::Error> {
        let path = file.path.join("/");
        self.conn
            .prepare("INSERT OR REPLACE INTO files (snapshot, path, size) VALUES (?, ?, ?)")?
            .execute(params![&*file.snapshot, path, file.size])
            .map(|_| ())
    }

    pub fn finish_snapshot(
        &self,
        snapshot: &Snapshot,
    ) -> Result<(), rusqlite::Error>
    {
        self.conn
            .prepare("INSERT INTO snapshots (id) VALUES (?)")?
            .execute(params![snapshot.id])
            .map(|_| ())
    }

    pub fn delete_snapshot(&mut self, id: &str) -> Result<(), rusqlite::Error>
    {
        let tx = self.conn.transaction()?;
        tx
            .prepare("DELETE FROM files WHERE snapshot = ?")?
            .execute(params![id])?;
        tx
            .prepare("DELETE FROM snapshots WHERE id = ?")?
            .execute(params![id])?;
        tx.commit()
    }
}

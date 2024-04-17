use std::pin::Pin;
use directories::ProjectDirs;

use futures::Stream;
use sqlx::{Connection, Row, SqliteConnection};
use sqlx::sqlite::{SqliteQueryResult, SqliteRow};

use crate::types::{File, Snapshot};

#[derive(Debug)]
pub enum Error {
    DirectoryError(String),
    SqlError(sqlx::Error),
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self { Error::SqlError(value) }
}

#[derive(Debug)]
pub struct Cache {
    file: String,
    conn: SqliteConnection,
}

impl Cache {
    pub async fn open(name: &str) -> Result<Self, Error>
    {
        let dir = ProjectDirs::from("eu", "drdo", "dorestic")
            .ok_or_else(|| Error::DirectoryError("could not determine appropriate cache location".to_string()))?
            .cache_dir()
            .to_string_lossy()
            .into_owned();
        let file = format!("{dir}/{name}.db");
        std::fs::create_dir_all(&dir).map_err(|e| Error::DirectoryError(e.to_string()))?;
        let mut conn = SqliteConnection::connect(format!("sqlite://{file}?mode=rwc").as_str()).await?;
        let mut transaction = conn.begin().await?;
        let stmts = [
            "CREATE TABLE IF NOT EXISTS snapshots (\
                id TEXT PRIMARY KEY\
            )",
            "CREATE TABLE IF NOT EXISTS files (\
                snapshot TEXT NOT NULL,\
                path TEXT NOT NULL,\
                size INTEGER NOT NULL,\
                PRIMARY KEY (snapshot, path)\
            )",
            "CREATE TABLE IF NOT EXISTS max_files_cache (\
                path TEXT PRIMARY KEY,\
                size INTEGER NOT NULL\
            )",
            "CREATE TABLE IF NOT EXISTS flags (\
                name TEXT PRIMARY KEY\
            )",
        ];
        for stmt in stmts {
            sqlx::query(stmt).execute(transaction.as_mut()).await?;
        }
        transaction.commit().await?;
        Ok(Cache{file, conn})
    }

    pub fn file(&self) -> &str { self.file.as_str() }

    pub async fn get_snapshots<'c>(&mut self) -> Result<Vec<Snapshot>, Error>
    {
        Ok(sqlx::query_as("SELECT id FROM snapshots")
            .fetch_all(&mut self.conn)
            .await?)
    }

    pub async fn add_file<'a>(
        &mut self,
        entry: &File,
    ) -> Result<(), Error>
    {
        let mut transaction = self.conn.begin().await?;
        let stmts = [
            sqlx::query("INSERT OR REPLACE INTO files (snapshot, path, size) VALUES (?, ?, ?)")
                .bind(&*entry.snapshot)
                .bind(entry.path.join("/"))
                .bind(entry.size as i64),
            sqlx::query("INSERT OR REPLACE INTO flags (name) VALUES ('max_files_cache_dirty')"),
        ];
        for stmt in stmts { stmt.execute(transaction.as_mut()).await?; }
        Ok(transaction.commit().await?)
    }

    pub async fn finish_snapshot<'c>(
        &mut self,
        id: &str,
    ) -> Result<SqliteQueryResult, Error>
    {
        Ok(sqlx::query("INSERT INTO snapshots (id) VALUES (?)")
            .bind(id)
            .execute(&mut self.conn)
            .await?)
    }

    pub async fn delete_snapshot<'c>(
        &mut self,
        id: &str,
    ) -> Result<(), Error>
    {
        let mut transaction = self.conn.begin().await?;
        let stmts = [
            sqlx::query("DELETE FROM files WHERE snapshot = ?").bind(id),
            sqlx::query("DELETE FROM snapshots WHERE id = ?").bind(id),
            sqlx::query("INSERT OR REPLACE INTO flags (name) VALUES ('max_files_cache_dirty')")
        ];
        for stmt in stmts { stmt.execute(transaction.as_mut()).await?; }
        Ok(transaction.commit().await?)
    }

    pub async fn get_max_file_sizes<'s, 'c: 's>(
        &'c mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(String, u64), sqlx::Error>> + Send + 's>>, Error>
    {
        let is_cache_dirty = sqlx::query("SELECT name FROM flags WHERE name = 'max_files_cache_dirty'")
            .fetch_optional(&mut self.conn)
            .await
            .map(|row| row.map_or(false, |_| true))?;
        if is_cache_dirty {
            let mut transaction = self.conn.begin().await?;
            sqlx::query(
                "INSERT OR REPLACE INTO max_files_cache (path, size) \
             SELECT path, MAX(size) AS size \
             FROM files JOIN snapshots ON files.snapshot = snapshots.id \
             GROUP BY path \
             ORDER BY path"
            )
                .execute(&mut *transaction)
                .await?;
            sqlx::query("DELETE FROM flags WHERE name = 'max_files_cache_dirty'")
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
        Ok(sqlx::query("SELECT path, size FROM max_files_cache")
            .map(|row: SqliteRow| (row.get::<String, _>("path"), row.get::<i64, _>("size") as u64))
            .fetch(&mut self.conn)
        )
    }
}

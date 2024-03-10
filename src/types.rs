use serde::Deserialize;
use sqlx::FromRow;

#[derive(Debug, Deserialize, FromRow, PartialEq, Eq)]
pub struct Snapshot {
    pub id: String,
}

#[derive(Debug)]
pub struct File<'a> {
    pub snapshot: &'a str,
    pub path: String,
    pub size: u64,
}

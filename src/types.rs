use std::sync::Arc;

use serde::Deserialize;
use sqlx::FromRow;

#[derive(Debug, Deserialize, FromRow, PartialEq, Eq)]
pub struct Snapshot {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct File {
    pub path: Arc<[String]>,
    pub snapshot: Arc<str>,
    pub size: u64,
}

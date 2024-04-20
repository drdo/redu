use std::sync::Arc;

use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub id: Box<str>,
}

#[derive(Clone, Debug)]
pub struct File {
    pub snapshot: Arc<str>,
    pub path: Arc<[String]>,
    pub size: u64,
}

use std::rc::Rc;
use camino::Utf8PathBuf;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub id: Box<str>,
}

#[derive(Clone, Debug)]
pub struct File {
    pub snapshot: Rc<str>,
    pub path: Utf8PathBuf,
    pub size: usize,
}

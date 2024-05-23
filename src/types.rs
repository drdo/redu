use camino::{Utf8Path, Utf8PathBuf};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Snapshot {
    pub id: Box<str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    pub path: Utf8PathBuf,
    pub size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Directory {
    pub path: Utf8PathBuf,
    pub size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    Directory(Directory),
    File(File),
}

impl Entry {
    pub fn size(&self) -> usize {
        match self {
            Entry::Directory(d) => d.size,
            Entry::File(f) => f.size,
        }
    }

    pub fn path(&self) -> &Utf8Path {
        match self {
            Entry::Directory(d) => &d.path,
            Entry::File(f) => &f.path,
        }
    }
}

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use crate::types::File;

pub struct Files {
    children: HashMap<Box<str>, Files>,
    items: HashMap<Arc<str>, u64>,
}

pub struct FileData {
    pub snapshot: Arc<str>,
    pub size: u64,
}

impl Files {
    pub fn new() -> Self {
        Files {
            children: HashMap::new(),
            items: HashMap::new(),
        }
    }

    pub fn get<S, P>(&self, path: P) -> Box<dyn Iterator<Item=FileData> + '_>
        where
            S: Borrow<str>,
            P: IntoIterator<Item=S>,
    {
        const EMPTY: &[FileData] = &[];
        let mut current = self;
        for segment in path.into_iter() {
            if let Some(next) = current.children.get(segment.borrow()) {
                current = next;
            } else {
                return Box::new(EMPTY.iter().map(|filedata| FileData {
                    snapshot: filedata.snapshot.clone(),
                    size: filedata.size,
                }))
            }
        }
        Box::new(current.items.iter().map(|(snapshot, size)| FileData {
            snapshot: snapshot.clone(),
            size: *size,
        }))
    }

    pub fn insert(&mut self, file: &File) {
        let mut current = self;
        for segment in file.path.iter() {
            current = current.children
                .entry(segment.clone().into())
                .or_insert_with(|| Files::new());
        }
    }
}

pub struct State {
    pub path: Vec<String>,
    pub files: HashMap<Box<str>, FileData>,
}

impl State {
    pub fn new() -> Self {
        State {
            path: Vec::new(),
            files: HashMap::new(),
        }
    }
}
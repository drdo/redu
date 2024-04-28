use std::borrow::Borrow;
use std::cmp;
use std::collections::HashMap;

use camino::Utf8PathBuf;

use crate::types::File;

pub struct Files {
    children: HashMap<Box<str>, Files>,
    items: HashMap<Box<str>, u64>,
}

pub struct FileData {
    pub snapshot: Box<str>,
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
                .entry(segment.into())
                .or_insert_with(|| Files::new());
        }
    }
}

pub struct State {
    pub path: Option<Utf8PathBuf>,
    pub files: Vec<(Box<str>, usize)>,
    pub selected: Option<usize>,
}

impl State {
    pub fn move_selection(&mut self, delta: isize) {
        let len = match self.files.len() {
            0 => return,
            n => n,
        };
        let selected = self.selected.get_or_insert(0);
        *selected = (*selected as isize + delta).rem_euclid(len as isize) as usize;
    }
}
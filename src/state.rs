use std::borrow::{Borrow, Cow};
use std::collections::HashMap;

use camino::{Utf8Path, Utf8PathBuf};

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
    path: Option<Utf8PathBuf>,
    files: Vec<(Utf8PathBuf, usize)>,
    selected: Option<usize>,
    pub offset: usize,
}

impl State {
    pub fn new<'a, P>(
        path: Option<P>,
        files: Vec<(Utf8PathBuf, usize)>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        State {
            path: path.map(|p| p.into().into_owned()),
            files,
            selected: None,
            offset: 0,
        }
    }

    pub fn set_files<'a, P>(
        &mut self,
        path: Option<P>,
        files: Vec<(Utf8PathBuf, usize)>,
    )
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        self.path = path.map(|p| p.into().into_owned());
        self.files = files;
    }
    pub fn files(&self) -> &[(Utf8PathBuf, usize)] {
        &self.files
    }

    pub fn path(&self) -> Option<&Utf8Path> {
        self.path.as_deref()
    }

    pub fn selected_file(&self) -> Option<(&Utf8Path, usize)> {
        self.selected.map(|i| {
            let (name, size) = &self.files[i];
            (&**name, *size)
        })
    }

    pub fn selected(&self) -> Option<usize> {
        self.selected
    }

    pub fn is_selected(&self, index: usize) -> bool {
        Some(index) == self.selected
    }

    pub fn move_selection(&mut self, delta: isize) {
        let len = match self.files.len() {
            0 => return,
            n => n,
        };
        let selected = self.selected.get_or_insert(0);
        *selected = (*selected as isize + delta).rem_euclid(len as isize) as usize;
    }
}
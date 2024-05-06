use std::collections::{hash_map, HashMap};

use camino::{Utf8Path, Utf8PathBuf};

use crate::types::{Directory, Entry, File};

pub struct FileTree {
    size: usize,
    children: HashMap<Box<str>, FileTree>,
}

impl FileTree {
    pub fn new() -> FileTree {
        FileTree {
            size: 0,
            children: HashMap::new(),
        }
    }

    pub fn insert(&mut self, path: &Utf8Path, size: usize) {
        let mut current = self;
        for c in path.iter() {
            current.size += size;
            current = current.children.entry(Box::from(c)).or_insert(FileTree::new());
        }
        current.size = size;
    }

    pub fn iter(&self) -> Iter {
        Iter {
            stack: vec![Breadcrumb {
                path: None,
                size: self.size,
                children: self.children.iter(),
            }]
        }
    }
}

pub struct Iter<'a> {
    stack: Vec<Breadcrumb<'a>>,
}

struct Breadcrumb<'a> {
    path: Option<Utf8PathBuf>,
    size: usize,
    children: hash_map::Iter<'a, Box<str>, FileTree>,
}

impl<'a> Breadcrumb<'a> {
    fn path_extend(&self, path: &(impl AsRef<str> + ?Sized)) -> Utf8PathBuf {
        let mut extended_path = self.path.clone().unwrap_or_default();
        extended_path.push(Utf8Path::new(path));
        extended_path
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // Depth first traversal
        loop {
            match self.stack.pop() {
                None => break None,
                Some(mut breadcrumb) => {
                    match breadcrumb.children.next() {
                        None =>
                            break breadcrumb.path.map(|p| {
                                let dir = Directory{ path: p, size: breadcrumb.size };
                                Entry::Directory(dir)
                            }),
                        Some((name, tree)) => {
                            let new_path = breadcrumb.path_extend(name);
                            self.stack.push(breadcrumb);
                            if tree.children.is_empty() {
                                break Some(Entry::File(File { path: new_path, size: tree.size }))
                            } else {
                                let new_breadcrumb = {
                                    Breadcrumb {
                                        path: Some(new_path),
                                        size: tree.size,
                                        children: tree.children.iter(),
                                    }
                                };
                                self.stack.push(new_breadcrumb);
                            }
                        }
                    }
                }
            }
        }
    }
}
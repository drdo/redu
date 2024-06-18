use std::cmp::max;
use std::collections::{hash_map, HashMap};

use camino::{Utf8Path, Utf8PathBuf};

use crate::types::{Directory, Entry, File};

#[derive(Debug, Default, Eq, PartialEq)]
pub struct FileTree {
    size: usize,
    children: HashMap<Box<str>, FileTree>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct EntryExistsError;

impl FileTree {
    pub fn new() -> FileTree {
        FileTree { size: 0, children: HashMap::new() }
    }

    pub fn insert(
        &mut self,
        path: &Utf8Path,
        size: usize,
    ) -> Result<(), EntryExistsError> {
        let (mut breadcrumbs, remaining) = {
            let (breadcrumbs, remaining) = self.find(path);
            (breadcrumbs, remaining.map(Ok).unwrap_or(Err(EntryExistsError))?)
        };

        for node in breadcrumbs.iter_mut() {
            unsafe { (**node).size += size };
        }
        let mut current = unsafe { &mut **breadcrumbs.last().unwrap() };
        for c in remaining.iter() {
            current =
                current.children.entry(Box::from(c)).or_insert(FileTree::new());
            current.size = size;
        }
        Ok(())
    }

    pub fn merge(self, other: FileTree) -> Self {
        fn sorted_children(filetree: FileTree) -> Vec<(Box<str>, FileTree)> {
            let mut children =
                filetree.children.into_iter().collect::<Vec<_>>();
            children
                .sort_unstable_by(|(name0, _), (name1, _)| name0.cmp(name1));
            children
        }

        let size = max(self.size, other.size);
        let mut self_children = sorted_children(self).into_iter();
        let mut other_children = sorted_children(other).into_iter();
        let mut children = HashMap::new();
        loop {
            match (self_children.next(), other_children.next()) {
                (Some((name0, tree0)), Some((name1, tree1))) => {
                    if name0 == name1 {
                        children.insert(name0, tree0.merge(tree1));
                    } else {
                        children.insert(name0, tree0);
                        children.insert(name1, tree1);
                    }
                }
                (None, Some((name, tree))) => {
                    children.insert(name, tree);
                }
                (Some((name, tree)), None) => {
                    children.insert(name, tree);
                }
                (None, None) => {
                    break;
                }
            }
        }
        FileTree { size, children }
    }

    pub fn iter(&self) -> Iter {
        Iter {
            stack: vec![Breadcrumb {
                path: None,
                size: self.size,
                children: self.children.iter(),
            }],
        }
    }

    /// Returns the breadcrumbs of the largest prefix of the path.
    /// If the file is in the tree the last breadcrumb will be the file itself.
    /// Does not modify self at all.
    /// The cdr is the remaining path that did not match, if any.
    fn find(
        &mut self,
        path: &Utf8Path,
    ) -> (Vec<*mut FileTree>, Option<Utf8PathBuf>) {
        let mut breadcrumbs: Vec<*mut FileTree> = vec![self];
        let mut prefix = Utf8PathBuf::new();
        for c in path.iter() {
            let current = unsafe { &mut **breadcrumbs.last().unwrap() };
            match current.children.get_mut(c) {
                Some(next) => {
                    breadcrumbs.push(next);
                    prefix.push(c);
                }
                None => break,
            }
        }
        let remaining_path = {
            let suffix = path.strip_prefix(prefix).unwrap();
            if suffix.as_str().is_empty() {
                None
            } else {
                Some(suffix.to_path_buf())
            }
        };
        (breadcrumbs, remaining_path)
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
                Some(mut breadcrumb) => match breadcrumb.children.next() {
                    None =>
                        break breadcrumb.path.map(|p| {
                            let dir =
                                Directory { path: p, size: breadcrumb.size };
                            Entry::Directory(dir)
                        }),
                    Some((name, tree)) => {
                        let new_path = breadcrumb.path_extend(name);
                        self.stack.push(breadcrumb);
                        if tree.children.is_empty() {
                            break Some(Entry::File(File {
                                path: new_path,
                                size: tree.size,
                            }));
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
                },
            }
        }
    }
}

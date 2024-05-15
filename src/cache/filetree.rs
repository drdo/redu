use std::collections::{hash_map, HashMap};

use camino::{Utf8Path, Utf8PathBuf};

use crate::types::{Directory, Entry, File};

pub struct FileTree {
    size: usize,
    children: HashMap<Box<str>, FileTree>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct EntryExistsError;

impl FileTree {
    pub fn new() -> FileTree {
        FileTree {
            size: 0,
            children: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        path: &Utf8Path,
        size: usize,
    ) -> Result<(), EntryExistsError>
    {
        let (mut breadcrumbs, remaining) = {
            let (breadcrumbs, remaining) = self.find(path);
            (breadcrumbs, remaining.map(Ok).unwrap_or(Err(EntryExistsError))?)
        };

        for node in breadcrumbs.iter_mut() {
            unsafe { (**node).size += size };
        }
        let mut current = unsafe { &mut **breadcrumbs.last().unwrap() };
        for c in remaining.iter() {
            current = current.children.entry(Box::from(c)).or_insert(FileTree::new());
            current.size = size;
        }
        Ok(())
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

    /// Returns the breadcrumbs of the largest prefix of the path.
    /// If the file is in the tree the last breadcrumb will be the file itself.
    /// Does not modify self at all.
    /// The cdr is the remaining path that did not match, if any.
    fn find(
        &mut self,
        path: &Utf8Path,
    ) -> (Vec<*mut FileTree>, Option<Utf8PathBuf>)
    {
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
            if suffix.as_str().is_empty() { None }
            else { Some(suffix.to_path_buf()) }
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

#[cfg(test)]
mod tests {
    use crate::cache::filetree::{EntryExistsError, FileTree};
    use crate::types::{Directory, Entry, File};

    fn sort_entries(entries: &mut Vec<Entry>) {
        entries.sort_unstable_by(|e0, e1| e0.path().cmp(e1.path()));
    }

    fn example_tree() -> FileTree {
        let mut filetree = FileTree::new();
        assert_eq!(filetree.insert("a/0/x".into(), 1), Ok(()));
        assert_eq!(filetree.insert("a/0/y".into(), 2), Ok(()));
        assert_eq!(filetree.insert("a/1/x/0".into(), 7), Ok(()));
        assert_eq!(filetree.insert("a/0/z/0".into(), 1), Ok(()));
        assert_eq!(filetree.insert("a/1/x/1".into(), 2), Ok(()));
        filetree
    }

    #[test]
    fn insert_uniques() {
        let filetree = example_tree();
        let mut entries = filetree.iter().collect::<Vec<_>>();
        sort_entries(&mut entries);
        assert_eq!(entries, vec![
            Entry::Directory(Directory { path: "a".into(), size: 13 }),
            Entry::Directory(Directory { path: "a/0".into(), size: 4 }),
            Entry::File(File { path: "a/0/x".into(), size: 1 }),
            Entry::File(File { path: "a/0/y".into(), size: 2 }),
            Entry::Directory(Directory { path: "a/0/z".into(), size: 1 }),
            Entry::File(File { path: "a/0/z/0".into(), size: 1 }),
            Entry::Directory(Directory { path: "a/1".into(), size: 9 }),
            Entry::Directory(Directory { path: "a/1/x".into(), size: 9 }),
            Entry::File(File { path: "a/1/x/0".into(), size: 7 }),
            Entry::File(File { path: "a/1/x/1".into(), size: 2 }),
        ]);
    }
 
    #[test]
    fn insert_existing() {
        let mut filetree = example_tree();
        assert_eq!(filetree.insert("".into(), 1), Err(EntryExistsError));
        assert_eq!(filetree.insert("a/0".into(), 1), Err(EntryExistsError));
        assert_eq!(filetree.insert("a/0/z/0".into(), 1), Err(EntryExistsError));
    }
}

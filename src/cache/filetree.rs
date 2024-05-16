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

    pub fn merge(self, other: FileTree) -> Self {
        fn sorted_children(filetree: FileTree) -> Vec<(Box<str>, FileTree)> {
            let mut children = filetree.children.into_iter().collect::<Vec<_>>();
            children.sort_unstable_by(|(name0, _), (name1, _)| name0.cmp(name1));
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
                (None, Some((name, tree))) => { children.insert(name, tree); }
                (Some((name, tree)), None) => { children.insert(name, tree); }
                (None, None) => { break; }
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

    fn example_tree_0() -> FileTree {
        let mut filetree = FileTree::new();
        assert_eq!(filetree.insert("a/0/x".into(), 1), Ok(()));
        assert_eq!(filetree.insert("a/0/y".into(), 2), Ok(()));
        assert_eq!(filetree.insert("a/1/x/0".into(), 7), Ok(()));
        assert_eq!(filetree.insert("a/0/z/0".into(), 1), Ok(()));
        assert_eq!(filetree.insert("a/1/x/1".into(), 2), Ok(()));
        filetree
    }

    fn example_tree_1() -> FileTree {
        let mut filetree = FileTree::new();
        assert_eq!(filetree.insert("a/0/x".into(), 3), Ok(()));
        assert_eq!(filetree.insert("a/0/y".into(), 2), Ok(()));
        assert_eq!(filetree.insert("a/2/x/0".into(), 7), Ok(()));
        assert_eq!(filetree.insert("a/0/z/0".into(), 9), Ok(()));
        assert_eq!(filetree.insert("a/1/x/1".into(), 1), Ok(()));
        filetree
    }

    fn example_tree_2() -> FileTree {
        let mut filetree = FileTree::new();
        assert_eq!(filetree.insert("b/0/x".into(), 3), Ok(()));
        assert_eq!(filetree.insert("b/0/y".into(), 2), Ok(()));
        assert_eq!(filetree.insert("a/2/x/0".into(), 7), Ok(()));
        assert_eq!(filetree.insert("b/0/z/0".into(), 9), Ok(()));
        assert_eq!(filetree.insert("a/1/x/1".into(), 1), Ok(()));
        filetree
    }
 
    #[test]
    fn insert_uniques_0() {
        let mut entries = example_tree_0().iter().collect::<Vec<_>>();
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
    fn insert_uniques_1() {
        let mut entries = example_tree_1().iter().collect::<Vec<_>>();
        sort_entries(&mut entries);
        assert_eq!(entries, vec![
            Entry::Directory(Directory { path: "a".into(), size: 22 }),
            Entry::Directory(Directory { path: "a/0".into(), size: 14 }),
            Entry::File(File { path: "a/0/x".into(), size: 3 }),
            Entry::File(File { path: "a/0/y".into(), size: 2 }),
            Entry::Directory(Directory { path: "a/0/z".into(), size: 9 }),
            Entry::File(File { path: "a/0/z/0".into(), size: 9 }),
            Entry::Directory(Directory { path: "a/1".into(), size: 1 }),
            Entry::Directory(Directory { path: "a/1/x".into(), size: 1 }),
            Entry::File(File { path: "a/1/x/1".into(), size: 1 }),
            Entry::Directory(Directory { path: "a/2".into(), size: 7 }),
            Entry::Directory(Directory { path: "a/2/x".into(), size: 7 }),
            Entry::File(File { path: "a/2/x/0".into(), size: 7 }),
        ]);
    }
 
    #[test]
    fn insert_existing() {
        let mut filetree = example_tree_0();
        assert_eq!(filetree.insert("".into(), 1), Err(EntryExistsError));
        assert_eq!(filetree.insert("a/0".into(), 1), Err(EntryExistsError));
        assert_eq!(filetree.insert("a/0/z/0".into(), 1), Err(EntryExistsError));
    }
    
    #[test]
    fn merge_test() {
        let filetree = example_tree_0().merge(example_tree_1());
        let mut entries = filetree.iter().collect::<Vec<_>>();
        sort_entries(&mut entries);
        assert_eq!(entries, vec![
            Entry::Directory(Directory { path: "a".into(), size: 22 }),
            Entry::Directory(Directory { path: "a/0".into(), size: 14 }),
            Entry::File(File { path: "a/0/x".into(), size: 3 }),
            Entry::File(File { path: "a/0/y".into(), size: 2 }),
            Entry::Directory(Directory { path: "a/0/z".into(), size: 9 }),
            Entry::File(File { path: "a/0/z/0".into(), size: 9 }),
            Entry::Directory(Directory { path: "a/1".into(), size: 9 }),
            Entry::Directory(Directory { path: "a/1/x".into(), size: 9 }),
            Entry::File(File { path: "a/1/x/0".into(), size: 7 }),
            Entry::File(File { path: "a/1/x/1".into(), size: 2 }),
            Entry::Directory(Directory { path: "a/2".into(), size: 7 }),
            Entry::Directory(Directory { path: "a/2/x".into(), size: 7 }),
            Entry::File(File { path: "a/2/x/0".into(), size: 7 }),
        ]);
    }
 
    #[test]
    fn merge_reflexivity() {
        assert_eq!(example_tree_0().merge(example_tree_0()), example_tree_0());
        assert_eq!(example_tree_1().merge(example_tree_1()), example_tree_1());
    }

    #[test]
    fn merge_associativity() {
        assert_eq!(example_tree_0().merge(example_tree_1()).merge(example_tree_2()),
                   example_tree_0().merge(example_tree_1().merge(example_tree_2())));
    }
 
    #[test]
    fn merge_commutativity() {
        assert_eq!(example_tree_0().merge(example_tree_1()),
                   example_tree_1().merge(example_tree_0()));
    }
}

use camino::Utf8PathBuf;

use crate::cache::filetree::{EntryExistsError, FileTree};

pub struct PathGenerator {
    branching_factor: usize,
    state: Vec<(usize, Utf8PathBuf, usize)>,
}

impl PathGenerator {
    pub fn new(depth: usize, branching_factor: usize) -> Self {
        let mut state = Vec::with_capacity(depth);
        state.push((depth, Utf8PathBuf::new(), 0));
        PathGenerator { branching_factor, state }
    }
}

impl Iterator for PathGenerator {
    type Item = Utf8PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (depth, prefix, child) = self.state.pop()?;
            if child < self.branching_factor {
                let mut new_prefix = prefix.clone();
                new_prefix.push(Utf8PathBuf::from(child.to_string()));
                self.state.push((depth, prefix, child + 1));
                if depth == 1 {
                    break (Some(new_prefix));
                } else {
                    self.state.push((depth - 1, new_prefix, 0));
                }
            }
        }
    }
}

pub fn generate_filetree(
    depth: usize,
    branching_factor: usize,
) -> FileTree {
    let mut filetree = FileTree::new();
    for path in PathGenerator::new(depth, branching_factor) {
        filetree.insert(&path, 1).unwrap();
    }
    filetree
}

fn sort_entries(entries: &mut Vec<crate::types::Entry>) {
    entries.sort_unstable_by(|e0, e1| e0.path().cmp(e1.path()));
}

fn example_tree_0() -> crate::cache::filetree::FileTree {
    let mut filetree = crate::cache::filetree::FileTree::new();
    assert_eq!(filetree.insert("a/0/x".into(), 1), Ok(()));
    assert_eq!(filetree.insert("a/0/y".into(), 2), Ok(()));
    assert_eq!(filetree.insert("a/1/x/0".into(), 7), Ok(()));
    assert_eq!(filetree.insert("a/0/z/0".into(), 1), Ok(()));
    assert_eq!(filetree.insert("a/1/x/1".into(), 2), Ok(()));
    filetree
}

fn example_tree_1() -> crate::cache::filetree::FileTree {
    let mut filetree = crate::cache::filetree::FileTree::new();
    assert_eq!(filetree.insert("a/0/x".into(), 3), Ok(()));
    assert_eq!(filetree.insert("a/0/y".into(), 2), Ok(()));
    assert_eq!(filetree.insert("a/2/x/0".into(), 7), Ok(()));
    assert_eq!(filetree.insert("a/0/z/0".into(), 9), Ok(()));
    assert_eq!(filetree.insert("a/1/x/1".into(), 1), Ok(()));
    filetree
}

fn example_tree_2() -> crate::cache::filetree::FileTree {
    let mut filetree = crate::cache::filetree::FileTree::new();
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
        crate::types::Entry::Directory(crate::types::Directory { path: "a".into(), size: 13 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/0".into(), size: 4 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/x".into(), size: 1 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/y".into(), size: 2 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/0/z".into(), size: 1 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/z/0".into(), size: 1 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/1".into(), size: 9 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/1/x".into(), size: 9 }),
        crate::types::Entry::File(crate::types::File { path: "a/1/x/0".into(), size: 7 }),
        crate::types::Entry::File(crate::types::File { path: "a/1/x/1".into(), size: 2 }),
    ]);
}

#[test]
fn insert_uniques_1() {
    let mut entries = example_tree_1().iter().collect::<Vec<_>>();
    sort_entries(&mut entries);
    assert_eq!(entries, vec![
        crate::types::Entry::Directory(crate::types::Directory { path: "a".into(), size: 22 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/0".into(), size: 14 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/x".into(), size: 3 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/y".into(), size: 2 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/0/z".into(), size: 9 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/z/0".into(), size: 9 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/1".into(), size: 1 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/1/x".into(), size: 1 }),
        crate::types::Entry::File(crate::types::File { path: "a/1/x/1".into(), size: 1 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/2".into(), size: 7 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/2/x".into(), size: 7 }),
        crate::types::Entry::File(crate::types::File { path: "a/2/x/0".into(), size: 7 }),
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
        crate::types::Entry::Directory(crate::types::Directory { path: "a".into(), size: 22 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/0".into(), size: 14 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/x".into(), size: 3 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/y".into(), size: 2 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/0/z".into(), size: 9 }),
        crate::types::Entry::File(crate::types::File { path: "a/0/z/0".into(), size: 9 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/1".into(), size: 9 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/1/x".into(), size: 9 }),
        crate::types::Entry::File(crate::types::File { path: "a/1/x/0".into(), size: 7 }),
        crate::types::Entry::File(crate::types::File { path: "a/1/x/1".into(), size: 2 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/2".into(), size: 7 }),
        crate::types::Entry::Directory(crate::types::Directory { path: "a/2/x".into(), size: 7 }),
        crate::types::Entry::File(crate::types::File { path: "a/2/x/0".into(), size: 7 }),
    ]);
}

#[test]
fn merge_reflexivity() {
    assert_eq!(example_tree_0().merge(example_tree_0()), example_tree_0());
    assert_eq!(example_tree_1().merge(example_tree_1()), example_tree_1());
}

#[test]
fn merge_associativity() {
    assert_eq!(
        example_tree_0().merge(example_tree_1()).merge(example_tree_2()),
        example_tree_0().merge(example_tree_1().merge(example_tree_2()))
    );
}

#[test]
fn merge_commutativity() {
    assert_eq!(
        example_tree_0().merge(example_tree_1()),
        example_tree_1().merge(example_tree_0())
    );
}

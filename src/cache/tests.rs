use std::cmp::Reverse;
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use refinery::Target;
use scopeguard::defer;
use uuid::Uuid;

use crate::cache::Cache;
use crate::cache::filetree::{EntryExistsError, FileTree};
use crate::types::{Directory, Entry, File};

pub fn with_cache_open_with_target(
    migration_target: Target,
    body: impl FnOnce(Cache),
) {
    let mut file = std::env::temp_dir();
    file.push(Uuid::new_v4().to_string());

    defer! { fs::remove_file(&file).unwrap(); }
    let migrator = Cache::open_with_target(&file, migration_target).unwrap();
    body(migrator.migrate().unwrap());
}

pub fn with_cache_open(body: impl FnOnce(Cache)) {
    with_cache_open_with_target(Target::Latest, body);
}

pub fn path_parent(path: &Utf8Path) -> Option<Utf8PathBuf> {
    let parent = path.parent().map(ToOwned::to_owned);
    parent.and_then(|p| {
        if p.as_str().is_empty() { None }
        else { Some(p) }
    })
}

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

#[test]
fn cache_snapshots_entries() {
    with_cache_open(|mut cache| {
        fn test_snapshots(cache: &Cache, hashes: Vec<&str>) {
            let mut db_snapshots = cache.get_snapshots().unwrap();
            db_snapshots.sort();
            let mut hashes = hashes.into_iter().map(Box::from).collect::<Vec<Box<str>>>();
            hashes.sort();
            assert_eq!(db_snapshots, hashes);
        }
 
        fn test_max_file_sizes(
            cache: &Cache,
            filetree: FileTree,
            path: Option<&str>
        ) {
            let mut db_entries = cache.get_max_file_sizes(path).unwrap();
            db_entries.sort_by_key(|e| e.path().to_string());
            let mut entries = filetree
                .iter()
                .filter(|e| path_parent(e.path()) == path.map(|s| Utf8PathBuf::from(s)))
                .map(|e| {
                    if let Some(parent) = path {
                        match e {
                            Entry::Directory(Directory { path, size }) =>
                                Entry::Directory(Directory {
                                    path: path.strip_prefix(parent).unwrap().to_owned(),
                                    size,
                                }),
                            Entry::File(File { path, size }) =>
                                Entry::File(File {
                                    path: path.strip_prefix(parent).unwrap().to_owned(),
                                    size,
                                }),
                        }
                    } else {
                        e
                    }
                })
                .collect::<Vec<Entry>>();
            entries.sort_by_key(|e| Reverse(e.size()));
            entries.sort_by_key(|e| e.path().to_string());
            assert_eq!(db_entries, entries);
        }

        cache.save_snapshot("foo", example_tree_0()).unwrap();
        cache.save_snapshot("bar", example_tree_1()).unwrap();
        cache.save_snapshot("wat", example_tree_2()).unwrap();

        // Max sizes
        fn test_entries(cache: &Cache, filetree: FileTree) {
            test_max_file_sizes(cache, filetree.clone(), None);
            test_max_file_sizes(cache, filetree.clone(), Some("a"));
            test_max_file_sizes(cache, filetree.clone(), Some("b"));
            test_max_file_sizes(cache, filetree.clone(), Some("a/0"));
            test_max_file_sizes(cache, filetree.clone(), Some("a/1"));
            test_max_file_sizes(cache, filetree.clone(), Some("a/2"));
            test_max_file_sizes(cache, filetree.clone(), Some("b/0"));
            test_max_file_sizes(cache, filetree.clone(), Some("b/1"));
            test_max_file_sizes(cache, filetree.clone(), Some("b/2"));
            test_max_file_sizes(cache, filetree.clone(), Some("something"));
            test_max_file_sizes(cache, filetree.clone(), Some("a/something"));
        }

        test_snapshots(&cache, vec!["foo", "bar", "wat"]);
        test_entries(
            &cache,
            example_tree_0().merge(example_tree_1()).merge(example_tree_2())
        ); 

        // Deleting a non-existent snapshot does nothing
        cache.delete_snapshot("non-existent").unwrap();
        test_snapshots(&cache, vec!["foo", "bar", "wat"]);
        test_entries(
            &cache,
            example_tree_0().merge(example_tree_1()).merge(example_tree_2())
        );
        
        // Remove bar
        cache.delete_snapshot("bar").unwrap();
        test_snapshots(&cache, vec!["foo", "wat"]);
        test_entries(
            &cache,
            example_tree_0().merge(example_tree_2())
        );
    });
}

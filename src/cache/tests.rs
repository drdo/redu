use std::{cmp::Reverse, convert::Infallible, fs};

use camino::{Utf8Path, Utf8PathBuf};
use scopeguard::defer;
use uuid::Uuid;

use super::LATEST_VERSION;
use crate::cache::{
    filetree::{InsertError, SizeTree},
    Cache, Migrator, VersionId,
};

pub fn with_cache_open_with_target(
    target: VersionId,
    body: impl FnOnce(Cache),
) {
    let mut file = std::env::temp_dir();
    file.push(Uuid::new_v4().to_string());

    defer! { fs::remove_file(&file).unwrap(); }
    let migrator = Migrator::open_with_target(&file, target).unwrap();
    body(migrator.migrate().unwrap());
}

pub fn with_cache_open(body: impl FnOnce(Cache)) {
    with_cache_open_with_target(LATEST_VERSION, body);
}

pub fn path_parent(path: &Utf8Path) -> Option<Utf8PathBuf> {
    let parent = path.parent().map(ToOwned::to_owned);
    parent.and_then(|p| if p.as_str().is_empty() { None } else { Some(p) })
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

pub fn generate_sizetree(depth: usize, branching_factor: usize) -> SizeTree {
    let mut sizetree = SizeTree::new();
    for path in PathGenerator::new(depth, branching_factor) {
        sizetree.insert(path.components(), 1).unwrap();
    }
    sizetree
}

fn sort_entries(entries: &mut [(Vec<&str>, usize, bool)]) {
    entries.sort_unstable_by(|e0, e1| e0.0.cmp(&e1.0));
}

fn to_sorted_entries(tree: &SizeTree) -> Vec<(Vec<&str>, usize, bool)> {
    let mut entries = Vec::new();
    tree.0
        .traverse_with_context(|context, component, size, is_dir| {
            let mut path = Vec::from(context);
            path.push(component);
            entries.push((path, *size, is_dir));
            Ok::<&str, Infallible>(component)
        })
        .unwrap();
    sort_entries(&mut entries);
    entries
}

fn example_tree_0() -> SizeTree {
    let mut sizetree = SizeTree::new();
    assert_eq!(sizetree.insert(["a", "0", "x"], 1), Ok(()));
    assert_eq!(sizetree.insert(["a", "0", "y"], 2), Ok(()));
    assert_eq!(sizetree.insert(["a", "1", "x", "0"], 7), Ok(()));
    assert_eq!(sizetree.insert(["a", "0", "z", "0"], 1), Ok(()));
    assert_eq!(sizetree.insert(["a", "1", "x", "1"], 2), Ok(()));
    sizetree
}

fn example_tree_1() -> SizeTree {
    let mut sizetree = SizeTree::new();
    assert_eq!(sizetree.insert(["a", "0", "x"], 3), Ok(()));
    assert_eq!(sizetree.insert(["a", "0", "y"], 2), Ok(()));
    assert_eq!(sizetree.insert(["a", "2", "x", "0"], 7), Ok(()));
    assert_eq!(sizetree.insert(["a", "0", "z", "0"], 9), Ok(()));
    assert_eq!(sizetree.insert(["a", "1", "x", "1"], 1), Ok(()));
    sizetree
}

fn example_tree_2() -> SizeTree {
    let mut sizetree = SizeTree::new();
    assert_eq!(sizetree.insert(["b", "0", "x"], 3), Ok(()));
    assert_eq!(sizetree.insert(["b", "0", "y"], 2), Ok(()));
    assert_eq!(sizetree.insert(["a", "2", "x", "0"], 7), Ok(()));
    assert_eq!(sizetree.insert(["b", "0", "z", "0"], 9), Ok(()));
    assert_eq!(sizetree.insert(["a", "1", "x", "1"], 1), Ok(()));
    sizetree
}

#[test]
fn sizetree_iter_empty() {
    let sizetree = SizeTree::new();
    assert_eq!(sizetree.iter().next(), None);
}

#[test]
fn insert_uniques_0() {
    let tree = example_tree_0();
    let entries = to_sorted_entries(&tree);
    assert_eq!(entries, vec![
        (vec!["a"], 13, true),
        (vec!["a", "0"], 4, true),
        (vec!["a", "0", "x"], 1, false),
        (vec!["a", "0", "y"], 2, false),
        (vec!["a", "0", "z"], 1, true),
        (vec!["a", "0", "z", "0"], 1, false),
        (vec!["a", "1"], 9, true),
        (vec!["a", "1", "x"], 9, true),
        (vec!["a", "1", "x", "0"], 7, false),
        (vec!["a", "1", "x", "1"], 2, false),
    ]);
}

#[test]
fn insert_uniques_1() {
    let tree = example_tree_1();
    let entries = to_sorted_entries(&tree);
    assert_eq!(entries, vec![
        (vec!["a"], 22, true),
        (vec!["a", "0"], 14, true),
        (vec!["a", "0", "x"], 3, false),
        (vec!["a", "0", "y"], 2, false),
        (vec!["a", "0", "z"], 9, true),
        (vec!["a", "0", "z", "0"], 9, false),
        (vec!["a", "1"], 1, true),
        (vec!["a", "1", "x"], 1, true),
        (vec!["a", "1", "x", "1"], 1, false),
        (vec!["a", "2"], 7, true),
        (vec!["a", "2", "x"], 7, true),
        (vec!["a", "2", "x", "0"], 7, false),
    ]);
}

#[test]
fn insert_uniques_2() {
    let tree = example_tree_2();
    let entries = to_sorted_entries(&tree);
    assert_eq!(entries, vec![
        (vec!["a"], 8, true),
        (vec!["a", "1"], 1, true),
        (vec!["a", "1", "x"], 1, true),
        (vec!["a", "1", "x", "1"], 1, false),
        (vec!["a", "2"], 7, true),
        (vec!["a", "2", "x"], 7, true),
        (vec!["a", "2", "x", "0"], 7, false),
        (vec!["b"], 14, true),
        (vec!["b", "0"], 14, true),
        (vec!["b", "0", "x"], 3, false),
        (vec!["b", "0", "y"], 2, false),
        (vec!["b", "0", "z"], 9, true),
        (vec!["b", "0", "z", "0"], 9, false),
    ]);
}

#[test]
fn insert_existing() {
    let mut sizetree = example_tree_0();
    assert_eq!(
        sizetree.insert(Vec::<&str>::new(), 1),
        Err(InsertError::EntryExists)
    );
    assert_eq!(sizetree.insert(["a", "0"], 1), Err(InsertError::EntryExists));
    assert_eq!(
        sizetree.insert(["a", "0", "z", "0"], 1),
        Err(InsertError::EntryExists)
    );
}

#[test]
fn merge_test() {
    let tree = example_tree_0().merge(example_tree_1());
    let entries = to_sorted_entries(&tree);
    assert_eq!(entries, vec![
        (vec!["a"], 22, true),
        (vec!["a", "0"], 14, true),
        (vec!["a", "0", "x"], 3, false),
        (vec!["a", "0", "y"], 2, false),
        (vec!["a", "0", "z"], 9, true),
        (vec!["a", "0", "z", "0"], 9, false),
        (vec!["a", "1"], 9, true),
        (vec!["a", "1", "x"], 9, true),
        (vec!["a", "1", "x", "0"], 7, false),
        (vec!["a", "1", "x", "1"], 2, false),
        (vec!["a", "2"], 7, true),
        (vec!["a", "2", "x"], 7, true),
        (vec!["a", "2", "x", "0"], 7, false),
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
            let mut hashes =
                hashes.into_iter().map(Box::from).collect::<Vec<Box<str>>>();
            hashes.sort();
            assert_eq!(db_snapshots, hashes);
        }

        fn test_get_max_file_sizes<P: AsRef<Utf8Path>>(
            cache: &Cache,
            tree: SizeTree,
            path: P,
        ) {
            let mut db_entries = {
                let path_id = if path.as_ref().as_str().is_empty() {
                    None
                } else {
                    cache.get_path_id_by_path(path.as_ref()).unwrap()
                };
                if path_id.is_none() && !path.as_ref().as_str().is_empty() {
                    // path was not found
                    vec![]
                } else {
                    cache
                        .get_max_file_sizes(path_id)
                        .unwrap()
                        .into_iter()
                        .map(|e| (e.component, e.size, e.is_dir))
                        .collect::<Vec<_>>()
                }
            };
            db_entries.sort_by_key(|(component, _, _)| component.clone());
            let mut entries = to_sorted_entries(&tree)
                .iter()
                .filter_map(|(components, size, is_dir)| {
                    // keep only the ones with parent == loc
                    let (last, parent_cs) = components.split_last()?;
                    let parent = parent_cs.iter().collect::<Utf8PathBuf>();
                    if parent == path.as_ref() {
                        Some((last.to_string(), *size, *is_dir))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            entries.sort_by_key(|(_, size, _)| Reverse(*size));
            entries.sort_by_key(|(component, _, _)| component.clone());
            assert_eq!(db_entries, entries);
        }

        cache.save_snapshot("foo", example_tree_0()).unwrap();
        cache.save_snapshot("bar", example_tree_1()).unwrap();
        cache.save_snapshot("wat", example_tree_2()).unwrap();

        // Max sizes
        fn test_entries(cache: &Cache, sizetree: SizeTree) {
            test_get_max_file_sizes(cache, sizetree.clone(), "");
            test_get_max_file_sizes(cache, sizetree.clone(), "a");
            test_get_max_file_sizes(cache, sizetree.clone(), "b");
            test_get_max_file_sizes(cache, sizetree.clone(), "a/0");
            test_get_max_file_sizes(cache, sizetree.clone(), "a/1");
            test_get_max_file_sizes(cache, sizetree.clone(), "a/2");
            test_get_max_file_sizes(cache, sizetree.clone(), "b/0");
            test_get_max_file_sizes(cache, sizetree.clone(), "b/1");
            test_get_max_file_sizes(cache, sizetree.clone(), "b/2");
            test_get_max_file_sizes(cache, sizetree.clone(), "something");
            test_get_max_file_sizes(cache, sizetree.clone(), "a/something");
        }

        test_snapshots(&cache, vec!["foo", "bar", "wat"]);
        test_entries(
            &cache,
            example_tree_0().merge(example_tree_1()).merge(example_tree_2()),
        );

        // Deleting a non-existent snapshot does nothing
        cache.delete_snapshot("non-existent").unwrap();
        test_snapshots(&cache, vec!["foo", "bar", "wat"]);
        test_entries(
            &cache,
            example_tree_0().merge(example_tree_1()).merge(example_tree_2()),
        );

        // Remove bar
        cache.delete_snapshot("bar").unwrap();
        test_snapshots(&cache, vec!["foo", "wat"]);
        test_entries(&cache, example_tree_0().merge(example_tree_2()));
    });
}

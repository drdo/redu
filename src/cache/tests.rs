use std::{
    cmp::Reverse, collections::HashSet, convert::Infallible, env, fs, iter,
    mem, path::PathBuf,
};

use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

use crate::{
    cache::{
        determine_version,
        filetree::{InsertError, SizeTree},
        get_tables, timestamp_to_datetime, Cache, EntryDetails, Migrator,
    },
    restic::Snapshot,
};

pub fn mk_datetime(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> DateTime<Utc> {
    NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month, day).unwrap(),
        NaiveTime::from_hms_opt(hour, minute, second).unwrap(),
    )
    .and_utc()
}

pub struct Tempfile(pub PathBuf);

impl Drop for Tempfile {
    fn drop(&mut self) {
        fs::remove_file(mem::take(&mut self.0)).unwrap();
    }
}

impl Tempfile {
    pub fn new() -> Self {
        let mut path = env::temp_dir();
        path.push(Uuid::new_v4().to_string());
        Tempfile(path)
    }
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
                    break Some(new_prefix);
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

fn assert_get_entries_correct_at_path<P: AsRef<Utf8Path>>(
    cache: &Cache,
    tree: &SizeTree,
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
                .get_entries(path_id)
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
    fn test_snapshots(cache: &Cache, mut snapshots: Vec<&Snapshot>) {
        let mut db_snapshots = cache.get_snapshots().unwrap();
        db_snapshots.sort_unstable_by(|s0, s1| s0.id.cmp(&s1.id));
        snapshots.sort_unstable_by(|s0, s1| s0.id.cmp(&s1.id));
        for (s0, s1) in iter::zip(db_snapshots.iter(), snapshots.iter()) {
            assert_eq!(s0.id, s1.id);
            assert_eq!(s0.time, s1.time);
            assert_eq!(s0.parent, s1.parent);
            assert_eq!(s0.tree, s1.tree);
            assert_eq!(s0.hostname, s1.hostname);
            assert_eq!(s0.username, s1.username);
            assert_eq!(s0.uid, s1.uid);
            assert_eq!(s0.gid, s1.gid);
            assert_eq!(s0.original_id, s1.original_id);
            assert_eq!(s0.program_version, s1.program_version);

            let mut s0_paths: Vec<String> = s0.paths.iter().cloned().collect();
            s0_paths.sort();
            let mut s1_paths: Vec<String> = s1.paths.iter().cloned().collect();
            s1_paths.sort();
            assert_eq!(s0_paths, s1_paths);

            let mut s0_excludes: Vec<String> =
                s0.excludes.iter().cloned().collect();
            s0_excludes.sort();
            let mut s1_excludes: Vec<String> =
                s1.excludes.iter().cloned().collect();
            s1_excludes.sort();
            assert_eq!(s0_excludes, s1_excludes);

            let mut s0_tags: Vec<String> = s0.tags.iter().cloned().collect();
            s0_tags.sort();
            let mut s1_tags: Vec<String> = s1.tags.iter().cloned().collect();
            s1_tags.sort();
            assert_eq!(s0_tags, s1_tags);
        }
    }

    let tempfile = Tempfile::new();
    let mut cache = Migrator::open(&tempfile.0).unwrap().migrate().unwrap();

    let foo = Snapshot {
        id: "foo".to_string(),
        time: mk_datetime(2024, 4, 12, 12, 00, 00),
        parent: Some("bar".to_string()),
        tree: "sometree".to_string(),
        paths: vec![
            "/home/user".to_string(),
            "/etc".to_string(),
            "/var".to_string(),
        ]
        .into_iter()
        .collect(),
        hostname: Some("foo.com".to_string()),
        username: Some("user".to_string()),
        uid: Some(123),
        gid: Some(456),
        excludes: vec![
            ".cache".to_string(),
            "Cache".to_string(),
            "/home/user/Downloads".to_string(),
        ]
        .into_iter()
        .collect(),
        tags: vec!["foo_machine".to_string(), "rewrite".to_string()]
            .into_iter()
            .collect(),
        original_id: Some("fefwfwew".to_string()),
        program_version: Some("restic 0.16.0".to_string()),
    };

    let bar = Snapshot {
        id: "bar".to_string(),
        time: mk_datetime(2025, 5, 12, 17, 00, 00),
        parent: Some("wat".to_string()),
        tree: "anothertree".to_string(),
        paths: vec!["/home/user".to_string()].into_iter().collect(),
        hostname: Some("foo.com".to_string()),
        username: Some("user".to_string()),
        uid: Some(123),
        gid: Some(456),
        excludes: vec![
            ".cache".to_string(),
            "Cache".to_string(),
            "/home/user/Downloads".to_string(),
        ]
        .into_iter()
        .collect(),
        tags: vec!["foo_machine".to_string(), "rewrite".to_string()]
            .into_iter()
            .collect(),
        original_id: Some("fefwfwew".to_string()),
        program_version: Some("restic 0.16.0".to_string()),
    };

    let wat = Snapshot {
        id: "wat".to_string(),
        time: mk_datetime(2023, 5, 12, 17, 00, 00),
        parent: None,
        tree: "fwefwfwwefwefwe".to_string(),
        paths: HashSet::new(),
        hostname: None,
        username: None,
        uid: None,
        gid: None,
        excludes: HashSet::new(),
        tags: HashSet::new(),
        original_id: None,
        program_version: None,
    };

    cache.save_snapshot(&foo, example_tree_0()).unwrap();
    cache.save_snapshot(&bar, example_tree_1()).unwrap();
    cache.save_snapshot(&wat, example_tree_2()).unwrap();

    test_snapshots(&cache, vec![&foo, &bar, &wat]);

    fn test_entries(cache: &Cache, sizetree: SizeTree) {
        assert_get_entries_correct_at_path(cache, &sizetree, "");
        assert_get_entries_correct_at_path(cache, &sizetree, "a");
        assert_get_entries_correct_at_path(cache, &sizetree, "b");
        assert_get_entries_correct_at_path(cache, &sizetree, "a/0");
        assert_get_entries_correct_at_path(cache, &sizetree, "a/1");
        assert_get_entries_correct_at_path(cache, &sizetree, "a/2");
        assert_get_entries_correct_at_path(cache, &sizetree, "b/0");
        assert_get_entries_correct_at_path(cache, &sizetree, "b/1");
        assert_get_entries_correct_at_path(cache, &sizetree, "b/2");
        assert_get_entries_correct_at_path(cache, &sizetree, "something");
        assert_get_entries_correct_at_path(cache, &sizetree, "a/something");
    }

    test_entries(
        &cache,
        example_tree_0().merge(example_tree_1()).merge(example_tree_2()),
    );

    // Deleting a non-existent snapshot does nothing
    cache.delete_snapshot("non-existent").unwrap();
    test_snapshots(&cache, vec![&foo, &bar, &wat]);
    test_entries(
        &cache,
        example_tree_0().merge(example_tree_1()).merge(example_tree_2()),
    );

    // Remove bar
    cache.delete_snapshot("bar").unwrap();
    test_snapshots(&cache, vec![&foo, &wat]);
    test_entries(&cache, example_tree_0().merge(example_tree_2()));
}

// TODO: Ideally we would run more than 10_000 but at the moment this is too slow.
#[test]
fn lots_of_snapshots() {
    let tempfile = Tempfile::new();
    let mut cache = Migrator::open(&tempfile.0).unwrap().migrate().unwrap();

    const NUM_SNAPSHOTS: usize = 10_000;

    // Insert lots of snapshots
    for i in 0..NUM_SNAPSHOTS {
        let snapshot = Snapshot {
            id: i.to_string(),
            time: timestamp_to_datetime(i as i64).unwrap(),
            parent: None,
            tree: i.to_string(),
            paths: HashSet::new(),
            hostname: None,
            username: None,
            uid: None,
            gid: None,
            excludes: HashSet::new(),
            tags: HashSet::new(),
            original_id: None,
            program_version: None,
        };
        cache.save_snapshot(&snapshot, example_tree_0()).unwrap();
    }

    // get_entries
    let tree = example_tree_0();
    for path in ["", "a", "a/0", "a/1", "a/1/x", "a/something"] {
        assert_get_entries_correct_at_path(&cache, &tree, path);
    }

    // get_entry_details
    let path_id = cache.get_path_id_by_path("a/0".into()).unwrap().unwrap();
    let details = cache.get_entry_details(path_id).unwrap().unwrap();
    assert_eq!(details, EntryDetails {
        max_size: 4,
        max_size_snapshot_hash: (NUM_SNAPSHOTS - 1).to_string(),
        first_seen: timestamp_to_datetime(0).unwrap(),
        first_seen_snapshot_hash: 0.to_string(),
        last_seen: timestamp_to_datetime((NUM_SNAPSHOTS - 1) as i64).unwrap(),
        last_seen_snapshot_hash: (NUM_SNAPSHOTS - 1).to_string(),
    });
}

////////// Migrations //////////////////////////////////////////////////////////
fn assert_tables(conn: &Connection, tables: &[&str]) {
    let mut actual_tables: Vec<String> =
        get_tables(conn).unwrap().into_iter().collect();
    actual_tables.sort();
    let mut expected_tables: Vec<String> =
        tables.iter().map(ToString::to_string).collect();
    expected_tables.sort();
    assert_eq!(actual_tables, expected_tables);
}

fn assert_marks(cache: &Cache, marks: &[&str]) {
    let mut actual_marks = cache.get_marks().unwrap();
    actual_marks.sort();
    let mut expected_marks: Vec<Utf8PathBuf> =
        marks.iter().map(Utf8PathBuf::from).collect();
    expected_marks.sort();
    assert_eq!(actual_marks, expected_marks);
}

fn populate_v0<'a>(
    marks: impl IntoIterator<Item = &'a str>,
) -> Result<Tempfile, anyhow::Error> {
    let file = Tempfile::new();
    let mut cache = Migrator::open_with_target(&file.0, 0)?.migrate()?;
    let tx = cache.conn.transaction()?;
    {
        let mut marks_stmt =
            tx.prepare("INSERT INTO marks (path) VALUES (?)")?;
        for mark in marks {
            marks_stmt.execute([mark])?;
        }
    }
    tx.commit()?;
    Ok(file)
}

#[test]
fn test_migrate_v0_to_v1() {
    let marks = ["/foo", "/bar/wat", "foo/a/b/c", "something"];
    let file = populate_v0(marks).unwrap();

    let cache =
        Migrator::open_with_target(&file.0, 1).unwrap().migrate().unwrap();

    assert_tables(&cache.conn, &[
        "metadata_integer",
        "paths",
        "snapshots",
        "snapshot_paths",
        "snapshot_excludes",
        "snapshot_tags",
        "marks",
    ]);

    assert_marks(&cache, &marks);

    assert_eq!(determine_version(&cache.conn).unwrap(), Some(1));

    cache_snapshots_entries();
}

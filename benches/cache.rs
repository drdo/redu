use std::cell::Cell;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redu::{cache::tests::*, restic::Snapshot};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("merge sizetree", |b| {
        let sizetree0 =
            Cell::new(generate_sizetree(black_box(6), black_box(12)));
        let sizetree1 =
            Cell::new(generate_sizetree(black_box(5), black_box(14)));
        b.iter(move || sizetree0.take().merge(black_box(sizetree1.take())));
    });

    c.bench_function("create and save snapshot", |b| {
        with_cache_open(|mut cache| {
            let foo = Snapshot {
                id: "foo".to_string(),
                time: mk_datetime(2024, 4, 12, 12, 00, 00),
                parent: Some("bar".to_string()),
                tree: "sometree".to_string(),
                paths: vec![
                    "/home/user".to_string(),
                    "/etc".to_string(),
                    "/var".to_string(),
                ],
                hostname: Some("foo.com".to_string()),
                username: Some("user".to_string()),
                uid: Some(123),
                gid: Some(456),
                excludes: vec![
                    ".cache".to_string(),
                    "Cache".to_string(),
                    "/home/user/Downloads".to_string(),
                ],
                tags: vec!["foo_machine".to_string(), "rewrite".to_string()],
                original_id: Some("fefwfwew".to_string()),
                program_version: Some("restic 0.16.0".to_string()),
            };
            b.iter(move || {
                cache
                    .save_snapshot(
                        &foo,
                        generate_sizetree(black_box(6), black_box(12)),
                    )
                    .unwrap();
            });
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);

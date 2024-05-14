use std::fs;
use std::path::PathBuf;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use uuid::Uuid;
use dorestic::cache::Cache;

use dorestic::cache::tests::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    let filetree = generate_filetree(black_box(6), black_box(12));

    c.bench_function("save filetree", |b| {
        let snapshot = "foo";
        let file: PathBuf = Uuid::new_v4().to_string().into();
        {
            let mut cache = Cache::open(&file).unwrap();
            b.iter(||
                cache.save_snapshot(black_box(snapshot), black_box(&filetree))
            );
        }
        fs::remove_file(&file).unwrap();
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);

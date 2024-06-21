use std::cell::Cell;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redu::cache::tests::*;

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
            b.iter(move || {
                cache
                    .save_snapshot(
                        "foo",
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

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use dorestic::cache::tests::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("generate filetree", |b| b.iter(||
        generate_filetree(black_box(10), black_box(20))
    ));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

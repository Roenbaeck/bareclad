use criterion::{black_box, criterion_group, criterion_main, Criterion};
use bareclad::traqula::ResultSet;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut r1 = ResultSet::new();
    let mut r2 = ResultSet::new();
    println!("{:?}", r1);
    c.bench_function("intersect 0", |b| b.iter(|| r1.intersect_with(&r2)));
    r1.push(42);
    r2.push(42);
    println!("{:?}", r1);
    c.bench_function("intersect 1", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 1..1000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 1k", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 100000..200000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 100k", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 1000000..2000000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 1M", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 10000000..20000000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 10M", |b| b.iter(|| r1.intersect_with(&r2)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
use criterion::{criterion_group, criterion_main, Criterion};
use ring_rs::ring;
use std::thread;

fn bench_throughput(c: &mut Criterion) {
    c.bench_function("spsc_1024", |b| {
        b.iter(|| {
            let (tx, rx) = ring::<u64>(1024);
            let count = 10_000usize;
            let producer = thread::spawn(move || {
                for i in 0..count {
                    while tx.try_push(i as u64).is_err() {
                        std::hint::spin_loop();
                    }
                }
            });
            let consumer = thread::spawn(move || {
                let mut n = 0usize;
                while n < count {
                    if rx.try_pop().is_some() {
                        n += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
            producer.join().unwrap();
            consumer.join().unwrap();
        })
    });
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);

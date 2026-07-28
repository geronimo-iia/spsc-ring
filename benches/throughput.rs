use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ring_rs::ring;
use std::thread;

fn throughput_1m(c: &mut Criterion) {
    let count = 1_000_000usize;

    c.bench_function("spsc_1M_events", |b| {
        b.iter(|| {
            let (tx, rx) = ring(1024);

            let producer = thread::spawn(move || {
                for i in 0..count {
                    while tx.try_push(black_box(i)).is_err() {
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
                n
            });

            producer.join().unwrap();
            let received = consumer.join().unwrap();
            assert_eq!(received, count);
        });
    });
}

criterion_group!(benches, throughput_1m);
criterion_main!(benches);

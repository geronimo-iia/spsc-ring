use criterion::{Criterion, black_box, criterion_group, criterion_main};
use spsc_ring::ring;
use std::thread;

fn throughput_1m(c: &mut Criterion) {
    let count = 1_000_000usize;

    c.bench_function("spsc_1M_events", |b| {
        b.iter(|| {
            let (tx, rx) = ring(1024).unwrap();

            let producer = thread::spawn(move || {
                for i in 0..count {
                    loop {
                        match tx.try_push(black_box(i)) {
                            Ok(()) => break,
                            Err(spsc_ring::TrySendError::Full(_)) => std::hint::spin_loop(),
                            Err(spsc_ring::TrySendError::Disconnected(_)) => return,
                        }
                    }
                }
            });

            let consumer = thread::spawn(move || {
                let mut n = 0usize;
                while n < count {
                    match rx.try_pop() {
                        Ok(_) => n += 1,
                        Err(spsc_ring::TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(spsc_ring::TryRecvError::Disconnected) => break,
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

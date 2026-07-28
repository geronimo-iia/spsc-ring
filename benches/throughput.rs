use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use spsc_ring::ring;
use std::thread;

fn throughput_1m(c: &mut Criterion) {
    let count = 1_000_000usize;

    c.bench_function("spsc_1M_events", |b| {
        b.iter(|| {
            let (tx, rx) = ring(1024).unwrap();

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
                    if rx.try_pop().is_ok() {
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

fn bench_push_slice_1m(c: &mut Criterion) {
    const COUNT: usize = 1_000_000;
    const CHUNK: usize = 64;
    const CAP: usize = 4096;

    c.bench_function("spsc_push_slice_1M_chunk64", |b| {
        b.iter(|| {
            let (tx, rx) = ring::<u32>(CAP).unwrap();
            let src = [black_box(42u32); CHUNK];

            let producer = thread::spawn(move || {
                let mut sent = 0usize;
                while sent < COUNT {
                    let remaining = COUNT - sent;
                    let chunk = &src[..remaining.min(CHUNK)];
                    loop {
                        let n = tx.push_slice(chunk);
                        sent += n;
                        if n == chunk.len() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            });

            let consumer = thread::spawn(move || {
                let mut dst = [0u32; CHUNK];
                let mut received = 0usize;
                while received < COUNT {
                    let n = rx.pop_into_slice(&mut dst);
                    received += n;
                    if n == 0 {
                        std::hint::spin_loop();
                    }
                }
                received
            });

            producer.join().unwrap();
            let received = consumer.join().unwrap();
            assert_eq!(received, COUNT);
        });
    });
}

fn bench_push_slice_chunk_sizes(c: &mut Criterion) {
    const COUNT: usize = 1_000_000;
    const CAP: usize = 4096;

    let mut group = c.benchmark_group("spsc_push_slice_chunk_size");

    for &chunk_size in &[1usize, 8, 32, 64, 256] {
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &chunk_size,
            |b, &chunk_size| {
                b.iter(|| {
                    let (tx, rx) = ring::<u32>(CAP).unwrap();
                    let src = vec![black_box(42u32); chunk_size];

                    let producer = thread::spawn(move || {
                        let mut sent = 0usize;
                        while sent < COUNT {
                            let remaining = COUNT - sent;
                            let chunk = &src[..remaining.min(chunk_size)];
                            loop {
                                let n = tx.push_slice(chunk);
                                sent += n;
                                if n == chunk.len() {
                                    break;
                                }
                                std::hint::spin_loop();
                            }
                        }
                    });

                    let consumer = thread::spawn(move || {
                        let mut dst = vec![0u32; chunk_size];
                        let mut received = 0usize;
                        while received < COUNT {
                            let n = rx.pop_into_slice(&mut dst);
                            received += n;
                            if n == 0 {
                                std::hint::spin_loop();
                            }
                        }
                        received
                    });

                    producer.join().unwrap();
                    let received = consumer.join().unwrap();
                    assert_eq!(received, COUNT);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    throughput_1m,
    bench_push_slice_1m,
    bench_push_slice_chunk_sizes
);
criterion_main!(benches);

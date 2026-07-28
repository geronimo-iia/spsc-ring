use spsc_ring::{WaitStrategy, ring};
use std::thread;
use std::time::Duration;

fn run(name: &str, strategy: WaitStrategy) {
    let (tx, rx) = ring::<u64>(64);
    let count = 10_000u64;

    let producer = thread::spawn(move || {
        for i in 0..count {
            tx.push(i, &strategy);
        }
    });

    let consumer = thread::spawn(move || {
        let mut last = 0u64;
        for _ in 0..count {
            last = rx.pop(&strategy);
        }
        last
    });

    producer.join().unwrap();
    let last = consumer.join().unwrap();
    println!("{}: received {} items, last={}", name, count, last);
}

fn main() {
    run("SpinLoop", WaitStrategy::SpinLoop);
    run("Yield", WaitStrategy::Yield);
    run("Sleep(1µs)", WaitStrategy::Sleep(Duration::from_micros(1)));
}

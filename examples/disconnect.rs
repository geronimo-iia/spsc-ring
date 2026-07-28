/// Demonstrates disconnect detection: producer sends a finite stream, consumer
/// drains buffered items then exits cleanly when the producer is dropped.
use spsc_ring::{TryRecvError, ring};
use std::thread;

fn main() {
    let (tx, rx) = ring::<u32>(64).unwrap();

    let producer = thread::spawn(move || {
        for i in 0..20u32 {
            while tx.try_push(i).is_err() {
                std::hint::spin_loop();
            }
        }
        // tx dropped here — sets the closed flag
    });

    let consumer = thread::spawn(move || {
        let mut received = Vec::new();
        loop {
            match rx.try_pop() {
                Ok(v) => received.push(v),
                Err(TryRecvError::Empty) => std::hint::spin_loop(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        received
    });

    producer.join().unwrap();
    let received = consumer.join().unwrap();

    assert_eq!(received, (0..20).collect::<Vec<_>>());
    println!("received {} items, exited cleanly on disconnect", received.len());
}

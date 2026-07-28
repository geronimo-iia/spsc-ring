use spsc_ring::ring;
use std::thread;

fn main() {
    let (tx, rx) = ring::<u64>(64);

    let producer = thread::spawn(move || {
        for i in 0..100u64 {
            while tx.try_push(i).is_err() {
                std::hint::spin_loop();
            }
        }
        println!("produced 100 items");
    });

    let consumer = thread::spawn(move || {
        let mut received = Vec::with_capacity(100);
        while received.len() < 100 {
            if let Some(v) = rx.try_pop() {
                received.push(v);
            } else {
                std::hint::spin_loop();
            }
        }
        println!(
            "consumed {} items, last={}",
            received.len(),
            received.last().unwrap()
        );
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

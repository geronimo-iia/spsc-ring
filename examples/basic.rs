use spsc_ring::ring;
use std::thread;

fn main() {
    let (tx, rx) = ring::<u64>(64).unwrap();

    let producer = thread::spawn(move || {
        for i in 0..100u64 {
            loop {
                match tx.try_push(i) {
                    Ok(()) => break,
                    Err(spsc_ring::TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(spsc_ring::TrySendError::Disconnected(_)) => return,
                }
            }
        }
        println!("produced 100 items");
    });

    let consumer = thread::spawn(move || {
        let mut received = Vec::with_capacity(100);
        while received.len() < 100 {
            match rx.try_pop() {
                Ok(v) => received.push(v),
                Err(spsc_ring::TryRecvError::Empty) => std::hint::spin_loop(),
                Err(spsc_ring::TryRecvError::Disconnected) => break,
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

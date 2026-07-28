use spsc_ring::ring;
use std::thread;

fn main() {
    let (tx, rx) = ring::<u32>(256);
    let data: Vec<u32> = (0..1024).collect();
    let data_clone = data.clone();

    let producer = thread::spawn(move || {
        let mut sent = 0;
        while sent < data_clone.len() {
            sent += tx.push_slice(&data_clone[sent..]);
            std::hint::spin_loop();
        }
        println!("pushed {} items in bulk", sent);
    });

    let consumer = thread::spawn(move || {
        let mut received: Vec<u32> = Vec::with_capacity(1024);
        let mut buf = [0u32; 32];
        while received.len() < 1024 {
            let n = rx.pop_into_slice(&mut buf);
            received.extend_from_slice(&buf[..n]);
            std::hint::spin_loop();
        }
        assert_eq!(received, data);
        println!("received {} items in bulk, all correct", received.len());
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

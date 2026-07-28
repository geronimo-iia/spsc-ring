# spsc-ring

Lock-free SPSC ring buffer. Sequence-number protocol, cache-line padded, zero dependencies.

[![CI](https://github.com/geronimo-iia/spsc-ring/actions/workflows/ci.yml/badge.svg)](https://github.com/geronimo-iia/spsc-ring/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/spsc-ring.svg)](https://crates.io/crates/spsc-ring)
[![docs.rs](https://docs.rs/spsc-ring/badge.svg)](https://docs.rs/spsc-ring)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

## Background

This crate implements the single-producer/single-consumer (SPSC) variant of the [LMAX Disruptor pattern](https://lmax-exchange.github.io/disruptor/disruptor.html) — a lock-free ring buffer built around sequence numbers and `Acquire`/`Release` memory barriers, with no CAS operations on the hot path.

The key insight from the Disruptor paper: in the SPSC case, coordination requires no mutex and no compare-and-swap. Each slot carries a sequence stamp. The producer writes a value then releases the stamp; the consumer acquires the stamp then reads the value. The stamp is the only synchronisation point.

False sharing — the invisible performance killer — is eliminated by padding the producer and consumer cursors onto separate cache lines. Slots are aligned to prevent adjacent-slot invalidation under load.

The pattern is compelling enough that I had to explore it hands-on. `spsc-ring` is the result.

If you need more — multi-producer, consumer dependency graphs, pipeline fan-out, async, or static allocation — the Rust ecosystem has you covered: [disruptor-rs](https://crates.io/crates/disruptor), [rtrb](https://crates.io/crates/rtrb), [ringbuf](https://crates.io/crates/ringbuf).

## Features

- **Lock-free SPSC** — sequence-number protocol, Acquire/Release only
- **Cache-line padded** — producer/consumer cursors on separate cache lines (false-sharing eliminated by design)
- **Zero dependencies** — pure `std`, no optional feature flags pulling in extra crates
- **Compile-time SPSC contract** — `Producer<T>` and `Consumer<T>` are `Send` but not `Clone` and not `Sync` — sharing a half across threads is a compile error
- **Minimal surface** — `try_push`, `try_pop`, `push_slice`, `pop_into_slice`, `len` — nothing else to audit

## Performance

Measured on PoC-E, 1024-slot buffer.

| Benchmark                          | Mean (ms) | ± σ (ms) | Throughput      |
|------------------------------------|-----------|----------|-----------------|
| spsc_1M_events                     | 7.14      | ± 0.56   | ~140M ev/s      |
| spsc_push_slice_1M_chunk64         | 5.92      | ± 0.20   | ~169M ev/s      |
| spsc_push_slice_chunk_size/1       | 8.01      | ± 0.52   | ~125M ev/s      |
| spsc_push_slice_chunk_size/8       | 3.69      | ± 0.25   | ~271M ev/s      |
| spsc_push_slice_chunk_size/32      | 4.19      | ± 0.48   | ~239M ev/s      |
| spsc_push_slice_chunk_size/64      | 4.18      | ± 0.27   | ~239M ev/s      |
| spsc_push_slice_chunk_size/256     | 4.25      | ± 0.62   | ~235M ev/s      |

Median of 3 runs, ±σ across runs. Results vary by hardware and thermal state.

Slice ops yield ~1.9× throughput over single-item path at chunk≥8.

## Usage

```toml
[dependencies]
spsc-ring = "0.1"
```

```rust
use spsc_ring::{ring, TryRecvError, TrySendError};
use std::thread;

let (tx, rx) = ring::<u64>(64).unwrap();

thread::spawn(move || {
    for i in 0..100u64 {
        loop {
            match tx.try_push(i) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                Err(TrySendError::Disconnected(_)) => return,
            }
        }
    }
});

let mut received = Vec::new();
while received.len() < 100 {
    match rx.try_pop() {
        Ok(v) => received.push(v),
        Err(TryRecvError::Empty) => std::hint::spin_loop(),
        Err(TryRecvError::Disconnected) => break,
    }
}
assert_eq!(received, (0..100).collect::<Vec<_>>());
```

### Bulk throughput with `push_slice` / `pop_into_slice`

For `T: Copy`, slice ops amortise the per-item overhead and yield ~1.9× throughput at chunk≥8.

```rust
use spsc_ring::ring;
use std::thread;

let (tx, rx) = ring::<u32>(1024).unwrap();

let producer = thread::spawn(move || {
    let batch = [1u32; 32];
    let mut sent = 0;
    while sent < 1_000_000 {
        sent += tx.push_slice(&batch);
    }
});

let consumer = thread::spawn(move || {
    let mut buf = [0u32; 32];
    let mut received = 0;
    while received < 1_000_000 {
        received += rx.pop_into_slice(&mut buf);
    }
});

producer.join().unwrap();
consumer.join().unwrap();
```

### Disconnect detection

Both halves set a shared `closed` flag on drop. The other side observes it on the next call.

```rust
use spsc_ring::{ring, TryRecvError};
use std::thread;

let (tx, rx) = ring::<u32>(16).unwrap();

thread::spawn(move || {
    tx.try_push(42).unwrap();
    // tx dropped here — signals disconnect
});

loop {
    match rx.try_pop() {
        Ok(v) => println!("got {v}"),
        Err(TryRecvError::Empty) => std::hint::spin_loop(),
        Err(TryRecvError::Disconnected) => break,
    }
}
```

## API

| Symbol                               | Description                                                                                                         |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `ring<T>(capacity)`                  | Create buffer (capacity must be power of 2). Returns `Ok((Producer<T>, Consumer<T>))` or `Err(InvalidCapacity)`.    |
| `Producer::try_push(T)`              | Push value. Returns `Err(TrySendError::Full(T))` if full, `Err(TrySendError::Disconnected(T))` if consumer dropped. |
| `Consumer::try_pop()`                | Pop value. Returns `Err(TryRecvError::Empty)` if empty, `Err(TryRecvError::Disconnected)` if producer dropped.      |
| `Producer::push(T, &WaitStrategy)`   | Blocking push. Returns `Err(SendError(T))` if consumer dropped.                                                     |
| `Consumer::pop(&WaitStrategy)`       | Blocking pop. Returns `Err(RecvError)` if producer dropped and buffer empty.                                        |
| `Producer::is_disconnected()`        | Returns `true` if consumer has been dropped.                                                                        |
| `Consumer::is_disconnected()`        | Returns `true` if producer has been dropped.                                                                        |
| `{Producer,Consumer}::len()`         | Approximate item count (Relaxed snapshot — use for hints only, not correctness).                                    |
| `{Producer,Consumer}::capacity()`    | Buffer capacity.                                                                                                    |
| `Producer::is_empty()`               | Returns `true` if buffer appears empty.                                                                             |
| `Producer::is_full()`                | Returns `true` if buffer appears full.                                                                              |
| `Consumer::is_empty()`               | Returns `true` if buffer appears empty.                                                                             |
| `Producer::push_slice(&[T])`         | Push items from slice until full or disconnected. Returns count pushed. Requires `T: Copy`.                         |
| `Consumer::pop_into_slice(&mut [T])` | Pop items into slice until empty, full, or disconnected. Returns count popped. Requires `T: Copy`.                  |

## MSRV

Rust 1.95 (stable). No nightly required.

## License

[MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE)

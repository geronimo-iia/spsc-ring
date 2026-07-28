# ring-rs

Lock-free SPSC ring buffer. Sequence-number protocol, cache-line padded, zero dependencies.

[![CI](https://github.com/geronimo-iia/ring-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/geronimo-iia/ring-rs/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/ring-rs.svg)](https://crates.io/crates/ring-rs)
[![docs.rs](https://docs.rs/ring-rs/badge.svg)](https://docs.rs/ring-rs)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

## Features

- **Lock-free SPSC** — sequence-number protocol, Acquire/Release only
- **Cache-line padded** — producer/consumer cursors on separate cache lines
- **Zero dependencies** — pure `std`
- **Compile-time SPSC contract** — `Producer<T>` and `Consumer<T>` are `Send` but not `Clone`

## Performance

118M events/sec, 8ns/event (1024-slot buffer, measured on PoC-E).

## Usage

```toml
[dependencies]
ring-rs = "0.1"
```

```rust
use ring_rs::ring;
use std::thread;

let (tx, rx) = ring::<u64>(64);

thread::spawn(move || {
    for i in 0..100 {
        while tx.try_push(i).is_err() {
            std::hint::spin_loop();
        }
    }
});

let mut received = Vec::new();
while received.len() < 100 {
    if let Some(v) = rx.try_pop() {
        received.push(v);
    }
}
assert_eq!(received, (0..100).collect::<Vec<_>>());
```

## API

| Symbol | Description |
|--------|-------------|
| `ring<T>(capacity)` | Create buffer (capacity must be power of 2). Returns `(Producer<T>, Consumer<T>)`. |
| `Producer::try_push(T)` | Push value. Returns `Err(value)` if full. |
| `Consumer::try_pop()` | Pop value. Returns `None` if empty. |
| `{Producer,Consumer}::len()` | Approximate item count. |
| `{Producer,Consumer}::capacity()` | Buffer capacity. |
| `Producer::is_empty()` | Returns `true` if buffer appears empty. |
| `Producer::is_full()` | Returns `true` if buffer appears full. |
| `Consumer::is_empty()` | Returns `true` if buffer appears empty. |

## MSRV

Rust 1.95 (stable). No nightly required.

## License

MIT OR Apache-2.0

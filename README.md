# spsc-ring

Lock-free SPSC ring buffer. Sequence-number protocol, cache-line padded, zero dependencies.

[![CI](https://github.com/geronimo-iia/spsc-ring/actions/workflows/ci.yml/badge.svg)](https://github.com/geronimo-iia/spsc-ring/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/spsc-ring.svg)](https://crates.io/crates/spsc-ring)
[![docs.rs](https://docs.rs/spsc-ring/badge.svg)](https://docs.rs/spsc-ring)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

## Features

- **Lock-free SPSC** — sequence-number protocol, Acquire/Release only
- **Cache-line padded** — producer/consumer cursors on separate cache lines (false-sharing eliminated by design)
- **Zero dependencies** — pure `std`, no optional feature flags pulling in extra crates
- **Compile-time SPSC contract** — `Producer<T>` and `Consumer<T>` are `Send` but not `Clone`
- **Minimal surface** — `try_push`, `try_pop`, `push_slice`, `pop_into_slice`, `len` — nothing else to audit

## Performance

118M events/sec, 8ns/event (1024-slot buffer, measured on PoC-E).

## Comparison

| | spsc-ring | [ringbuf](https://crates.io/crates/ringbuf) | [rtrb](https://crates.io/crates/rtrb) |
|---|---|---|---|
| Cache-line padded | ✅ | ❌ | ✅ |
| Explicit memory ordering (Acq/Rel) | ✅ | unspecified | ✅ |
| Zero dependencies | ✅ | optional dep | ✅ |
| API surface | minimal | large | medium |
| Bulk slice ops | ✅ | ✅ | ✅ |
| `no_alloc` / static storage | ❌ | ✅ | ❌ |
| Async / blocking variants | ❌ | ✅ | ❌ |

Choose `spsc-ring` when you want the smallest, most auditable SPSC queue with guaranteed cache-line isolation and no transitive dependencies. Choose `ringbuf` when you need static allocation or async wrappers.

## Usage

```toml
[dependencies]
spsc-ring = "0.1"
```

```rust
use spsc_ring::ring;
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
| `Producer::push_slice(&[T])` | Push items from slice until full. Returns count pushed. Requires `T: Copy`. |
| `Consumer::pop_into_slice(&mut [T])` | Pop items into slice until empty or full. Returns count popped. Requires `T: Copy`. |

## MSRV

Rust 1.95 (stable). No nightly required.

## License

MIT OR Apache-2.0

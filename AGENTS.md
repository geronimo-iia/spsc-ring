# spsc-ring — Agent Context

Lock-free SPSC ring buffer. Sequence-number protocol, cache-line padded, zero dependencies.

Single crate — no workspace. All implementation in `src/lib.rs`.

## Layout

```
src/
  lib.rs          # everything: WaitStrategy, Producer<T>, Consumer<T>, ring(), 9 unit tests
benches/
  throughput.rs   # Criterion: 1M event SPSC throughput
docs/
  superpowers/
    plans/        # implementation plans
```

## Public API

```rust
// Create buffer (capacity must be power of 2)
pub fn ring<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>)

pub struct Producer<T> {
    pub fn try_push(&self, value: T) -> Result<(), T>     // non-blocking
    pub fn push(&self, value: T, strategy: &WaitStrategy) // blocking (0.2.0+)
    pub fn len(&self) -> usize
    pub fn is_empty(&self) -> bool
    pub fn is_full(&self) -> bool
    pub fn capacity(&self) -> usize
}

pub struct Consumer<T> {
    pub fn try_pop(&self) -> Option<T>                    // non-blocking
    pub fn pop(&self, strategy: &WaitStrategy) -> T       // blocking (0.2.0+)
    pub fn len(&self) -> usize
    pub fn is_empty(&self) -> bool
    pub fn capacity(&self) -> usize
}

pub enum WaitStrategy {
    SpinLoop,            // hint::spin_loop() — lowest latency, highest CPU
    Yield,               // thread::yield_now() — balanced
    Sleep(Duration),     // thread::sleep(d) — lowest CPU, highest latency
}
```

## Design invariants — do not change without understanding these

- **Sequence-number protocol**: slot carries a stamp written by producer *after* storing the value; consumer checks *before* reading. Acquire/Release only — no SeqCst.
- **Cache-line padding**: `PaddedAtomicUsize` pads producer and consumer cursors to 64 bytes. Prevents false sharing. Do not remove the `_pad` field.
- **SPSC contract**: `ring()` returns one `Producer` and one `Consumer`. Each is `Send` but not `Clone`. The type system enforces single-producer/single-consumer — never add `Clone`.
- **Power-of-two capacity**: mask trick (`tail & rb.mask`) requires power of two. The panic in `ring()` is intentional and load-bearing.
- **Unbounded sequence counters**: head/tail never wrap to zero; only slot index uses modulo. This is correct — do not add manual wrapping.

## Toolchain

- Rust 1.95, edition 2024
- `rustfmt.toml` max_width=100
- `clippy.toml` `avoid-breaking-exported-api = false`

## Commands

```bash
cargo test                        # 9 unit tests (+ doc test)
cargo test <name>                 # single test by name
cargo fmt --all -- --check        # must pass clean
cargo clippy --all-targets -- -D warnings   # must pass clean
cargo bench --no-run              # verify bench compiles
cargo bench                       # run Criterion throughput (1M events)
cargo doc --no-deps               # build rustdoc
cargo publish --dry-run           # verify crates.io package
```

## Versioning

| Version | Content |
|---------|---------|
| 0.1.0 | `try_push`, `try_pop`, `ring()` |
| 0.2.0 | `WaitStrategy`, blocking `push`/`pop` |

## Writing plans

When asked to write a plan:
1. Read `src/lib.rs` — derive exact signatures and test patterns from the live code
2. Save to `docs/superpowers/plans/YYYY-MM-DD-<feature-name>.md`
3. Self-review: every requirement has a task; every task shows exact code, not placeholders; tasks compile independently in sequence

## Executing plans

When asked to execute a plan:
1. Read `AGENTS.md` (this file) first
2. Execute tasks in order
3. After all tasks complete, run full verification: `cargo test`, `cargo fmt --check`, `cargo clippy`, `cargo publish --dry-run`

## Commit convention

Conventional commits, single line: `type(scope): short description`
Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `bench`, `ci`, `chore`, `perf`
Scope: `spsc-ring` (or omit for trivial changes)
One commit per plan task.

# spsc-ring — Agent Context

Lock-free SPSC ring buffer. Sequence-number protocol, cache-line padded, zero dependencies.

Single crate — no workspace. All implementation in `src/lib.rs`.

## Layout

```
src/
  lib.rs          # everything: WaitStrategy, Producer<T>, Consumer<T>, ring(), 41 unit tests
benches/
  throughput.rs   # Criterion: 1M event SPSC throughput
examples/
  basic.rs        # single-item try_push/try_pop
  bulk.rs         # push_slice/pop_into_slice
  wait_strategy.rs
docs/
  backlog.md      # known gaps and future work
.claude/
  plans/          # implementation plans (YYYY-MM-DD-<feature-name>.md)
```

## Public API

```rust
// Create buffer (capacity must be power of 2)
pub fn ring<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), InvalidCapacity>

pub struct Producer<T> {  // Send, !Sync, !Clone
    pub fn try_push(&self, value: T) -> Result<(), TrySendError<T>>
    pub fn push(&self, value: T, strategy: &WaitStrategy) -> Result<(), SendError<T>>
    pub fn push_slice(&self, src: &[T]) -> usize          // T: Copy
    pub fn len(&self) -> usize
    pub fn is_empty(&self) -> bool
    pub fn is_full(&self) -> bool
    pub fn capacity(&self) -> usize
    pub fn is_disconnected(&self) -> bool
}

pub struct Consumer<T> {  // Send, !Sync, !Clone
    pub fn try_pop(&self) -> Result<T, TryRecvError>
    pub fn pop(&self, strategy: &WaitStrategy) -> Result<T, RecvError>
    pub fn pop_into_slice(&self, dst: &mut [T]) -> usize  // T: Copy
    pub fn len(&self) -> usize
    pub fn is_empty(&self) -> bool
    pub fn capacity(&self) -> usize
    pub fn is_disconnected(&self) -> bool
}

pub enum WaitStrategy {
    SpinLoop,            // hint::spin_loop() — lowest latency, highest CPU
    Yield,               // thread::yield_now() — balanced
    Sleep(Duration),     // thread::sleep(d) — lowest CPU, highest latency
}
```

## Design invariants — do not change without understanding these

- **Sequence-number protocol**: slot carries a stamp written by producer *after* storing the value; consumer checks *before* reading. Acquire/Release only — no SeqCst.
- **`MaybeUninit<T>` slots**: `Slot<T>` uses `UnsafeCell<MaybeUninit<T>>`. The sequence number encodes occupancy — no `Option` discriminant write on pop. `assume_init_read` on pop, `assume_init_drop` in `RingBuffer::drop`.
- **`#[repr(align(32))]` on `Slot<T>`**: 2 slots per cache line. Halves false-sharing vs unpadded 16B slots while keeping 1024-slot ring (32KB) within L1D. Do not remove.
- **Cache-line padding**: `PaddedAtomicUsize` pads producer and consumer cursors to 64 bytes. Prevents false sharing on head/tail. Do not remove the `_pad` field.
- **`closed` field placement**: `AtomicBool closed` sits before `head`/`tail` in `RingBuffer`, grouped with write-once `mask`. Keeps hot head/tail cache lines uncontaminated by the disconnect write.
- **SPSC contract**: `ring()` returns one `Producer` and one `Consumer`. Each is `Send` but not `Clone` and not `Sync` (`PhantomData<Cell<()>>`). Never add `Clone` or remove the `PhantomData`.
- **Power-of-two capacity**: mask trick (`tail & rb.mask`) requires power of two. The `Err(InvalidCapacity)` in `ring()` is intentional and load-bearing.
- **Unbounded sequence counters**: head/tail never wrap to zero; only slot index uses modulo. This is correct — do not add manual wrapping.

## Toolchain

- Rust 1.95, edition 2024
- `rustfmt.toml` max_width=100
- `clippy.toml` `avoid-breaking-exported-api = false`

## Commands

```bash
cargo test                        # 41 unit tests (+ doc tests)
cargo test <name>                 # single test by name
cargo fmt --all -- --check        # must pass clean
cargo clippy --all-targets -- -D warnings   # must pass clean
cargo bench --no-run              # verify bench compiles
cargo bench                       # run Criterion throughput (1M events)
cargo doc --no-deps               # build rustdoc
cargo publish --dry-run           # verify crates.io package
```

## Writing plans

When asked to write a plan:
1. Read `src/lib.rs` — derive exact signatures and test patterns from the live code
2. Save to `.claude/plans/YYYY-MM-DD-<feature-name>.md`
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

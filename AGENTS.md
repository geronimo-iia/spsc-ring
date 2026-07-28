# spsc-ring — Agent Context

Lock-free SPSC ring buffer. Sequence-number protocol, cache-line padded, zero dependencies.

Single crate — no workspace. All implementation in `src/lib.rs`.

## Layout

```
src/
  lib.rs              # everything: internals, public API, unit tests, loom tests
benches/
  throughput.rs       # Criterion: 1M event throughput, slice chunk sizes
examples/
  basic.rs            # try_push / try_pop single-item loop
  bulk.rs             # push_slice / pop_into_slice batch throughput
  wait_strategy.rs    # blocking push / pop with WaitStrategy
  disconnect.rs       # producer drops early; consumer drains then exits
docs/
  backlog.md          # known gaps and future work
.claude/
  plans/              # implementation plans (YYYY-MM-DD-<feature-name>.md)
```

## Public API

Read `src/lib.rs` for authoritative signatures. Do not duplicate them here.

Key entry point: `pub fn ring<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), InvalidCapacity>`

Capacity must be a non-zero power of two. Returns one `Producer<T>` and one `Consumer<T>` — each `Send`, not `Clone`, not `Sync`.

## Design invariants — do not change without understanding these

- **Sequence-number protocol**: slot stamp written by producer *after* storing value; consumer checks *before* reading. `Acquire`/`Release` only — no `SeqCst`. Changing ordering breaks the protocol.
- **`MaybeUninit<T>` slots**: `Slot<T>` uses `UnsafeCell<MaybeUninit<T>>`. Sequence number encodes occupancy — no `Option` discriminant write on pop. `assume_init_read` on pop, `assume_init_drop` in `RingBuffer::drop`. Never replace with `Option<T>`.
- **`#[repr(align(32))]` on `Slot<T>`**: 2 slots per cache line. Halves false-sharing vs unpadded 16B slots while keeping 1024-slot ring (32KB) within L1D. `align(64)` blows past L1 and regresses ~37%. Do not remove or increase.
- **Cache-line padding on cursors**: `PaddedAtomicUsize` pads `head` and `tail` to 64 bytes each. Prevents false sharing. Do not remove `_pad`.
- **`closed` field placement**: `AtomicBool closed` sits before `head`/`tail` in `RingBuffer`, grouped with write-once `mask`. Keeps hot head/tail cache lines uncontaminated by the disconnect write.
- **SPSC contract**: `PhantomData<Cell<()>>` on both halves enforces `!Sync`. Never add `Clone` or remove the `PhantomData`.
- **Power-of-two capacity**: mask trick `tail & rb.mask` requires power of two. `Err(InvalidCapacity)` in `ring()` is load-bearing.
- **Unbounded sequence counters**: `head`/`tail` never wrap to zero — only slot index uses modulo. Do not add manual wrapping.

## Toolchain

- Rust 1.95, edition 2024
- `rustfmt.toml` max_width=100
- `clippy.toml` `avoid-breaking-exported-api = false`

## Commands

```bash
cargo test                                          # unit tests + doc tests
cargo test <name>                                   # single test by name
RUSTFLAGS="--cfg loom" cargo test --test '*'        # loom model-checker (slow)
cargo fmt --all -- --check                          # must pass clean
cargo clippy --all-targets -- -D warnings           # must pass clean
cargo run --example basic                           # smoke-test examples
cargo run --example bulk
cargo run --example wait_strategy
cargo run --example disconnect
cargo bench --no-run                                # verify bench compiles
cargo bench                                         # run Criterion throughput
cargo doc --no-deps                                 # build rustdoc
cargo publish --dry-run                             # final gate before release
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

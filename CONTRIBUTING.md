# Contributing

## Prerequisites

- Rust stable (MSRV 1.95). No nightly required.
- `cargo` only — no build scripts, no external tools required for basic dev.
- Optional: `cargo-instruments` (macOS) for profiling. Install: `cargo install cargo-instruments`.

## Building

```bash
cargo build
```

## Testing

```bash
cargo test
```

All 9 tests must pass before a PR.

## Formatting and linting

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

Both must be clean before committing.

## Running benchmarks

```bash
cargo bench
```

Benchmark results are not committed. Record significant results manually in a comment on the relevant PR or issue.

## Commit style

Conventional commits, single line. Format: `type(scope): short description`.
Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `ci`, `chore`.

## Pull requests

- One logical change per PR
- Tests must pass
- `cargo fmt` and `cargo clippy` must be clean

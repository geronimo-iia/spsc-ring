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

All tests must pass before a PR.

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

## Release process

### Branch strategy

`main` is always releasable — tagged commits only. Feature work lands on a
`release/vX.Y.Z` integration branch, not directly on `main`.

```
feat/xxx  ─┐
feat/yyy  ─┼─▶  release/vX.Y.Z  ─▶  main  (tag vX.Y.Z)
feat/zzz  ─┘
```

1. Open `release/vX.Y.Z` from `main` at the start of the milestone.
2. Each `feat/...` PR targets `release/vX.Y.Z`, not `main`.
3. Run the pre-release checklist as commits on `release/vX.Y.Z`.
4. One final PR merges `release/vX.Y.Z` → `main`; tag on the merge commit.

Hotfixes branch from the relevant tag and merge back to `main`.

### Pre-release checklist

- [ ] All tests pass: `cargo test`
- [ ] Doc tests pass: `cargo test --doc`
- [ ] Formatted: `cargo fmt --all -- --check`
- [ ] No lint issues: `cargo clippy --all-targets -- -D warnings`
- [ ] Release build clean: `cargo build --release --locked`
- [ ] Bench compiles: `cargo bench --no-run`
- [ ] Dry-run publish clean: `cargo publish --dry-run`
- [ ] `CHANGELOG.md` section dated and complete
- [ ] Public types have `///` rustdoc; `cargo doc --no-deps` zero warnings
- [ ] Version bumped in `Cargo.toml` and `Cargo.lock` updated

### Tagging and publishing

```bash
# 1. Bump version in Cargo.toml, update CHANGELOG date
cargo update -p spsc-ring

# 2. Commit on release branch, push, open PR
git commit -am "chore: release vX.Y.Z"
git push origin release/vX.Y.Z
gh pr create --title "chore: release vX.Y.Z" --base main

# 3. Wait for CI to pass, merge to main
git checkout main
git merge --no-ff release/vX.Y.Z

# 4. Tag and push — GitHub Actions publishes to crates.io
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

Tags containing `-rc` (e.g. `v0.2.0-rc1`) follow the same steps but the
`publish` job is skipped — nothing sent to crates.io.

### Hotfix

```bash
git checkout -b hotfix/vX.Y.Z+1 vX.Y.Z
# apply fix, bump patch version in Cargo.toml
git commit -am "fix: description"
git tag -a vX.Y.Z+1 -m "Hotfix vX.Y.Z+1"
git push origin hotfix/vX.Y.Z+1 vX.Y.Z+1
git checkout main
git merge --no-ff hotfix/vX.Y.Z+1
git push origin main
```

### CHANGELOG format

Move `[Unreleased]` entries to a versioned section:

```markdown
## [0.2.0] — 2026-MM-DD

### Added
- …

### Fixed
- …

## [Unreleased]
```

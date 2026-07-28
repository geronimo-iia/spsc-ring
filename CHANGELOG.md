# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-28

### Added

- `ring<T>(capacity)` — create SPSC ring buffer; returns `(Producer<T>, Consumer<T>)` pair
- `Producer<T>` — write half; `try_push(T) -> Result<(), T>`; `len`, `is_empty`, `is_full`, `capacity`
- `Consumer<T>` — read half; `try_pop() -> Option<T>`; `len`, `is_empty`, `capacity`
- Sequence-number protocol with Acquire/Release ordering (no SeqCst)
- Cache-line padding (64 bytes) on producer/consumer cursors to prevent false sharing
- SPSC contract enforced at compile time: each half is `Send` but not `Clone`
- Criterion benchmark: 1M events SPSC throughput
- 9 unit tests covering push/pop, fullness, emptiness, FIFO order, wrap-around, concurrent SPSC, panics

[Unreleased]: https://github.com/geronimo-iia/ring-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/geronimo-iia/ring-rs/releases/tag/v0.1.0

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ring<T>(capacity)` — create SPSC ring buffer; returns `(Producer<T>, Consumer<T>)` pair
- `Producer<T>` — write half; `try_push(T) -> Result<(), T>`; `len`, `is_empty`, `is_full`, `capacity`
- `Consumer<T>` — read half; `try_pop() -> Option<T>`; `len`, `is_empty`, `capacity`
- `Producer::push_slice(&[T]) -> usize` — bulk push, requires `T: Copy`
- `Consumer::pop_into_slice(&mut [T]) -> usize` — bulk drain, requires `T: Copy`
- `WaitStrategy` enum — `SpinLoop`, `Yield`, `Sleep(Duration)` — controls blocking behavior
- `Producer::push(value, &WaitStrategy)` — blocking push; spins/yields/sleeps until slot free
- `Consumer::pop(&WaitStrategy)` — blocking pop; spins/yields/sleeps until value available
- Sequence-number protocol with Acquire/Release ordering (no SeqCst)
- Cache-line padding (64 bytes) on producer/consumer cursors to prevent false sharing
- SPSC contract enforced at compile time: each half is `Send` but not `Clone`
- Criterion benchmark: 1M events SPSC throughput


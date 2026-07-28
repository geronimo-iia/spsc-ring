# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ring<T>(capacity) -> Result<(Producer<T>, Consumer<T>), InvalidCapacity>` — create SPSC ring buffer; capacity must be power of 2
- `Producer<T>` — write half; `len`, `is_empty`, `is_full`, `capacity`, `is_disconnected`
- `Consumer<T>` — read half; `len`, `is_empty`, `capacity`, `is_disconnected`
- `Producer::try_push(T) -> Result<(), TrySendError<T>>` — non-blocking push; `Full(T)` if full, `Disconnected(T)` if consumer dropped
- `Consumer::try_pop() -> Result<T, TryRecvError>` — non-blocking pop; `Empty` if empty, `Disconnected` if producer dropped
- `Producer::push(T, &WaitStrategy) -> Result<(), SendError<T>>` — blocking push; returns `Err` if consumer dropped
- `Consumer::pop(&WaitStrategy) -> Result<T, RecvError>` — blocking pop; returns `Err` if producer dropped and buffer empty
- `Producer::push_slice(&[T]) -> usize` — bulk push, requires `T: Copy`
- `Consumer::pop_into_slice(&mut [T]) -> usize` — bulk drain, requires `T: Copy`
- `WaitStrategy` enum (`Copy`) — `SpinLoop`, `Yield`, `Sleep(Duration)` — controls blocking behavior
- Disconnect detection via `AtomicBool` closed flag; set on `Drop` of either half
- Sequence-number protocol with Acquire/Release ordering (no SeqCst)
- Cache-line padding (64 bytes) on producer/consumer cursors to prevent false sharing
- SPSC contract enforced at compile time: each half is `Send` but not `Clone`
- Criterion benchmark: 1M events SPSC throughput
- Loom concurrency tests for core push/pop protocol


//! # spsc-ring
//!
//! Lock-free SPSC ring buffer.
//!
//! ## Design
//!
//! Sequence-number protocol: each slot carries a stamp that the producer writes
//! *after* storing the value, and the consumer checks *before* reading.
//! Acquire/Release ordering only (no `SeqCst`). Cache-line padding prevents
//! false sharing between producer and consumer cursors.
//!
//! The SPSC contract is enforced at compile time: [`ring`] returns a
//! `(Producer<T>, Consumer<T>)` pair. Each half is `Send` but not `Clone`.
//!
//! ## Example
//!
//! ```
//! use spsc_ring::{ring, TryRecvError, TrySendError};
//! use std::thread;
//!
//! let (tx, rx) = ring::<u64>(64).unwrap();
//!
//! thread::spawn(move || {
//!     for i in 0..100u64 {
//!         loop {
//!             match tx.try_push(i) {
//!                 Ok(()) => break,
//!                 Err(TrySendError::Full(_)) => std::hint::spin_loop(),
//!                 Err(TrySendError::Disconnected(_)) => return,
//!             }
//!         }
//!     }
//! });
//!
//! let mut received = Vec::new();
//! while received.len() < 100 {
//!     match rx.try_pop() {
//!         Ok(v) => received.push(v),
//!         Err(TryRecvError::Empty) => std::hint::spin_loop(),
//!         Err(TryRecvError::Disconnected) => break,
//!     }
//! }
//! assert_eq!(received, (0..100).collect::<Vec<_>>());
//! ```

#![deny(missing_docs)]
#![allow(unsafe_code)]

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Error returned by [`Consumer::pop`] when the producer has been dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer disconnected")
    }
}

impl std::error::Error for RecvError {}

/// Error returned by [`Producer::push`] when the consumer has been dropped.
#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T: std::fmt::Debug> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "consumer disconnected; value: {:?}", self.0)
    }
}

impl<T: std::fmt::Debug + 'static> std::error::Error for SendError<T> {}

/// Error returned by [`Consumer::try_pop`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Buffer is empty; try again later.
    Empty,
    /// Producer has been dropped; no more items will arrive.
    Disconnected,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "buffer empty"),
            TryRecvError::Disconnected => write!(f, "producer disconnected"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Error returned by [`Producer::try_push`].
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// Buffer is full; value returned unchanged.
    Full(T),
    /// Consumer has been dropped; value returned unchanged.
    Disconnected(T),
}

impl<T: std::fmt::Debug> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(v) => write!(f, "buffer full; value: {:?}", v),
            TrySendError::Disconnected(v) => write!(f, "consumer disconnected; value: {:?}", v),
        }
    }
}

impl<T: std::fmt::Debug + 'static> std::error::Error for TrySendError<T> {}

/// Error returned by [`ring`] when capacity is zero or not a power of two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCapacity(pub usize);

impl std::fmt::Display for InvalidCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "capacity {} is not a non-zero power of two", self.0)
    }
}

impl std::error::Error for InvalidCapacity {}

/// Strategy used by blocking [`Producer::push`] and [`Consumer::pop`] while waiting.
#[derive(Debug, Clone, Copy)]
pub enum WaitStrategy {
    /// Spin with [`std::hint::spin_loop`]. Lowest latency, highest CPU burn.
    SpinLoop,
    /// Yield the thread with [`std::thread::yield_now`]. Balanced.
    Yield,
    /// Sleep for a fixed duration. Lowest CPU burn, highest latency.
    Sleep(std::time::Duration),
}

impl WaitStrategy {
    #[inline]
    fn wait(&self) {
        match self {
            WaitStrategy::SpinLoop => std::hint::spin_loop(),
            WaitStrategy::Yield => std::thread::yield_now(),
            WaitStrategy::Sleep(d) => std::thread::sleep(*d),
        }
    }
}

const CACHE_LINE: usize = 64;

#[repr(C)]
struct PaddedAtomicUsize {
    value: AtomicUsize,
    _pad: [u8; CACHE_LINE - size_of::<AtomicUsize>()],
}

impl PaddedAtomicUsize {
    const fn new(v: usize) -> Self {
        Self {
            value: AtomicUsize::new(v),
            _pad: [0; CACHE_LINE - size_of::<AtomicUsize>()],
        }
    }
}

// 32-byte alignment: 2 slots per cache line. Halves false-sharing vs unpadded 16B slots
// while keeping 1024-slot ring (32KB) within typical L1D cache.
#[repr(align(32))]
struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    mask: usize,
    head: PaddedAtomicUsize,
    tail: PaddedAtomicUsize,
    closed: AtomicBool,
}

// SAFETY: The SPSC contract is enforced by the type system — only one Producer
// and one Consumer exist. The sequence-number protocol ensures no data race.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Drain any items remaining in the buffer so their destructors run.
        // head and tail are exclusively owned at this point (both Arc halves dropped).
        let mut head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Relaxed);
        while head != tail {
            let slot = &self.slots[head & self.mask];
            // SAFETY: head != tail means producer wrote this slot and consumer
            // has not yet read it. No other thread is alive (both halves dropped).
            unsafe { (*slot.value.get()).assume_init_drop() };
            head = head.wrapping_add(1);
        }
    }
}

/// Create an SPSC ring buffer with the given capacity (must be a power of 2).
///
/// Returns `(Producer, Consumer)` — send each half to its own thread.
///
/// # Errors
///
/// Returns [`InvalidCapacity`] if `capacity` is zero or not a power of two.
pub fn ring<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), InvalidCapacity> {
    if capacity == 0 || !capacity.is_power_of_two() {
        return Err(InvalidCapacity(capacity));
    }

    let slots: Vec<Slot<T>> = (0..capacity)
        .map(|i| Slot {
            sequence: AtomicUsize::new(i),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect();

    let inner = Arc::new(RingBuffer {
        slots: slots.into_boxed_slice(),
        mask: capacity - 1,
        head: PaddedAtomicUsize::new(0),
        tail: PaddedAtomicUsize::new(0),
        closed: AtomicBool::new(false),
    });

    Ok((
        Producer {
            inner: Arc::clone(&inner),
            _not_sync: PhantomData,
        },
        Consumer {
            inner,
            _not_sync: PhantomData,
        },
    ))
}

/// Write half of the SPSC ring. Not `Clone` — only one producer exists.
pub struct Producer<T> {
    inner: Arc<RingBuffer<T>>,
    _not_sync: PhantomData<Cell<()>>,
}

/// Read half of the SPSC ring. Not `Clone` — only one consumer exists.
pub struct Consumer<T> {
    inner: Arc<RingBuffer<T>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T> Producer<T> {
    /// Returns `true` if the consumer has been dropped.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Push a value. Returns `Err` if the buffer is full or the consumer has been dropped.
    ///
    /// # Errors
    ///
    /// - [`TrySendError::Full`] — buffer is full; value is returned unchanged.
    /// - [`TrySendError::Disconnected`] — consumer has been dropped; value is returned unchanged.
    #[inline]
    pub fn try_push(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        let rb = &*self.inner;
        let tail = rb.tail.value.load(Ordering::Relaxed);
        let slot = &rb.slots[tail & rb.mask];
        let seq = slot.sequence.load(Ordering::Acquire);

        if seq != tail {
            return Err(TrySendError::Full(value));
        }

        // SAFETY: Sole producer. Sequence check guarantees consumer finished reading this slot.
        unsafe { (*slot.value.get()).write(value) };
        slot.sequence.store(tail + 1, Ordering::Release);
        rb.tail.value.store(tail + 1, Ordering::Relaxed);
        Ok(())
    }

    /// Approximate number of items currently in the buffer.
    ///
    /// # Why approximate
    ///
    /// Reads both `tail` (owned by the producer) and `head` (owned by the
    /// consumer) with [`Ordering::Relaxed`]. The returned value can differ
    /// from the true count in either direction: a concurrent pop can lower it,
    /// and a stale Relaxed read of `head` can raise it. The result is a
    /// best-effort snapshot, not a linearizable read.
    ///
    /// # Safe uses
    ///
    /// - Capacity planning and monitoring dashboards.
    /// - Backpressure hints (e.g., slow down if `len() > threshold`).
    ///
    /// # Must NOT be used for
    ///
    /// - Deciding whether `try_push` will succeed — use the `Err` return value
    ///   of `try_push` instead.
    /// - Any correctness decision that requires an exact count.
    ///
    /// # Example
    ///
    /// ```
    /// use spsc_ring::ring;
    /// let (tx, _rx) = ring::<u32>(16).unwrap();
    /// tx.try_push(1).unwrap();
    /// tx.try_push(2).unwrap();
    /// // len() is a hint — do not assert == 2 across threads.
    /// let _ = tx.len(); // safe: backpressure hint only
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        let rb = &*self.inner;
        let tail = rb.tail.value.load(Ordering::Relaxed);
        let head = rb.head.value.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Returns `true` if the buffer appears empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the buffer appears full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    /// Buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.mask + 1
    }

    /// Push a value, blocking with `strategy` until a slot is free.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the value if the consumer has been dropped.
    pub fn push(&self, value: T, strategy: &WaitStrategy) -> Result<(), SendError<T>> {
        let mut v = value;
        loop {
            match self.try_push(v) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(returned)) => return Err(SendError(returned)),
                Err(TrySendError::Full(returned)) => {
                    strategy.wait();
                    v = returned;
                }
            }
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
    }
}

impl<T: Copy> Producer<T> {
    /// Push as many items from `src` as fit. Returns count pushed.
    ///
    /// Stops early if the buffer is full or the consumer has been dropped.
    /// If `count < src.len()`, call [`Producer::is_disconnected`] to distinguish
    /// the two cases — a full buffer is retriable, a disconnect is permanent.
    #[inline]
    pub fn push_slice(&self, src: &[T]) -> usize {
        let mut count = 0;
        for &item in src {
            match self.try_push(item) {
                Ok(()) => count += 1,
                Err(_) => break,
            }
        }
        count
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
    }
}

impl<T> Consumer<T> {
    /// Returns `true` if the producer has been dropped.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Pop a value.
    ///
    /// # Errors
    ///
    /// - [`TryRecvError::Empty`] — buffer is empty; try again later.
    /// - [`TryRecvError::Disconnected`] — producer has been dropped and buffer is empty.
    #[inline]
    pub fn try_pop(&self) -> Result<T, TryRecvError> {
        let rb = &*self.inner;
        let head = rb.head.value.load(Ordering::Relaxed);
        let slot = &rb.slots[head & rb.mask];
        let seq = slot.sequence.load(Ordering::Acquire);

        if seq != head + 1 {
            if rb.closed.load(Ordering::Acquire) {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }

        // SAFETY: Sole consumer. Sequence check guarantees producer finished writing.
        // assume_init_read performs a bitwise copy — safe because the producer wrote
        // a valid T and we release the slot immediately after, ensuring no double-read.
        let value = unsafe { (*slot.value.get()).assume_init_read() };
        slot.sequence.store(head + rb.mask + 1, Ordering::Release);
        rb.head.value.store(head + 1, Ordering::Relaxed);
        Ok(value)
    }

    /// Approximate number of items currently in the buffer.
    ///
    /// # Why approximate
    ///
    /// Reads both `tail` (owned by the producer) and `head` (owned by the
    /// consumer) with [`Ordering::Relaxed`]. The producer may have advanced
    /// `tail` between the two loads, so the returned value can be *lower* than
    /// the true count. The result is a best-effort snapshot, not a
    /// linearizable read.
    ///
    /// # Safe uses
    ///
    /// - Capacity planning and monitoring dashboards.
    /// - Backpressure hints (e.g., slow down if `len() < threshold` before
    ///   sleeping).
    ///
    /// # Must NOT be used for
    ///
    /// - Deciding whether `try_pop` will return `Ok` — use the `Err` return
    ///   value of `try_pop` instead.
    /// - Any correctness decision that requires an exact count.
    ///
    /// # Example
    ///
    /// ```
    /// use spsc_ring::ring;
    /// let (tx, rx) = ring::<u32>(16).unwrap();
    /// tx.try_push(1).unwrap();
    /// tx.try_push(2).unwrap();
    /// // len() is a hint — do not assert == 2 across threads.
    /// let _ = rx.len(); // safe: backpressure hint only
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        let rb = &*self.inner;
        let tail = rb.tail.value.load(Ordering::Relaxed);
        let head = rb.head.value.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Returns `true` if the buffer appears empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.mask + 1
    }

    /// Pop a value, blocking with `strategy` until one is available.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] if the producer has been dropped and the buffer is empty.
    pub fn pop(&self, strategy: &WaitStrategy) -> Result<T, RecvError> {
        loop {
            match self.try_pop() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => strategy.wait(),
            }
        }
    }
}

impl<T: Copy> Consumer<T> {
    /// Pop as many items into `dst` as are available. Returns count popped.
    ///
    /// Stops early if the buffer is empty or the producer has been dropped.
    /// If `count < dst.len()`, call [`Consumer::is_disconnected`] to distinguish
    /// the two cases — an empty buffer may refill, a disconnect will not.
    #[inline]
    pub fn pop_into_slice(&self, dst: &mut [T]) -> usize {
        let mut count = 0;
        for slot in dst.iter_mut() {
            match self.try_pop() {
                Ok(v) => {
                    *slot = v;
                    count += 1;
                }
                Err(_) => break,
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn push_pop_single() {
        let (tx, rx) = ring(4).unwrap();
        assert!(tx.try_push(42).is_ok());
        assert_eq!(rx.try_pop(), Ok(42));
    }

    #[test]
    fn full_returns_err() {
        let (tx, _rx) = ring(2).unwrap();
        assert!(tx.try_push(1).is_ok());
        assert!(tx.try_push(2).is_ok());
        assert_eq!(tx.try_push(3), Err(TrySendError::Full(3)));
    }

    #[test]
    fn empty_returns_none() {
        let (_tx, rx) = ring::<i32>(4).unwrap();
        assert_eq!(rx.try_pop(), Err(TryRecvError::Empty));
    }

    #[test]
    fn fifo_order() {
        let (tx, rx) = ring(4).unwrap();
        for i in 0..4 {
            tx.try_push(i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
    }

    #[test]
    fn wrap_around() {
        let (tx, rx) = ring(4).unwrap();
        for round in 0..3 {
            for i in 0..4 {
                tx.try_push(round * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(rx.try_pop(), Ok(round * 4 + i));
            }
        }
    }

    #[test]
    fn concurrent_spsc() {
        let (tx, rx) = ring(64).unwrap();
        let count = 100_000;

        let producer = thread::spawn(move || {
            for i in 0..count {
                loop {
                    match tx.try_push(i) {
                        Ok(()) => break,
                        Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                        Err(TrySendError::Disconnected(_)) => return,
                    }
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(count);
            while received.len() < count {
                match rx.try_pop() {
                    Ok(v) => received.push(v),
                    Err(TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();
        let expected: Vec<usize> = (0..count).collect();
        assert_eq!(received, expected);
    }

    #[test]
    fn len_and_capacity() {
        let (tx, rx) = ring(4).unwrap();
        assert_eq!(tx.capacity(), 4);
        assert!(rx.is_empty());
        tx.try_push(1).unwrap();
        assert_eq!(rx.len(), 1);
        tx.try_push(2).unwrap();
        tx.try_push(3).unwrap();
        tx.try_push(4).unwrap();
        assert!(tx.is_full());
    }

    #[test]
    fn push_slice_all_fit() {
        let (tx, rx) = ring(8).unwrap();
        let data = [1u32, 2, 3, 4];
        let pushed = tx.push_slice(&data);
        assert_eq!(pushed, 4);
        for &expected in &data {
            assert_eq!(rx.try_pop(), Ok(expected));
        }
    }

    #[test]
    fn push_slice_partial_when_full() {
        let (tx, _rx) = ring(2).unwrap();
        let _ = tx.push_slice(&[10u32, 20]);
        let pushed = tx.push_slice(&[30u32, 40]);
        assert_eq!(pushed, 0);
    }

    #[test]
    fn pop_into_slice_all_available() {
        let (tx, rx) = ring(8).unwrap();
        for i in 0..4u32 {
            tx.try_push(i).unwrap();
        }
        let mut dst = [0u32; 4];
        let popped = rx.pop_into_slice(&mut dst);
        assert_eq!(popped, 4);
        assert_eq!(dst, [0, 1, 2, 3]);
    }

    #[test]
    fn pop_into_slice_empty_buffer() {
        let (_tx, rx) = ring::<u32>(4).unwrap();
        let mut dst = [0u32; 4];
        let popped = rx.pop_into_slice(&mut dst);
        assert_eq!(popped, 0);
        assert_eq!(dst, [0u32; 4]);
    }

    #[test]
    fn pop_into_slice_partial_dst() {
        let (tx, rx) = ring(8).unwrap();
        for i in 0..6u32 {
            tx.try_push(i).unwrap();
        }
        let mut dst = [0u32; 3];
        let popped = rx.pop_into_slice(&mut dst);
        assert_eq!(popped, 3);
        assert_eq!(dst, [0, 1, 2]);
        assert_eq!(rx.try_pop(), Ok(3));
    }

    #[test]
    fn push_pop_slice_roundtrip_concurrent() {
        let (tx, rx) = ring(256).unwrap();
        let data: Vec<u32> = (0..1024).collect();
        let data_clone = data.clone();

        let producer = thread::spawn(move || {
            let mut sent = 0;
            while sent < data_clone.len() {
                sent += tx.push_slice(&data_clone[sent..]);
                std::hint::spin_loop();
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(1024);
            let mut buf = [0u32; 32];
            while received.len() < 1024 {
                let n = rx.pop_into_slice(&mut buf);
                received.extend_from_slice(&buf[..n]);
                std::hint::spin_loop();
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();
        let expected: Vec<u32> = (0..1024).collect();
        assert_eq!(received, expected);
    }

    #[test]
    fn ring_returns_err_on_non_power_of_two() {
        assert!(ring::<u32>(3).is_err());
        assert!(ring::<u32>(0).is_err());
    }

    #[test]
    fn ring_returns_ok_on_valid_capacity() {
        assert!(ring::<u32>(4).is_ok());
        assert!(ring::<u32>(1).is_ok());
    }

    #[test]
    fn is_disconnected_false_while_both_live() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        assert!(!tx.is_disconnected());
        assert!(!rx.is_disconnected());
    }

    #[test]
    fn is_disconnected_true_after_drop() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        drop(rx);
        assert!(tx.is_disconnected());
    }

    #[test]
    fn producer_drop_signals_disconnected() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        drop(tx);
        assert_eq!(rx.try_pop(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn consumer_drop_signals_disconnected() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        drop(rx);
        assert_eq!(tx.try_push(1), Err(TrySendError::Disconnected(1)));
    }

    #[test]
    fn try_pop_returns_empty_when_buffer_empty() {
        let (_tx, rx) = ring::<u32>(4).unwrap();
        assert_eq!(rx.try_pop(), Err(TryRecvError::Empty));
    }

    #[test]
    fn try_push_returns_full_when_buffer_full() {
        let (tx, _rx) = ring::<u32>(2).unwrap();
        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();
        assert_eq!(tx.try_push(3), Err(TrySendError::Full(3)));
    }

    #[test]
    fn pop_returns_recv_error_on_disconnect() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        drop(tx);
        assert_eq!(rx.pop(&WaitStrategy::SpinLoop), Err(RecvError));
    }

    #[test]
    fn push_returns_send_error_on_disconnect() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        drop(rx);
        assert_eq!(tx.push(42, &WaitStrategy::SpinLoop), Err(SendError(42)));
    }

    #[test]
    fn pop_returns_value_before_checking_disconnect() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        tx.try_push(99).unwrap();
        drop(tx);
        assert_eq!(rx.pop(&WaitStrategy::SpinLoop), Ok(99));
        assert_eq!(rx.pop(&WaitStrategy::SpinLoop), Err(RecvError));
    }

    #[test]
    fn try_pop_drains_buffered_items_after_producer_drop() {
        let (tx, rx) = ring::<u32>(4).unwrap();
        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();
        tx.try_push(3).unwrap();
        drop(tx);
        assert_eq!(rx.try_pop(), Ok(1));
        assert_eq!(rx.try_pop(), Ok(2));
        assert_eq!(rx.try_pop(), Ok(3));
        assert_eq!(rx.try_pop(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn error_types_exist() {
        let _: RecvError = RecvError;
        let _: SendError<u32> = SendError(42);
        let _: TryRecvError = TryRecvError::Empty;
        let _: TryRecvError = TryRecvError::Disconnected;
        let _: TrySendError<u32> = TrySendError::Full(1);
        let _: TrySendError<u32> = TrySendError::Disconnected(2);
    }

    #[test]
    fn wait_strategy_is_copy() {
        let s = WaitStrategy::Sleep(std::time::Duration::from_millis(1));
        let _a = s;
        let _b = s; // would fail to compile if not Copy
    }

    #[test]
    fn push_slice_partial_fit() {
        let (tx, rx) = ring(4).unwrap();
        tx.push_slice(&[1u32, 2]);
        let pushed = tx.push_slice(&[3u32, 4, 5, 6]);
        assert_eq!(pushed, 2);
        assert_eq!(rx.try_pop(), Ok(1));
        assert_eq!(rx.try_pop(), Ok(2));
        assert_eq!(rx.try_pop(), Ok(3));
        assert_eq!(rx.try_pop(), Ok(4));
        assert_eq!(rx.try_pop(), Err(TryRecvError::Empty));
    }

    #[test]
    fn wait_strategy_spin_loop_waits_until_slot_free() {
        let (tx, rx) = ring(2).unwrap();
        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();

        let consumer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(5));
            rx.try_pop().unwrap();
            rx
        });

        tx.push(3, &WaitStrategy::SpinLoop).unwrap();
        let rx = consumer.join().unwrap();
        assert_eq!(rx.try_pop(), Ok(2));
        assert_eq!(rx.try_pop(), Ok(3));
    }

    #[test]
    fn wait_strategy_yield_waits_until_value_available() {
        let (tx, rx) = ring(4).unwrap();

        let producer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(5));
            tx.try_push(42).unwrap();
        });

        let value = rx.pop(&WaitStrategy::Yield).unwrap();
        assert_eq!(value, 42u32);
        producer.join().unwrap();
    }

    #[test]
    fn wait_strategy_sleep_push_pop() {
        use std::time::Duration;
        let (tx, rx) = ring(4).unwrap();
        tx.push(99u64, &WaitStrategy::Sleep(Duration::from_millis(1)))
            .unwrap();
        assert_eq!(
            rx.pop(&WaitStrategy::Sleep(Duration::from_millis(1)))
                .unwrap(),
            99u64
        );
    }

    #[test]
    fn wait_strategy_sleep_exercises_spin_on_full_buffer() {
        use std::time::Duration;
        let (tx, rx) = ring(2).unwrap();
        tx.try_push(1u32).unwrap();
        tx.try_push(2u32).unwrap();

        // Consumer drains after a delay — forces push to spin-sleep before slot is free.
        let consumer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            rx.try_pop().unwrap();
            rx
        });

        tx.push(3u32, &WaitStrategy::Sleep(Duration::from_millis(1)))
            .unwrap();
        let rx = consumer.join().unwrap();
        assert_eq!(rx.try_pop(), Ok(2));
        assert_eq!(rx.try_pop(), Ok(3));
    }

    #[test]
    fn push_slice_stops_on_disconnect() {
        let (tx, rx) = ring::<u32>(8).unwrap();
        drop(rx);
        // Buffer is empty and consumer dropped — first try_push returns Disconnected.
        let pushed = tx.push_slice(&[1, 2, 3, 4]);
        assert_eq!(pushed, 0);
        assert!(tx.is_disconnected());
    }

    #[test]
    fn pop_into_slice_stops_on_disconnect() {
        let (tx, rx) = ring::<u32>(8).unwrap();
        tx.try_push(10).unwrap();
        tx.try_push(20).unwrap();
        drop(tx);
        let mut dst = [0u32; 4];
        // Drains buffered items, then stops at Disconnected.
        let popped = rx.pop_into_slice(&mut dst);
        assert_eq!(popped, 2);
        assert_eq!(dst[0], 10);
        assert_eq!(dst[1], 20);
    }

    #[test]
    fn both_halves_drop_simultaneously() {
        // Dropping both ends from separate threads must not double-free or panic.
        let (tx, rx) = ring::<u32>(4).unwrap();
        let t1 = std::thread::spawn(move || drop(tx));
        let t2 = std::thread::spawn(move || drop(rx));
        t1.join().unwrap();
        t2.join().unwrap();
    }
}

#[cfg(loom)]
mod loom_tests {
    use super::ring;
    use loom::thread;

    /// Loom explores all thread interleavings of a single push followed by a
    /// single pop on a 2-slot buffer.  A 2-slot buffer is the smallest
    /// power-of-two that lets both slots be exercised.  Keep the iteration
    /// count tiny — loom's state space is exponential in the number of
    /// synchronisation operations.
    #[test]
    fn push_then_pop_all_interleavings() {
        loom::model(|| {
            let (tx, rx) = ring(2).expect("valid capacity");

            let producer = thread::spawn(move || {
                tx.try_push(42usize).ok();
            });

            let consumer = thread::spawn(move || rx.try_pop());

            producer.join().unwrap();
            let _result = consumer.join().unwrap();
        });
    }

    /// Producer pushes two items; consumer pops both.  Verifies wrap-around
    /// under loom's scheduler.
    #[test]
    fn push_pop_two_items() {
        loom::model(|| {
            let (tx, rx) = ring(2).expect("valid capacity");

            let producer = thread::spawn(move || {
                loop {
                    match tx.try_push(1usize) {
                        Ok(()) => break,
                        Err(_) => loom::hint::spin_loop(),
                    }
                }
                loop {
                    match tx.try_push(2usize) {
                        Ok(()) => break,
                        Err(_) => loom::hint::spin_loop(),
                    }
                }
            });

            let consumer = thread::spawn(move || {
                let mut got = Vec::new();
                while got.len() < 2 {
                    match rx.try_pop() {
                        Ok(v) => got.push(v),
                        Err(_) => loom::hint::spin_loop(),
                    }
                }
                got
            });

            producer.join().unwrap();
            let got = consumer.join().unwrap();
            assert_eq!(got, vec![1, 2]);
        });
    }
}

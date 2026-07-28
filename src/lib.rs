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
//! ## Performance
//!
//! 118M events/sec, 8ns/event (measured on PoC-E, 1024-slot buffer).
//! Target: >100M events/sec, <100ns/event.
//!
//! ## Example
//!
//! ```
//! use spsc_ring::ring;
//! use std::thread;
//!
//! let (tx, rx) = ring::<u64>(64);
//!
//! thread::spawn(move || {
//!     for i in 0..100 {
//!         while tx.try_push(i).is_err() {
//!             std::hint::spin_loop();
//!         }
//!     }
//! });
//!
//! let mut received = Vec::new();
//! while received.len() < 100 {
//!     if let Some(v) = rx.try_pop() {
//!         received.push(v);
//!     }
//! }
//! assert_eq!(received, (0..100).collect::<Vec<_>>());
//! ```

#![deny(missing_docs)]
#![allow(unsafe_code)]

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<Option<T>>,
}

struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    mask: usize,
    head: PaddedAtomicUsize,
    tail: PaddedAtomicUsize,
}

// SAFETY: The SPSC contract is enforced by the type system — only one Producer
// and one Consumer exist. The sequence-number protocol ensures no data race.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

/// Create an SPSC ring buffer with the given capacity (must be a power of 2).
///
/// Returns `(Producer, Consumer)` — send each half to its own thread.
///
/// # Panics
///
/// Panics if `capacity` is zero or not a power of two.
#[must_use]
pub fn ring<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity > 0 && capacity.is_power_of_two(),
        "capacity must be a non-zero power of two"
    );

    let slots: Vec<Slot<T>> = (0..capacity)
        .map(|i| Slot {
            sequence: AtomicUsize::new(i),
            value: UnsafeCell::new(None),
        })
        .collect();

    let inner = Arc::new(RingBuffer {
        slots: slots.into_boxed_slice(),
        mask: capacity - 1,
        head: PaddedAtomicUsize::new(0),
        tail: PaddedAtomicUsize::new(0),
    });

    (
        Producer {
            inner: Arc::clone(&inner),
        },
        Consumer { inner },
    )
}

/// Write half of the SPSC ring. Not `Clone` — only one producer exists.
pub struct Producer<T> {
    inner: Arc<RingBuffer<T>>,
}

/// Read half of the SPSC ring. Not `Clone` — only one consumer exists.
pub struct Consumer<T> {
    inner: Arc<RingBuffer<T>>,
}

impl<T> Producer<T> {
    /// Push a value. Returns `Err(value)` if the buffer is full.
    ///
    /// # Errors
    ///
    /// Returns `Err(value)` if the buffer is full.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let rb = &*self.inner;
        let tail = rb.tail.value.load(Ordering::Relaxed);
        let slot = &rb.slots[tail & rb.mask];
        let seq = slot.sequence.load(Ordering::Acquire);

        if seq != tail {
            return Err(value);
        }

        // SAFETY: Sole producer. Sequence check guarantees consumer is done with this slot.
        unsafe { *slot.value.get() = Some(value) };
        slot.sequence.store(tail + 1, Ordering::Release);
        rb.tail.value.store(tail + 1, Ordering::Relaxed);
        Ok(())
    }

    /// Approximate number of items in the buffer.
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
}

impl<T: Copy> Producer<T> {
    /// Push as many items from `src` as fit. Returns count pushed.
    pub fn push_slice(&self, src: &[T]) -> usize {
        let mut count = 0;
        for &item in src {
            if self.try_push(item).is_err() {
                break;
            }
            count += 1;
        }
        count
    }
}

impl<T> Consumer<T> {
    /// Pop a value. Returns `None` if the buffer is empty.
    #[must_use]
    pub fn try_pop(&self) -> Option<T> {
        let rb = &*self.inner;
        let head = rb.head.value.load(Ordering::Relaxed);
        let slot = &rb.slots[head & rb.mask];
        let seq = slot.sequence.load(Ordering::Acquire);

        if seq != head + 1 {
            return None;
        }

        // SAFETY: Sole consumer. Sequence check guarantees producer finished writing.
        let value = unsafe { (*slot.value.get()).take() };
        slot.sequence.store(head + rb.mask + 1, Ordering::Release);
        rb.head.value.store(head + 1, Ordering::Relaxed);
        value
    }

    /// Approximate number of items in the buffer.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn push_pop_single() {
        let (tx, rx) = ring(4);
        assert!(tx.try_push(42).is_ok());
        assert_eq!(rx.try_pop(), Some(42));
    }

    #[test]
    fn full_returns_err() {
        let (tx, _rx) = ring(2);
        assert!(tx.try_push(1).is_ok());
        assert!(tx.try_push(2).is_ok());
        assert_eq!(tx.try_push(3), Err(3));
    }

    #[test]
    fn empty_returns_none() {
        let (_tx, rx) = ring::<i32>(4);
        assert_eq!(rx.try_pop(), None);
    }

    #[test]
    fn fifo_order() {
        let (tx, rx) = ring(4);
        for i in 0..4 {
            tx.try_push(i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(rx.try_pop(), Some(i));
        }
    }

    #[test]
    fn wrap_around() {
        let (tx, rx) = ring(4);
        for round in 0..3 {
            for i in 0..4 {
                tx.try_push(round * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(rx.try_pop(), Some(round * 4 + i));
            }
        }
    }

    #[test]
    fn concurrent_spsc() {
        let (tx, rx) = ring(64);
        let count = 100_000;

        let producer = thread::spawn(move || {
            for i in 0..count {
                while tx.try_push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(count);
            while received.len() < count {
                if let Some(v) = rx.try_pop() {
                    received.push(v);
                } else {
                    std::hint::spin_loop();
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
        let (tx, rx) = ring(4);
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
    #[should_panic(expected = "capacity must be a non-zero power of two")]
    fn non_power_of_two_panics() {
        let _ = ring::<i32>(3);
    }

    #[test]
    #[should_panic(expected = "capacity must be a non-zero power of two")]
    fn zero_capacity_panics() {
        let _ = ring::<i32>(0);
    }

    #[test]
    fn push_slice_all_fit() {
        let (tx, rx) = ring(8);
        let data = [1u32, 2, 3, 4];
        let pushed = tx.push_slice(&data);
        assert_eq!(pushed, 4);
        for &expected in &data {
            assert_eq!(rx.try_pop(), Some(expected));
        }
    }

    #[test]
    fn push_slice_partial_when_full() {
        let (tx, _rx) = ring(2);
        let _ = tx.push_slice(&[10u32, 20]);
        let pushed = tx.push_slice(&[30u32, 40]);
        assert_eq!(pushed, 0);
    }

    #[test]
    fn push_slice_partial_fit() {
        let (tx, rx) = ring(4);
        tx.push_slice(&[1u32, 2]);
        let pushed = tx.push_slice(&[3u32, 4, 5, 6]);
        assert_eq!(pushed, 2);
        assert_eq!(rx.try_pop(), Some(1));
        assert_eq!(rx.try_pop(), Some(2));
        assert_eq!(rx.try_pop(), Some(3));
        assert_eq!(rx.try_pop(), Some(4));
        assert_eq!(rx.try_pop(), None);
    }
}

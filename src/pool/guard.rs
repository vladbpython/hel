use super::instance::{NONE, State};
use crate::helper::panic::PanicReason;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::Ordering,
};

pub struct OwnerGuard<'a> {
    state: &'a State,
    id: usize,
}

impl<'a> OwnerGuard<'a> {
    pub fn new(state: &'a State, id: usize) -> Self {
        Self { state, id }
    }
}

impl Drop for OwnerGuard<'_> {
    fn drop(&mut self) {
        // Runs on normal exit AND on unwind: release every shard this worker still owns,
        // so a handler panic cannot unused a shard.
        for shard in 0..self.state.shards() {
            let _ = self.state.owner(shard).compare_exchange(
                self.id,
                NONE,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
    }
}

/// Holds the item while the handler runs, so it is never dropped on the floor.
/// The worker owns the item until the handler commits it `slot.take()`,
/// which for an async handler happens on the far side of an `.await`.
/// If the runtime cancels the worker task at that await, the whole frame is dropped,
/// without this guard the item went with it, counted by nothing and delivered nowhere.
/// A panic is already handled by the worker, which is why the guard is `disarm`ed as soon as the handler's future resolves,
/// from that point the worker decides what happens to the item.
pub(crate) struct CommitGuard<'a, T, D: Fn(T, PanicReason)> {
    slot: Option<T>,
    dead_letter: &'a D,
    armed: bool,
}

impl<'a, T, D: Fn(T, PanicReason)> CommitGuard<'a, T, D> {
    pub(crate) fn new(item: T, dead_letter: &'a D) -> Self {
        Self {
            slot: Some(item),
            dead_letter,
            armed: true,
        }
    }

    /// The slot the handler writes through.
    pub(crate) fn slot(&mut self) -> &mut Option<T> {
        &mut self.slot
    }

    /// The handler finished or panicked, hand the item back to the worker.
    pub(crate) fn disarm(mut self) -> Option<T> {
        self.armed = false;
        self.slot.take()
    }
}

impl<T, D: Fn(T, PanicReason)> Drop for CommitGuard<'_, T, D> {
    fn drop(&mut self) {
        // Only fires when the guard was never disarmed, i.e. the frame is going
        // away without the handler ever finishing -> task cancellation.
        if !self.armed {
            return;
        }
        if let Some(item) = self.slot.take() {
            // Dead letter sink that panics must not turn a cancellation into double panic.
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                (self.dead_letter)(item, PanicReason::cancelled())
            }));
        }
    }
}

pub(crate) struct CommitBatchGuard<'a, T, D: Fn(T, PanicReason)> {
    buf: &'a mut Vec<T>,
    dead_letter: &'a D,
}

impl<'a, T, D: Fn(T, PanicReason)> CommitBatchGuard<'a, T, D> {
    /// Takes ownership of every item currently in `buf`.
    pub(crate) fn new(buf: &'a mut Vec<T>, dead_letter: &'a D) -> Self {
        buf.reverse();
        Self { buf, dead_letter }
    }

    /// Next item in the order the batch was received.
    pub(crate) fn next(&mut self) -> Option<T> {
        self.buf.pop()
    }
}

impl<T, D: Fn(T, PanicReason)> Drop for CommitBatchGuard<'_, T, D> {
    fn drop(&mut self) {
        while let Some(item) = self.buf.pop() {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                (self.dead_letter)(item, PanicReason::cancelled())
            }));
        }
    }
}

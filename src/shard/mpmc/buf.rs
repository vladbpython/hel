use std::collections::VecDeque;

/// Internal helper for error paths of keyed batch methods:
/// returns the failed group's remainder AND every not-yet-attempted
/// group back into `buf`. Without this, the early `return Err` would
/// silently DROP all unprocessed groups (data loss).
/// NOTE: after an error `buf` holds elements grouped by shard, not in
/// the original insertion order; per-key FIFO order within each group
/// is preserved, which is the only ordering ShardKey guarantees.
#[inline]
pub fn refill_on_error<T>(
    buf: &mut Vec<T>,
    failed_group: Vec<T>,
    remaining: impl Iterator<Item = (usize, Vec<T>)>,
) {
    buf.extend(failed_group);
    for (_, g) in remaining {
        buf.extend(g);
    }
}

// Holds the single item that is "in flight" inside `send_async_from`.
// Why a guard is needed at all: `send_batch_async` is itself an async fn,
// so its locals live in ITS future's frame and die when that future is dropped.
// Only `buf` survives a cancellation, it is borrowed from the caller.
// This guard bridges the two: `Drop` moves the pending item back into `buf`.
// Runs on every exit (a no op, `pending` is already `None`), disconnect, panic, and cancellation at the `.await`.

pub struct RestoreOne<'a, T> {
    buf: &'a mut Vec<T>,
    // Borrowed by `send_async_from`; `None` once the item reached the channel.
    pending: Option<T>,
    front: bool,
}

impl<'a, T> RestoreOne<'a, T> {
    // Taken from the back (`buf.pop()`, round robin has no ordering guarantee) -> put back to the back.
    pub fn back(buf: &'a mut Vec<T>, value: T) -> Self {
        Self {
            buf,
            pending: Some(value),
            front: false,
        }
    }

    pub fn slot(&mut self) -> &mut Option<T> {
        &mut self.pending
    }
}

impl<T> Drop for RestoreOne<'_, T> {
    fn drop(&mut self) {
        if let Some(v) = self.pending.take() {
            if self.front {
                self.buf.insert(0, v);
            } else {
                self.buf.push(v);
            }
        }
    }
}

// Owns the whole routed batch while it is being sent.
// Everything lives here instead of in the async fn's locals, because locals die
// with the future on cancellation. `buf` is borrowed from the caller and outlives us,
// so `Drop` can pour the leftovers back into it.
pub struct RestoreGroups<'a, T> {
    buf: &'a mut Vec<T>,
    // Per shard FIFO queues; index = shard. A deque, not a Vec,
    // because backpressure path takes the head one item at a time: `Vec::remove(0)`
    groups: Vec<VecDeque<T>>,
    // Reusable staging area for the bulk path. `try_send_batch` needs `&mut Vec<T>`,
    // so a chunk is moved here, sent, and whatever did not fit goes back to the front of its group.
    scratch: Vec<T>,
    // Which group `scratch` was filled from, so an unwind can put it back.
    scratch_shard: usize,
    // Items whose key is not in the map.
    unused: Vec<T>,
    // The single item currently inside `send_async_from`.
    pending: Option<T>,
    // Which group `pending` was taken from.
    pending_shard: usize,
}

impl<'a, T> RestoreGroups<'a, T> {
    pub fn new(buf: &'a mut Vec<T>, groups: Vec<Vec<T>>, unused: Vec<T>) -> Self {
        Self {
            buf,
            groups: groups.into_iter().map(VecDeque::from).collect(),
            scratch: Vec::new(),
            scratch_shard: 0,
            unused,
            pending: None,
            pending_shard: 0,
        }
    }

    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    pub fn is_group_empty(&self, shard: usize) -> bool {
        self.groups[shard].is_empty()
    }

    /// The FIFO head. Callers read the key off it before it moves into pending.
    pub fn head(&self, shard: usize) -> &T {
        self.groups[shard].front().expect("group is empty")
    }

    /// Move up to max items from the head of `shard` into the staging buffer and hand it to `try_send_batch`.
    /// Must be followed by `bulk_return`, with no await in between.
    pub fn bulk_stage(&mut self, shard: usize, max: usize) -> &mut Vec<T> {
        debug_assert!(self.scratch.is_empty(), "staging buffer was not returned");
        let take = max.min(self.groups[shard].len());
        self.scratch.extend(self.groups[shard].drain(..take));
        self.scratch_shard = shard;
        &mut self.scratch
    }

    /// Whatever the channel could not take goes back to the front of its group,
    /// in order, so per key FIFO survives.
    pub fn bulk_return(&mut self) {
        let g = &mut self.groups[self.scratch_shard];
        for v in self.scratch.drain(..).rev() {
            g.push_front(v);
        }
    }

    // Moves the FIFO head of `shard` into the pending slot and hands the slot to `send_async_from`.
    // The item never leaves the guard, so cancellation at the `.await` cannot swallow it.
    // O(1).
    pub fn take_head(&mut self, shard: usize) -> &mut Option<T> {
        debug_assert!(self.pending.is_none(), "pending slot must be free");
        self.pending = Some(self.groups[shard].pop_front().expect("group is empty"));
        self.pending_shard = shard;
        &mut self.pending
    }
}

impl<T> Drop for RestoreGroups<'_, T> {
    fn drop(&mut self) {
        // Staged items first: they sit ahead of the rest of their group.
        if !self.scratch.is_empty() {
            let g = &mut self.groups[self.scratch_shard];
            for v in self.scratch.drain(..).rev() {
                g.push_front(v);
            }
        }
        // The in flight item goes back to the head of its own group, so per key FIFO survives.
        if let Some(v) = self.pending.take() {
            self.groups[self.pending_shard].push_front(v);
        }
        // Groups first, orphans last, same contract as `refill_on_error`.
        for group in self.groups.iter_mut() {
            self.buf.extend(group.drain(..));
        }
        self.buf.append(&mut self.unused);
    }
}

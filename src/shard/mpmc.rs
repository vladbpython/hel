//! Segmented channels: N independent ring buffers without blocking.
//!
//! Two types:
//! -[`ShardRoundRobin`] -cyclic routing, uniform load distribution, without a key
//! -[`ShardKey`] — routing by key, sorting by symbol, hash(key) → shard
//!
//! # Type selection
//!
//! ```text
//! Stateless workers (HTTP, logs, tasks) → ShardRoundRobin
//! Routing using symbols (trading, actors) → ShardKey
//! ```
//!
//! Example
//!
//! ```
//! use hel::channel::mpmc::{round_robin, shard_key};
//!
//! //RoundRobin: without a key
//! let (tx, rx) = round_robin::<u64, 128>(4);
//! tx.try_send(42).unwrap();
//!
//! //ByKey: with a key, the order is guaranteed
//! let (tx, rx) = shard_key::<u64, 128>(4);
//! tx.try_send("AAPL", 150).unwrap();
//! ```

use super::errors as shard_error;

use crate::internal_channel::{
    core::MPMCInner,
    errors::{AsyncRecvError, AsyncSendError, SendError, TryRecvError, TrySendError},
    mpmc_bounded, nearest_power_of_two,
    receiver::Receiver,
    sender::Sender,
    sync::{AsyncSlot, SyncList},
    traits::InnerChannel,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize, atomic::Ordering},
    task::{Context, Poll},
    time::Duration,
};

// Hash routing───────────────

#[inline(always)]
fn fnv1a(key: &str) -> usize {
    key.bytes().fold(14695981039346656037u64, |h, b| {
        (h ^ b as u64).wrapping_mul(1099511628211)
    }) as usize
}

#[inline(always)]
fn xxhash(key: &str) -> usize {
    xxhash_rust::xxh3::xxh3_64(key.as_bytes()) as usize
}

/// Adaptive hash: ≤16 bytes → FNV-1a, >16 bytes → xxHash3.
#[inline(always)]
pub(crate) fn hash_key(key: &str) -> usize {
    if key.len() <= 16 {
        fnv1a(key)
    } else {
        xxhash(key)
    }
}

// Helper trair

trait IntoValue<T> {
    fn into_value(self) -> T;
}

impl<T> IntoValue<T> for TrySendError<T> {
    fn into_value(self) -> T {
        match self {
            TrySendError::Full(v) | TrySendError::Disconnected(v) => v,
        }
    }
}
impl<T> IntoValue<T> for SendError<T> {
    fn into_value(self) -> T {
        match self {
            SendError::Disconnected(v) => v,
            SendError::TimeOut((v, _)) => v,
        }
    }
}
impl<T> IntoValue<T> for AsyncSendError<T> {
    fn into_value(self) -> T {
        match self {
            AsyncSendError::Disconnected(v) => v,
        }
    }
}

/// Sharded channel with round robin routing.
/// Each `push` goes to the next shard in sequence.
/// No key even load distribution across consumers.
/// Cloning
/// Each clone has an independent cursor starts from 0.
pub struct ShardRoundRobin<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + 'static = MPMCInner<T, CAP>,
> {
    senders: Arc<[Sender<T, CAP, I>]>,
    cursor: AtomicUsize,
    mask: usize,
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> ShardRoundRobin<T, CAP, I> {
    #[inline(always)]
    fn next_shard(&self) -> usize {
        self.cursor.fetch_add(1, Ordering::Relaxed) & self.mask
    }

    /// Non-blocking sending to the next shard.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), shard_error::ShardTrySendError<T>> {
        let shard = self.next_shard();
        self.senders[shard]
            .try_send(value)
            .map_err(|err| shard_error::ShardTrySendError { shard, err })
    }

    /// Blocking sending to the next shard.
    #[inline]
    pub fn send(&self, value: T) -> Result<(), shard_error::ShardSendError<T>> {
        let shard = self.next_shard();
        self.senders[shard]
            .send(value)
            .map_err(|err| shard_error::ShardSendError { shard, err })
    }

    /// Blocking sending with deadline.
    #[inline]
    pub fn send_timeout(
        &self,
        value: T,
        d: Duration,
    ) -> Result<(), shard_error::ShardSendError<T>> {
        let shard = self.next_shard();
        self.senders[shard]
            .send_timeout(value, d)
            .map_err(|err| shard_error::ShardSendError { shard, err })
    }

    /// Async sending to the next shard.
    #[inline]
    pub async fn send_async(&self, value: T) -> Result<(), shard_error::ShardAsyncSendError<T>> {
        let shard = self.next_shard();
        self.senders[shard]
            .send_async(value)
            .await
            .map_err(|err| shard_error::ShardAsyncSendError { shard, err })
    }

    /// Number of shards.
    #[inline]
    pub fn shards(&self) -> usize {
        self.mask + 1
    }

    /// Non-blocking batch to the next shard.
    pub fn try_send_batch(
        &self,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardTryBatchSendError> {
        let shard = self.next_shard();
        self.senders[shard]
            .try_send_batch(buf)
            .map_err(|e| shard_error::ShardTryBatchSendError {
                shard,
                sent: e.sent,
                reason: e.err,
            })
    }

    /// Blocking batch.
    pub fn send_batch(&self, buf: &mut Vec<T>) -> Result<usize, shard_error::ShardBatchSendError> {
        let shard = self.next_shard();
        self.senders[shard]
            .send_batch(buf)
            .map_err(|e| shard_error::ShardBatchSendError {
                shard,
                sent: e.sent,
                reason: e.err,
            })
    }

    /// Blocking batch с deadline.
    pub fn send_batch_timeout(
        &self,
        buf: &mut Vec<T>,
        d: Duration,
    ) -> Result<usize, shard_error::ShardBatchSendError> {
        let shard = self.next_shard();
        self.senders[shard].send_batch_timeout(buf, d).map_err(|e| {
            shard_error::ShardBatchSendError {
                shard,
                sent: e.sent,
                reason: e.err,
            }
        })
    }

    /// Async batch to the next shard.
    /// Waits for space → only Disconnected interrupts.
    pub async fn send_batch_async(
        &self,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardAsyncBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let shard = self.next_shard();
        let mut total = 0usize;
        let mut items = Vec::new();
        for v in buf.drain(..) {
            match self.senders[shard].send_async(v).await {
                Ok(()) => total += 1,
                Err(e) => items.push(e.into_value()),
            }
        }
        if items.is_empty() {
            Ok(total)
        } else {
            Err(shard_error::ShardAsyncBatchSendError { shard, sent: total })
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Clone
    for ShardRoundRobin<T, CAP, I>
{
    fn clone(&self) -> Self {
        let senders: Arc<[Sender<T, CAP, I>]> = self
            .senders
            .iter()
            .map(Sender::clone)
            .collect::<Vec<_>>()
            .into();
        Self {
            senders,
            cursor: AtomicUsize::new(0),
            mask: self.mask,
        }
    }
}

// ShardedKey (ByKey)

/// Sharded channel with ByKey routing.
/// `hash(key) & mask` → deterministic shard.
/// All events with one key always go to one consumer ordering is guaranteed.
/// Cloning
/// The clones share the same routing `shard_for("AAPL")` is the same for all clones.
pub struct ShardKey<T: Send + 'static, const CAP: usize> {
    senders: Arc<[Sender<T, CAP>]>,
    mask: usize,
}

impl<T: Send + 'static, const CAP: usize> ShardKey<T, CAP> {
    /// Non-blocking sending via hash(key).
    #[inline]
    pub fn try_send(
        &self,
        key: &str,
        value: T,
    ) -> Result<(), shard_error::ShardKeyTrySendError<T>> {
        let shard = hash_key(key) & self.mask;
        self.senders[shard]
            .try_send(value)
            .map_err(|err| shard_error::ShardKeyTrySendError {
                key: key.to_string(),
                shard,
                err,
            })
    }

    /// Blocking sending by hash(key).
    #[inline]
    pub fn send(&self, key: &str, value: T) -> Result<(), shard_error::ShardKeySendError<T>> {
        let shard = hash_key(key) & self.mask;
        self.senders[shard]
            .send(value)
            .map_err(|err| shard_error::ShardKeySendError {
                key: key.to_string(),
                shard,
                err,
            })
    }

    /// Async sending using hash(key).
    #[inline]
    pub async fn send_async(
        &self,
        key: &str,
        value: T,
    ) -> Result<(), shard_error::ShardKeyAsyncSendError<T>> {
        let shard = hash_key(key) & self.mask;
        self.senders[shard].send_async(value).await.map_err(|err| {
            shard_error::ShardKeyAsyncSendError {
                key: key.to_string(),
                shard,
                err,
            }
        })
    }

    /// The shard index for a given key is deterministic.
    #[inline]
    pub fn shard_for(&self, key: &str) -> usize {
        hash_key(key) & self.mask
    }

    /// Number of shards.
    #[inline]
    pub fn shards(&self) -> usize {
        self.mask + 1
    }

    /// Internal helper: groups `buf` by shards.
    /// Zero overhead inlined by the compiler.
    #[inline]
    fn group_by_shard(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Vec<Vec<T>> {
        let n = self.mask + 1;
        let mut groups: Vec<Vec<T>> = (0..n).map(|_| Vec::new()).collect();
        for item in buf.drain(..) {
            let shard = hash_key(key_fn(&item)) & self.mask;
            groups[shard].push(item);
        }
        groups
    }

    /// Non-blocking batch by hash(key_fn).
    /// Returns `Ok(sent)` if all elements have been sent.
    /// Returns `Err(sent)` if at least one shard was full or closed
    /// `buf` contains unsent elements.
    pub fn try_send_batch_keyed(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyTryBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        for (shard, mut group) in self.group_by_shard(buf, &key_fn).into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            match self.senders[shard].try_send_batch(&mut group) {
                Ok(sent) => total += sent,
                Err(e) => {
                    total += e.sent;
                    let first_key = group
                        .first()
                        .map(|item| key_fn(item).to_string())
                        .unwrap_or_default();
                    buf.extend(group);
                    return Err(shard_error::ShardKeyTryBatchSendError {
                        key: first_key,
                        shard,
                        sent: total,
                        reason: e.err,
                    });
                }
            }
        }
        Ok(total)
    }

    /// Blocking batch by hash(key_fn).
    /// Waits until all items have been sent.
    /// After calling `buf` is guaranteed to be empty (unless receiver is closed).
    /// Returns `Ok(sent)` if all elements have been sent.
    /// Returns `Err(ShardedKeyBatchError)` if receiver is closed.
    pub fn send_batch(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        for (shard, mut group) in self.group_by_shard(buf, &key_fn).into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            match self.senders[shard].send_batch(&mut group) {
                Ok(sent) => total += sent,
                Err(e) => {
                    total += e.sent;
                    let first_key = group
                        .first()
                        .map(|item| key_fn(item).to_string())
                        .unwrap_or_default();
                    buf.extend(group);
                    return Err(shard_error::ShardKeyBatchSendError {
                        key: first_key,
                        shard,
                        sent: total,
                        reason: e.err,
                    });
                }
            }
        }
        Ok(total)
    }

    /// Blocking batch с deadline по hash(key_fn).
    pub fn send_batch_timeout(
        &self,
        buf: &mut Vec<T>,
        d: Duration,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        for (shard, mut group) in self.group_by_shard(buf, &key_fn).into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            match self.senders[shard].send_batch_timeout(&mut group, d) {
                Ok(sent) => total += sent,
                Err(e) => {
                    total += e.sent;
                    let first_key = group
                        .first()
                        .map(|item| key_fn(item).to_string())
                        .unwrap_or_default();
                    buf.extend(group);
                    return Err(shard_error::ShardKeyBatchSendError {
                        key: first_key,
                        shard,
                        sent: total,
                        reason: e.err,
                    });
                }
            }
        }
        Ok(total)
    }

    /// Async batch by hash(key_fn).
    /// Waits for space in each shard → only Disconnected interrupts.
    /// Returns `Ok(sent)` or `Err(ShardedKeyAsyncBatchError)`.
    pub async fn send_batch_async(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyAsyncBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = self.mask + 1;
        let mut groups: Vec<(String, Vec<T>)> =
            (0..n).map(|_| (String::new(), Vec::new())).collect();
        for item in buf.drain(..) {
            let key = key_fn(&item);
            let shard = hash_key(key) & self.mask;
            if groups[shard].0.is_empty() {
                groups[shard].0 = key.to_string();
            }
            groups[shard].1.push(item);
        }
        let mut total = 0usize;
        for (shard, (first_key, group)) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut iter = group.into_iter();
            for v in &mut iter {
                match self.senders[shard].send_async(v).await {
                    Ok(()) => total += 1,
                    Err(e) => {
                        buf.push(e.into_inner());
                        buf.extend(iter);
                        return Err(shard_error::ShardKeyAsyncBatchSendError {
                            key: first_key,
                            shard,
                            sent: total,
                        });
                    }
                }
            }
        }
        Ok(total)
    }
}

impl<T: Send + 'static, const CAP: usize> Clone for ShardKey<T, CAP> {
    fn clone(&self) -> Self {
        let senders: Arc<[Sender<T, CAP>]> = self
            .senders
            .iter()
            .map(Sender::clone)
            .collect::<Vec<_>>()
            .into();
        Self {
            senders,
            mask: self.mask,
        }
    }
}

/// Sharded receiver common for `Sharded` and `ShardedKey`.
pub struct ShardReceiver<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + 'static = MPMCInner<T, CAP>,
> {
    pub(crate) receivers: Vec<Receiver<T, CAP, I>>,
    cursor: usize,
    mask: usize,
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> ShardReceiver<T, CAP, I> {
    pub(crate) fn new(receivers: Vec<Receiver<T, CAP, I>>) -> Self {
        let mask = receivers.len().saturating_sub(1);
        Self {
            receivers,
            cursor: 0,
            mask,
        }
    }

    pub fn shards(&self) -> usize {
        self.receivers.len()
    }

    /// The shard index for the key is only for ByKey (ShardedKey).
    pub fn shard_for(&self, key: &str) -> usize {
        hash_key(key) & self.mask
    }

    pub fn try_recv(&mut self, shard: usize) -> Result<T, shard_error::ShardTryRecvError> {
        let idx = shard % self.receivers.len();
        self.receivers[idx]
            .try_recv()
            .map_err(|err| shard_error::ShardTryRecvError { shard: idx, err })
    }

    pub fn recv(&self, shard: usize) -> Result<T, shard_error::ShardRecvError> {
        let idx = shard % self.receivers.len();
        self.receivers[idx]
            .recv()
            .map_err(|err| shard_error::ShardRecvError { shard: idx, err })
    }

    pub async fn recv_async(&self, shard: usize) -> Result<T, shard_error::ShardAsyncRecvError> {
        let idx = shard % self.receivers.len();
        self.receivers[idx]
            .recv_async()
            .await
            .map_err(|err| shard_error::ShardAsyncRecvError { shard: idx, err })
    }

    pub fn try_recv_any(&mut self) -> Option<(usize, T)> {
        let n = self.receivers.len();
        for i in 0..n {
            let idx = (self.cursor + i) % n;
            match self.receivers[idx].try_recv() {
                Ok(v) => {
                    self.cursor = (idx + 1) % n;
                    return Some((idx, v));
                }
                _ => {}
            }
        }
        None
    }

    pub fn recv_batch(&mut self, shard: usize, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        self.receivers[shard % self.receivers.len()].recv_batch(buf, max)
    }

    /// Async batch recv from a specific shard.
    /// Returns `Ok(count)` or `Err(ShardedBatchRecvError)` if sender is closed.
    pub async fn recv_batch_async(
        &self,
        shard: usize,
        buf: &mut Vec<T>,
        max: usize,
    ) -> (usize, bool) {
        let idx = shard % self.receivers.len();
        self.receivers[idx].recv_batch_async(buf, max).await
    }

    pub fn try_recv_batch_any(&mut self, buf: &mut Vec<T>, max_per_shard: usize) -> usize {
        let n = self.receivers.len();
        let start = self.cursor;
        let mut total = 0usize;
        for i in 0..n {
            let idx = (start + i) % n;
            let (count, _) = self.receivers[idx].recv_batch(buf, max_per_shard);
            total += count;
        }
        if total > 0 {
            self.cursor = (self.cursor + 1) % n;
        }
        total
    }

    pub fn recv_any(&mut self) -> RecvAnyFuture<'_, T, CAP, I> {
        let n = self.receivers.len();
        RecvAnyFuture {
            rx: self,
            slots: (0..n).map(|_| None).collect(),
        }
    }

    pub fn into_receivers(self) -> Vec<Receiver<T, CAP, I>> {
        self.receivers
    }

    pub fn receiver(&self, shard: usize) -> &Receiver<T, CAP, I> {
        &self.receivers[shard % self.receivers.len()]
    }
}

// Recv any future

pub struct RecvAnyFuture<'a, T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP> + 'static>
{
    rx: &'a mut ShardReceiver<T, CAP, I>,
    slots: Vec<Option<Arc<AsyncSlot>>>,
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Future
    for RecvAnyFuture<'_, T, CAP, I>
{
    type Output = Result<(usize, T), shard_error::ShardRecvAnyError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let n = this.rx.receivers.len();
        let start = this.rx.cursor;

        for i in 0..n {
            let idx = (start + i) % n;
            match this.rx.receivers[idx].try_recv() {
                Ok(v) => {
                    this.rx.cursor = (idx + 1) % n;
                    for slot in this.slots.iter_mut().flatten() {
                        SyncList::cancel_async_slot(slot);
                    }
                    return Poll::Ready(Ok((idx, v)));
                }
                Err(TryRecvError::Disconnected) => {}
                _ => {}
            }
        }

        let disconnected = this
            .rx
            .receivers
            .iter()
            .filter(|r| r.inner_ref().is_tx_closed() && r.inner_ref().is_empty())
            .count();
        if disconnected == n {
            return Poll::Ready(Err(shard_error::ShardRecvAnyError {
                disconnected_shards: disconnected,
                err: AsyncRecvError::Disconnected,
            }));
        }

        let waker = cx.waker().clone();
        for i in 0..n {
            match &this.slots[i] {
                None => {
                    this.slots[i] = Some(
                        this.rx.receivers[i]
                            .inner_ref()
                            .receiver_waiters()
                            .push_async_slot(waker.clone()),
                    );
                }
                Some(s) if s.in_queue.load(Ordering::Acquire) => {
                    s.waker.register(&waker);
                }
                Some(_) => {
                    this.slots[i] = Some(
                        this.rx.receivers[i]
                            .inner_ref()
                            .receiver_waiters()
                            .push_async_slot(waker.clone()),
                    );
                }
            }
        }

        for i in 0..n {
            let idx = (start + i) % n;
            match this.rx.receivers[idx].try_recv() {
                Ok(v) => {
                    this.rx.cursor = (idx + 1) % n;
                    for slot in this.slots.iter_mut().flatten() {
                        SyncList::cancel_async_slot(slot);
                    }
                    return Poll::Ready(Ok((idx, v)));
                }
                _ => {}
            }
        }

        Poll::Pending
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Drop
    for RecvAnyFuture<'_, T, CAP, I>
{
    fn drop(&mut self) {
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if let Some(s) = slot.take() {
                SyncList::cancel_async_slot(&s);
                self.rx.receivers[i]
                    .inner_ref()
                    .receiver_waiters()
                    .notify_one();
            }
        }
    }
}

// Constructors

/// RoundRobin sharded channel. `num_shards` is a power of two.
pub fn round_robin<T: Send + 'static, const CAP: usize>(
    num_shards: usize,
) -> (ShardRoundRobin<T, CAP>, ShardReceiver<T, CAP>) {
    let num_shards = nearest_power_of_two(num_shards);
    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..num_shards).map(|_| mpmc_bounded::<T, CAP>()).unzip();
    (
        ShardRoundRobin {
            senders: senders.into(),
            cursor: AtomicUsize::new(0),
            mask: num_shards - 1,
        },
        ShardReceiver::new(receivers),
    )
}

/// ByKey sharded channel. `num_shards` is a power of two.
pub fn shard_key<T: Send + 'static, const CAP: usize>(
    num_shards: usize,
) -> (ShardKey<T, CAP>, ShardReceiver<T, CAP>) {
    let num_shards = nearest_power_of_two(num_shards);
    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..num_shards).map(|_| mpmc_bounded::<T, CAP>()).unzip();
    (
        ShardKey {
            senders: senders.into(),
            mask: num_shards - 1,
        },
        ShardReceiver::new(receivers),
    )
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_channel::errors::RecvError;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        thread,
    };

    // Sharded (RoundRobin)

    #[test]
    fn rr_try_push_basic() {
        let (tx, mut rx) = round_robin::<u64, 8>(4);
        tx.try_send(42).unwrap();
        // try_recv_any non blocking poll all shards
        let (_, v) = rx.try_recv_any().unwrap();
        assert_eq!(v, 42);
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn rr_async_push() {
        let (tx, mut rx) = round_robin::<u64, 8>(4);
        tx.send_async(42).await.unwrap();
        let (_, v) = rx.try_recv_any().unwrap();
        assert_eq!(v, 42);
    }

    #[test]
    fn rr_distributes_evenly() {
        let (tx, mut rx) = round_robin::<u64, 8>(4);
        for i in 0..8u64 {
            tx.try_send(i).unwrap();
        }
        let counts: Vec<usize> = (0..4)
            .map(|s| {
                let mut buf = Vec::new();
                rx.recv_batch(s, &mut buf, 8);
                buf.len()
            })
            .collect();
        assert_eq!(counts, vec![2, 2, 2, 2]);
    }

    #[test]
    fn rr_no_key_in_api() {
        let (tx, _rx) = round_robin::<u64, 8>(4);
        // Sharded doesn't have send/try_send methods with key
        // Only push/try_push/push_async compiles
        let _ = tx.try_send(1);
        let _ = tx.shards();
    }

    #[test]
    fn rr_clone_independent_cursor() {
        let (tx1, mut rx) = round_robin::<u64, 8>(4);
        let tx2 = tx1.clone();
        for i in 0..4u64 {
            tx1.try_send(i).unwrap();
        }
        for i in 4..8u64 {
            tx2.try_send(i).unwrap();
        }
        let total: usize = (0..4)
            .map(|s| {
                let mut buf = Vec::new();
                rx.recv_batch(s, &mut buf, 8);
                buf.len()
            })
            .sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn rr_disconnected() {
        let (tx, rx) = round_robin::<u64, 8>(4);
        drop(rx);
        assert!(tx.try_send(1).is_err()); // Sharded try send error::disconnected
    }

    // ShardedKey (ByKey)

    #[test]
    fn key_deterministic_routing() {
        let (tx, _rx) = shard_key::<u64, 8>(4);
        assert_eq!(tx.shard_for("AAPL"), tx.shard_for("AAPL"));
        assert_eq!(tx.shard_for("BTC-USD"), tx.shard_for("BTC-USD"));
        assert!(tx.shard_for("AAPL") < tx.shards());
    }

    #[test]
    fn key_try_send_basic() {
        let (tx, rx) = shard_key::<u64, 8>(4);
        let shard = tx.shard_for("AAPL");
        tx.try_send("AAPL", 42).unwrap();
        assert_eq!(rx.receiver(shard).try_recv().unwrap(), 42);
    }

    #[test]
    fn key_ordering_within_shard() {
        let (tx, rx) = shard_key::<u64, 32>(4);
        let shard = tx.shard_for("AAPL");
        for i in 0..10u64 {
            tx.try_send("AAPL", i).unwrap();
        }
        let mut buf = Vec::new();
        rx.receiver(shard).recv_batch(&mut buf, 10);
        assert_eq!(buf, (0..10u64).collect::<Vec<_>>());
    }

    #[test]
    fn key_isolation_between_symbols() {
        let (tx, rx) = shard_key::<String, 16>(4);
        let sa = tx.shard_for("AAPL");
        let sb = tx.shard_for("MSFT");
        if sa == sb {
            return;
        }
        tx.try_send("AAPL", "a".to_string()).unwrap();
        tx.try_send("MSFT", "b".to_string()).unwrap();
        assert_eq!(rx.receiver(sa).try_recv().unwrap(), "a");
        assert_eq!(rx.receiver(sb).try_recv().unwrap(), "b");
        assert!(rx.receiver(sa).try_recv().is_err());
    }

    #[test]
    fn key_receiver_shard_for_matches_sender() {
        let (tx, rx) = shard_key::<u64, 8>(4);
        for key in &["AAPL", "MSFT", "BTC-USD", "user_550e8400-e29b-41d4"] {
            assert_eq!(tx.shard_for(key), rx.shard_for(key));
        }
    }

    #[test]
    fn key_batch_keyed() {
        let (tx, mut rx) = shard_key::<(String, u64), 16>(4);
        let sa = tx.shard_for("AAPL");
        let sb = tx.shard_for("MSFT");
        if sa == sb {
            return;
        }
        let mut batch = vec![
            ("AAPL".to_string(), 1u64),
            ("MSFT".to_string(), 2u64),
            ("AAPL".to_string(), 3u64),
            ("MSFT".to_string(), 4u64),
        ];
        let sent = tx
            .try_send_batch_keyed(&mut batch, |(sym, _)| sym.as_str())
            .unwrap_or_else(|e| e.sent);
        assert_eq!(sent, 4);
        let mut ba = Vec::new();
        let mut bb = Vec::new();
        rx.recv_batch(sa, &mut ba, 10);
        rx.recv_batch(sb, &mut bb, 10);
        assert!(ba.iter().all(|(s, _)| s == "AAPL"));
        assert!(bb.iter().all(|(s, _)| s == "MSFT"));
    }

    #[test]
    fn key_disconnected() {
        let (tx, rx) = shard_key::<u64, 8>(4);
        drop(rx);
        assert!(tx.try_send("AAPL", 1).is_err()); // Sharded key try send error::disconnected
    }

    // General: ShardedReceiver

    #[test]
    fn recv_into_receivers() {
        let (tx, rx) = shard_key::<u64, 8>(4);
        let receivers = rx.into_receivers();
        assert_eq!(receivers.len(), 4);
        tx.try_send("AAPL", 99).unwrap();
        // check that the element is in the right place
        let shard = tx.shard_for("AAPL");
        assert_eq!(receivers[shard].try_recv().unwrap(), 99);
    }

    #[test]
    fn recv_try_recv_any() {
        let (tx, mut rx) = round_robin::<u64, 8>(4);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        let (_, v1) = rx.try_recv_any().unwrap();
        let (_, v2) = rx.try_recv_any().unwrap();
        let mut vals = [v1, v2];
        vals.sort();
        assert_eq!(vals, [1, 2]);
    }

    // Blocking threads (Miri)

    #[test]
    fn rr_blocking_2p_2s_no_loss() {
        const N: u64 = 16;
        let (tx, rx) = round_robin::<u64, 16>(2);
        let total = Arc::new(AtomicU64::new(0));
        let mut receivers = rx.into_receivers();
        let r0 = receivers.remove(0);
        let r1 = receivers.remove(0);
        let t0 = total.clone();
        let c0 = thread::spawn(move || {
            let mut s = 0u64;
            loop {
                match r0.recv() {
                    Ok(v) => s += v,
                    Err(RecvError::Disconnected) => break,
                    Err(RecvError::TimeOut(_)) => unreachable!(), // recv without deadline
                }
            }
            t0.fetch_add(s, Relaxed);
        });
        let t1 = total.clone();
        let c1 = thread::spawn(move || {
            let mut s = 0u64;
            loop {
                match r1.recv() {
                    Ok(v) => s += v,
                    Err(RecvError::Disconnected) => break,
                    Err(RecvError::TimeOut(_)) => unreachable!(), // recv without deadline
                }
            }
            t1.fetch_add(s, Relaxed);
        });
        let tx2 = tx.clone();
        let p0 = thread::spawn(move || {
            for i in 0..N / 2 {
                tx.send(i).unwrap();
            }
        });
        let p1 = thread::spawn(move || {
            for i in N / 2..N {
                tx2.send(i).unwrap();
            }
        });
        p0.join().unwrap();
        p1.join().unwrap();
        c0.join().unwrap();
        c1.join().unwrap();
        assert_eq!(total.load(Relaxed), (0..N).sum::<u64>());
    }

    #[test]
    fn key_blocking_ordering() {
        const N: u64 = 32;
        let (tx, rx) = shard_key::<u64, 64>(4);
        let shard = tx.shard_for("AAPL");
        let receivers = rx.into_receivers();
        let handle = thread::spawn(move || {
            let mut last = 0u64;
            let mut count = 0u64;
            loop {
                match receivers[shard].recv() {
                    Ok(v) => {
                        assert!(v >= last);
                        last = v;
                        count += 1;
                        if count == N {
                            break;
                        }
                    }
                    Err(RecvError::Disconnected) => break,
                    Err(RecvError::TimeOut(_)) => unreachable!(),
                }
            }
            count
        });
        for i in 0..N {
            tx.send("AAPL", i).unwrap();
        }
        drop(tx);
        assert_eq!(handle.join().unwrap(), N);
    }

    // Async

    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_async_send() {
        let (tx, rx) = shard_key::<String, 8>(4);
        let shard = tx.shard_for("AAPL");
        tx.send_async("AAPL", "trade".to_string()).await.unwrap();
        assert_eq!(rx.receiver(shard).recv_async().await.unwrap(), "trade");
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn recv_any_async() {
        let (tx, mut rx) = shard_key::<u64, 8>(4);
        let shard = tx.shard_for("AAPL");
        tx.send_async("AAPL", 99).await.unwrap();
        let (s, v) = rx.recv_any().await.unwrap(); // Ok((shard, value))
        assert_eq!(s, shard);
        assert_eq!(v, 99);
    }

    // Miri: drop safety and memory
    // Async tests are skipped under Miri (#[cfg(not(miri))]).
    // Sync tests check:
    // correct drop Vec<Receiver> in ShardReceiver
    // group_by_shard drain/extend boundaries
    // ShardKey routing without UB
    // disconnect detection

    #[test]
    fn miri_rr_drop_sender_first() {
        let (tx, rx) = round_robin::<u64, 8>(2);
        tx.try_send(1).unwrap();
        drop(tx);
        // receivers are alive, data is available
        let mut buf = Vec::new();
        rx.receiver(0).recv_batch(&mut buf, 8);
        // rx drops last correct order
    }

    #[test]
    fn miri_rr_drop_receiver_first() {
        let (tx, rx) = round_robin::<u64, 8>(2);
        drop(rx);
        // sender sees Disconnected
        assert!(tx.try_send(1).is_err());
        drop(tx);
    }

    #[test]
    fn miri_rr_into_receivers_drop_order() {
        let (tx, rx) = round_robin::<u64, 8>(4);
        let receivers = rx.into_receivers();
        // send to different shards
        for i in 0..4u64 {
            tx.try_send(i).unwrap();
        }
        drop(tx);
        // drop receivers in reverse order
        let mut rev: Vec<_> = receivers.into_iter().collect();
        rev.reverse();
        for r in rev {
            drop(r);
        }
    }

    #[test]
    fn miri_key_group_by_shard_drain_extend() {
        let (tx, _rx) = shard_key::<u64, 8>(2);
        let mut batch: Vec<u64> = vec![10, 20, 30, 40];
        // We deliberately overflow the buffer to get an error and check extend
        // CAP=8 is enough, everyone must pass
        let result =
            tx.try_send_batch_keyed(&mut batch, |v| if *v <= 20 { "AAPL" } else { "MSFT" });
        assert!(result.is_ok());
        assert!(batch.is_empty());
        drop(tx);
    }

    #[test]
    fn miri_key_disconnect_mid_batch() {
        let (tx, rx) = shard_key::<String, 8>(2);
        drop(rx); // receiver will be dropped before sending
        let mut batch = vec!["hello".to_string(), "world".to_string()];
        let result = tx.try_send_batch_keyed(&mut batch, |s| s.as_str());
        // Error -receiver is closed, elements are returned to batch
        assert!(result.is_err());
        drop(tx);
    }

    #[test]
    fn miri_receiver_try_recv_any_empty() {
        let (tx, mut rx) = round_robin::<u64, 8>(4);
        // Empty channel try_recv_any should return None without UB
        assert!(rx.try_recv_any().is_none());
        tx.try_send(42).unwrap();
        let (shard, v) = rx.try_recv_any().unwrap();
        assert!(shard < 4);
        assert_eq!(v, 42);
        drop(tx);
    }

    #[test]
    fn miri_receiver_shard_for_consistency() {
        let (tx, rx) = shard_key::<u64, 8>(4);
        // shard_for deterministic tx and rx are the same
        for key in &["AAPL", "MSFT", "BTC", "user_550e8400"] {
            assert_eq!(tx.shard_for(key), rx.shard_for(key));
        }
        drop(tx);
        drop(rx);
    }

    #[test]
    fn miri_sharded_receiver_clone_of_inner_receiver() {
        let (tx, rx) = round_robin::<u64, 8>(2);
        // receiver() returns &Receiver clone and use
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        let r0 = rx.receiver(0).clone();
        let r1 = rx.receiver(1).clone();
        // One of them contains data
        let got = r0.try_recv().ok().or_else(|| r1.try_recv().ok());
        assert!(got.is_some());
        drop((rx, r0, r1, tx));
    }

    #[test]
    fn miri_key_send_recv_no_loss() {
        const N: u64 = 8; // small N for Miri
        let (tx, rx) = shard_key::<u64, 16>(2);
        let shard = tx.shard_for("KEY");
        for i in 0..N {
            tx.try_send("KEY", i).unwrap();
        }
        drop(tx);
        let mut buf = Vec::new();
        let r = rx.receiver(shard);
        loop {
            let (n, dc) = r.recv_batch(&mut buf, 8);
            if dc || n == 0 {
                break;
            }
        }
        assert_eq!(buf, (0..N).collect::<Vec<_>>());
    }
}

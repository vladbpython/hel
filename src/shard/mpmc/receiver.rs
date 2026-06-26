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

use super::hash::hash_key;
use super::super::errors as shard_error;

use crate::internal_channel::{
    core::SeqInner,
    errors::{AsyncRecvError,TryRecvError},
    receiver::Receiver,
    sync::{AsyncSlot, SyncList},
    traits::InnerChannel,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
};

/// Sharded receiver common for `ShardRoundRobin`, `ShardedKey`, `ShardGroup`.
pub struct ShardReceiver<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + 'static = SeqInner<T, CAP>,
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
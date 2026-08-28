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

use super::super::errors as shard_error;
use super::hash::hash_key;

use crate::internal_channel::{
    core::SeqInner,
    errors::{AsyncRecvError, TryRecvError},
    receiver::Receiver,
    sync::AsyncSlot,
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

    #[inline]
    #[track_caller]
    fn check_shard(&self, shard: usize) -> usize {
        assert!(
            shard < self.receivers.len(),
            "shard index {shard} out of range: this channel has {} shards",
            self.receivers.len()
        );
        shard
    }

    pub fn try_recv(&mut self, shard: usize) -> Result<T, shard_error::ShardTryRecvError> {
        let idx = self.check_shard(shard);
        self.receivers[idx]
            .try_recv()
            .map_err(|err| shard_error::ShardTryRecvError { shard: idx, err })
    }

    pub fn recv(&self, shard: usize) -> Result<T, shard_error::ShardRecvError> {
        let idx = self.check_shard(shard);
        self.receivers[idx]
            .recv()
            .map_err(|err| shard_error::ShardRecvError { shard: idx, err })
    }

    pub async fn recv_async(&self, shard: usize) -> Result<T, shard_error::ShardAsyncRecvError> {
        let idx = self.check_shard(shard);
        self.receivers[idx]
            .recv_async()
            .await
            .map_err(|err| shard_error::ShardAsyncRecvError { shard: idx, err })
    }

    /// Non-blocking receive from any shard.
    /// `Ok(None)` means "nothing available right now", `Err` means every
    /// sender of every shard is gone and nothing is left to drain,
    /// documented `while let` polling loop must stop instead of spinning
    /// core forever (the old `Option` return could not tell the two apart).
    pub fn try_recv_any(&mut self) -> Result<Option<(usize, T)>, shard_error::ShardRecvAnyError> {
        let n = self.receivers.len();
        let mut disconnected = 0usize;
        for i in 0..n {
            let idx = (self.cursor + i) % n;
            match self.receivers[idx].try_recv() {
                Ok(v) => {
                    self.cursor = (idx + 1) % n;
                    return Ok(Some((idx, v)));
                }
                Err(TryRecvError::Disconnected) => disconnected += 1,
                Err(TryRecvError::Empty) => {}
            }
        }
        if disconnected == n {
            return Err(shard_error::ShardRecvAnyError {
                disconnected_shards: disconnected,
                err: AsyncRecvError::Disconnected,
            });
        }
        Ok(None)
    }

    pub fn recv_batch(&mut self, shard: usize, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        self.receivers[self.check_shard(shard)].recv_batch(buf, max)
    }

    /// Async batch recv from a specific shard.
    /// Returns `Ok(count)` or `Err(ShardedBatchRecvError)` if sender is closed.
    pub async fn recv_batch_async(
        &self,
        shard: usize,
        buf: &mut Vec<T>,
        max: usize,
    ) -> (usize, bool) {
        let idx = self.check_shard(shard);
        self.receivers[idx].recv_batch_async(buf, max).await
    }

    /// Non-blocking batch receive across all shards.
    /// Returns `(count, disconnected)`; same convention as `pop_batch`:
    /// `disconnected` is true only when nothing was received and every shard is closed and drained,
    /// so a polling loop knows when to stop instead of spinning core forever.
    pub fn try_recv_batch_any(&mut self, buf: &mut Vec<T>, max_per_shard: usize) -> (usize, bool) {
        let n = self.receivers.len();
        let start = self.cursor;
        let mut total = 0usize;
        let mut disconnected = 0usize;
        for i in 0..n {
            let idx = (start + i) % n;
            let (count, dc) = self.receivers[idx].try_recv_batch(buf, max_per_shard);
            total += count;
            if dc {
                disconnected += 1;
            }
        }
        if total > 0 {
            self.cursor = (self.cursor + 1) % n;
        }
        (total, total == 0 && disconnected == n)
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
        &self.receivers[self.check_shard(shard)]
    }

    /// No panic access to a shard, `slice::get` style: `None` for an out of range index.
    ///
    /// ```ignore
    /// let v = rx.get_receiver(i).ok_or("no such shard")?.try_recv()?;
    /// ```
    ///
    #[inline]
    pub fn get_receiver(&self, shard: usize) -> Option<&Receiver<T, CAP, I>> {
        self.receivers.get(shard)
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
                    for (i, slot) in this.slots.iter_mut().enumerate() {
                        if let Some(s) = slot.take() {
                            this.rx.receivers[i]
                                .inner_ref()
                                .receiver_waiters()
                                .cancel_async_slot(&s);
                            this.rx.receivers[i]
                                .inner_ref()
                                .receiver_waiters()
                                .notify_one();
                        }
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
            if let Ok(v) = this.rx.receivers[idx].try_recv() {
                this.rx.cursor = (idx + 1) % n;
                for (i, slot) in this.slots.iter_mut().enumerate() {
                    if let Some(s) = slot.take() {
                        this.rx.receivers[i]
                            .inner_ref()
                            .receiver_waiters()
                            .cancel_async_slot(&s);
                        this.rx.receivers[i]
                            .inner_ref()
                            .receiver_waiters()
                            .notify_one();
                    }
                }
                return Poll::Ready(Ok((idx, v)));
            }
        }

        // Re-check disconnect after registration, mirroring the single shard future.
        // close racing with this poll fires `wake_all` on every shard before our slots exist,
        // pre registration check above has already passed,
        // so without this recheck nobody ever wakes the slots
        // and the future parks forever instead of returning `AsyncRecvError::Disconnected`.
        let disconnected = this
            .rx
            .receivers
            .iter()
            .filter(|r| r.inner_ref().is_tx_closed() && r.inner_ref().is_empty())
            .count();
        if disconnected == n {
            for (i, slot) in this.slots.iter_mut().enumerate() {
                if let Some(s) = slot.take() {
                    this.rx.receivers[i]
                        .inner_ref()
                        .receiver_waiters()
                        .cancel_async_slot(&s);
                    // Same baton pass as on the Ready paths and in Drop.
                    this.rx.receivers[i]
                        .inner_ref()
                        .receiver_waiters()
                        .notify_one();
                }
            }
            return Poll::Ready(Err(shard_error::ShardRecvAnyError {
                disconnected_shards: disconnected,
                err: AsyncRecvError::Disconnected,
            }));
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
                self.rx.receivers[i]
                    .inner_ref()
                    .receiver_waiters()
                    .cancel_async_slot(&s);
                self.rx.receivers[i]
                    .inner_ref()
                    .receiver_waiters()
                    .notify_one();
            }
        }
    }
}

#[cfg(test)]
mod recv_any_tests {
    use crate::{channel::mpmc::round_robin, internal_channel::traits::InnerChannel};
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Waker},
        time::Duration,
    };
    const CAP: usize = 16;

    // Poll a fresh `recv_any()` once with a no-op waker so it parks and
    // registers an async slot on every shard, then drop it cancellation.
    fn park_then_cancel_recv_any(rx: &mut super::ShardReceiver<u64, CAP>) {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = std::pin::pin!(rx.recv_any());
        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "recv_any on empty shards must park, not resolve"
        );
        // `fut` drops here -> RecvAnyFuture::Drop runs cancel_async_slot on every shard.
    }

    #[test]
    fn recv_any_cancel_does_not_leak_async_slots() {
        let (_tx, mut rx) = round_robin::<u64, CAP>(4);

        for _ in 0..100 {
            park_then_cancel_recv_any(&mut rx);
        }

        for i in 0..rx.receivers.len() {
            let queued = rx.receivers[i]
                .inner_ref()
                .receiver_waiters()
                .async_count_seqcst();
            assert!(
                queued <= 1,
                "shard {i} leaked {queued} cancelled recv_any slots, expected <= 1"
            );
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn try_recv_batch_any_does_not_block_on_an_empty_shard() {
        let (tx, rx) = round_robin::<u64, 16>(2);
        tx.try_send(1).unwrap(); // шард 0
        tx.try_send(2).unwrap(); // шард 1
        let mut srx = rx;
        // Empty shard 0 directly, keeping its sender alive.
        let _ = srx.get_receiver(0).unwrap().try_recv().unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let d2 = done.clone();
        let h = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let (n, _) = srx.try_recv_batch_any(&mut buf, 8);
            d2.store(true, Ordering::Release);
            (n, buf)
        });
        std::thread::sleep(Duration::from_millis(300));
        let finished = done.load(Ordering::Acquire);
        if !finished {
            drop(tx); // unblock the hung thread so the test can fail cleanly
            let _ = h.join();
            panic!("try_recv_batch_any blocked on a connected empty shard");
        }
        let (n, buf) = h.join().unwrap();
        assert_eq!((n, buf), (1, vec![2]), "must drain exactly the full shard");
    }
}

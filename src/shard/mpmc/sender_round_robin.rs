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
use super::receiver::ShardReceiver;

use crate::internal_channel::{
    core::SeqInner,
    errors::AsyncSendError,
    mpmc_bounded, nearest_power_of_two,
    sender::Sender,
    traits::InnerChannel,
};
use std::{
    sync::{Arc, atomic::AtomicUsize, atomic::Ordering},
    time::Duration,
};

/// Sharded channel with round robin routing.
/// Each `push` goes to the next shard in sequence.
/// No key even load distribution across consumers.
/// Cloning
/// Each clone has an independent cursor starts from 0.
pub struct ShardRoundRobin<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + 'static = SeqInner<T, CAP>,
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

    /// Async batch send with round-robin retry.
    /// Fast path: the whole remaining batch goes to the next shard via
    /// `try_send_batch`. If that shard is full, ONE element is awaited into
    /// the following shard (back-pressure point), then the fast path retries.
    /// NOTE: unlike the single-shot `try_send_batch`, retries spread the batch
    /// across shards — per-batch shard affinity is NOT guaranteed (RR never
    /// guarantees ordering anyway).
    /// Only `Disconnected` interrupts; on error the unsent elements (including
    /// the one being awaited) remain in `buf`.
    pub async fn send_batch_async(
        &self,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardAsyncBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        loop {
            let shard = self.next_shard();
            match self.senders[shard].try_send_batch(buf) {
                Ok(sent) => {
                    total += sent;
                    return Ok(total);
                }
                Err(e) => {
                    total += e.sent;
                    if buf.is_empty() {
                        return Ok(total);
                    }
                    // Back-pressure: await one element into the next shard.
                    // RR gives no ordering guarantees → pop() from the back, O(1).
                    let value = buf.pop().expect("checked non empty above");
                    let shard2 = self.next_shard();
                    match self.senders[shard2].send_async(value).await {
                        Ok(()) => total += 1,
                        Err(AsyncSendError::Disconnected(v)) => {
                            // Return the element: `buf` must contain all unsent items.
                            buf.push(v);
                            return Err(shard_error::ShardAsyncBatchSendError {
                                shard: shard2,
                                sent: total,
                            });
                        }
                    }
                }
            }
        }
    }
}

impl<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + crate::internal_channel::traits::MultiProducer,
> Clone for ShardRoundRobin<T, CAP, I>
{
    fn clone(&self) -> Self {
        let senders: Arc<[Sender<T, CAP, I>]> = self
            .senders
            .iter()
            .map(Sender::clone)
            .collect::<Vec<_>>()
            .into();

        // fetch_add atomically increments and returns different values
        // for each clone → different starting shards
        let offset = self.cursor.fetch_add(1, Ordering::Relaxed);
        Self {
            senders,
            cursor: AtomicUsize::new(offset),
            mask: self.mask,
        }
    }
}

/// Constructor RoundRobin sharded channel. `num_shards` is a power of two.
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

    #[cfg(not(miri))]
    #[tokio::test]
    async fn rr_batch_async_disconnect_no_loss() {
        let (tx, rx) = round_robin::<u64, 4>(2);
        drop(rx);
        let mut batch: Vec<u64> = (0..6).collect();
        let before = batch.len();
        let r = tx.send_batch_async(&mut batch).await;
        let sent = r.unwrap_err().sent;
        assert_eq!(
            sent + batch.len(),
            before,
            "RR async batch: ни один элемент не потерян при Disconnected"
        );
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
}

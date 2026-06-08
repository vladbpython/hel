//! Sharded SPSC channel: N independent SPSC channels.
//!
//! Each shard is a separate pair of `(SingleSender, SingleReceiver)`.
//! There is no hash routing the producer knows its shard directly.
//!
//! Two use cases
//!
//! ```
//! use hel::channel::spsc::SpscShard;
//!
//! let rt = tokio::runtime::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let ch = SpscShard::<u64, 256>::new(4);
//!     for (shard_id, tx, rx) in ch.into_pairs() {
//!         let s = shard_id as u64;
//!         let p = tokio::spawn(async move {
//!             tx.send_async(s).await.unwrap();
//!         });
//!         let c = tokio::spawn(async move {
//!             let _ = rx.recv_async().await;
//!         });
//!         p.await.unwrap();
//!         c.await.unwrap();
//!     }
//! });
//! ```
//!
//! ```
//! use hel::channel::spsc::SpscShard;
//! let mut ch = SpscShard::<u64, 256>::new(4);
//! let (tx0, rx0) = ch.take_pair(0).unwrap();  //shard 0 → core 0
//! let (tx2, rx2) = ch.take_pair(2).unwrap();  //shard 2 → core 2
//! //shards 1, 3 — not used
//! ```

use super::errors as shard_error;
use crate::internal_channel::{core::SingleInner, receiver::SingleReceiver, sender::SingleSender};
use std::sync::Arc;

// Spsc sharded channel

/// Builder for sharded SPSC channel.
/// Stores all `(SingleSender, SingleReceiver)` pairs before distribution.
/// Use `into_pairs()` or `take_pair(i)` to get pairs.
pub struct SpscShard<T: Send + 'static, const CAP: usize> {
    /// Option<> allows take_pair() to retrieve pairs independently
    pairs: Vec<Option<(SingleSender<T, CAP>, SingleReceiver<T, CAP>)>>,
}

impl<T: Send + 'static, const CAP: usize> SpscShard<T, CAP> {
    /// We create N independent SPSC channels.
    /// `num_shards` does not have to be a power of two (no hash routing).
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        let pairs: Vec<
            Option<(
                crate::internal_channel::sender::Sender<T, CAP, SingleInner<T, CAP>>,
                SingleReceiver<T, CAP>,
            )>,
        > = (0..num_shards)
            .map(|_| {
                let inner = Arc::new(SingleInner::new());
                let tx = SingleSender::new(inner.clone());
                let rx = SingleReceiver::new(inner);
                Some((tx, rx))
            })
            .collect();
        Self { pairs }
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.pairs.len()
    }

    // Option A: into_pairs

    /// Consumes builder, returns an iterator over all `(shard_id, tx, rx)` pairs.
    /// Guarantee: all shards are distributed, none are lost.
    /// Suitable for the symmetric case: N producers + N consumers.
    /// ```
    /// use hel::channel::spsc::SpscShard;
    /// let ch = SpscShard::<u64, 64>::new(4);
    /// for (shard_id, tx, rx) in ch.into_pairs() {
    ///     println!("shard {shard_id}");
    ///     drop((tx, rx));
    /// }
    /// ```
    pub fn into_pairs(
        self,
    ) -> impl Iterator<Item = (usize, SingleSender<T, CAP>, SingleReceiver<T, CAP>)> {
        self.pairs
            .into_iter()
            .enumerate()
            .filter_map(|(i, opt)| opt.map(|(tx, rx)| (i, tx, rx)))
    }

    // Option B: take_pair

    /// Retrieves the `(tx, rx)` pair for a specific shard.
    /// Returns `None` if the shard has already been taken or the index is out of range.
    /// Suitable for the asymmetric case: we take only the necessary shards.
    /// ```
    /// use hel::channel::spsc::SpscShard;
    /// let mut ch = SpscShard::<u64, 64>::new(4);
    /// let (tx0, rx0) = ch.take_pair(0).unwrap();
    /// let (tx2, rx2) = ch.take_pair(2).unwrap();
    /// assert!(ch.take_pair(0).is_none(), "already taken");
    /// assert!(ch.take_pair(4).is_none(), "out of range");
    /// ```
    pub fn take_pair(
        &mut self,
        shard: usize,
    ) -> Option<(SingleSender<T, CAP>, SingleReceiver<T, CAP>)> {
        self.pairs.get_mut(shard)?.take()
    }

    /// Checks whether all pairs have been taken.
    pub fn is_empty(&self) -> bool {
        self.pairs.iter().all(|p| p.is_none())
    }

    /// List of indexes of shards not yet taken.
    pub fn remaining(&self) -> Vec<usize> {
        self.pairs
            .iter()
            .enumerate()
            .filter(|(_, p)| p.is_some())
            .map(|(i, _)| i)
            .collect()
    }
}

/// SpscSender /SpscReceiver: thin wrappers с shard_id
/// Wrapper over `SingleSender` adds `shard_id` for convenience.
pub struct SpscSender<T: Send + 'static, const CAP: usize> {
    pub shard_id: usize,
    inner: SingleSender<T, CAP>,
}

/// Wrapper over `SingleReceiver` adds `shard_id` for convenience.
pub struct SpscReceiver<T: Send + 'static, const CAP: usize> {
    pub shard_id: usize,
    inner: SingleReceiver<T, CAP>,
}

impl<T: Send + 'static, const CAP: usize> SpscSender<T, CAP> {
    pub fn try_send(&self, v: T) -> Result<(), shard_error::ShardTrySendError<T>> {
        self.inner
            .try_send(v)
            .map_err(|err| shard_error::ShardTrySendError {
                shard: self.shard_id,
                err,
            })
    }

    pub fn send(&self, v: T) -> Result<(), shard_error::ShardSendError<T>> {
        self.inner
            .send(v)
            .map_err(|err| shard_error::ShardSendError {
                shard: self.shard_id,
                err,
            })
    }

    pub async fn send_async(&self, v: T) -> Result<(), shard_error::ShardAsyncSendError<T>> {
        self.inner
            .send_async(v)
            .await
            .map_err(|err| shard_error::ShardAsyncSendError {
                shard: self.shard_id,
                err,
            })
    }
    /// Non-blocking batch send: one lock for the entire batch.
    pub fn try_send_batch(
        &self,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardTryBatchSendError> {
        self.inner
            .try_send_batch(buf)
            .map_err(|e| shard_error::ShardTryBatchSendError {
                shard: self.shard_id,
                sent: e.sent,
                reason: e.err,
            })
    }
    /// Blocking batch send: waits until everything is sent.
    pub fn send_batch(&self, buf: &mut Vec<T>) -> Result<usize, shard_error::ShardBatchSendError> {
        self.inner
            .send_batch(buf)
            .map_err(|e| shard_error::ShardBatchSendError {
                shard: self.shard_id,
                sent: e.sent,
                reason: e.err,
            })
    }
}

impl<T: Send + 'static, const CAP: usize> SpscReceiver<T, CAP> {
    pub fn try_recv(&self) -> Result<T, shard_error::ShardTryRecvError> {
        self.inner
            .try_recv()
            .map_err(|err| shard_error::ShardTryRecvError {
                shard: self.shard_id,
                err,
            })
    }

    pub fn recv(&self) -> Result<T, shard_error::ShardRecvError> {
        self.inner
            .recv()
            .map_err(|err| shard_error::ShardRecvError {
                shard: self.shard_id,
                err,
            })
    }

    pub async fn recv_async(&self) -> Result<T, shard_error::ShardAsyncRecvError> {
        self.inner
            .recv_async()
            .await
            .map_err(|err| shard_error::ShardAsyncRecvError {
                shard: self.shard_id,
                err,
            })
    }

    pub fn recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        self.inner.recv_batch(buf, max)
    }

    pub async fn recv_batch_async(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        self.inner.recv_batch_async(buf, max).await
    }
}

// Constructor with wrappers

impl<T: Send + 'static, const CAP: usize> SpscShard<T, CAP> {
    /// Like `into_pairs()` but returns `(SpscSender, SpscReceiver)` with shard_id.
    pub fn into_wrapped_pairs(
        self,
    ) -> impl Iterator<Item = (SpscSender<T, CAP>, SpscReceiver<T, CAP>)> {
        self.pairs
            .into_iter()
            .enumerate()
            .filter_map(|(shard_id, opt)| {
                opt.map(|(tx, rx)| {
                    (
                        SpscSender {
                            shard_id,
                            inner: tx,
                        },
                        SpscReceiver {
                            shard_id,
                            inner: rx,
                        },
                    )
                })
            })
    }
}

// Builder через Channel::sharded_spsc

/// Creates a sharded SPSC channel short alias.
/// `num_shards` the number of independent SPSC channels (not necessarily degree 2).
pub fn shard_spsc<T: Send + 'static, const CAP: usize>(num_shards: usize) -> SpscShard<T, CAP> {
    SpscShard::new(num_shards)
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_channel::errors::{AsyncRecvError, RecvError};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    use std::thread;

    #[test]
    fn new_creates_correct_shard_count() {
        let ch = SpscShard::<u64, 8>::new(4);
        assert_eq!(ch.shards(), 4);
        assert_eq!(ch.remaining().len(), 4);
        assert!(!ch.is_empty());
    }

    #[test]
    fn non_power_of_two_shards_ok() {
        let ch = SpscShard::<u64, 8>::new(3);
        assert_eq!(ch.shards(), 3);
    }

    #[test]
    fn panic_on_zero_shards() {
        let r = std::panic::catch_unwind(|| SpscShard::<u64, 8>::new(0));
        assert!(r.is_err());
    }

    #[test]
    fn take_pair_basic() {
        let mut ch = SpscShard::<u64, 8>::new(4);
        let (tx, rx) = ch.take_pair(0).unwrap();
        let tx = SpscSender {
            shard_id: 0,
            inner: tx,
        };
        let rx = SpscReceiver {
            shard_id: 0,
            inner: rx,
        };
        tx.try_send(42).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 42);
    }

    #[test]
    fn take_pair_returns_none_when_already_taken() {
        let mut ch = SpscShard::<u64, 8>::new(4);
        let _ = ch.take_pair(0).unwrap();
        assert!(ch.take_pair(0).is_none());
    }

    #[test]
    fn take_pair_partial_remaining() {
        let mut ch = SpscShard::<u64, 8>::new(4);
        let _ = ch.take_pair(0);
        let _ = ch.take_pair(2);
        assert_eq!(ch.remaining(), vec![1, 3]);
    }

    #[test]
    fn wrapped_pairs_have_shard_id() {
        let ch = SpscShard::<u64, 8>::new(3);
        for (tx, rx) in ch.into_wrapped_pairs() {
            assert_eq!(tx.shard_id, rx.shard_id);
            tx.try_send(tx.shard_id as u64).unwrap();
            assert_eq!(rx.try_recv().unwrap(), rx.shard_id as u64);
        }
    }

    #[test]
    fn blocking_4_spsc_no_message_loss() {
        const N: u64 = 8;
        let ch = SpscShard::<u64, 16>::new(4);
        let total = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for (shard_id, tx, rx) in ch.into_pairs() {
            let tx = SpscSender {
                shard_id,
                inner: tx,
            };
            let rx = SpscReceiver {
                shard_id,
                inner: rx,
            };
            let t = total.clone();
            let c = thread::spawn(move || {
                let mut s = 0u64;
                loop {
                    match rx.recv() {
                        Ok(v) => s += v,
                        Err(e) if e.err == RecvError::Disconnected => break,
                        Err(_) => unreachable!(),
                    }
                }
                t.fetch_add(s, Relaxed);
            });
            let p = thread::spawn(move || {
                for i in 0..N / 4 {
                    tx.send(shard_id as u64 * N / 4 + i).unwrap();
                }
            });
            handles.push((p, c));
        }
        for (p, c) in handles {
            p.join().unwrap();
            c.join().unwrap();
        }
        assert_eq!(total.load(Relaxed), (0..N).sum::<u64>());
    }

    #[test]
    fn disconnected_when_sender_dropped() {
        let mut ch = SpscShard::<u64, 8>::new(2);
        let (tx, rx) = ch.take_pair(0).unwrap();
        let tx = SpscSender {
            shard_id: 0,
            inner: tx,
        };
        let rx = SpscReceiver {
            shard_id: 0,
            inner: rx,
        };
        tx.try_send(1).unwrap();
        drop(tx);
        assert_eq!(rx.recv().unwrap(), 1);
        assert!(matches!(
            rx.recv().unwrap_err().err,
            RecvError::Disconnected
        ));
    }

    #[test]
    fn disconnected_when_receiver_dropped() {
        let mut ch = SpscShard::<u64, 8>::new(2);
        let (tx, rx) = ch.take_pair(0).unwrap();
        let tx = SpscSender {
            shard_id: 0,
            inner: tx,
        };
        drop(rx);
        assert!(tx.try_send(1).is_err());
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn async_send_recv_basic() {
        let mut ch = SpscShard::<String, 8>::new(4);
        let (tx, rx) = ch.take_pair(0).unwrap();
        let tx = SpscSender {
            shard_id: 0,
            inner: tx,
        };
        let rx = SpscReceiver {
            shard_id: 0,
            inner: rx,
        };
        tx.send_async("hello".to_string()).await.unwrap();
        assert_eq!(rx.recv_async().await.unwrap(), "hello");
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn async_all_pairs_concurrent() {
        let ch = SpscShard::<u64, 32>::new(4);
        let total = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = ch
            .into_wrapped_pairs()
            .map(|(tx, rx)| {
                let t = total.clone();
                let c = tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match rx.recv_async().await {
                            Ok(v) => s += v,
                            Err(e) if e.err == AsyncRecvError::Disconnected => break,
                            Err(_) => unreachable!(),
                        }
                    }
                    t.fetch_add(s, Relaxed);
                });
                let p = tokio::spawn(async move {
                    for i in 0..10u64 {
                        tx.send_async(i).await.unwrap();
                    }
                });
                (p, c)
            })
            .collect();

        for (p, c) in handles {
            p.await.unwrap();
            c.await.unwrap();
        }
        assert_eq!(total.load(Relaxed), 4 * (0..10u64).sum::<u64>());
    }
}

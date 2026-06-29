use super::super::errors as shard_error;
use super::{
    buf::refill_on_error,
    hash::hash_key, receiver::ShardReceiver
};
use crate::internal_channel::{
    errors::AsyncSendError, mpmc_bounded, nearest_power_of_two, sender::Sender,
};
use std::{sync::Arc, time::Duration};

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

    /// Non blocking batch by hash(key_fn).
    /// Returns `Ok(sent)` if all elements have been sent.
    /// Returns `Err(sent)` if at least one shard was full or closed.
    /// `buf` contains all unsent elements (grouped by shard; per key order kept).
    pub fn try_send_batch(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyTryBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        let mut groups = self.group_by_shard(buf, &key_fn).into_iter().enumerate();
        while let Some((shard, mut group)) = groups.next() {
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
                    refill_on_error(buf, group, groups);
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
    /// Returns `Err(ShardedKeyBatchError)` if receiver is closed;
    /// `buf` then contains all unsent elements.
    pub fn send_batch(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        let mut groups = self.group_by_shard(buf, &key_fn).into_iter().enumerate();
        while let Some((shard, mut group)) = groups.next() {
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
                    refill_on_error(buf, group, groups);
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
    /// On error (deadline or disconnect) `buf` contains all unsent elements.
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
        let mut groups = self.group_by_shard(buf, &key_fn).into_iter().enumerate();
        while let Some((shard, mut group)) = groups.next() {
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
                    refill_on_error(buf, group, groups);
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
    /// Fast path: `try_send_batch_keyed` for the whole remaining batch.
    /// When a shard is full, ONE element (head of the failed group) is
    /// awaited into its shard the back pressure point then fast path
    /// retries. Only `Disconnected` interrupts; on error `buf` contains
    /// all unsent elements (including the one that was being awaited).
    pub async fn send_batch_async(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyAsyncBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        loop {
            match self.try_send_batch(buf, &key_fn) {
                Ok(sent) => {
                    total += sent;
                    return Ok(total);
                }
                Err(e) => {
                    total += e.sent;
                    if buf.is_empty() {
                        return Ok(total);
                    }
                    // buf[0] is the head of the failed (full) shard's group
                    // awaiting it waits for space in exactly that shard.
                    // remove(0) is O(n) but this is the slow path; taking from
                    // the FRONT preserves per key FIFO (pop() would break it).
                    let first = buf.remove(0);
                    let shard = hash_key(key_fn(&first)) & self.mask;
                    match self.senders[shard].send_async(first).await {
                        Ok(()) => total += 1,
                        Err(AsyncSendError::Disconnected(v)) => {
                            let key = key_fn(&v).to_string();
                            // Return the element: `buf` must contain all unsent.
                            buf.insert(0, v);
                            return Err(shard_error::ShardKeyAsyncBatchSendError {
                                key,
                                shard,
                                sent: total,
                            });
                        }
                    }
                }
            }
        }
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

/// Constructor ByKey sharded channel. `num_shards` is a power of two.
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
    use std::thread;

    /// Deterministically finds two keys mapping to DIFFERENT shards.
    fn two_keys_in_different_shards<T: Send + 'static, const CAP: usize>(
        tx: &ShardKey<T, CAP>,
    ) -> (&'static str, &'static str) {
        const KEYS: [&str; 8] = ["K0", "K1", "K2", "K3", "K4", "K5", "K6", "K7"];
        let ka = KEYS[0];
        let kb = KEYS
            .iter()
            .find(|k| tx.shard_for(k) != tx.shard_for(ka))
            .expect("among 8 keys at least one maps to another shard");
        (ka, kb)
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
            .try_send_batch(&mut batch, |(sym, _)| sym.as_str())
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

    // Regression: keyed batch error paths must NOT drop unprocessed groups.
    #[test]
    fn key_batch_err_returns_all_unsent() {
        let (tx, _rx) = shard_key::<u64, 2>(2); // CAP=2 — переполняется мгновенно
        let (ka, kb) = two_keys_in_different_shards(&tx);
        let mut batch: Vec<u64> = vec![1, 2, 3, 10, 20, 30]; // 3 на шард при CAP=2
        let before = batch.len();
        let r = tx.try_send_batch(&mut batch, |v| if *v < 10 { ka } else { kb });
        let sent = r.unwrap_err().sent;
        assert_eq!(sent + batch.len(), before, "ни один элемент не потерян");
        // Per-key FIFO preserved inside the returned remainder.
        let small: Vec<u64> = batch.iter().copied().filter(|v| *v < 10).collect();
        let big: Vec<u64> = batch.iter().copied().filter(|v| *v >= 10).collect();
        assert!(small.windows(2).all(|w| w[0] < w[1]));
        assert!(big.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn key_batch_err_identity_of_unsent() {
        // Не только количество, но и СОСТАВ: sent + returned == исходный набор.
        let (tx, rx) = shard_key::<u64, 2>(2);
        let (ka, kb) = two_keys_in_different_shards(&tx);
        let original: Vec<u64> = vec![1, 2, 3, 10, 20, 30];
        let mut batch = original.clone();
        let _ = tx.try_send_batch(&mut batch, |v| if *v < 10 { ka } else { kb });
        // Дренируем оба шарда и складываем с остатком.
        let mut all: Vec<u64> = batch.clone();
        let receivers = rx.into_receivers();
        for r in &receivers {
            while let Ok(v) = r.try_recv() {
                all.push(v);
            }
        }
        all.sort_unstable();
        let mut expected = original;
        expected.sort_unstable();
        assert_eq!(all, expected, "ни потерь, ни дублей");
    }

    #[test]
    fn key_blocking_batch_disconnect_returns_all_unsent() {
        let (tx, rx) = shard_key::<u64, 8>(2);
        drop(rx);
        let mut batch: Vec<u64> = (0..6).collect();
        let before = batch.len();
        let r = tx.send_batch(&mut batch, |_| "K");
        let sent = r.unwrap_err().sent;
        assert_eq!(sent + batch.len(), before);
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_batch_async_disconnect_no_loss() {
        let (tx, rx) = shard_key::<u64, 2>(2);
        drop(rx);
        let mut batch: Vec<u64> = (0..6).collect();
        let before = batch.len();
        let r = tx.send_batch_async(&mut batch, |_| "K").await;
        let sent = r.unwrap_err().sent;
        assert_eq!(
            sent + batch.len(),
            before,
            "keyed async batch: ни один элемент не потерян при Disconnected"
        );
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

    // Blocking threads (Miri)

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
    fn miri_key_group_by_shard_drain_extend() {
        let (tx, _rx) = shard_key::<u64, 8>(2);
        let mut batch: Vec<u64> = vec![10, 20, 30, 40];
        // We deliberately overflow the buffer to get an error and check extend
        // CAP=8 is enough, everyone must pass
        let result = tx.try_send_batch(&mut batch, |v| if *v <= 20 { "AAPL" } else { "MSFT" });
        assert!(result.is_ok());
        assert!(batch.is_empty());
        drop(tx);
    }

    #[test]
    fn miri_key_disconnect_mid_batch() {
        let (tx, rx) = shard_key::<String, 8>(2);
        drop(rx); // receiver will be dropped before sending
        let mut batch = vec!["hello".to_string(), "world".to_string()];
        let before = batch.len();
        let result = tx.try_send_batch(&mut batch, |s| s.as_str());
        // Error -receiver is closed, all elements are returned to batch
        assert!(result.is_err());
        assert_eq!(batch.len(), before, "all elements returned, none dropped");
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

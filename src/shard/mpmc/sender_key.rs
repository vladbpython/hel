use super::super::errors as shard_error;
use super::{
    buf::{RestoreGroups, refill_on_error},
    hash::hash_key,
    receiver::ShardReceiver,
};
use crate::internal_channel::{mpmc_bounded, shard_power_of_two, sender::Sender};
use std::{
    any::Any,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
        resume_unwind
    },
    sync::Arc, 
    time::{Duration,Instant}
};

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

    /// Cancel safe async send using `hash(key)` without value loss.
    /// Deterministic routing: a retry after cancellation targets the same shard.
    /// `key` is cloned into the error only on the disconnect path; cancellation allocates nothing.
    #[inline]
    pub async fn send_ref_async(
        &self,
        key: &str,
        slot: &mut Option<T>,
    ) -> Result<(), shard_error::ShardKeyAsyncSendRefError> {
        let shard = hash_key(key) & self.mask;
        self.senders[shard]
            .send_ref_async(slot)
            .await
            .map_err(|err| shard_error::ShardKeyAsyncSendRefError {
                key: key.to_string(),
                shard,
                err,
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
        let mut poisoned: Option<(Box<dyn Any + Send>, Vec<T>)> = None;
        for item in buf.drain(..) {
            match &mut poisoned {
                Some((_,data)) => data.push(item),
                None => match catch_unwind(AssertUnwindSafe(|| hash_key(key_fn(&item)) & self.mask))  {
                    Ok(shard) => groups[shard].push(item),
                    Err(p) => poisoned = Some((p, vec![item]))
                }
            }
        }
        if let Some((p, mut rescue)) = poisoned {
            for g in &mut groups {
                buf.append(g);
            }
            buf.append(&mut rescue);
            resume_unwind(p);
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
                    refill_on_error(buf, group, groups);
                    let first_key = buf
                        .first()
                        .map(|item| key_fn(item).to_string())
                        .unwrap_or_default();
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
                    refill_on_error(buf, group, groups);
                    let first_key = buf
                        .first()
                        .map(|item| key_fn(item).to_string())
                        .unwrap_or_default();
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
        let deadline = Instant::now() + d;
        let mut total = 0usize;
        let mut groups = self.group_by_shard(buf, &key_fn).into_iter().enumerate();
        while let Some((shard, mut group)) = groups.next() {
            if group.is_empty() {
                continue;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            match self.senders[shard].send_batch_timeout(&mut group, left) {
                Ok(sent) => total += sent,
                Err(e) => {
                    total += e.sent;
                    refill_on_error(buf, group, groups);
                    let first_key = buf
                        .first()
                        .map(|item| key_fn(item).to_string())
                        .unwrap_or_default();
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
        let groups = self.group_by_shard(buf, &key_fn);
        let mut g = RestoreGroups::new(buf, groups, Vec::new());
        let mut total = 0usize;

        for shard in 0..g.num_groups() {
            loop {
                if g.is_group_empty(shard) {
                    break;
                }
                // Bulk while there is room: stage a chunk, send it, put back
                // whatever did not fit. Both moves are O(chunk),
                // so bulk stays usable at every batch size.
                let free = CAP.saturating_sub(self.senders[shard].queued());
                if free > 0 {
                    let stage = g.bulk_stage(shard, free);
                    let sent_now = match self.senders[shard].try_send_batch(stage) {
                        Ok(n) => n,
                        Err(e) => e.sent,
                    };
                    g.bulk_return();
                    total += sent_now;
                    if sent_now > 0 {
                        continue; // made progress, try for more
                    }
                }
                if g.is_group_empty(shard) {
                    break;
                }
                // Channel is full: park on the head. O(1) to take it, so a long
                // backpressure stretch stays linear.
                // the difference is that the key is in error -> remove it from the head of the group before
                // the element will go to pending.
                let key = key_fn(g.head(shard)).to_string();
                let disconnected = self.senders[shard]
                    .send_async_from(g.take_head(shard))
                    .await
                    .is_err();
                if disconnected {
                    return Err(shard_error::ShardKeyAsyncBatchSendError {
                        key,
                        shard,
                        sent: total,
                    });
                }
                total += 1;
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

/// Constructor ByKey sharded channel. `num_shards` is a power of two.
pub fn shard_key<T: Send + 'static, const CAP: usize>(
    num_shards: usize,
) -> (ShardKey<T, CAP>, ShardReceiver<T, CAP>) {
    let num_shards = shard_power_of_two(num_shards);
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
#[cfg_attr(miri, allow(unused_imports))]
mod tests {
    use super::*;
    use crate::internal_channel::errors::{AsyncSendRefError, RecvError};
    use std::{thread, time::Duration};

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

    // Select a key for each shard: the hash is opaque, so we select the keys
    // through shard_for, rather than guessing.
    #[cfg(not(miri))]
    fn keys_covering_all_shards<T: Send + 'static, const CAP: usize>(
        tx: &ShardKey<T, CAP>,
        shards: usize,
    ) -> Vec<&'static str> {
        const CANDIDATES: &[&str] = &[
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
        ];
        let mut by_shard: Vec<Option<&'static str>> = vec![None; shards];
        for &k in CANDIDATES {
            let s = tx.shard_for(k);
            if by_shard[s].is_none() {
                by_shard[s] = Some(k);
            }
        }
        by_shard
            .into_iter()
            .map(|o| o.expect("For each shard there must be a candidate key"))
            .collect()
    }

    // Cancellation must not take anything: `send_async_from` keeps the in flight
    // item inside the guard, whose `Drop` returns it to `buf`.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_batch_async_cancel_keeps_batch() {
        const CAP: usize = 2;
        const SHARDS: usize = 2;
        let (tx, _rx) = shard_key::<(String, u64), CAP>(SHARDS);

        // Fill EVERY shard: only then is `sent == 0` guaranteed and
        // `before - batch.len()` honestly means "lost". With a single shard
        // filled, part of the batch would go into the free ones and the
        // difference would stop being a loss.
        let keys = keys_covering_all_shards(&tx, SHARDS);
        for &k in &keys {
            for i in 0..CAP as u64 {
                tx.try_send(k, (k.to_string(), i)).unwrap();
            }
        }
        let expected: Vec<(String, u64)> = vec![
            (keys[0].to_string(), 10),
            (keys[1].to_string(), 20),
            (keys[1].to_string(), 21),
        ];
        let mut batch = expected.clone();
        let r = tokio::time::timeout(
            Duration::ZERO,
            tx.send_batch_async(&mut batch, |(s, _)| s.as_str()),
        )
        .await;
        // Proves the cancellation path was actually hit: the future returned
        // Pending and was dropped mid `.await`.
        assert!(r.is_err(), "all shards are full, must wait");
        assert_eq!(
            batch, expected,
            "cancellation must lose nothing and reorder nothing"
        );
    }

    // The FIFO inside the key survives the cancellation.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_batch_async_cancel_keeps_per_key_fifo() {
        const CAP: usize = 2;
        const SHARDS: usize = 2;
        let (tx, _rx) = shard_key::<(String, u64), CAP>(SHARDS);
        let keys = keys_covering_all_shards(&tx, SHARDS);
        for &k in &keys {
            for i in 0..CAP as u64 {
                tx.try_send(k, (k.to_string(), i)).unwrap();
            }
        }
        // One key, increasing values.
        let expected: Vec<(String, u64)> = (10..15).map(|i| (keys[0].to_string(), i)).collect();
        let mut batch = expected.clone();
        let r = tokio::time::timeout(
            Duration::ZERO,
            tx.send_batch_async(&mut batch, |(s, _)| s.as_str()),
        )
        .await;
        assert!(r.is_err(), "all shards are full, must wait");
        assert_eq!(
            batch, expected,
            "cancellation must lose nothing and keep per key FIFO"
        );
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_send_ref_async_success() {
        let (tx, rx) = shard_key::<u64, 8>(4);
        let shard = tx.shard_for("AAPL");
        let mut slot = Some(99u64);
        tx.send_ref_async("AAPL", &mut slot).await.unwrap();
        assert_eq!(slot, None);
        assert_eq!(rx.receiver(shard).recv_async().await.unwrap(), 99);
    }

    // Cancellation keeps the value; deterministic hash routing means a retry
    // with the same key targets the same shard.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_send_ref_async_cancel_keeps_value() {
        const CAP: usize = 2;
        let (tx, rx) = shard_key::<u64, CAP>(2);
        let shard = tx.shard_for("AAPL");
        // Fill exactly the target shard through the same key.
        for i in 0..CAP as u64 {
            tx.send_async("AAPL", i).await.unwrap();
        }

        let mut slot = Some(500u64);
        let r = tokio::time::timeout(Duration::ZERO, tx.send_ref_async("AAPL", &mut slot)).await;
        assert!(
            r.is_err(),
            "target shard is full, the future must be pending"
        );
        assert_eq!(
            slot,
            Some(500),
            "cancellation must keep the value in the slot"
        );

        // Only the fill items are in the shard.
        for i in 0..CAP as u64 {
            assert_eq!(rx.receiver(shard).recv_async().await.unwrap(), i);
        }
        assert!(rx.receiver(shard).try_recv().is_err());
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn key_send_ref_async_disconnect_reports_key_and_shard() {
        let (tx, rx) = shard_key::<u64, 4>(4);
        let shard = tx.shard_for("MSFT");
        drop(rx);
        let mut slot = Some(1u64);
        let err = tx.send_ref_async("MSFT", &mut slot).await.unwrap_err();
        assert_eq!(err.key, "MSFT");
        assert_eq!(err.shard, shard);
        assert_eq!(err.err, AsyncSendRefError::Disconnected);
        assert_eq!(slot, Some(1));
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn drain_mode_keeps_per_key_fifo() {
        const CAP: usize = 4;
        const N: u64 = 40;
        let (tx, rx) = shard_key::<(String, u64), CAP>(2);

        // One key -> one shard. CAP is far smaller than N, so the bulk path
        // fills the ring and everything after it goes through enter_drain.
        let mut batch: Vec<(String, u64)> = (0..N).map(|i| ("AAA".to_string(), i)).collect();
        let shard = rx.shard_for("AAA");

        let consumer = tokio::spawn(async move {
            let mut got = Vec::new();
            while got.len() < N as usize {
                match rx.recv_async(shard).await {
                    Ok((_, seq)) => got.push(seq),
                    Err(_) => break,
                }
            }
            got
        });

        let sent = tx
            .send_batch_async(&mut batch, |(s, _)| s.as_str())
            .await
            .unwrap();
        assert_eq!(sent, N as usize, "whole batch should go out");
        assert!(batch.is_empty(), "nothing should be left behind");

        let got = consumer.await.unwrap();
        let want: Vec<u64> = (0..N).collect();
        assert_eq!(got, want, "per key FIFO broken by the reversed drain");
    }

    /// Cancelling mid drain must leave `buf` in FIFO order, not reversed.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn cancel_mid_drain_restores_fifo_order() {
        const CAP: usize = 4;
        const N: u64 = 32;
        let (tx, _rx) = shard_key::<(String, u64), CAP>(2);

        let mut batch: Vec<(String, u64)> = (0..N).map(|i| ("AAA".to_string(), i)).collect();
        // Nobody consumes: the ring fills, the rest enters drain mode and parks.
        let r = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            tx.send_batch_async(&mut batch, |(s, _)| s.as_str()),
        )
        .await;
        assert!(r.is_err(), "must still be waiting on a full shard");

        assert!(!batch.is_empty(), "leftovers must come back");
        let seqs: Vec<u64> = batch.iter().map(|(_, s)| *s).collect();
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(seqs, sorted, "leftovers came back reversed, FIFO is broken");
    }

    #[cfg(not(miri))]
    #[test]
    fn send_batch_timeout_is_one_budget_across_shards() {
        const CAP: usize = 2;
        const SHARDS: usize = 4;
        let (tx, rx) = shard_key::<(String, u64), CAP>(SHARDS);

        let mut keys: Vec<&'static str> = Vec::new();
        let mut seen = vec![false; SHARDS];
        for k in ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "kA", "kB"] {
            let sh = tx.shard_for(k);
            if !seen[sh] {
                seen[sh] = true;
                keys.push(k);
            }
        }
        assert_eq!(keys.len(), SHARDS, "could not cover every shard");

        // Fill every shard.
        for &k in &keys {
            for i in 0..CAP as u64 {
                tx.try_send(k, (k.to_string(), i)).unwrap();
            }
        }

        let d = Duration::from_millis(40);
        let mut rx = rx;
        let drainer = thread::spawn(move || {
            for shard in 0..SHARDS {
                thread::sleep(Duration::from_millis(30));
                let _ = rx.try_recv(shard);
            }
            rx
        });

        let mut batch: Vec<(String, u64)> = keys.iter().map(|k| (k.to_string(), 99)).collect();
        let t = std::time::Instant::now();
        let _ = tx.send_batch_timeout(&mut batch, d, |(s, _)| s.as_str());
        let took = t.elapsed();
        let _ = drainer.join();

        assert!(
            took < d * 2,
            "took {took:?} for a {d:?} budget over {SHARDS} shards: the timeout is being spent per shard"
        );
    }
    
}

#[cfg(all(test, not(loom)))]
mod key_fn_panic_tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn panic_key_fn_does_not_lose_the_batch() {
        let (tx, _rx) = shard_key::<u64, 8>(2);
        let mut buf = vec![1u64, 2, 3, 4];
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _ = tx.try_send_batch(&mut buf, |v| {
                if *v == 3 {
                    panic!("malformed item");
                }
                "key"
            });
        }));
        assert!(r.is_err(), "key_fn must have panicked");
        assert_eq!(buf, vec![1, 2, 3, 4], "batch lost on key_fn panic");
    }
}
use super::super::errors as shard_error;
use super::{
    buf::{RestoreGroups, refill_on_error},
    receiver::ShardReceiver,
};
use crate::internal_channel::{
    core::SeqInner, mpmc_bounded, nearest_power_of_two, sender::Sender, traits::InnerChannel,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// SymbolHandle

/// Symbol handle is the shard index of its group, calculated ONCE during subscription.
/// Copy, placed in the register. On a hot path, sending along it = indexing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SymbolHandle {
    shard: usize,
}

impl SymbolHandle {
    #[inline(always)]
    pub fn shard(&self) -> usize {
        self.shard
    }
}

//ShardHandle resolver symbol → shard

#[derive(Clone)]
pub struct ShardHandle {
    route: Arc<HashMap<String, usize>>,
}

impl ShardHandle {
    pub fn new(route: Arc<HashMap<String, usize>>) -> Self {
        Self { route }
    }

    #[inline]
    pub fn handle(&self, symbol: &str) -> Option<SymbolHandle> {
        self.route.get(symbol).map(|&shard| SymbolHandle { shard })
    }
}

// ShardGroup

/// Grouping many to few: many keys → few shards according to an explicit map.
pub struct ShardGroup<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + 'static = SeqInner<T, CAP>,
> {
    senders: Arc<[Sender<T, CAP, I>]>,
    /// key → shard index (group). For Arc cheap to clone and divide
    /// with ShardHandle. Used when resolving (handle), not on the hot path.
    route: Arc<HashMap<String, usize>>,
    mask: usize,
}

impl<T: Send + 'static, const CAP: usize> ShardGroup<T, CAP> {
    /// Create from an explicit list of groups. Group i → shard i.
    pub fn from_groups(groups: &[&[&str]]) -> (Self, ShardReceiver<T, CAP>) {
        let n = nearest_power_of_two(groups.len().max(1));
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..n).map(|_| mpmc_bounded::<T, CAP>()).unzip();

        let mut route = HashMap::new();
        for (shard, keys) in groups.iter().enumerate() {
            for &k in keys.iter() {
                route.insert(k.to_string(), shard);
            }
        }

        let group = Self {
            senders: senders.into(),
            route: Arc::new(route),
            mask: n - 1,
        };
        (group, ShardReceiver::new(receivers))
    }

    /// Creation from a ready-made map “key → group index” + number of groups.
    /// Indexes are normalized to the range of shards via `%n`.
    pub fn from_map(
        map: HashMap<String, usize>,
        num_groups: usize,
    ) -> (Self, ShardReceiver<T, CAP>) {
        let n = nearest_power_of_two(num_groups.max(1));
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..n).map(|_| mpmc_bounded::<T, CAP>()).unzip();

        let route: HashMap<String, usize> = map.into_iter().map(|(k, g)| (k, g % n)).collect();

        let group = Self {
            senders: senders.into(),
            route: Arc::new(route),
            mask: n - 1,
        };
        (group, ShardReceiver::new(receivers))
    }

    #[inline]
    pub fn handle(&self, symbol: &str) -> Option<SymbolHandle> {
        self.route.get(symbol).map(|&shard| SymbolHandle { shard })
    }

    pub fn shard_handle(&self) -> ShardHandle {
        ShardHandle::new(self.route.clone())
    }

    /// Index of the symbol shard (without wrapping in the handle).
    #[inline]
    pub fn shard_for(&self, symbol: &str) -> Option<usize> {
        self.route.get(symbol).copied()
    }

    /// Number of shards (groups).
    #[inline(always)]
    pub fn shards(&self) -> usize {
        self.mask + 1
    }

    /// How many keys are registered.
    #[inline(always)]
    pub fn keys_count(&self) -> usize {
        self.route.len()
    }

    /// Non blocking sending by handle. Pure indexing of senders.
    #[inline(always)]
    pub fn try_send(
        &self,
        h: SymbolHandle,
        value: T,
    ) -> Result<(), shard_error::ShardTrySendError<T>> {
        let idx = h.shard & self.mask;
        self.senders[idx]
            .try_send(value)
            .map_err(|err| shard_error::ShardTrySendError { shard: idx, err })
    }

    /// Blocking sending by handle.
    #[inline(always)]
    pub fn send(&self, h: SymbolHandle, value: T) -> Result<(), shard_error::ShardSendError<T>> {
        let idx = h.shard & self.mask;
        self.senders[idx]
            .send(value)
            .map_err(|err| shard_error::ShardSendError { shard: idx, err })
    }

    /// Blocking sending by handle with deadline.
    #[inline(always)]
    pub fn send_timeout(
        &self,
        h: SymbolHandle,
        value: T,
        d: Duration,
    ) -> Result<(), shard_error::ShardSendError<T>> {
        let idx = h.shard & self.mask;
        self.senders[idx]
            .send_timeout(value, d)
            .map_err(|err| shard_error::ShardSendError { shard: idx, err })
    }

    /// Async sending by handle.
    #[inline(always)]
    pub async fn send_async(
        &self,
        h: SymbolHandle,
        value: T,
    ) -> Result<(), shard_error::ShardAsyncSendError<T>> {
        let idx = h.shard & self.mask;
        self.senders[idx]
            .send_async(value)
            .await
            .map_err(|err| shard_error::ShardAsyncSendError { shard: idx, err })
    }

    /// Lays out buf among shards according to the map. key_fn extracts the character.
    /// Returns groups by shards AND unused (unregistered keys)
    /// as a separate vector the caller decides where to put them.
    #[inline]
    fn group_by_route(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> (Vec<Vec<T>>, Vec<T>) {
        let n = self.mask + 1;
        let mut groups: Vec<Vec<T>> = (0..n).map(|_| Vec::new()).collect();
        let mut unused: Vec<T> = Vec::new();
        for item in buf.drain(..) {
            match self.route.get(key_fn(&item)) {
                Some(&shard) => groups[shard].push(item),
                None => unused.push(item),
            }
        }
        (groups, unused)
    }

    /// Non blocking batch: a pack of different instruments → by group.
    /// Returns `Ok(sent)` if all (except unused) have been sent.
    /// Returns `Err(ShardKeyTryBatchSendError)` if the shard is full or closed
    /// (stop on the first error.
    /// The output of `buf` contains ALL the raw data: unused (not in the map) and
    /// unsent (remainder of the fallen group + untouched groups) no losses.
    pub fn try_send_batch(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyTryBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let (groups, mut unused) = self.group_by_route(buf, &key_fn);
        let mut total = 0usize;
        let mut groups = groups.into_iter().enumerate();
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
                    // return the remainder of the fallen group + untouched groups
                    refill_on_error(buf, group, groups);
                    buf.append(&mut unused); // сирот тоже в buf
                    return Err(shard_error::ShardKeyTryBatchSendError {
                        key: first_key,
                        shard,
                        sent: total,
                        reason: e.err,
                    });
                }
            }
        }
        buf.append(&mut unused);
        Ok(total)
    }

    /// Blocking batch: a pack of different instruments → by group.
    /// Blocked until the entire pack goes to its shards (waiting for space).
    /// After calling `buf` contains unused (the key is not in the map) and when closed
    /// receiver unsent elements. Stores the FIFO within the group.
    /// Returns `Ok(sent)` if everything has been sent (except unused).
    /// Returns `Err(ShardKeyBatchSendError)` if the receiver is closed.
    pub fn send_batch(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let (groups, mut unused) = self.group_by_route(buf, &key_fn);
        let mut total = 0usize;
        let mut groups = groups.into_iter().enumerate();
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
                    buf.append(&mut unused);
                    return Err(shard_error::ShardKeyBatchSendError {
                        key: first_key,
                        shard,
                        sent: total,
                        reason: e.err,
                    });
                }
            }
        }
        buf.append(&mut unused);
        Ok(total)
    }

    /// Blocking batch with timeout: a pack of different instruments → by groups.
    /// The `d` timeout is applied to each shard (as in shard_key::send_batch_timeout).
    /// On error (deadline or disconnect), `buf` contains unsent elements.
    /// Unused (the key is not in the map) are also placed in `buf`. Stores the FIFO within the group.
    /// Returns `Ok(sent)` if everything has been sent (except unused).
    pub fn send_batch_timeout(
        &self,
        buf: &mut Vec<T>,
        d: Duration,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardKeyBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let (groups, mut unused) = self.group_by_route(buf, &key_fn);
        let mut total = 0usize;
        let mut groups = groups.into_iter().enumerate();
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
                    buf.append(&mut unused);
                    return Err(shard_error::ShardKeyBatchSendError {
                        key: first_key,
                        shard,
                        sent: total,
                        reason: e.err,
                    });
                }
            }
        }
        buf.append(&mut unused);
        Ok(total)
    }

    /// Async batch with back-pressure. One puts the pack into groups
    /// once, sends to shards; when the shard is full awaiting the head of the group (FIFO),
    /// then continues. Unused (the key is not in the card) in buf at the end.
    /// The output of `buf` contains unused and (if Disconnected) unsent.
    /// Unlike shard_key (fast path retry via try_send_batch): here
    /// layout ONCE, because unused cannot be driven through hash to retry
    /// Strict routing control requires one time grouping by map.
    pub async fn send_batch_async(
        &self,
        buf: &mut Vec<T>,
        key_fn: impl for<'k> Fn(&'k T) -> &'k str,
    ) -> Result<usize, shard_error::ShardAsyncBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Route ONCE. From here `buf` is empty and everything lives in the guard.
        let (groups, unused) = self.group_by_route(buf, &key_fn);
        let mut g = RestoreGroups::new(buf, groups, unused);
        let mut total = 0usize;

        for shard in 0..g.num_groups() {
            loop {
                match self.senders[shard].try_send_batch(g.group_mut(shard)) {
                    Ok(sent) => {
                        total += sent;
                        break; // group is gone
                    }
                    Err(e) => {
                        total += e.sent;
                        if g.group_mut(shard).is_empty() {
                            break;
                        }
                        // Backpressure: await the head (FIFO), then retry the
                        // batch. No rerouting, the group is already laid out.
                        let disconnected = self.senders[shard]
                            .send_async_from(g.take_head(shard))
                            .await
                            .is_err();
                        if disconnected {
                            // `g` drops here: groups, orphans and the pending
                            // item all go back into `buf`.
                            return Err(shard_error::ShardAsyncBatchSendError {
                                shard,
                                sent: total,
                            });
                        }
                        total += 1;
                    }
                }
            }
        }
        // `g` drops on the way out: groups are empty by now, orphans go to `buf`.
        Ok(total)
    }
}

/// Cases for creating ShardGroup: explicit groups or map.
pub enum ShardGroupCase<'a> {
    Groups {
        groups: &'a [&'a [&'a str]],
    },
    Map {
        map: HashMap<String, usize>,
        num_groups: usize,
    },
}

/// Single constructor for ShardGroup from ShardGroupCase.
/// Dispatch on from_groups /from_map.
pub fn shard_group<T: Send + 'static, const CAP: usize>(
    case: ShardGroupCase<'_>,
) -> (ShardGroup<T, CAP>, ShardReceiver<T, CAP>) {
    match case {
        ShardGroupCase::Groups { groups } => ShardGroup::from_groups(groups),
        ShardGroupCase::Map { map, num_groups } => ShardGroup::from_map(map, num_groups),
    }
}

// Clone

impl<T: Send + 'static, const CAP: usize> Clone for ShardGroup<T, CAP> {
    fn clone(&self) -> Self {
        let senders: Arc<[Sender<T, CAP>]> = self
            .senders
            .iter()
            .map(Sender::clone)
            .collect::<Vec<_>>()
            .into();
        Self {
            senders,
            route: self.route.clone(),
            mask: self.mask,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn groups_route_deterministically() {
        let (tx, _rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[
                &["BTCUSDT", "ETHUSDT"],
                &["BTC-PERP", "ETH-PERP"],
                &["BTC-USD", "ETH-USD"],
            ],
        });
        assert_eq!(tx.shard_for("BTCUSDT"), tx.shard_for("ETHUSDT"));
        assert_eq!(tx.shard_for("BTC-PERP"), tx.shard_for("ETH-PERP"));
        assert_ne!(tx.shard_for("BTCUSDT"), tx.shard_for("BTC-PERP"));
        assert_eq!(tx.shard_for("DOGE"), None);
        assert_eq!(tx.keys_count(), 6);
    }

    #[test]
    fn handle_resolves_once() {
        let (tx, _rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        let b = tx.handle("BBB").unwrap();
        assert_ne!(a.shard(), b.shard());
        assert!(tx.handle("ZZZ").is_none());
    }

    #[test]
    fn shard_handle_shares_route() {
        let (tx, _rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let resolver = tx.shard_handle();
        // отдельный резолвер видит ту же карту
        assert_eq!(
            resolver.handle("AAA").unwrap().shard(),
            tx.handle("AAA").unwrap().shard()
        );
        assert!(resolver.handle("ZZZ").is_none());
    }

    #[test]
    fn send_by_handle_lands_in_group_shard() {
        let (tx, rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        tx.try_send(a, 42).unwrap();
        assert_eq!(rx.receiver(a.shard()).try_recv().unwrap(), 42);
    }

    #[test]
    fn batch_single_instrument_fast() {
        let (tx, rx) = shard_group::<(String, u64), 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        let mut buf = vec![
            ("AAA".to_string(), 1),
            ("AAA".to_string(), 2),
            ("AAA".to_string(), 3),
            ("AAA".to_string(), 4),
        ];
        let sent = tx.try_send_batch(&mut buf, |(s, _)| s.as_str()).unwrap();
        assert_eq!(sent, 4);
        assert!(buf.is_empty());
        let mut out = Vec::new();
        rx.receiver(a.shard()).recv_batch(&mut out, 8);
        assert_eq!(
            out,
            vec![
                ("AAA".to_string(), 1),
                ("AAA".to_string(), 2),
                ("AAA".to_string(), 3),
                ("AAA".to_string(), 4),
            ]
        );
    }

    #[test]
    fn batch_two_shards_via_into_receivers() {
        let (tx, rx) = shard_group::<(String, u64), 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        let b = tx.handle("BBB").unwrap();
        assert_ne!(a.shard(), b.shard());
        let mut buf = vec![
            ("AAA".to_string(), 1),
            ("BBB".to_string(), 10),
            ("AAA".to_string(), 2),
            ("BBB".to_string(), 20),
            ("AAA".to_string(), 3),
            ("BBB".to_string(), 30),
            ("AAA".to_string(), 4),
        ];
        let sent = tx.try_send_batch(&mut buf, |(s, _)| s.as_str()).unwrap();
        assert_eq!(sent, 7);
        assert!(buf.is_empty());
        let receivers = rx.into_receivers();
        let mut out_a = Vec::new();
        receivers[a.shard()].recv_batch(&mut out_a, 8);
        assert_eq!(
            out_a,
            vec![
                ("AAA".to_string(), 1),
                ("AAA".to_string(), 2),
                ("AAA".to_string(), 3),
                ("AAA".to_string(), 4),
            ]
        );
        let mut out_b = Vec::new();
        receivers[b.shard()].recv_batch(&mut out_b, 8);
        assert_eq!(
            out_b,
            vec![
                ("BBB".to_string(), 10),
                ("BBB".to_string(), 20),
                ("BBB".to_string(), 30),
            ]
        );
        // изоляция по группам
        assert!(out_a.iter().all(|(s, _)| s == "AAA"));
        assert!(out_b.iter().all(|(s, _)| s == "BBB"));
    }

    #[test]
    fn from_map_constructor() {
        let mut m = HashMap::new();
        m.insert("X".to_string(), 0);
        m.insert("Y".to_string(), 1);
        m.insert("Z".to_string(), 0);
        let (tx, _rx) = shard_group::<u64, 64>(ShardGroupCase::Map {
            map: m,
            num_groups: 2,
        });
        assert_eq!(tx.shard_for("X"), tx.shard_for("Z"));
        assert_ne!(tx.shard_for("X"), tx.shard_for("Y"));
    }

    #[test]
    fn shard_group_via_case() {
        let (tx, _rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        assert_ne!(tx.shard_for("AAA"), tx.shard_for("BBB"));
        let mut m = HashMap::new();
        m.insert("X".to_string(), 0usize);
        m.insert("Y".to_string(), 1usize);
        let (tx2, _rx2) = shard_group::<u64, 64>(ShardGroupCase::Map {
            map: m,
            num_groups: 2,
        });
        assert_ne!(tx2.shard_for("X"), tx2.shard_for("Y"));
    }

    #[test]
    fn clone_shares_routing() {
        let (tx1, _rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let tx2 = tx1.clone();
        assert_eq!(tx1.shard_for("AAA"), tx2.shard_for("AAA"));
        assert_eq!(tx1.shards(), tx2.shards());
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn async_send_by_handle() {
        let (tx, rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        tx.send_async(a, 7).await.unwrap();
        assert_eq!(rx.receiver(a.shard()).recv_async().await.unwrap(), 7);
    }

    // Disconnected should NOT lose groups that the loop has not reached.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn group_batch_async_disconnect_no_loss() {
        let (tx, rx) = shard_group::<(String, u64), 4>(ShardGroupCase::Groups {
            groups: &[&["A"], &["B"], &["C"]],
        });
        drop(rx);

        let mut batch: Vec<(String, u64)> = vec![
            ("A".into(), 1),
            ("A".into(), 2),
            ("B".into(), 3),
            ("B".into(), 4),
            ("C".into(), 5),
            ("ZZZ".into(), 6), // unused
        ];
        let before = batch.len();

        let r = tx.send_batch_async(&mut batch, |(s, _)| s.as_str()).await;
        let sent = r.expect_err("receiver is closed").sent;

        assert_eq!(
            sent + batch.len(),
            before,
            "group async batch: Disconnected should not lose intact groups"
        );
    }

    // Cancellation should not take away the entire batch.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn group_batch_async_cancel_keeps_batch() {
        const CAP: usize = 2;
        let (tx, _rx) = shard_group::<(String, u64), CAP>(ShardGroupCase::Groups {
            groups: &[&["A"], &["B"]],
        });

        // Fill the shard of group A to capacity.
        let ha = tx.handle("A").unwrap();
        for i in 0..CAP as u64 {
            tx.try_send(ha, ("A".into(), i)).unwrap();
        }

        let mut batch: Vec<(String, u64)> = vec![
            ("A".into(), 10), // will run into a clogged shard
            ("B".into(), 20), // shard free
            ("B".into(), 21),
            ("ZZZ".into(), 30), // unused
        ];
        let before = batch.len();

        let r = tokio::time::timeout(
            Duration::ZERO,
            tx.send_batch_async(&mut batch, |(s, _)| s.as_str()),
        )
        .await;
        assert!(r.is_err(), "шард A полон — обязан ждать");

        // Cancellation has the right to take away a maximum of ONE element, the one that was in flight
        // inside send_async. Everything else must be in buf.
        let lost = before - batch.len();
        assert!(
            lost <= 1,
            "cancellation took away {lost} elements instead of at most one (buf.len() = {})",
            batch.len()
        );
        assert!(
            !batch.is_empty(),
            "buf is empty: the layout has leaked into local variables"
        );
    }

    // A FIFO inside a key survives cancellation: elements of the same character are returned to buf in their original order.
    #[cfg(not(miri))]
    #[tokio::test]
    async fn group_batch_async_cancel_keeps_per_key_fifo() {
        const CAP: usize = 2;
        let (tx, _rx) = shard_group::<(String, u64), CAP>(ShardGroupCase::Groups {
            groups: &[&["A"], &["B"]],
        });

        let ha = tx.handle("A").unwrap();
        for i in 0..CAP as u64 {
            tx.try_send(ha, ("A".into(), i)).unwrap();
        }

        let mut batch: Vec<(String, u64)> = (10..15).map(|i| ("A".to_string(), i)).collect();
        let before = batch.len();

        let _ = tokio::time::timeout(
            Duration::ZERO,
            tx.send_batch_async(&mut batch, |(s, _)| s.as_str()),
        )
        .await;
        assert_eq!(
            batch.len(),
            before,
            "shard A is full, send_batch_async must wait"
        );
        // What is returned should be in ascending order: the FIFO of the key is not broken.
        let vals: Vec<u64> = batch.iter().map(|(_, v)| *v).collect();
        let mut sorted = vals.clone();
        sorted.sort_unstable();
        assert_eq!(vals, sorted, "cancellation must neither lose nor rearrange");
    }
}

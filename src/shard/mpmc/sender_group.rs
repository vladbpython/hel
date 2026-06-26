use super::super::errors as shard_error;
use super::receiver::ShardReceiver;
use crate::internal_channel::{
    core::SeqInner, errors::AsyncSendError, mpmc_bounded, nearest_power_of_two, sender::Sender,
    traits::InnerChannel,
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

    /// Non-blocking batch in the handle shard. All elements → one shard.
    #[inline]
    pub fn try_send_batch(
        &self,
        h: SymbolHandle,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardTryBatchSendError> {
        let idx = h.shard & self.mask;
        self.senders[idx]
            .try_send_batch(buf)
            .map_err(|e| shard_error::ShardTryBatchSendError {
                shard: idx,
                sent: e.sent,
                reason: e.err,
            })
    }

    /// Blocking batch in the handle shard.
    #[inline]
    pub fn send_batch(
        &self,
        h: SymbolHandle,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardBatchSendError> {
        let idx = h.shard & self.mask;
        self.senders[idx]
            .send_batch(buf)
            .map_err(|e| shard_error::ShardBatchSendError {
                shard: idx,
                sent: e.sent,
                reason: e.err,
            })
    }

    /// Async batch in the handle shard. Fast path the whole pack into a shard; when filling
    /// await one element (FIFO, from the beginning), then repeat.
    pub async fn send_batch_async(
        &self,
        h: SymbolHandle,
        buf: &mut Vec<T>,
    ) -> Result<usize, shard_error::ShardAsyncBatchSendError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let idx = h.shard & self.mask;
        let mut total = 0usize;
        loop {
            match self.senders[idx].try_send_batch(buf) {
                Ok(sent) => {
                    total += sent;
                    return Ok(total);
                }
                Err(e) => {
                    total += e.sent;
                    if buf.is_empty() {
                        return Ok(total);
                    }
                    let first = buf.remove(0); // FIFO
                    match self.senders[idx].send_async(first).await {
                        Ok(()) => total += 1,
                        Err(AsyncSendError::Disconnected(v)) => {
                            buf.insert(0, v);
                            return Err(shard_error::ShardAsyncBatchSendError {
                                shard: idx,
                                sent: total,
                            });
                        }
                    }
                }
            }
        }
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
        let (tx, rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        let mut buf = vec![1u64, 2, 3, 4];
        let sent = tx.try_send_batch(a, &mut buf).unwrap();
        assert_eq!(sent, 4);
        let mut out = Vec::new();
        rx.receiver(a.shard()).recv_batch(&mut out, 8);
        assert_eq!(out, vec![1, 2, 3, 4]); // FIFO сохранён
    }

    #[test]
    fn batch_two_shards_via_into_receivers() {
        let (tx, rx) = shard_group::<u64, 64>(ShardGroupCase::Groups {
            groups: &[&["AAA"], &["BBB"]],
        });
        let a = tx.handle("AAA").unwrap();
        let b = tx.handle("BBB").unwrap();
        assert_ne!(a.shard(), b.shard());
        let mut buf_a = vec![1u64, 2, 3, 4];
        let mut buf_b = vec![10u64, 20, 30];
        assert_eq!(tx.try_send_batch(a, &mut buf_a).unwrap(), 4);
        assert_eq!(tx.try_send_batch(b, &mut buf_b).unwrap(), 3);
        let receivers = rx.into_receivers();
        let mut out_a = Vec::new();
        receivers[a.shard()].recv_batch(&mut out_a, 8);
        assert_eq!(out_a, vec![1, 2, 3, 4]);
        let mut out_b = Vec::new();
        receivers[b.shard()].recv_batch(&mut out_b, 8);
        assert_eq!(out_b, vec![10, 20, 30]);
        assert!(out_a.iter().all(|&v| v < 10));
        assert!(out_b.iter().all(|&v| v >= 10));
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
}

//! Segmented channels: N independent ring buffers without blocking.
//!
//! Two types:
//! -[`ShardRoundRobin`] - cyclic routing, uniform load distribution, without a key
//! -[`ShardKey`] — routing by key, sorting by symbol, hash(key) → shard
//! -[`ShardGroup`] — many to few routing by an explicit map many keys → few shards, group is chosen by you
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
//! use hel::channel::mpmc::{round_robin, shard_key, shard_group, ShardGroupCase};
//!
//! //RoundRobin: without a key
//! let (tx, rx) = round_robin::<u64, 128>(4);
//! tx.try_send(42).unwrap();
//!
//! //ByKey: with a key, the order is guaranteed
//! let (tx, rx) = shard_key::<u64, 128>(4);
//! tx.try_send("AAPL", 150).unwrap();
//!
//! // ByGroup: explicit grouping, many keys → few shards
//! let (tx, rx) = shard_group::<u64, 128>(ShardGroupCase::Groups {
//!     groups: &[
//!         &["BTCUSDT", "ETHUSDT"], // group 0 → shard 0 (e.g. Binance)
//!         &["BTC-PERP", "ETH-PERP"], // group 1 → shard 1 (e.g. Bybit)
//!     ],
//! });
//! let h = tx.handle("BTCUSDT").unwrap(); // resolve once
//! tx.try_send(h, 150).unwrap(); // send by handle on the hot path
//! ```

mod buf;
mod hash;
pub mod receiver;
pub mod sender_group;
pub mod sender_key;
pub mod sender_round_robin;

use crate::internal_channel::errors;
use std::fmt::{Debug, Display, Formatter, Result};

/// `try_send` error in `Shard` (RoundRobin) Full or Disconnected.
pub struct ShardTrySendError<T> {
    pub shard: usize,
    pub err: errors::TrySendError<T>,
}

impl<T> Display for ShardTrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardTrySendError {{ shard: {}, err: {:?} }}",
            self.shard, self.err
        )
    }
}

impl<T> Debug for ShardTrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ShardTrySendError")
            .field("shard", &self.shard)
            .field("err", &self.err)
            .finish()
    }
}

/// Error `send` /`send_timeout` in `Shard` (RoundRobin) Disconnected or TimeOut.
pub struct ShardSendError<T> {
    pub shard: usize,
    pub err: errors::SendError<T>,
}

impl<T> Display for ShardSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardSendError {{ shard: {}, err: {:?} }}",
            self.shard, self.err
        )
    }
}

impl<T> Debug for ShardSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ShardSendError")
            .field("shard", &self.shard)
            .field("err", &self.err)
            .finish()
    }
}

/// `try_send` error in `ShardKey` (ByKey) Full or Disconnected.
pub struct ShardKeyTrySendError<T> {
    pub key: String,
    pub shard: usize,
    pub err: errors::TrySendError<T>,
}

impl<T> Display for ShardKeyTrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardKeyTrySendError {{ key: {}, shard: {}, err: {:?} }}",
            self.key, self.shard, self.err
        )
    }
}

impl<T> Debug for ShardKeyTrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ShardKeyTrySendError")
            .field("key", &self.key)
            .field("shard", &self.shard)
            .field("err", &self.err)
            .finish()
    }
}

/// `send` /`send_timeout` error in `ShardKey` (ByKey) Disconnected or TimeOut.
pub struct ShardKeySendError<T> {
    pub key: String,
    pub shard: usize,
    pub err: errors::SendError<T>,
}

impl<T> Display for ShardKeySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardKeySendError {{ key: {}, shard: {}, err: {:?} }}",
            self.key, self.shard, self.err
        )
    }
}

impl<T> Debug for ShardKeySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ShardKeySendError")
            .field("key", &self.key)
            .field("shard", &self.shard)
            .field("err", &self.err)
            .finish()
    }
}

/// Error sending async to `Shard` (RoundRobin).
/// Only `Disconnected` async cannot return `Full`.
#[derive(PartialEq)]
pub struct ShardAsyncSendError<T> {
    pub shard: usize,
    pub err: errors::AsyncSendError<T>,
}

impl<T> Display for ShardAsyncSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardAsyncSendError {{ shard: {}, err: {:?} }}",
            self.shard, self.err
        )
    }
}

impl<T> Debug for ShardAsyncSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ShardAsyncSendError")
            .field("shard", &self.shard)
            .field("err", &self.err)
            .finish()
    }
}

/// Error sending async to `ShardKey` (ByKey).
#[derive(PartialEq)]
pub struct ShardKeyAsyncSendError<T> {
    pub key: String,
    pub shard: usize,
    pub err: errors::AsyncSendError<T>,
}

impl<T> Display for ShardKeyAsyncSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardKeyAsyncSendError {{ key: {}, shard: {}, err: {:?} }}",
            self.key, self.shard, self.err
        )
    }
}

impl<T> Debug for ShardKeyAsyncSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ShardKeyAsyncSendError")
            .field("key", &self.key)
            .field("shard", &self.shard)
            .field("err", &self.err)
            .finish()
    }
}

/// `try_recv` error from `ShardReceiver` is Empty or Disconnected.
#[derive(Debug, PartialEq)]
pub struct ShardTryRecvError {
    pub shard: usize,
    pub err: errors::TryRecvError,
}

impl Display for ShardTryRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardTryRecvError {{ shard: {}, err: {:?} }}",
            self.shard, self.err
        )
    }
}

/// `recv` /`recv_timeout` error from specific shard `ShardReceiver`.
#[derive(Debug, PartialEq)]
pub struct ShardRecvError {
    /// The index of the shard from which it was read.
    pub shard: usize,
    /// Reason: `Disconnected` or `TimeOut`.
    pub err: errors::RecvError,
}

impl Display for ShardRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardRecvError {{ shard: {}, err: {:?} }}",
            self.shard, self.err
        )
    }
}

/// Error async recv from specific shard.
#[derive(Debug, PartialEq)]
pub struct ShardAsyncRecvError {
    pub shard: usize,
    pub err: errors::AsyncRecvError,
}

impl Display for ShardAsyncRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardAsyncRecvError {{ shard: {}, err: {:?} }}",
            self.shard, self.err
        )
    }
}

/// `try_send_batch` error in `Shard` (RoundRobin).
/// Unsent items remain in `buf`.
#[derive(Debug, PartialEq)]
pub struct ShardTryBatchSendError {
    pub shard: usize,
    pub sent: usize,
    pub reason: errors::TrySendBatchError,
}

impl std::fmt::Display for ShardTryBatchSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShardTryBatchSendError {{ shard: {}, sent: {}, reason: {:?} }}",
            self.shard, self.sent, self.reason
        )
    }
}

/// `send_batch` /`send_batch_timeout` error in `Shard` (RoundRobin).
/// Unsent items remain in `buf`.
#[derive(Debug, PartialEq)]
pub struct ShardBatchSendError {
    pub shard: usize,
    pub sent: usize,
    pub reason: errors::SendBatchError,
}

impl std::fmt::Display for ShardBatchSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShardBatchSendError {{ shard: {}, sent: {}, reason: {:?} }}",
            self.shard, self.sent, self.reason
        )
    }
}

/// Error sending async batch to `Shard` (RoundRobin).
/// Unsent items are returned to `buf`.
#[derive(Debug, PartialEq)]
pub struct ShardAsyncBatchSendError {
    pub shard: usize,
    pub sent: usize,
}

impl Display for ShardAsyncBatchSendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardAsyncBatchSendError {{ shard: {}, sent: {} }}",
            self.shard, self.sent
        )
    }
}

// `try_send_batch_keyed` error in `ShardedKey` (ByKey).
/// Unsent items remain in `buf`.
#[derive(Debug, PartialEq)]
pub struct ShardKeyTryBatchSendError {
    pub key: String,
    pub shard: usize,
    pub sent: usize,
    pub reason: errors::TrySendBatchError,
}

impl Display for ShardKeyTryBatchSendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardKeyTryBatchSendError {{ key: {}, shard: {}, sent: {}, reason: {:?} }}",
            self.key, self.shard, self.sent, self.reason
        )
    }
}

/// `send_batch_keyed` /`send_batch_keyed_timeout` error in `ShardedKey` (ByKey).
/// Unsent items remain in `buf`.
#[derive(Debug, PartialEq)]
pub struct ShardKeyBatchSendError {
    pub key: String,
    pub shard: usize,
    pub sent: usize,
    pub reason: errors::SendBatchError,
}

impl Display for ShardKeyBatchSendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardKeyBatchSendError {{ key: {}, shard: {}, sent: {}, reason: {:?} }}",
            self.key, self.shard, self.sent, self.reason
        )
    }
}

/// `send_batch_keyed_async` error in `ShardedKey` (ByKey).
/// Unsent items remain in `buf`.
#[derive(Debug, PartialEq)]
pub struct ShardKeyAsyncBatchSendError {
    pub key: String,
    pub shard: usize,
    pub sent: usize,
}

impl Display for ShardKeyAsyncBatchSendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardKeyAsyncBatchSendError {{ key: {}, shard: {}, sent: {} }}",
            self.key, self.shard, self.sent
        )
    }
}

/// recv_any error all shards are closed.
#[derive(Debug, PartialEq)]
pub struct ShardRecvAnyError {
    /// The number of shards that returned Disconnected.
    pub disconnected_shards: usize,
    pub err: errors::AsyncRecvError,
}

impl Display for ShardRecvAnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ShardRecvAnyError {{ disconnected: {}, err: {:?} }}",
            self.disconnected_shards, self.err
        )
    }
}

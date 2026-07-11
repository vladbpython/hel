use super::sync::SyncList;
use std::sync::atomic::Ordering;

/// Trait for all channel inner state implementations.
/// T: 'static is required for async channels (tasks require 'static bounds)
pub trait InnerChannel<T: Send + 'static, const CAP: usize>: Send + Sync {
    fn push(&self, v: T) -> Result<(), T>;
    fn push_blocking(&self, v: T) -> Result<(), T>;
    fn pop(&self) -> Option<T>;
    fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool);
    fn push_batch(&self, buf: &mut Vec<T>) -> usize;
    fn is_empty(&self) -> bool;
    fn is_tx_closed(&self) -> bool;
    fn is_rx_closed(&self) -> bool;
    fn notify_receivers(&self);
    fn notify_senders(&self);
    fn notify_all_on_tx_close(&self);
    fn notify_all_on_rx_close(&self);
    fn receiver_add(&self, o: Ordering) -> usize;
    fn receiver_sub(&self, o: Ordering) -> usize;
    fn sender_add(&self, o: Ordering) -> usize;
    fn sender_sub(&self, o: Ordering) -> usize;
    fn receiver_waiters(&self) -> &SyncList;
    fn sender_waiters(&self) -> &SyncList;
    fn tx_close(&self);
    fn rx_close(&self);
    #[inline]
    fn yield_before_park(&self) -> bool {
        true
    }
    fn queued(&self) -> usize;
}

// Minimal trait over inner state needed by sender logic.
// Implemented for InnerChannel.
// All methods are statically dispatched zero overhead.

pub trait SenderOps<T: Send + 'static, const CAP: usize>: Send + 'static {
    fn is_rx_closed(&self) -> bool;
    /// Non blocking push returns Err(value) if full.
    fn push(&self, value: T) -> Result<(), T>;
    /// Blocking push — on ARM SeqInner uses fetch_add (no retry).
    /// On Intel / SPSC falls back to push() (CAS).
    /// Only correct for send() without deadline.
    fn push_blocking(&self, value: T) -> Result<(), T>;
    fn push_batch(&self, buf: &mut Vec<T>) -> usize;
    fn notify_receivers(&self);
    fn sender_waiters(&self) -> &SyncList;
    fn sender_add(&self, o: Ordering) -> usize;
    fn sender_sub(&self, o: Ordering) -> usize;
    fn tx_close(&self);
    fn notify_all_on_tx_close(&self);
}

// Blanket impl for any InnerChannel
impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP> + Send + 'static>
    SenderOps<T, CAP> for I
{
    fn is_rx_closed(&self) -> bool {
        self.is_rx_closed()
    }

    fn push(&self, v: T) -> Result<(), T> {
        self.push(v)
    }

    fn push_blocking(&self, v: T) -> Result<(), T> {
        self.push_blocking(v)
    }

    fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        self.push_batch(buf)
    }

    fn notify_receivers(&self) {
        self.notify_receivers()
    }

    fn sender_waiters(&self) -> &SyncList {
        self.sender_waiters()
    }

    fn sender_add(&self, ordering: Ordering) -> usize {
        self.sender_add(ordering)
    }

    fn sender_sub(&self, ordering: Ordering) -> usize {
        self.sender_sub(ordering)
    }

    fn tx_close(&self) {
        self.tx_close()
    }

    fn notify_all_on_tx_close(&self) {
        self.notify_all_on_tx_close()
    }
}

// Minimal trait over inner state needed by receiver logic.
// Implemented for InnerChannel.
// All methods are statically dispatched zero overhead.

pub trait ReceiverOps<T, const CAP: usize>: Send + 'static {
    fn pop(&self) -> Option<T>;
    fn is_tx_closed(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn notify_senders(&self);
    fn receiver_waiters(&self) -> &SyncList;
    fn receiver_add(&self, o: Ordering) -> usize;
    fn receiver_sub(&self, o: Ordering) -> usize;
    fn rx_close(&self);
    fn notify_all_on_rx_close(&self);
    fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool);
    #[inline]
    fn yield_before_park(&self) -> bool {
        true
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP> + Send + 'static>
    ReceiverOps<T, CAP> for I
{
    fn pop(&self) -> Option<T> {
        self.pop()
    }

    fn is_tx_closed(&self) -> bool {
        self.is_tx_closed()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn notify_senders(&self) {
        self.notify_senders()
    }

    fn receiver_waiters(&self) -> &SyncList {
        self.receiver_waiters()
    }

    fn receiver_add(&self, ordering: Ordering) -> usize {
        self.receiver_add(ordering)
    }

    fn receiver_sub(&self, ordering: Ordering) -> usize {
        self.receiver_sub(ordering)
    }

    fn rx_close(&self) {
        self.rx_close()
    }

    fn notify_all_on_rx_close(&self) {
        self.notify_all_on_rx_close()
    }

    fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        self.pop_batch(buf, max)
    }

    #[inline]
    fn yield_before_park(&self) -> bool {
        InnerChannel::yield_before_park(self)
    }
}

/// Marker: inner protocol allows MORE than ONE producer
/// (slot reservation via CAS/fetch_add on tail).
/// `SingleInner` does NOT implement it so `Clone` for `SingleSender`
/// does not exist at the type level: two producers on the SPSC protocol
/// (push without CAS on tail) this is a data race on tail and slots.
/// Any new inner gets a `Clone` for its `Sender` only
/// consciously declaring himself a multi producer.
pub trait MultiProducer {}

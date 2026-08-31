use super::{
    core::{SeqInner, SingleInner},
    errors::{AsyncRecvError, RecvError, TryRecvError},
    helper::deadline_after,
    sync::{AsyncSlot, SyncNode},
    traits::{InnerChannel, MultiConsumer, ReceiverOps},
};
use crate::shim::loom::{park, park_timeout, yield_now};
use futures_core::Stream;
use std::{
    fmt::Debug,
    future::Future,
    hint::spin_loop,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};

const SPIN_COUNT: u32 = 128;

#[inline]
pub fn try_recv<T, const CAP: usize>(inner: &impl ReceiverOps<T, CAP>) -> Result<T, TryRecvError> {
    match inner.pop() {
        Some(v) => {
            inner.notify_senders();
            Ok(v)
        }
        None if inner.is_tx_closed() && inner.is_empty() => Err(TryRecvError::Disconnected),
        None => Err(TryRecvError::Empty),
    }
}

pub fn recv_impl<T, const CAP: usize>(
    inner: &impl ReceiverOps<T, CAP>,
    deadline: Option<Instant>,
) -> Result<T, RecvError> {
    loop {
        match inner.pop() {
            Some(v) => {
                inner.notify_senders();
                return Ok(v);
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                return Err(RecvError::Disconnected);
            }
            None => {}
        }
        if let Some(dl) = deadline
            && dl
                .checked_duration_since(Instant::now())
                .is_none_or(|d| d == Duration::ZERO)
        {
            return Err(RecvError::TimeOut(dl.elapsed()));
        }

        if inner.yield_before_park() {
            // Adaptive spin before parking symmetry to SPIN_COUNT on
            // sender's side. Without it, consumer in the streaming pattern
            // parks on EVERY message: futex wait+wake ~1-3 \u00b5s per
            // circle \u2192 collapse on Linux (17-24% high-severe outliers, rr worse than key).
            // Spin holds the consumer hot while the producer adds the next element.
            for _ in 0..SPIN_COUNT {
                spin_loop();
                if let Some(v) = inner.pop() {
                    inner.notify_senders();
                    return Ok(v);
                }
            }
            yield_now();
        }

        match inner.pop() {
            Some(v) => {
                inner.notify_senders();
                return Ok(v);
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                return Err(RecvError::Disconnected);
            }
            None => {}
        }

        // Sleep phase only after yield didn't help
        let mut node = SyncNode::new_blocking();
        let parked = inner.receiver_waiters().sync_guard(&mut node);
        match inner.pop() {
            Some(v) => {
                drop(parked);
                inner.notify_senders();
                return Ok(v);
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                return Err(RecvError::Disconnected);
            }
            None => {}
        }
        match deadline {
            Some(dl) => park_timeout(dl.saturating_duration_since(Instant::now())),
            None => park(),
        }
    }
}

#[inline]
pub fn batch<T, const CAP: usize>(
    inner: &impl ReceiverOps<T, CAP>,
    buf: &mut Vec<T>,
    max: usize,
) -> (usize, bool) {
    let (n, dc) = inner.pop_batch(buf, max);
    inner.notify_senders_n(n);
    (n, dc)
}

pub fn recv_batch<T, const CAP: usize>(
    inner: &impl ReceiverOps<T, CAP>,
    buf: &mut Vec<T>,
    max: usize,
    deadline: Option<Instant>,
) -> (usize, bool) {
    if max == 0 {
        return (0, inner.is_tx_closed() && inner.is_empty());
    }
    match recv_impl(inner, deadline) {
        Ok(v) => buf.push(v),
        Err(RecvError::Disconnected) => return (0, true),
        Err(_) => return (0, false),
    }
    let (n, dc) = batch(inner, buf, max - 1);
    (1 + n, dc)
}

pub struct GenericRecvFuture<'a, T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> {
    inner: &'a Arc<I>,
    slot: Option<Arc<AsyncSlot>>,
    _t: PhantomData<T>,
}

unsafe impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP> + Send + Sync> Send
    for GenericRecvFuture<'_, T, CAP, I>
{
}

impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP> + Send + Sync> Future
    for GenericRecvFuture<'_, T, CAP, I>
{
    type Output = Result<T, AsyncRecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = this.inner;
        macro_rules! cancel {
            () => {
                if let Some(s) = this.slot.take() {
                    inner.receiver_waiters().cancel_async_slot(&s);
                }
            };
        }
        match inner.pop() {
            Some(v) => {
                cancel!();
                inner.notify_senders();
                return Poll::Ready(Ok(v));
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                cancel!();
                return Poll::Ready(Err(AsyncRecvError::Disconnected));
            }
            None => {}
        }
        match &this.slot {
            None => {
                this.slot = Some(inner.receiver_waiters().push_async_slot(cx.waker().clone()));
            }
            Some(s) if s.in_queue.load(Ordering::Acquire) => {
                s.waker.register(cx.waker());
            }
            Some(_) => {
                this.slot = Some(inner.receiver_waiters().push_async_slot(cx.waker().clone()));
            }
        }
        match inner.pop() {
            Some(v) => {
                cancel!();
                inner.notify_senders();
                Poll::Ready(Ok(v))
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                cancel!();
                Poll::Ready(Err(AsyncRecvError::Disconnected))
            }
            None => Poll::Pending,
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Drop
    for GenericRecvFuture<'_, T, CAP, I>
{
    fn drop(&mut self) {
        if let Some(s) = self.slot.take() {
            self.inner.receiver_waiters().cancel_async_slot(&s);
            self.inner.receiver_waiters().notify_one();
        }
    }
}

/// Generic Stream Receiver
pub struct GenericRecvStream<'a, T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> {
    inner: &'a Arc<I>,
    slot: Option<Arc<AsyncSlot>>,
    _t: PhantomData<T>,
}

unsafe impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP> + Send + Sync> Send
    for GenericRecvStream<'_, T, CAP, I>
{
}

impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Stream
    for GenericRecvStream<'_, T, CAP, I>
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = this.inner;
        macro_rules! cancel {
            () => {
                if let Some(s) = this.slot.take() {
                    inner.receiver_waiters().cancel_async_slot(&s);
                }
            };
        }
        match inner.pop() {
            Some(v) => {
                cancel!();
                inner.notify_senders();
                return Poll::Ready(Some(v));
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                cancel!();
                return Poll::Ready(None);
            }
            None => {}
        }
        match &this.slot {
            None => {
                this.slot = Some(inner.receiver_waiters().push_async_slot(cx.waker().clone()));
            }
            Some(s) if s.in_queue.load(Ordering::Acquire) => {
                s.waker.register(cx.waker());
            }
            Some(_) => {
                this.slot = Some(inner.receiver_waiters().push_async_slot(cx.waker().clone()));
            }
        }
        match inner.pop() {
            Some(v) => {
                cancel!();
                inner.notify_senders();
                Poll::Ready(Some(v))
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                cancel!();
                Poll::Ready(None)
            }
            None => Poll::Pending,
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Drop
    for GenericRecvStream<'_, T, CAP, I>
{
    fn drop(&mut self) {
        if let Some(s) = self.slot.take() {
            self.inner.receiver_waiters().cancel_async_slot(&s);
            self.inner.receiver_waiters().notify_one();
        }
    }
}

// Receiver (MPMC)

pub struct Receiver<
    T: Send + 'static,
    const CAP: usize,
    I: InnerChannel<T, CAP> + 'static = SeqInner<T, CAP>,
> {
    inner: Arc<I>,
    _t: PhantomData<T>,
}

pub type ReceiverFuture<'a, T, const CAP: usize, I> = GenericRecvFuture<'a, T, CAP, I>;
pub type ReceiverStream<'a, T, const CAP: usize, I> = GenericRecvStream<'a, T, CAP, I>;

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Receiver<T, CAP, I> {
    pub fn new(inner: Arc<I>) -> Self {
        Self {
            inner,
            _t: PhantomData,
        }
    }

    pub fn inner_ref(&self) -> &Arc<I> {
        &self.inner
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        try_recv(self.inner.as_ref())
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        recv_impl(self.inner.as_ref(), None)
    }

    pub fn recv_timeout(&self, d: Duration) -> Result<T, RecvError> {
        let start = Instant::now();
        recv_impl(self.inner.as_ref(), Some(deadline_after(d))).map_err(|e| match e {
            RecvError::TimeOut(_) => RecvError::TimeOut(start.elapsed()),
            other => other,
        })
    }

    #[inline]
    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub fn try_recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0, self.inner.is_tx_closed() && self.inner.is_empty());
        }
        let (n, dc) = self.inner.pop_batch(buf, max);
        if n > 0 {
            self.inner.notify_senders_n(n);
            (n, false)
        } else {
            (0, dc)
        }
    }

    /// Deadline too large for an `Instant` (e.g. `Duration::MAX`) is clamped to the farthest representable point - centuries away,
    /// so effectively unbounded, but the call still returns `TimeOut` eventually instead of hanging forever.
    /// Deadline bounds only the wait for the first element, once something arrived, the rest of the batch is collected without
    /// blocking (latency first). The send-side twin `send_batch_timeout` bounds the whole batch instead - the pair is asymmetric on purpose.
    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub fn recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, None)
    }

    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub fn recv_batch_timeout(&self, buf: &mut Vec<T>, max: usize, d: Duration) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, Some(deadline_after(d)))
    }

    pub fn recv_async(&self) -> ReceiverFuture<'_, T, CAP, I> {
        GenericRecvFuture {
            inner: &self.inner,
            slot: None,
            _t: PhantomData,
        }
    }

    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub async fn recv_batch_async(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0, self.inner.is_tx_closed() && self.inner.is_empty());
        }
        match self.recv_async().await {
            Ok(v) => buf.push(v),
            Err(AsyncRecvError::Disconnected) => return (0, true),
        }
        let (n, dc) = batch(self.inner.as_ref(), buf, max - 1);
        (1 + n, dc)
    }

    pub fn iter(&self) -> Iter<'_, T, CAP, I> {
        Iter { r: self }
    }

    pub fn stream(&self) -> ReceiverStream<'_, T, CAP, I> {
        GenericRecvStream {
            inner: &self.inner,
            slot: None,
            _t: PhantomData,
        }
    }

    /// Approximate number of items currently in this shard's queue (tail \u2212 head).
    /// A concurrent snapshot may be off by a few under active producers/consumers.
    /// Cheap (two relaxed atomic loads).
    #[inline]
    pub fn queued(&self) -> usize {
        self.inner.queued()
    }

    /// Whether the queue appears empty right now (approximate see `queued`).
    #[inline]
    pub fn is_queued_empty(&self) -> bool {
        self.queued() == 0
    }

    /// Fixed capacity of this shard.
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP> + MultiConsumer> Clone
    for Receiver<T, CAP, I>
{
    fn clone(&self) -> Self {
        self.inner.receiver_add(Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
            _t: PhantomData,
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Drop for Receiver<T, CAP, I> {
    fn drop(&mut self) {
        if self.inner.receiver_sub(Ordering::AcqRel) == 1 {
            self.inner.rx_close();
            self.inner.notify_all_on_rx_close();
        }
    }
}

/// MPMC Iterators
pub struct Iter<'a, T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP> + 'static> {
    r: &'a Receiver<T, CAP, I>,
}
impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Iterator
    for Iter<'_, T, CAP, I>
{
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.r.recv().ok()
    }
}

pub struct IntoIter<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP> + 'static> {
    r: Receiver<T, CAP, I>,
}
impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Iterator
    for IntoIter<T, CAP, I>
{
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.r.recv().ok()
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> IntoIterator
    for Receiver<T, CAP, I>
{
    type Item = T;
    type IntoIter = IntoIter<T, CAP, I>;
    fn into_iter(self) -> IntoIter<T, CAP, I> {
        IntoIter { r: self }
    }
}

impl<'a, T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> IntoIterator
    for &'a Receiver<T, CAP, I>
{
    type Item = T;
    type IntoIter = Iter<'a, T, CAP, I>;
    fn into_iter(self) -> Iter<'a, T, CAP, I> {
        Iter { r: self }
    }
}

// SingleReceiver (SPSC)
// Separate struct no Clone (SPSC invariant: exactly 1 receiver)

pub struct SingleReceiver<T, const CAP: usize> {
    pub(crate) inner: Arc<SingleInner<T, CAP>>,
}

pub type SingleRecvFuture<'a, T, const CAP: usize> =
    GenericRecvFuture<'a, T, CAP, SingleInner<T, CAP>>;

pub type SingleRecvStream<'a, T, const CAP: usize> =
    GenericRecvStream<'a, T, CAP, SingleInner<T, CAP>>;

impl<T: Send + 'static, const CAP: usize> SingleReceiver<T, CAP> {
    pub fn new(inner: Arc<SingleInner<T, CAP>>) -> Self {
        Self { inner }
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        try_recv(self.inner.as_ref())
    }

    pub fn recv(&mut self) -> Result<T, RecvError> {
        recv_impl(self.inner.as_ref(), None)
    }

    pub fn recv_timeout(&mut self, d: Duration) -> Result<T, RecvError> {
        let start = Instant::now();
        recv_impl(self.inner.as_ref(), Some(deadline_after(d))).map_err(|e| match e {
            RecvError::TimeOut(_) => RecvError::TimeOut(start.elapsed()),
            other => other,
        })
    }

    /// Non blocking batch receive
    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub fn try_recv_batch(&mut self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            // Still report disconnect (audit 2, N13).
            return (0, self.inner.is_tx_closed() && self.inner.is_empty());
        }
        let (n, dc) = self.inner.pop_batch(buf, max);
        if n > 0 {
            crate::internal_channel::traits::InnerChannel::notify_senders_n(self.inner.as_ref(), n);
            (n, false)
        } else {
            (0, dc)
        }
    }

    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub fn recv_batch(&mut self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, None)
    }

    /// Deadline too large for an `Instant` (e.g. `Duration::MAX`) is clamped to the farthest representable point -centuries away
    /// so effectively unbounded, but the call still returns `TimeOut` eventually instead of hanging forever.
    /// Deadline bounds only the wait for the first element; the rest is collected without blocking.
    /// See the MPMC for the rationale.
    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub fn recv_batch_timeout(
        &mut self,
        buf: &mut Vec<T>,
        max: usize,
        d: Duration,
    ) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, Some(deadline_after(d)))
    }

    pub fn recv_async(&mut self) -> SingleRecvFuture<'_, T, CAP> {
        GenericRecvFuture {
            inner: &self.inner,
            slot: None,
            _t: PhantomData,
        }
    }

    #[must_use = "the bool is the disconnect flag - dropping it loses the only exit condition of a drain loop"]
    pub async fn recv_batch_async(&mut self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0, self.inner.is_tx_closed() && self.inner.is_empty());
        }
        match self.recv_async().await {
            Ok(v) => buf.push(v),
            Err(AsyncRecvError::Disconnected) => return (0, true),
        }
        let (n, dc) = batch(self.inner.as_ref(), buf, max - 1);
        (1 + n, dc)
    }

    /// Queue depth right now (approximate under concurrency).
    #[inline]
    pub fn queued(&self) -> usize {
        self.inner.queued()
    }

    /// Whether the queue appears empty right now (approximate, see `queued`).
    #[inline]
    pub fn is_queued_empty(&self) -> bool {
        self.queued() == 0
    }

    /// Fixed capacity of this ring.
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    pub fn iter(&mut self) -> SingleIter<'_, T, CAP> {
        SingleIter { r: self }
    }

    pub fn stream(&mut self) -> SingleRecvStream<'_, T, CAP> {
        GenericRecvStream {
            inner: &self.inner,
            slot: None,
            _t: PhantomData,
        }
    }
}

impl<T, const CAP: usize> Drop for SingleReceiver<T, CAP> {
    fn drop(&mut self) {
        self.inner.rx_close();
        self.inner.notify_all_on_rx_close();
    }
}

// SPSC Iterators

pub struct SingleIter<'a, T, const CAP: usize> {
    r: &'a mut SingleReceiver<T, CAP>,
}
impl<T: Send + 'static, const CAP: usize> Iterator for SingleIter<'_, T, CAP> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.r.recv().ok()
    }
}

pub struct SingleIntoIter<T, const CAP: usize> {
    r: SingleReceiver<T, CAP>,
}
impl<T: Send + 'static, const CAP: usize> Iterator for SingleIntoIter<T, CAP> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.r.recv().ok()
    }
}

impl<T: Send + 'static, const CAP: usize> IntoIterator for SingleReceiver<T, CAP> {
    type Item = T;
    type IntoIter = SingleIntoIter<T, CAP>;
    fn into_iter(self) -> SingleIntoIter<T, CAP> {
        SingleIntoIter { r: self }
    }
}

// Iterating consumes messages, so it borrows the SPSC receiver exclusively:
// the single consumer invariant is the same one `&mut self` guards on the recv methods.
impl<'a, T: Send + 'static, const CAP: usize> IntoIterator for &'a mut SingleReceiver<T, CAP> {
    type Item = T;
    type IntoIter = SingleIter<'a, T, CAP>;
    fn into_iter(self) -> SingleIter<'a, T, CAP> {
        SingleIter { r: self }
    }
}

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Debug for Receiver<T, CAP, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}
impl<T, const CAP: usize> std::fmt::Debug for SingleReceiver<T, CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleReceiver").finish_non_exhaustive()
    }
}
impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Debug
    for GenericRecvFuture<'_, T, CAP, I>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericRecvFuture").finish_non_exhaustive()
    }
}
impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Debug
    for GenericRecvStream<'_, T, CAP, I>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericRecvStream").finish_non_exhaustive()
    }
}
impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Debug for Iter<'_, T, CAP, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter").finish_non_exhaustive()
    }
}
impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Debug for IntoIter<T, CAP, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntoIter").finish_non_exhaustive()
    }
}
impl<T, const CAP: usize> Debug for SingleIter<'_, T, CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleIter").finish_non_exhaustive()
    }
}
impl<T, const CAP: usize> Debug for SingleIntoIter<T, CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleIntoIter").finish_non_exhaustive()
    }
}

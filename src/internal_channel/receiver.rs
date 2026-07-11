use super::{
    core::{SeqInner, SingleInner},
    errors::{AsyncRecvError, RecvError, TryRecvError},
    sync::{AsyncSlot, SyncList, SyncNode},
    traits::{InnerChannel, ReceiverOps},
};
use std::{
    future::Future,
    hint::spin_loop,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    thread::{park, park_timeout, yield_now},
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
            // parks on EVERY message: futex wait+wake ~1-3 µs per
            // circle → collapse on Linux (17-24% high-severe outliers, rr worse than key).
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
        let ptr = &mut node as *mut SyncNode;
        inner.receiver_waiters().push_blocking(ptr);
        match inner.pop() {
            Some(v) => {
                inner.receiver_waiters().remove(ptr);
                inner.notify_senders();
                return Ok(v);
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                inner.receiver_waiters().remove(ptr);
                return Err(RecvError::Disconnected);
            }
            None => {}
        }
        match deadline {
            Some(dl) => park_timeout(dl.saturating_duration_since(Instant::now())),
            None => park(),
        }
        inner.receiver_waiters().remove(ptr);
    }
}

#[inline]
pub fn batch<T, const CAP: usize>(
    inner: &impl ReceiverOps<T, CAP>,
    buf: &mut Vec<T>,
    max: usize,
) -> (usize, bool) {
    let (n, dc) = inner.pop_batch(buf, max);
    if n > 0 {
        inner.notify_senders();
    }
    (n, dc)
}

pub fn recv_batch<T, const CAP: usize>(
    inner: &impl ReceiverOps<T, CAP>,
    buf: &mut Vec<T>,
    max: usize,
    deadline: Option<Instant>,
) -> (usize, bool) {
    if max == 0 {
        return (0, false);
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

unsafe impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Send
    for GenericRecvFuture<'_, T, CAP, I>
{
}

impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Future
    for GenericRecvFuture<'_, T, CAP, I>
{
    type Output = Result<T, AsyncRecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = this.inner;
        macro_rules! cancel {
            () => {
                if let Some(s) = this.slot.take() {
                    SyncList::cancel_async_slot(&s);
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
            SyncList::cancel_async_slot(&s);
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

unsafe impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> Send
    for GenericRecvStream<'_, T, CAP, I>
{
}

impl<T: Send + 'static, const CAP: usize, I: ReceiverOps<T, CAP>> futures::Stream
    for GenericRecvStream<'_, T, CAP, I>
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = this.inner;
        macro_rules! cancel {
            () => {
                if let Some(s) = this.slot.take() {
                    SyncList::cancel_async_slot(&s);
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
            SyncList::cancel_async_slot(&s);
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
        recv_impl(self.inner.as_ref(), Some(Instant::now() + d))
    }

    #[inline]
    pub fn try_recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0, false);
        }
        let (n, dc) = self.inner.pop_batch(buf, max);
        if n > 0 {
            self.inner.notify_senders();
            (n, false)
        } else {
            (0, dc)
        }
    }

    pub fn recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, None)
    }

    pub fn recv_batch_timeout(&self, buf: &mut Vec<T>, max: usize, d: Duration) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, Some(Instant::now() + d))
    }

    pub fn recv_async(&self) -> ReceiverFuture<'_, T, CAP, I> {
        GenericRecvFuture {
            inner: &self.inner,
            slot: None,
            _t: PhantomData,
        }
    }

    pub async fn recv_batch_async(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0, false);
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

    /// Approximate number of items currently in this shard's queue (tail − head).
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

impl<T: Send + 'static, const CAP: usize, I: InnerChannel<T, CAP>> Clone for Receiver<T, CAP, I> {
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

pub type SignleRecvStream<'a, T, const CAP: usize> =
    GenericRecvStream<'a, T, CAP, SingleInner<T, CAP>>;

impl<T: Send + 'static, const CAP: usize> SingleReceiver<T, CAP> {
    pub fn new(inner: Arc<SingleInner<T, CAP>>) -> Self {
        Self { inner }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        try_recv(self.inner.as_ref())
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        recv_impl(self.inner.as_ref(), None)
    }

    pub fn recv_timeout(&self, d: Duration) -> Result<T, RecvError> {
        recv_impl(self.inner.as_ref(), Some(Instant::now() + d))
    }

    pub fn recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, None)
    }

    pub fn recv_batch_timeout(&self, buf: &mut Vec<T>, max: usize, d: Duration) -> (usize, bool) {
        recv_batch(self.inner.as_ref(), buf, max, Some(Instant::now() + d))
    }

    pub fn recv_async(&self) -> SingleRecvFuture<'_, T, CAP> {
        GenericRecvFuture {
            inner: &self.inner,
            slot: None,
            _t: PhantomData,
        }
    }

    pub async fn recv_batch_async(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0, false);
        }
        match self.recv_async().await {
            Ok(v) => buf.push(v),
            Err(AsyncRecvError::Disconnected) => return (0, true),
        }
        let (n, dc) = batch(self.inner.as_ref(), buf, max - 1);
        (1 + n, dc)
    }

    pub fn iter(&self) -> SingleIter<'_, T, CAP> {
        SingleIter { r: self }
    }

    pub fn stream(&self) -> SignleRecvStream<'_, T, CAP> {
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
    r: &'a SingleReceiver<T, CAP>,
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

impl<'a, T: Send + 'static, const CAP: usize> IntoIterator for &'a SingleReceiver<T, CAP> {
    type Item = T;
    type IntoIter = SingleIter<'a, T, CAP>;
    fn into_iter(self) -> SingleIter<'a, T, CAP> {
        SingleIter { r: self }
    }
}

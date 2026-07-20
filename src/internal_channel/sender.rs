use super::{
    core::{SENDER_SPIN_COUNT, SeqInner, SingleInner},
    errors::{
        AsyncSendError, AsyncSendRefError, BatchSendError, SendBatchError, SendError,
        TrySendBatchError, TrySendError,
    },
    sync::{AsyncSlot, SyncList, SyncNode},
    traits::{MultiProducer, SenderOps},
};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    thread::{park, park_timeout},
    time::{Duration, Instant},
};

// Unify methods

#[inline]
pub fn try_send<T: Send + 'static, const CAP: usize>(
    inner: &impl SenderOps<T, CAP>,
    value: T,
) -> Result<(), TrySendError<T>> {
    if inner.is_rx_closed() {
        return Err(TrySendError::Disconnected(value));
    }
    match inner.push(value) {
        Ok(()) => {
            inner.notify_receivers();
            Ok(())
        }
        Err(v) => Err(TrySendError::Full(v)),
    }
}

pub fn send_impl<T: Send + 'static, const CAP: usize>(
    inner: &impl SenderOps<T, CAP>,
    mut value: T,
    deadline: Option<Instant>,
) -> Result<(), SendError<T>> {
    // No deadline: try push_blocking first.
    // ARM SeqInner: fetch_add blocks internally → Ok or Err(rx_closed) only.
    // Intel / SingleInner: push_blocking == push (CAS) → may return Err(Full) → fall through.
    if deadline.is_none() {
        if inner.is_rx_closed() {
            return Err(SendError::Disconnected(value));
        }
        match inner.push_blocking(value) {
            Ok(()) => {
                inner.notify_receivers();
                return Ok(());
            }
            Err(v) => {
                if inner.is_rx_closed() {
                    return Err(SendError::Disconnected(v));
                }
                value = v; // Err(Full) on Intel/SPSC fall through to spin+park
            }
        }
    }

    // Deadline path OR Intel/SPSC buffer full fallback.
    loop {
        if inner.is_rx_closed() {
            return Err(SendError::Disconnected(value));
        }
        match inner.push(value) {
            Ok(()) => {
                inner.notify_receivers();
                return Ok(());
            }
            Err(v) => value = v,
        }

        if let Some(dl) = deadline {
            match dl.checked_duration_since(Instant::now()) {
                Some(d) if d > Duration::ZERO => {}
                _ => return Err(SendError::TimeOut((value, dl.elapsed()))),
            }
        }

        // Adaptive spin: ~64ns wait without park().
        // On multi core: consumer can pop() while we spin.
        // On single core: pause instruction reduces power without blocking the core.
        for _ in 0..SENDER_SPIN_COUNT {
            std::hint::spin_loop();
            if inner.is_rx_closed() {
                return Err(SendError::Disconnected(value));
            }
            match inner.push(value) {
                Ok(()) => {
                    inner.notify_receivers();
                    return Ok(());
                }
                Err(v) => value = v,
            }
        }

        let mut node = SyncNode::new_blocking();
        let node_ptr = &mut node as *mut SyncNode;
        inner.sender_waiters().push_blocking(node_ptr);

        if inner.is_rx_closed() {
            inner.sender_waiters().remove(node_ptr);
            return Err(SendError::Disconnected(value));
        }
        match inner.push(value) {
            Ok(()) => {
                inner.sender_waiters().remove(node_ptr);
                inner.notify_receivers();
                return Ok(());
            }
            Err(v) => value = v,
        }

        match deadline {
            Some(dl) => park_timeout(dl.saturating_duration_since(Instant::now())),
            None => park(),
        }
        inner.sender_waiters().remove(node_ptr);
    }
}

pub fn send_batch_impl<T: Send + 'static, const CAP: usize>(
    inner: &impl SenderOps<T, CAP>,
    buf: &mut Vec<T>,
    deadline: Option<Instant>,
) -> Result<usize, BatchSendError<SendBatchError>> {
    if inner.is_rx_closed() {
        return Err(BatchSendError {
            sent: 0,
            err: SendBatchError::Disconnected,
        });
    }
    // Fast path: push entire batch if space available.
    let fast = inner.push_batch(buf);
    if fast > 0 {
        inner.notify_receivers();
    }
    if buf.is_empty() {
        return Ok(fast);
    }

    // Buffer full blocking fallback per item.
    // push_batch left buf in reversed order, restore FIFO.
    buf.reverse();
    let mut sent = fast;
    while let Some(value) = buf.pop() {
        match send_impl(inner, value, deadline) {
            Ok(()) => sent += 1,
            Err(SendError::Disconnected(v)) => {
                buf.push(v);
                buf.reverse();
                return Err(BatchSendError {
                    sent,
                    err: SendBatchError::Disconnected,
                });
            }
            Err(SendError::TimeOut((v, _))) => {
                buf.push(v);
                buf.reverse();
                return Err(BatchSendError {
                    sent,
                    err: SendBatchError::TimeOut,
                });
            }
        }
    }
    Ok(sent)
}

#[inline]
pub fn try_send_batch<T: Send + 'static, const CAP: usize>(
    inner: &impl SenderOps<T, CAP>,
    buf: &mut Vec<T>,
) -> Result<usize, BatchSendError<TrySendBatchError>> {
    if inner.is_rx_closed() {
        return Err(BatchSendError {
            sent: 0,
            err: TrySendBatchError::Disconnected,
        });
    }
    let sent = inner.push_batch(buf);
    if sent > 0 {
        inner.notify_receivers();
    }
    if buf.is_empty() {
        Ok(sent)
    } else {
        Err(BatchSendError {
            sent,
            err: TrySendBatchError::Full,
        })
    }
}

/// Removes the slot from the wait queue. The only place that does `slot.take()`.
#[inline]
fn cancel_slot(list: &SyncList, slot: &mut Option<Arc<AsyncSlot>>) {
    if let Some(s) = slot.take() {
        list.cancel_async_slot(&s);
    }
}

// `Ready(Ok(()))` the item is in the channel. `value` is now `None`.
// `Ready(Err(v))` the receiver is closed. `value` is now `None`; take the item from the return.
// `Pending` no room yet. The item is back in `value`, waiting for the next poll.
// That last line is the whole point. On `Pending` the item lives in the caller's `Option`,
// not inside the future, so dropping the future does not take it along.
// That is how `send_batch_async` survives cancellation, and it is why `send_async`,
// which owns its item, cannot.
#[inline]
fn poll_send<T, I, const CAP: usize>(
    inner: &Arc<I>,
    value: &mut Option<T>,
    slot: &mut Option<Arc<AsyncSlot>>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), T>>
where
    T: Send + 'static,
    I: SenderOps<T, CAP>,
{
    let mut v = match value.take() {
        Some(v) => v,
        None => {
            // Poll after completion (both Ready arms consume `value`).
            // This cannot lose data: if the future ended with Ready(Err(v)),
            // the caller already received the item inside that error, and the
            // disconnect was already reported. Answering Ok(()) here is a fused,
            // idempotent "this future is finished" - never Pending,
            // so no task can park forever on a completed future.
            cancel_slot(inner.sender_waiters(), slot);
            return Poll::Ready(Ok(()));
        }
    };

    // Fast path
    if inner.is_rx_closed() {
        cancel_slot(inner.sender_waiters(), slot);
        return Poll::Ready(Err(v));
    }
    match inner.push(v) {
        Ok(()) => {
            cancel_slot(inner.sender_waiters(), slot);
            inner.notify_receivers();
            return Poll::Ready(Ok(()));
        }
        Err(back) => v = back,
    }

    // Register or update waker
    match slot {
        None => *slot = Some(inner.sender_waiters().push_async_slot(cx.waker().clone())),
        Some(s) if s.in_queue.load(Ordering::Acquire) => s.waker.register(cx.waker()),
        Some(_) => *slot = Some(inner.sender_waiters().push_async_slot(cx.waker().clone())),
    }

    // Double check after registering waker
    if inner.is_rx_closed() {
        cancel_slot(inner.sender_waiters(), slot);
        return Poll::Ready(Err(v));
    }
    match inner.push(v) {
        Ok(()) => {
            cancel_slot(inner.sender_waiters(), slot);
            inner.notify_receivers();
            Poll::Ready(Ok(()))
        }
        Err(back) => {
            *value = Some(back);
            Poll::Pending
        }
    }
}

// Shared drop tail: release the slot and, if we are leaving without having sent,
// hand to the next waiting sender.
fn drop_send_state<T, I, const CAP: usize>(
    inner: &Arc<I>,
    value: &Option<T>,
    slot: &mut Option<Arc<AsyncSlot>>,
) where
    T: Send + 'static,
    I: SenderOps<T, CAP>,
{
    let had_slot = slot.is_some();
    cancel_slot(inner.sender_waiters(), slot);
    if had_slot && value.is_some() {
        inner.sender_waiters().notify_one();
    }
}

pub struct GenericSendFuture<'a, T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> {
    inner: &'a Arc<I>,
    value: Option<T>,
    slot: Option<Arc<AsyncSlot>>,
    _t: PhantomData<T>,
}

unsafe impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Send
    for GenericSendFuture<'_, T, CAP, I>
{
}

impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Future
    for GenericSendFuture<'_, T, CAP, I>
{
    type Output = Result<(), AsyncSendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        match poll_send::<T, I, CAP>(this.inner, &mut this.value, &mut this.slot, cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(v)) => Poll::Ready(Err(AsyncSendError::Disconnected(v))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Drop
    for GenericSendFuture<'_, T, CAP, I>
{
    fn drop(&mut self) {
        drop_send_state::<T, I, CAP>(self.inner, &self.value, &mut self.slot);
    }
}

// SendPending like GenericSendFuture, but the value is borrowed from the
// caller's frame, so cancellation cannot swallow it. Private: the only user is `send_batch_async`.
struct SendPending<'a, T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> {
    inner: &'a Arc<I>,
    value: &'a mut Option<T>,
    slot: Option<Arc<AsyncSlot>>,
}

// Same reason as GenericSendFuture: SenderOps does not require Sync, which
// `&Arc<I>: Send` would otherwise want.
unsafe impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Send
    for SendPending<'_, T, CAP, I>
{
}

impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Future
    for SendPending<'_, T, CAP, I>
{
    /// `Ok(())` -> sent, `*value == None`.
    /// `Err(())` -> disconnected, `*value == Some(v)`.
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        match poll_send::<T, I, CAP>(this.inner, this.value, &mut this.slot, cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(v)) => {
                // Put it back for the caller: `Restore::drop` returns it to `buf`.
                *this.value = Some(v);
                Poll::Ready(Err(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Drop
    for SendPending<'_, T, CAP, I>
{
    fn drop(&mut self) {
        drop_send_state::<T, I, CAP>(self.inner, self.value, &mut self.slot);
    }
}

// Keeps `buf` reversed while alive, so `pop()` yields the FIFO head in O(1).
// `Drop` restores the original order and puts back the item that was in flight.
// Runs on every exit: success, disconnect, panic, and cancellation at `.await`.
struct Restore<'a, T> {
    buf: &'a mut Vec<T>,
    pending: Option<T>,
}

impl<'a, T> Restore<'a, T> {
    // Reverses `buf` and takes it under guard for the guard's lifetime.
    fn new(buf: &'a mut Vec<T>) -> Self {
        buf.reverse();
        Self { buf, pending: None }
    }
}

impl<T> Drop for Restore<'_, T> {
    fn drop(&mut self) {
        if let Some(v) = self.pending.take() {
            self.buf.push(v); // into the reversed vec = FIFO head
        }
        self.buf.reverse();
    }
}

// Sender (MPMC)

pub struct Sender<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP> = SeqInner<T, CAP>> {
    inner: Arc<I>,
    _t: PhantomData<T>,
}

/// Async send future for MPMC channel.
pub type SenderFuture<'a, T, const CAP: usize, I> = GenericSendFuture<'a, T, CAP, I>;

/// SPSC sender has exactly one producer, `Clone` is missing at the type level.
pub type SingleSender<T, const CAP: usize> = Sender<T, CAP, SingleInner<T, CAP>>;

impl<T: Send, const CAP: usize, I: SenderOps<T, CAP>> Sender<T, CAP, I> {
    pub fn new(inner: Arc<I>) -> Self {
        Self {
            inner,
            _t: PhantomData,
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        try_send(self.inner.as_ref(), value)
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        send_impl(self.inner.as_ref(), value, None)
    }

    pub fn send_timeout(&self, value: T, d: Duration) -> Result<(), SendError<T>> {
        send_impl(self.inner.as_ref(), value, Some(Instant::now() + d))
    }

    /// Non blocking batch send: fast path only, no blocking fallback.
    /// Returns number sent. Unsent items remain in buf.
    pub fn try_send_batch(
        &self,
        buf: &mut Vec<T>,
    ) -> Result<usize, BatchSendError<TrySendBatchError>> {
        try_send_batch(self.inner.as_ref(), buf)
    }

    pub fn send_batch(&self, buf: &mut Vec<T>) -> Result<usize, BatchSendError<SendBatchError>> {
        send_batch_impl(self.inner.as_ref(), buf, None)
    }

    pub fn send_batch_timeout(
        &self,
        buf: &mut Vec<T>,
        d: Duration,
    ) -> Result<usize, BatchSendError<SendBatchError>> {
        send_batch_impl(self.inner.as_ref(), buf, Some(Instant::now() + d))
    }

    pub fn send_async(&self, value: T) -> SenderFuture<'_, T, CAP, I> {
        GenericSendFuture {
            inner: &self.inner,
            value: Some(value),
            slot: None,
            _t: PhantomData,
        }
    }

    /// Asynchronous sending without losing the value when canceled.
    /// The value lives in a `slot` in the caller's frame; future only borrows it.
    /// Completely cancel safe **without losing the value**.
    /// Invariant, after any outcome except `Ok())`, the value lies in `slot`:
    /// - `Ok(())` -> `*slot == None`, the value is published in the channel;
    /// - `Err(Disconnected)` -> `*slot == Some(v)`, recipients are closed;
    /// - cancellation (drop pending) -> `*slot == Some(v)`, there was no sending.
    ///
    /// An empty `slot` on input is interpreted as “nothing to send” and
    /// `Ok(())` ends idempotently (fused semantics).
    ///
    /// Future borrows `self` and `slot`, so is not `'static`
    /// and cannot be passed to `tokio::spawn` this is conscious
    /// trade off for the sake of zero overhead relative to `send_async`:
    /// the hot path is compiled into the same `poll_send`.
    ///
    /// # Example: `select!` without loss
    ///
    /// ```ignore
    /// let mut slot = Some(order);
    /// tokio::select! {
    ///     r = tx.send_ref_async(&mut slot) => match r {
    ///     Ok(()) => { /*sent, slot == None */}
    ///     Err(SendRefError::Disconnected) => {
    ///         let order = slot.take().unwrap(); //value is integer
    ///         }
    ///     },
    ///     _ = shutdown.recv() => {
    ///         //lost the race: slot is still Some(order)
    ///    }
    /// }
    /// ```
    ///
    pub async fn send_ref_async(&self, slot: &mut Option<T>) -> Result<(), AsyncSendRefError> {
        SendPending {
            inner: &self.inner,
            value: slot,
            slot: None,
        }
        .await
        .map_err(|()| AsyncSendRefError::Disconnected)
    }

    // batch methods of ShardKey / ShardGroup / ShardRoundRobin need it to be cancel safe.
    // Not part of the public surface.
    pub(crate) async fn send_async_from(&self, value: &mut Option<T>) -> Result<(), ()> {
        self.send_ref_async(value).await.map_err(|_| ())
    }

    pub async fn send_batch_async(&self, buf: &mut Vec<T>) -> usize {
        let fast = self.inner.push_batch(buf); // one publication per batch
        if fast > 0 {
            self.inner.notify_receivers();
        }
        if buf.is_empty() {
            return fast;
        } // the entire buffer is gone exit without a loop
        // channel is full -> old element wise path as fallback
        let mut g = Restore::new(buf); // reverses inside
        let mut sent = fast;

        while let Some(value) = g.buf.pop() {
            g.pending = Some(value);
            let done = SendPending {
                inner: &self.inner,
                value: &mut g.pending,
                slot: None,
            }
            .await;
            // The future is dropped here. On cancellation we never reach this
            // point, but `g.pending` already holds the value, so `Restore::drop`
            // puts it back into `buf`.
            match done {
                Ok(()) => sent += 1, // pending == None
                Err(()) => break,    // disconnected; the value is in pending
            }
        }
        sent
    }
}

// SingleSender needs ref_inner helper — add via inherent impl on the type alias
impl<T: Send, const CAP: usize> Sender<T, CAP, SingleInner<T, CAP>> {
    pub fn ref_inner(&self) -> &Arc<SingleInner<T, CAP>> {
        &self.inner
    }
}

// Clone exists ONLY for multi-producer inners (SeqInner).
// SingleSender (SPSC) does not implement Clone type level guarantee:
// a clone of an SPSC sender would mean two producers on a protocol without CAS → UB.
impl<T: Send, const CAP: usize, I: SenderOps<T, CAP> + MultiProducer> Clone for Sender<T, CAP, I> {
    fn clone(&self) -> Self {
        self.inner.sender_add(Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
            _t: PhantomData,
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Drop for Sender<T, CAP, I> {
    fn drop(&mut self) {
        if self.inner.sender_sub(Ordering::AcqRel) == 1 {
            self.inner.tx_close();
            self.inner.notify_all_on_tx_close();
        }
    }
}

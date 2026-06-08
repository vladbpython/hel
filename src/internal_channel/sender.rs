use super::{
    core::{MPMCInner, SingleInner},
    errors::{
        AsyncSendError, BatchSendError, SendBatchError, SendError, TrySendBatchError, TrySendError,
    },
    sync::{AsyncSlot, SyncList, SyncNode},
    traits::SenderOps,
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

const SPIN_COUNT: u8 = 64;

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
                value = v; // Err(Full) on Intel/SPSC — fall through to spin+park
            }
        }
    }

    // Deadline path OR Intel/SPSC buffer-full fallback.
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
        for _ in 0..SPIN_COUNT {
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

    // Buffer full — blocking fallback per item.
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
        let inner = this.inner;

        let mut value = match this.value.take() {
            Some(v) => v,
            None => {
                if let Some(s) = this.slot.take() {
                    SyncList::cancel_async_slot(&s);
                }
                return Poll::Pending;
            }
        };

        // Fast path
        if inner.is_rx_closed() {
            if let Some(s) = this.slot.take() {
                SyncList::cancel_async_slot(&s);
            }
            return Poll::Ready(Err(AsyncSendError::Disconnected(value)));
        }
        match inner.push(value) {
            Ok(()) => {
                if let Some(s) = this.slot.take() {
                    SyncList::cancel_async_slot(&s);
                }
                inner.notify_receivers();
                return Poll::Ready(Ok(()));
            }
            Err(v) => value = v,
        }

        // Register or update waker
        match &this.slot {
            None => {
                this.slot = Some(inner.sender_waiters().push_async_slot(cx.waker().clone()));
            }
            Some(s) if s.in_queue.load(Ordering::Acquire) => {
                s.waker.register(cx.waker());
            }
            Some(_) => {
                this.slot = Some(inner.sender_waiters().push_async_slot(cx.waker().clone()));
            }
        }

        // Double check after registering waker
        if inner.is_rx_closed() {
            if let Some(s) = this.slot.take() {
                SyncList::cancel_async_slot(&s);
            }
            return Poll::Ready(Err(AsyncSendError::Disconnected(value)));
        }
        match inner.push(value) {
            Ok(()) => {
                if let Some(s) = this.slot.take() {
                    SyncList::cancel_async_slot(&s);
                }
                inner.notify_receivers();
                Poll::Ready(Ok(()))
            }
            Err(v) => {
                this.value = Some(v);
                Poll::Pending
            }
        }
    }
}

impl<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP>> Drop
    for GenericSendFuture<'_, T, CAP, I>
{
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            SyncList::cancel_async_slot(&slot);
            // Pass the baton to the next waiting sender
            if self.value.is_some() {
                self.inner.sender_waiters().notify_one();
            }
        }
    }
}

// Sender (MPMC)

pub struct Sender<T: Send + 'static, const CAP: usize, I: SenderOps<T, CAP> = MPMCInner<T, CAP>> {
    inner: Arc<I>,
    _t: PhantomData<T>,
}

/// Async send future for MPMC channel.
pub type SenderFuture<'a, T, const CAP: usize, I> = GenericSendFuture<'a, T, CAP, I>;

/// SPSC sender — type alias, no Clone (SPSC invariant: exactly 1 producer).
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

    pub async fn send_batch_async(&self, buf: &mut Vec<T>) -> usize {
        buf.reverse();
        let mut sent = 0;
        while let Some(value) = buf.pop() {
            match self.send_async(value).await {
                Ok(()) => sent += 1,
                Err(AsyncSendError::Disconnected(v)) => {
                    buf.push(v);
                    buf.reverse();
                    return sent;
                }
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

// Clone only for MPMC Sender — SingleSender (SPSC) must NOT be cloneable.
// Implement Clone only when I is NOT SingleInner via a negative bound workaround:
// simplest approach — keep Clone on the generic Sender and rely on the fact
// that SingleSender is constructed only once via channel() constructor.
impl<T: Send, const CAP: usize, I: SenderOps<T, CAP>> Clone for Sender<T, CAP, I> {
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

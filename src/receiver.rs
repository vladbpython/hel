use crate::core::SingleInner;

use super::{
    errors::{RecvError,AsyncRecvError},
    result::{ResultReceiver,ResultAsyncReceiver},
    core::Inner,
    sync::SyncNode,
};
use std::{
    future::Future, 
    pin::Pin, 
    sync::{
        Arc,
        atomic::Ordering,
    }, 
    task::{Context,Poll}, 
    time::{Duration,Instant},
    thread::{park,park_timeout}
};

// Может быть как Single Consumer так и Milto Consumer
pub struct Receiver<T,const CAP: usize> {
    inner: Arc<Inner<T,CAP>>
}

impl<T,const CAP: usize>  Receiver<T,CAP> {
    
    pub (crate) fn new(inner: Arc<Inner<T,CAP>>) -> Self {
        Self { inner }
    }

    pub fn try_recv(&self) -> ResultReceiver<T> {
        match self.inner.pop() {
            Some(v) => {
                self.inner.notify_senders();
                Ok(v)
            },
            None => {
                if self.inner.is_tx_closed() && self.inner.is_empty() {
                    Err(RecvError::Disconnected)
                } else {
                    Err(RecvError::Empty)
                }
            }
        }
    }

    fn recv_impl(&self, deadline: Option<Instant>) -> ResultReceiver<T> 
    {
        loop {
            match self.inner.pop() {
                Some(v) => {
                    self.inner.notify_senders();
                    return Ok(v)
                },
                None if self.inner.is_tx_closed() && self.inner.is_empty() => {
                    return Err(RecvError::Disconnected);
                },
                None => {},
            }

            if let Some(dl) = deadline {
                match dl.checked_duration_since(Instant::now()) {
                    Some(d) if d > Duration::ZERO => {},
                    _ => return Err(RecvError::TimeOut(dl.elapsed()))
                }
            }


            let mut node = SyncNode::new_blocking();
            let node_ptr = &mut node as *mut SyncNode;
            self.inner.receiver_wait_counter_add(Ordering::SeqCst);
            self.inner.receiver_waiters().push_blocking(node_ptr);

            // Double-check после регистрации
            match self.inner.pop() {
                Some(v) => {
                    self.inner.receiver_waiters().remove(node_ptr);
                    self.inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    self.inner.notify_senders();
                    return Ok(v);
                },
                None if self.inner.is_tx_closed() && self.inner.is_empty() => {
                    self.inner.receiver_waiters().remove(node_ptr);
                    self.inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    return Err(RecvError::Disconnected);
                },
                None => {},
            }
            match deadline {
                Some(dl) => park_timeout(dl.saturating_duration_since(Instant::now())),
                None => park(),
            }
            // ОБЯЗАТЕЛЬНО: remove до следующей итерации (spurious wakeup protection).
            self.inner.receiver_waiters().remove(node_ptr);
            self.inner.receiver_wait_counter_sub(Ordering::Relaxed);
        }
    }

    pub fn recv(&self) -> ResultReceiver<T> {
        self.recv_impl(None)
    }

    pub fn recv_timeout(&self, duration: Duration) -> ResultReceiver<T> {
        self.recv_impl(Some(Instant::now() + duration))
    }

    // Drain up to `max` items into `buf` in one call.
    // Notifies senders **once** at the end instead of once per item —
    // reduces WaiterList::mutex contention by up to `max`x vs looping try_recv.
    // Returns number of items added to `buf`.
    // Returns 0 + sets disconnected=true when channel is closed and empty.
    fn _recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize,bool){
        let mut count = 0;
        while count < max {
            match self.inner.pop(){
                Some(v) => {
                    buf.push(v);
                    count += 1;
                },
                None => break
            }
        }

        if count > 0 {
            self.inner.notify_senders();
        }
        let disconnected = count == 0 && self.inner.is_tx_closed() && self.inner.is_empty();
        (count,disconnected)
    }

    fn _recv_batch_join(&self,buf: &mut Vec<T>, max: usize) -> (usize,bool) {
        let (count,disconeected) = self._recv_batch(buf, max-1);
        (1+ count,disconeected)
    }

    pub fn recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize,bool) {
        if max == 0 {
            return (0,false)
        }
        match self.recv(){
            Ok(v) => buf.push(v),
            Err(RecvError::Disconnected) => return (0,true),
            Err(RecvError::Empty) | Err(RecvError::TimeOut(_)) => return (0,false),
        }
        self._recv_batch_join(buf, max)
    }

    pub fn recv_batch_timeout(
        &self, 
        buf: &mut Vec<T>, 
        max: usize, 
        duration: Duration
    ) -> (usize,bool) {
        if max == 0 {
            return (0,false)
        }
        match self.recv_timeout(duration){
            Ok(v) => buf.push(v),
            Err(RecvError::Disconnected) => return (0,true),
            Err(RecvError::TimeOut(_)) => return (0,false),
            Err(RecvError::Empty) => unreachable!() ,
        }
        self._recv_batch_join(buf, max)
    }

    // Blocking iterator — блокирует поток до каждого item.
    pub fn iter(&self) -> Iter<'_, T, CAP> {
        Iter { receiver: self }
    }

    pub fn recv_async(&self) -> ReceiverFuture<'_, T, CAP> {
        ReceiverFuture { receiver: self, node: SyncNode::new(), in_queue: false }
    }

    // Async batch recv — ждёт хотя бы один item, затем дренирует до max.
    // Позволяет обрабатывать burstы без лишних await.
    pub async fn recv_batch_async(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0,false)
        }
        // Ждём первый item через обычный async recv
        match self.recv_async().await {
            Ok(v) => buf.push(v),
            Err(AsyncRecvError::Disconnected) => return (0, true),
        }
        // Дренируем остаток без ожидания
        self._recv_batch_join(buf, max-1)
    }

    // Async stream — совместим с StreamExt::next(), for_each, select!.
    // use futures::StreamExt;
    // let mut stream = std::pin::pin!(stream);  обязательно — RecvStream: !Unpin
    // while let Some(v) = stream.next().await { ... }
    pub fn stream(&self) -> ReceiverStream<'_, T, CAP> {
        ReceiverStream { receiver: self, node: SyncNode::new(), in_queue: false }
    }

}

impl <T,const CAP: usize> Clone for Receiver<T,CAP> {
    fn clone(&self) -> Self {
        self.inner.receiver_add(Ordering::Relaxed);
        Self { inner: self.inner.clone() }
    }
}


// Blocking iterator for Receiver
// for v in rx.iter()      — блокирует поток до следующего item
// for v in rx             — IntoIter, потребляет Receiver
// Оба заканчиваются когда channel закрыт и пуст (все Senders дропнуты).

pub struct Iter<'a, T, const CAP: usize> {
    receiver: &'a Receiver<T, CAP>,
}

impl<'a, T, const CAP: usize> Iterator for Iter<'a, T, CAP> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.receiver.recv() {
            Ok(v) => Some(v),
            Err(RecvError::Disconnected) => None,
            Err(RecvError::TimeOut(_)) => None,
            Err(RecvError::Empty) => unreachable!(),
        }
    }
}

pub struct IntoIter<T, const CAP: usize> {
    receiver: Receiver<T, CAP>,
}

impl<T, const CAP: usize> Iterator for IntoIter<T, CAP> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.receiver.recv() {
            Ok(v) => Some(v),
            Err(RecvError::Disconnected) => None,
            Err(RecvError::TimeOut(_)) => None,
            Err(RecvError::Empty) => unreachable!(),
        }
    }
}

impl<T, const CAP: usize> IntoIterator for Receiver<T, CAP> {
    type Item = T;
    type IntoIter = IntoIter<T, CAP>;

    fn into_iter(self) -> IntoIter<T, CAP> {
        IntoIter { receiver: self }
    }
}

impl<'a, T, const CAP: usize> IntoIterator for &'a Receiver<T, CAP> {
    type Item = T;
    type IntoIter = Iter<'a, T, CAP>;

    fn into_iter(self) -> Iter<'a, T, CAP> {
        Iter { receiver: self }
    }
}

pub struct ReceiverFuture<'a, T, const CAP: usize> {
    receiver: &'a Receiver<T, CAP>,
    node:     SyncNode,
    in_queue:   bool,
}

impl<T, const CAP: usize> Future for ReceiverFuture<'_, T, CAP> {
    type Output = ResultAsyncReceiver<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = &this.receiver.inner;
        let node_ptr = &mut this.node as *mut SyncNode;

        // Быстрый путь
        match inner.pop() {
            Some(v) => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                inner.notify_senders();
                return Poll::Ready(Ok(v));
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                // Если queued=true — нода может быть in_list=true (executor poll без wakeup) или in_list=false (wake_all уже убрал). remove — no-op во втором случае. Убираем здесь чтобы не оставлять мёртвый waker в списке и сразу корректировать счётчик.
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                return Poll::Ready(Err(AsyncRecvError::Disconnected));
            }
            None => {}
        }

        // Cтаём в очередь
        if !this.in_queue {
            inner.receiver_wait_counter_add(Ordering::SeqCst);
            inner.receiver_waiters().push_async(node_ptr, cx.waker().clone());
            this.in_queue = true;
        } else {
            inner.receiver_waiters().update_or_repush_async(node_ptr, cx.waker().clone());
        }

        // Double-check
        match inner.pop() {
            Some(v) => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                inner.notify_senders();
                Poll::Ready(Ok(v))
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                Poll::Ready(Err(AsyncRecvError::Disconnected))
            }
            None => Poll::Pending,
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for ReceiverFuture<'_, T, CAP> {}

impl<T, const CAP: usize> Drop for ReceiverFuture<'_, T, CAP> {
    fn drop(&mut self) {
        if self.in_queue {
            self.receiver.inner.receiver_waiters().remove(&mut self.node as *mut SyncNode);
            self.receiver.inner.receiver_wait_counter_sub(Ordering::Relaxed);
            self.receiver.inner.receiver_waiters().wake_one();
        }
    }
}

// Async stream for Receiver
// use futures::StreamExt;
// let mut stream = std::pin::pin_mut!(stream);  обязательно, т.к. ReceiverStream: !Unpin
// while let Some(v) = stream.next().await { ... }
// SyncNode живёт inline — нет heap alloc и нет пересоздания future на item.
// Логика poll_next идентична ReceivervFuture::poll, но нода переиспользуется.
// ReceiverStream: !Unpin (из-за PhantomPinned в SyncNode) — это ожидаемо

pub struct ReceiverStream<'a, T, const CAP: usize> {
    receiver: &'a Receiver<T, CAP>,
    node:     SyncNode,
    in_queue:   bool,
}

unsafe impl<T: Send, const CAP: usize> Send for ReceiverStream<'_, T, CAP> {}

impl<T, const CAP: usize> futures::Stream for ReceiverStream<'_, T, CAP> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = &this.receiver.inner;
        let node_ptr = &mut this.node as *mut SyncNode;

        // Быстрый путь
        match inner.pop() {
            Some(v) => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                inner.notify_senders();
                return Poll::Ready(Some(v));
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                return Poll::Ready(None); // Stream завершён
            }
            None => {}
        }

        // Встаём в очередь (нода переиспользуется между pollами)
        if !this.in_queue {
            inner.receiver_wait_counter_add(Ordering::SeqCst);
            inner.receiver_waiters().push_async(node_ptr, cx.waker().clone());
            this.in_queue = true;
        } else {
            inner.receiver_waiters().update_or_repush_async(node_ptr, cx.waker().clone());
        }

        // 3. Double-check
        match inner.pop() {
            Some(v) => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                inner.notify_senders();
                Poll::Ready(Some(v))
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                Poll::Ready(None)
            }
            None => Poll::Pending,
        }
    }
}

impl<T, const CAP: usize> Drop for ReceiverStream<'_, T, CAP> {
    fn drop(&mut self) {
        if self.in_queue {
            self.receiver.inner.receiver_waiters().remove(&mut self.node as *mut _);
            self.receiver.inner.receiver_wait_counter_sub(Ordering::Relaxed);
            self.receiver.inner.receiver_waiters().wake_one();
        }
    }
}

impl <T,const CAP: usize> Drop for Receiver<T,CAP> {
    fn drop(&mut self) {
        if self.inner.receiver_sub(Ordering::AcqRel) == 1{
            self.inner.rx_close();
            self.inner.notify_all_on_rx_close();
        }
    }
}


pub struct SingleReceiver<T, const CAP: usize> {
    inner: Arc<SingleInner<T, CAP>>,
}

impl<T, const CAP: usize> SingleReceiver<T, CAP> {

    pub (crate) fn new(inner: Arc<SingleInner<T,CAP>>) -> Self {
        Self { inner }
    }

    pub fn try_recv(&self) -> ResultReceiver<T> {
        match self.inner.pop() {
            Some(v) => { 
                self.inner.notify_senders(); 
                Ok(v) 
            }
            None => {
                if self.inner.is_tx_closed() && self.inner.is_empty() {
                    Err(RecvError::Disconnected)
                } else {
                    Err(RecvError::Empty)
                }
            }
        }
    }

    fn recv_impl(&self,deadline: Option<Instant>) -> ResultReceiver<T> {
        loop {
            match self.inner.pop() {
                Some(v) => { 
                    self.inner.notify_senders(); 
                    return Ok(v) 
                }
                None if self.inner.is_tx_closed() && self.inner.is_empty() => {
                    return Err(RecvError::Disconnected);
                }
                None => {}
            }

            if let Some(dl) = deadline{
                match dl.checked_duration_since(Instant::now()) {
                    Some(d) if d > Duration::ZERO => {},
                    _ => return Err(RecvError::TimeOut(dl.elapsed())),
                }
            }

            let mut node = SyncNode::new_blocking();
            let node_ptr = &mut node as *mut SyncNode;
            self.inner.receiver_wait_counter_add(Ordering::SeqCst);
            self.inner.receiver_waiters().push_blocking(node_ptr);
            match self.inner.pop() {
                Some(v) => {
                    self.inner.receiver_waiters().remove(node_ptr);
                    self.inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    self.inner.notify_senders();
                    return Ok(v);
                }
                None if self.inner.is_tx_closed() && self.inner.is_empty() => {
                    self.inner.receiver_waiters().remove(node_ptr);
                    self.inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    return Err(RecvError::Disconnected);
                }
                None => {}
            }
            match deadline {
                Some(dl) => park_timeout(dl.saturating_duration_since(Instant::now())),
                None => park(),
            }
            self.inner.receiver_waiters().remove(node_ptr);
            self.inner.receiver_wait_counter_sub(Ordering::Relaxed);
        }
    }


    pub fn recv(&self) -> ResultReceiver<T> {
        self.recv_impl(None)
    }

    pub fn recv_timeout(&self, duration: Duration) -> ResultReceiver<T> {
        self.recv_impl(Some(Instant::now() + duration))
    }


    fn _recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize,bool){
        let mut count = 0;
        while count < max {
            match self.inner.pop(){
                Some(v) => {
                    buf.push(v);
                    count += 1;
                },
                None => break
            }
        }

        if count > 0 {
            self.inner.notify_senders();
        }
        let disconnected = count == 0 && self.inner.is_tx_closed() && self.inner.is_empty();
        (count,disconnected)
    }

    fn _recv_batch_join(&self,buf: &mut Vec<T>, max: usize) -> (usize,bool) {
        let (count,disconeected) = self._recv_batch(buf, max-1);
        (1+ count,disconeected)
    }

    pub fn recv_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize,bool) {
        if max == 0 {
            return (0,false)
        }
        match self.recv(){
            Ok(v) => buf.push(v),
            Err(RecvError::Disconnected) => return (0,true),
            Err(RecvError::TimeOut(_)) => return (0,false),
            Err(RecvError::Empty) => unreachable!(),
        }
        self._recv_batch_join(buf, max)
    }

        pub fn recv_batch_timeout(
            &self, 
            buf: &mut Vec<T>, 
            max: usize,
            duration: Duration,
        ) -> (usize,bool) {
        if max == 0 {
            return (0,false)
        }
        match self.recv_timeout(duration){
            Ok(v) => buf.push(v),
            Err(RecvError::Disconnected) => return (0,true),
            Err(RecvError::TimeOut(_)) => return (0,false),
            Err(RecvError::Empty) => unreachable!(),
        }
        self._recv_batch_join(buf, max)
    }

    // Blocking iterator, эквивалентно for v in &rx.
    pub fn iter(&self) -> SingleIter<'_, T, CAP> {
        SingleIter { receiver: self }
    }

    pub fn recv_async(&self) -> SingleRecvFuture<'_, T, CAP> {
        SingleRecvFuture { receiver: self, node: SyncNode::new(), in_queue: false }
    }

    pub async fn recv_batch_async(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        if max == 0 {
            return (0,false)
        }
        match self.recv_async().await {
            Ok(v) => buf.push(v),
            Err(AsyncRecvError::Disconnected) => return (0, true),
        }
        self._recv_batch_join(buf, max-1)
    }

    pub fn stream(&self) -> SignleRecvStream<'_, T, CAP> {
        SignleRecvStream { receiver: self, node: SyncNode::new(), in_queue: false }
    }



}

//nBlocking iterator for SingleReceiver

pub struct SingleIter<'a, T, const CAP: usize> {
    receiver: &'a SingleReceiver<T, CAP>,
}

impl<'a, T, const CAP: usize> Iterator for SingleIter<'a, T, CAP> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.receiver.recv() {
            Ok(v) => Some(v),
            Err(RecvError::Disconnected) => None,
            Err(RecvError::TimeOut(_)) => None,
            Err(RecvError::Empty) => unreachable!(),
        }
    }
}

pub struct SingleIntoIter<T, const CAP: usize> {
    receiver: SingleReceiver<T, CAP>,
}

impl<T, const CAP: usize> Iterator for SingleIntoIter<T, CAP> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.receiver.recv() {
            Ok(v) => Some(v),
            Err(RecvError::Disconnected) => None,
            Err(RecvError::TimeOut(_)) => None,
            Err(RecvError::Empty) => unreachable!(),
        }
    }
}

impl<T, const CAP: usize> IntoIterator for SingleReceiver<T, CAP> {
    type Item     = T;
    type IntoIter = SingleIntoIter<T, CAP>;

    fn into_iter(self) -> SingleIntoIter<T, CAP> {
        SingleIntoIter { receiver: self }
    }
}

impl<'a, T, const CAP: usize> IntoIterator for &'a SingleReceiver<T, CAP> {
    type Item     = T;
    type IntoIter = SingleIter<'a, T, CAP>;

    fn into_iter(self) -> SingleIter<'a, T, CAP> {
        SingleIter { receiver: self }
    }
}

// Async

pub struct SingleRecvFuture<'a, T, const CAP: usize> {
    receiver: &'a SingleReceiver<T, CAP>,
    node:     SyncNode,
    in_queue:   bool,
}

unsafe impl<T: Send, const CAP: usize> Send for SingleRecvFuture<'_, T, CAP> {}

impl<T, const CAP: usize> Future for SingleRecvFuture<'_, T, CAP> {
    type Output = ResultAsyncReceiver<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = &this.receiver.inner;
        let node_ptr = &mut this.node as *mut SyncNode;

        match inner.pop() {
            Some(v) => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                inner.notify_senders();
                return Poll::Ready(Ok(v));
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                return Poll::Ready(Err(AsyncRecvError::Disconnected));
            }
            None => {}
        }

        if !this.in_queue {
            inner.receiver_wait_counter_add(Ordering::SeqCst);
            inner.receiver_waiters().push_async(node_ptr, cx.waker().clone());
            this.in_queue = true;
        } else {
            inner.receiver_waiters().update_or_repush_async(node_ptr, cx.waker().clone());
        }

        match inner.pop() {
            Some(v) => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                inner.notify_senders();
                Poll::Ready(Ok(v))
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                Poll::Ready(Err(AsyncRecvError::Disconnected))
            }
            None => Poll::Pending,
        }
    }
}

impl<T, const CAP: usize> Drop for SingleRecvFuture<'_, T, CAP> {
    fn drop(&mut self) {
        if self.in_queue {
            self.receiver.inner.receiver_waiters().remove(&mut self.node as *mut SyncNode);
            self.receiver.inner.receiver_wait_counter_sub(Ordering::Relaxed);
            self.receiver.inner.receiver_waiters().wake_one();
        }
    }
}

// Async stream for SingleReceiver

pub struct SignleRecvStream<'a, T, const CAP: usize> {
    receiver: &'a SingleReceiver<T, CAP>,
    node: SyncNode,
    in_queue: bool,
}

unsafe impl<T: Send, const CAP: usize> Send for SignleRecvStream<'_, T, CAP> {}

impl<T, const CAP: usize> futures::Stream for SignleRecvStream<'_, T, CAP> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = &this.receiver.inner;
        let node_ptr = &mut this.node as *mut SyncNode;

        match inner.pop() {
            Some(v) => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                inner.notify_senders();
                return Poll::Ready(Some(v));
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                if this.in_queue {
                    inner.receiver_waiters().remove(node_ptr);
                    inner.receiver_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                return Poll::Ready(None);
            }
            None => {}
        }

        if !this.in_queue {
            inner.receiver_wait_counter_add(Ordering::SeqCst);
            inner.receiver_waiters().push_async(node_ptr, cx.waker().clone());
            this.in_queue = true;
        } else {
            inner.receiver_waiters().update_or_repush_async(node_ptr, cx.waker().clone());
        }

        match inner.pop() {
            Some(v) => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                inner.notify_senders();
                Poll::Ready(Some(v))
            }
            None if inner.is_tx_closed() && inner.is_empty() => {
                inner.receiver_waiters().remove(node_ptr);
                inner.receiver_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                Poll::Ready(None)
            }
            None => Poll::Pending,
        }
    }
}

impl<T, const CAP: usize> Drop for SignleRecvStream<'_, T, CAP> {
    fn drop(&mut self) {
        if self.in_queue {
            self.receiver.inner.receiver_waiters().remove(&mut self.node as *mut SyncNode);
            self.receiver.inner.receiver_wait_counter_sub(Ordering::Relaxed);
            self.receiver.inner.receiver_waiters().wake_one();
        }
    }
}

impl<T, const CAP: usize> Drop for SingleReceiver<T, CAP> {
    fn drop(&mut self) {
        self.inner.rx_close();
        self.inner.notify_all_on_rx_close();
    }
}

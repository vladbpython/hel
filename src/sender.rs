use super::{
    errors::SendError,
    result::ResultSender,
    core::{
        Inner,
        SingleInner,
    },
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
    thread::park,
};

pub struct Sender<T, const CAP: usize> {
    inner: Arc<Inner<T,CAP>>
}

impl <T, const  CAP: usize> Sender<T,CAP> {
    
    pub (crate) fn new(inner: Arc<Inner<T,CAP>>) -> Self{
        Self { inner }
    }

    pub fn try_send(&self, value: T) -> ResultSender<(),T> {
        if self.inner.is_rx_closed() {
            return Err(SendError::Disconnected(value));
        }
        match self.inner.push(value) {
            Ok(()) => {
                self.inner.notify_receivers();
                Ok(())
            },
            Err(v) => Err(SendError::Full(v))
        }
    }

    pub fn send(&self, mut value: T) -> ResultSender<(),T> {
        loop{
            if self.inner.is_rx_closed() {
                return Err(SendError::Disconnected(value));
            }

            match self.inner.push(value) {
                Ok(()) => {
                    self.inner.notify_receivers();
                    return Ok(())
                },
                Err(v) => value = v,
            }

            // node живёт на стеке: мы не выходим из функции пока in_list
            let mut node = SyncNode::new_blocking();
            let node_ptr = &mut node as *mut SyncNode;
            self.inner.sender_wait_counter_add(Ordering::SeqCst);
            self.inner.sender_waiters().push_blocking(node_ptr);
            // Double-check после регистрации (race-free)
            if self.inner.is_rx_closed(){
                self.inner.sender_waiters().remove(node_ptr);
                self.inner.sender_wait_counter_sub(Ordering::Relaxed);
                return Err(SendError::Disconnected(value));
            }
            match self.inner.push(value) {
                Ok(()) => {
                    self.inner.sender_waiters().remove(node_ptr);
                    self.inner.sender_wait_counter_sub(Ordering::Relaxed);
                    self.inner.notify_receivers();
                    return Ok(())
                },
                Err(v) => value = v,
            }

            // Park — unpark придёт от notify_senders или close
            park();
            // ОБЯЗАТЕЛЬНО: remove до следующей итерации.
            // wake_one уже мог сделать remove (no-op), но spurious wakeup
            // возвращает из park() без вызова wake_one — нода ещё in_list=true.
            // Без remove следующая итерация создаёт новый node на том же
            // адресе стека → список содержит dangling pointer → SIGSEGV.
            self.inner.sender_waiters().remove(node_ptr);
            self.inner.sender_wait_counter_sub(Ordering::Relaxed);
        }
    }

    pub fn send_async(&self,value: T) -> SenderFuture<'_,T,CAP> {
        SenderFuture { 
            sender: self, 
            value: Some(value), 
            node: SyncNode::new(), 
            in_queue: false 
        }
    }
}


pub struct SenderFuture<'a,T,const CAP: usize> {
    sender: &'a Sender<T,CAP>,
    value: Option<T>,
    node: SyncNode,
    in_queue: bool,
}

impl<T, const CAP: usize> Future for SenderFuture<'_, T, CAP> {
    type Output = ResultSender<(),T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = &this.sender.inner;
        let node_ptr = &mut this.node as *mut SyncNode;

        let mut value = match this.value.take() {
            Some(v) => v,
            None => {
                if this.in_queue {
                    inner.sender_waiters().remove(node_ptr);
                    inner.sender_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                return Poll::Pending;
            }
        };

        // Быстрый путь
        if inner.is_rx_closed() {
            if this.in_queue {
                inner.sender_waiters().remove(node_ptr);
                inner.sender_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
            }
            return Poll::Ready(Err(SendError::Disconnected(value)));
        }
        match inner.push(value) {
            Ok(()) => {
                if this.in_queue {
                    inner.sender_waiters().remove(node_ptr);
                    inner.sender_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                inner.notify_receivers();
                return Poll::Ready(Ok(()));
            }
            Err(v) => value = v,
        }

        // Встаём в очередь
        if !this.in_queue {
            inner.sender_wait_counter_add(Ordering::SeqCst);
            inner.sender_waiters().push_async(node_ptr, cx.waker().clone());
            this.in_queue = true;
        } else {
            inner.sender_waiters().update_or_repush_async(node_ptr, cx.waker().clone());
        }

        // Double-check
        if inner.is_rx_closed() {
            inner.sender_waiters().remove(node_ptr);
            inner.sender_wait_counter_sub(Ordering::Relaxed);
            this.in_queue = false;
            return Poll::Ready(Err(SendError::Disconnected(value)));
        }
        match inner.push(value) {
            Ok(()) => {
                inner.sender_waiters().remove(node_ptr);
                inner.sender_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                inner.notify_receivers();
                Poll::Ready(Ok(()))
            }
            Err(v) => { this.value = Some(v); Poll::Pending }
        }
    }
}

impl<T, const CAP: usize> Drop for SenderFuture<'_, T, CAP> {
    fn drop(&mut self) {
        if self.in_queue {
            self.sender.inner.sender_waiters().remove(&mut self.node as *mut SyncNode);
            self.sender.inner.sender_wait_counter_sub(Ordering::Relaxed);
            if self.value.is_some() {
                self.sender.inner.sender_waiters().wake_one();
            }
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for SenderFuture<'_, T, CAP> {}

impl <T, const CAP: usize> Clone for Sender<T,CAP> {
    fn clone(&self) -> Self {
        self.inner.sender_add(Ordering::Relaxed);
        Self { inner: self.inner.clone() }
    }
}



impl <T, const CAP: usize> Drop for Sender<T,CAP> {
    fn drop(&mut self) {
        if self.inner.sender_sub(Ordering::AcqRel) == 1{
            self.inner.tx_close();
            self.inner.notify_all_on_tx_close();
        }
    }
}


// Намеренно нет реализации Clone — SPSC гарантия
pub struct SingleSender<T,const CAP: usize> {
    inner: Arc<SingleInner<T, CAP>>,
}

impl <T,const CAP: usize> SingleSender<T,CAP> {
    
    pub (crate) fn new(inner: Arc<SingleInner<T,CAP>>) -> Self {
        Self { inner }
    }

    pub fn try_send(&self, value: T) -> ResultSender<(),T> {
        if self.inner.is_rx_closed() { 
            return Err(SendError::Disconnected(value)) 
        }
        match self.inner.push(value) {
            Ok(()) => { self.inner.notify_receivers(); Ok(()) }
            Err(v) => Err(SendError::Full(v)),
        }
    }

    pub fn send(&self, mut value: T) -> ResultSender<(),T> {
        loop {
            if self.inner.is_rx_closed() { 
                return Err(SendError::Disconnected(value)); 
            }
            match self.inner.push(value) {
                Ok(()) => { 
                    self.inner.notify_receivers(); 
                    return Ok(())
                }
                Err(v) => value = v,
            }

            let mut node = SyncNode::new_blocking();
            let node_ptr = &mut node as *mut SyncNode;
            self.inner.sender_wait_counter_add(Ordering::SeqCst);
            self.inner.sender_waiters().push_blocking(node_ptr);
            if self.inner.is_rx_closed() {
                self.inner.sender_waiters().remove(node_ptr);
                self.inner.sender_wait_counter_sub(Ordering::Relaxed);
                return Err(SendError::Disconnected(value));
            }
            match self.inner.push(value) {
                Ok(()) => {
                    self.inner.sender_waiters().remove(node_ptr);
                    self.inner.sender_wait_counter_sub(Ordering::Relaxed);
                    self.inner.notify_receivers();
                    return Ok(());
                }
                Err(v) => value = v,
            }

            park();
            self.inner.sender_waiters().remove(node_ptr);
            self.inner.sender_wait_counter_sub(Ordering::Relaxed);
        }
    }
    pub fn send_async(&self, value: T) -> SingleSendFuture<'_,T,CAP> {
        SingleSendFuture { 
            sender: self, 
            value: Some(value), 
            node: SyncNode::new(), 
            in_queue: false 
        }
    }
}


pub struct SingleSendFuture<'a, T, const CAP: usize> {
    sender: &'a SingleSender<T, CAP>,
    value:  Option<T>,
    node:   SyncNode,
    in_queue: bool,
}

impl<T, const CAP: usize> Future for SingleSendFuture<'_, T, CAP> {
    type Output = ResultSender<(),T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let inner = &this.sender.inner;
        let node_ptr = &mut this.node as *mut SyncNode;

        let mut value = match this.value.take() {
            Some(v) => v,
            None => {
                if this.in_queue {
                    inner.sender_waiters().remove(node_ptr);
                    inner.sender_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                return Poll::Pending;
            }
        };

        // Быстрый путь
        if inner.is_rx_closed() {
            if this.in_queue {
                inner.sender_waiters().remove(node_ptr);
                inner.sender_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
            }
            return Poll::Ready(Err(SendError::Disconnected(value)));
        }
        match inner.push(value) {
            Ok(()) => {
                if this.in_queue {
                    inner.sender_waiters().remove(node_ptr);
                    inner.sender_wait_counter_sub(Ordering::Relaxed);
                    this.in_queue = false;
                }
                inner.notify_receivers();
                return Poll::Ready(Ok(()));
            }
            Err(v) => value = v,
        }

        // Cтаём в очередь
        if !this.in_queue {
            inner.sender_wait_counter_add(Ordering::SeqCst);
            inner.sender_waiters().push_async(node_ptr, cx.waker().clone());
            this.in_queue = true;
        } else {
            inner.sender_waiters().update_or_repush_async(node_ptr, cx.waker().clone());
        }

        // Double-check
        if inner.is_rx_closed() {
            inner.sender_waiters().remove(node_ptr);
            inner.sender_wait_counter_sub(Ordering::Relaxed);
            this.in_queue = false;
            return Poll::Ready(Err(SendError::Disconnected(value)));
        }
        match inner.push(value) {
            Ok(()) => {
                inner.sender_waiters().remove(node_ptr);
                inner.sender_wait_counter_sub(Ordering::Relaxed);
                this.in_queue = false;
                inner.notify_receivers();
                Poll::Ready(Ok(()))
            }
            Err(v) => { this.value = Some(v); Poll::Pending }
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for SingleSendFuture<'_, T, CAP> {}

impl<T, const CAP: usize> Drop for SingleSendFuture<'_, T, CAP> {
    fn drop(&mut self) {
        if self.in_queue {
            self.sender.inner.sender_waiters().remove(&mut self.node as *mut SyncNode);
            self.sender.inner.sender_wait_counter_sub(Ordering::Relaxed);
            if self.value.is_some() {
                self.sender.inner.sender_waiters().wake_one();
            }
        }
    }
}

impl<T, const CAP: usize> Drop for SingleSender<T, CAP> {
    fn drop(&mut self) {
        self.inner.tx_close();
        self.inner.notify_all_on_tx_close();
    }
}



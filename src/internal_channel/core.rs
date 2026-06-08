use super::{
    sync::{Slot, SyncList},
    traits::InnerChannel,
};
use crate::cache::Padding;
use std::{
    array::from_fn,
    hint,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
};

const SPIN_COUNT: u32 = 64;

#[inline]
fn notify_one_waiter(list: &SyncList) {
    let a = list.async_count_seqcst();
    let b = list.blocking_count_acquire();
    if a > 0 || b > 0 {
        list.notify_one_if(a, b);
    }
}

#[inline]
fn pop_batch_impl<T>(
    mut pop: impl FnMut() -> Option<T>,
    is_tx_closed: impl Fn() -> bool,
    is_empty: impl Fn() -> bool,
    buf: &mut Vec<T>,
    max: usize,
) -> (usize, bool) {
    let mut n = 0;
    while n < max {
        match pop() {
            Some(v) => {
                buf.push(v);
                n += 1;
            }
            None => break,
        }
    }
    (n, n == 0 && is_tx_closed() && is_empty())
}

#[inline]
fn push_batch_impl<T>(mut push: impl FnMut(T) -> Result<(), T>, buf: &mut Vec<T>) -> usize {
    buf.reverse();
    let mut sent = 0;
    while let Some(value) = buf.pop() {
        match push(value) {
            Ok(()) => sent += 1,
            Err(v) => {
                buf.push(v);
                break;
            }
        }
    }
    if !buf.is_empty() {
        buf.reverse();
    }
    sent
}

/// MPMC channel with lock free CAS ring buffer. for x86
#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
pub struct Inner<T, const CAP: usize> {
    slots: [Slot<T>; CAP],
    tail: Padding<AtomicUsize>,
    head: Padding<AtomicUsize>,
    send_waiters: SyncList,
    recv_waiters: SyncList,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    tx_closed: AtomicBool,
    rx_closed: AtomicBool,
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
impl<T, const CAP: usize> Inner<T, CAP> {
    pub fn new() -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        Self {
            slots: from_fn(|n| Slot::new(n)),
            tail: Padding(AtomicUsize::new(0)),
            head: Padding(AtomicUsize::new(0)),
            send_waiters: SyncList::new(),
            recv_waiters: SyncList::new(),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            tx_closed: AtomicBool::new(false),
            rx_closed: AtomicBool::new(false),
        }
    }

    #[inline]
    fn push_inner(&self, value: T) -> Result<(), T> {
        let mask = CAP - 1;
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[tail & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - tail as isize;
            let next = tail + 1;
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    tail,
                    next,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe { (*slot.data.get()).write(value) };
                        slot.sequence.store(next, Ordering::Release);
                        return Ok(());
                    }
                    Err(t) => tail = t,
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                tail = self.tail.load(Ordering::Acquire);
            }
        }
    }

    #[inline]
    fn pop_inner(&self) -> Option<T> {
        let mask = CAP - 1;
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[head & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (head + 1) as isize;
            let next_h = head + 1;
            let next_seq = head + CAP;
            if diff == 0 {
                match self.head.compare_exchange_weak(
                    head,
                    next_h,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let v = unsafe { (*slot.data.get()).assume_init_read() };
                        slot.sequence.store(next_seq, Ordering::Release);
                        return Some(v);
                    }
                    Err(h) => head = h,
                }
            } else if diff < 0 {
                return None;
            } else {
                head = self.head.load(Ordering::Acquire);
            }
        }
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
impl<T: Send + 'static, const CAP: usize> InnerChannel<T, CAP> for Inner<T, CAP> {
    #[inline]
    fn push(&self, v: T) -> Result<(), T> {
        self.push_inner(v)
    }
    #[inline]
    fn push_blocking(&self, v: T) -> Result<(), T> {
        self.push_inner(v)
    }
    #[inline]
    fn pop(&self) -> Option<T> {
        self.pop_inner()
    }

    fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        pop_batch_impl(
            || self.pop_inner(),
            || self.is_tx_closed(),
            || self.is_empty(),
            buf,
            max,
        )
    }
    fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        push_batch_impl(|v| self.push_inner(v), buf)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }
    #[inline]
    fn is_tx_closed(&self) -> bool {
        self.tx_closed.load(Ordering::Acquire)
    }
    #[inline]
    fn is_rx_closed(&self) -> bool {
        self.rx_closed.load(Ordering::Acquire)
    }

    #[inline]
    fn notify_receivers(&self) {
        notify_one_waiter(self.receiver_waiters());
    }
    #[inline]
    fn notify_senders(&self) {
        notify_one_waiter(self.sender_waiters());
    }
    #[inline]
    fn notify_all_on_tx_close(&self) {
        self.recv_waiters.wake_all();
    }
    #[inline]
    fn notify_all_on_rx_close(&self) {
        self.send_waiters.wake_all();
    }

    #[inline]
    fn receiver_add(&self, ordering: Ordering) -> usize {
        self.receivers.fetch_add(1, ordering)
    }
    #[inline]
    fn receiver_sub(&self, ordering: Ordering) -> usize {
        self.receivers.fetch_sub(1, ordering)
    }
    #[inline]
    fn sender_add(&self, ordering: Ordering) -> usize {
        self.senders.fetch_add(1, ordering)
    }
    #[inline]
    fn sender_sub(&self, ordering: Ordering) -> usize {
        self.senders.fetch_sub(1, ordering)
    }
    #[inline]
    fn receiver_waiters(&self) -> &SyncList {
        &self.recv_waiters
    }
    #[inline]
    fn sender_waiters(&self) -> &SyncList {
        &self.send_waiters
    }
    #[inline]
    fn tx_close(&self) {
        self.tx_closed.store(true, Ordering::Release);
    }
    #[inline]
    fn rx_close(&self) {
        self.rx_closed.store(true, Ordering::Release);
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
unsafe impl<T: Send, const CAP: usize> Send for Inner<T, CAP> {}
#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
unsafe impl<T: Send, const CAP: usize> Sync for Inner<T, CAP> {}

// SeqInner (ARM aarch64)

/// MPMC sequence-based ring buffer. This approach is more efficient on macOS ARM.
/// Instead of CAS on tail each producer does fetch_add and gets a unique slot.
/// Init: seq[i] = i
/// push: pos = tail.fetch_add(1), wait until seq[slot]==pos, write, seq[slot]=pos+1
/// pop: check seq[slot]==head+1, CAS head, read, seq[slot]=head+CAP
#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
pub struct SeqInner<T, const CAP: usize> {
    slots: [Slot<T>; CAP],
    tail: Padding<AtomicUsize>,
    head: Padding<AtomicUsize>,
    send_waiters: SyncList,
    recv_waiters: SyncList,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    tx_closed: AtomicBool,
    rx_closed: AtomicBool,
}

#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
impl<T, const CAP: usize> SeqInner<T, CAP> {
    pub fn new() -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        Self {
            slots: from_fn(|n| Slot::new(n)),
            tail: Padding(AtomicUsize::new(0)),
            head: Padding(AtomicUsize::new(0)),
            send_waiters: SyncList::new(),
            recv_waiters: SyncList::new(),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            tx_closed: AtomicBool::new(false),
            rx_closed: AtomicBool::new(false),
        }
    }

    /// try_push: fast seq check before CAS avoids atomic penalty
    /// on a CAS that is guaranteed to fail.
    #[inline]
    fn push_inner(&self, value: T) -> Result<(), T> {
        let mask = CAP - 1;
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[tail & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - tail as isize;
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    tail,
                    tail + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe { (*slot.data.get()).write(value) };
                        slot.sequence.store(tail + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(t) => tail = t,
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                tail = self.tail.load(Ordering::Acquire);
            }
        }
    }

    /// push_blocking: fetch_add — main ARM optimization.
    /// All N producers do fetch_add simultaneously — each gets a unique slot.
    /// No losers, no retry, no cache line bouncing between producers.
    #[inline]
    pub fn push_fetch_add(&self, value: T) -> Result<(), T> {
        if self.rx_closed.load(Ordering::Acquire) {
            return Err(value);
        }
        let pos = self.tail.fetch_add(1, Ordering::Relaxed);
        let slot = &self.slots[pos & (CAP - 1)];
        let mut spins = 0u32;
        loop {
            let seq = slot.sequence.load(Ordering::Acquire);
            if seq == pos {
                break;
            }
            if self.rx_closed.load(Ordering::Acquire) {
                slot.sequence.store(pos + 1, Ordering::Release);
                return Err(value);
            }
            if spins < SPIN_COUNT {
                hint::spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
                spins = 0;
            }
        }
        unsafe { (*slot.data.get()).write(value) };
        slot.sequence.store(pos + 1, Ordering::Release);
        Ok(())
    }

    #[inline]
    fn pop_inner(&self) -> Option<T> {
        let mask = CAP - 1;
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[head & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (head + 1) as isize;
            let next_seq = head + CAP;
            if diff == 0 {
                match self.head.compare_exchange_weak(
                    head,
                    head + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let v = unsafe { (*slot.data.get()).assume_init_read() };
                        slot.sequence.store(next_seq, Ordering::Release);
                        return Some(v);
                    }
                    Err(h) => head = h,
                }
            } else if diff < 0 {
                return None;
            } else {
                head = self.head.load(Ordering::Acquire);
            }
        }
    }
}

#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
impl<T: Send + 'static, const CAP: usize> InnerChannel<T, CAP> for SeqInner<T, CAP> {
    // push = CAS (try_send / async path)
    #[inline]
    fn push(&self, v: T) -> Result<(), T> {
        self.push_inner(v)
    }
    // push_blocking = fetch_add (send without deadline ARM optimization)
    #[inline]
    fn push_blocking(&self, v: T) -> Result<(), T> {
        self.push_fetch_add(v)
    }
    #[inline]
    fn pop(&self) -> Option<T> {
        self.pop_inner()
    }

    fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        push_batch_impl(|v| self.push_inner(v), buf)
    }
    fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        pop_batch_impl(
            || self.pop_inner(),
            || self.is_tx_closed(),
            || self.is_empty(),
            buf,
            max,
        )
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }
    #[inline]
    fn is_tx_closed(&self) -> bool {
        self.tx_closed.load(Ordering::Acquire)
    }
    #[inline]
    fn is_rx_closed(&self) -> bool {
        self.rx_closed.load(Ordering::Acquire)
    }

    #[inline]
    fn notify_receivers(&self) {
        notify_one_waiter(self.receiver_waiters())
    }
    #[inline]
    fn notify_senders(&self) {
        notify_one_waiter(self.sender_waiters());
    }
    #[inline]
    fn notify_all_on_tx_close(&self) {
        self.recv_waiters.wake_all();
    }
    #[inline]
    fn notify_all_on_rx_close(&self) {
        self.send_waiters.wake_all();
    }

    #[inline]
    fn receiver_add(&self, o: Ordering) -> usize {
        self.receivers.fetch_add(1, o)
    }
    #[inline]
    fn receiver_sub(&self, o: Ordering) -> usize {
        self.receivers.fetch_sub(1, o)
    }
    #[inline]
    fn sender_add(&self, o: Ordering) -> usize {
        self.senders.fetch_add(1, o)
    }
    #[inline]
    fn sender_sub(&self, o: Ordering) -> usize {
        self.senders.fetch_sub(1, o)
    }
    #[inline]
    fn receiver_waiters(&self) -> &SyncList {
        &self.recv_waiters
    }
    #[inline]
    fn sender_waiters(&self) -> &SyncList {
        &self.send_waiters
    }
    #[inline]
    fn tx_close(&self) {
        self.tx_closed.store(true, Ordering::Release);
    }
    #[inline]
    fn rx_close(&self) {
        self.rx_closed.store(true, Ordering::Release);
    }
}

#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
unsafe impl<T: Send, const CAP: usize> Send for SeqInner<T, CAP> {}
#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
unsafe impl<T: Send, const CAP: usize> Sync for SeqInner<T, CAP> {}

#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
pub type MPMCInner<T, const CAP: usize> = SeqInner<T, CAP>;
#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
pub type MPMCInner<T, const CAP: usize> = Inner<T, CAP>;

// SingleInner (SPSC, lock free)

pub struct SingleInner<T, const CAP: usize> {
    slots: [Slot<T>; CAP],
    tail: Padding<AtomicUsize>,
    head: Padding<AtomicUsize>,
    send_waiters: SyncList,
    recv_waiters: SyncList,
    tx_closed: AtomicBool,
    rx_closed: AtomicBool,
}

impl<T, const CAP: usize> SingleInner<T, CAP> {
    pub fn new() -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        Self {
            slots: from_fn(|n| Slot::new(n)),
            tail: Padding(AtomicUsize::new(0)),
            head: Padding(AtomicUsize::new(0)),
            send_waiters: SyncList::new(),
            recv_waiters: SyncList::new(),
            tx_closed: AtomicBool::new(false),
            rx_closed: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn push(&self, v: T) -> Result<(), T> {
        let mask = CAP - 1;
        let tail = self.tail.load(Ordering::Relaxed);
        let slot = &self.slots[tail & mask];
        if slot.sequence.load(Ordering::Acquire) as isize - tail as isize == 0 {
            unsafe { (*slot.data.get()).write(v) };
            slot.sequence.store(tail + 1, Ordering::Release);
            self.tail.store(tail + 1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(v)
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mask = CAP - 1;
        let head = self.head.load(Ordering::Relaxed);
        let slot = &self.slots[head & mask];
        let next_seq = head + CAP;
        if slot.sequence.load(Ordering::Acquire) as isize - (head + 1) as isize == 0 {
            let v = unsafe { (*slot.data.get()).assume_init_read() };
            slot.sequence.store(next_seq, Ordering::Release);
            self.head.store(head + 1, Ordering::Relaxed);
            Some(v)
        } else {
            None
        }
    }

    pub fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        pop_batch_impl(
            || self.pop(),
            || self.is_tx_closed(),
            || self.is_empty(),
            buf,
            max,
        )
    }

    pub fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        push_batch_impl(|v| self.push(v), buf)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }
    #[inline]
    pub fn is_tx_closed(&self) -> bool {
        self.tx_closed.load(Ordering::Acquire)
    }
    #[inline]
    pub fn is_rx_closed(&self) -> bool {
        self.rx_closed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn notify_receivers(&self) {
        notify_one_waiter(self.receiver_waiters())
    }

    #[inline]
    pub fn notify_senders(&self) {
        notify_one_waiter(self.sender_waiters())
    }

    pub fn notify_all_on_tx_close(&self) {
        self.recv_waiters.wake_all();
    }
    pub fn notify_all_on_rx_close(&self) {
        self.send_waiters.wake_all();
    }
    #[inline]
    pub fn receiver_waiters(&self) -> &SyncList {
        &self.recv_waiters
    }
    #[inline]
    pub fn sender_waiters(&self) -> &SyncList {
        &self.send_waiters
    }
    #[inline]
    pub fn tx_close(&self) {
        self.tx_closed.store(true, Ordering::Release);
    }
    #[inline]
    pub fn rx_close(&self) {
        self.rx_closed.store(true, Ordering::Release);
    }
}

impl<T: Send + 'static, const CAP: usize> InnerChannel<T, CAP> for SingleInner<T, CAP> {
    // push_blocking == push: SPSC has exactly 1 producer, no CAS contention.
    // All InnerChannel methods are #[inline] critical for the call chain:
    // send_impl → SenderOps::push_blocking → InnerChannel::push_blocking → SingleInner::push
    #[inline]
    fn push(&self, v: T) -> Result<(), T> {
        SingleInner::push(self, v)
    }
    #[inline]
    fn push_blocking(&self, v: T) -> Result<(), T> {
        SingleInner::push(self, v)
    }
    #[inline]
    fn pop(&self) -> Option<T> {
        SingleInner::pop(self)
    }

    #[inline]
    fn pop_batch(&self, buf: &mut Vec<T>, max: usize) -> (usize, bool) {
        self.pop_batch(buf, max)
    }
    #[inline]
    fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        self.push_batch(buf)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        SingleInner::is_empty(self)
    }
    #[inline]
    fn is_tx_closed(&self) -> bool {
        SingleInner::is_tx_closed(self)
    }
    #[inline]
    fn is_rx_closed(&self) -> bool {
        SingleInner::is_rx_closed(self)
    }

    #[inline]
    fn notify_receivers(&self) {
        SingleInner::notify_receivers(self);
    }
    #[inline]
    fn notify_senders(&self) {
        SingleInner::notify_senders(self);
    }
    #[inline]
    fn notify_all_on_tx_close(&self) {
        SingleInner::notify_all_on_tx_close(self);
    }
    #[inline]
    fn notify_all_on_rx_close(&self) {
        SingleInner::notify_all_on_rx_close(self);
    }

    // SPSC: always exactly 1 sender and 1 receiver no ref counting needed.
    // sender_sub returns 1 so Drop check (== 1) triggers tx_close correctly.
    #[inline]
    fn sender_add(&self, _: Ordering) -> usize {
        1
    }
    #[inline]
    fn sender_sub(&self, _: Ordering) -> usize {
        1
    }
    #[inline]
    fn receiver_add(&self, _: Ordering) -> usize {
        1
    }
    #[inline]
    fn receiver_sub(&self, _: Ordering) -> usize {
        1
    }

    #[inline]
    fn receiver_waiters(&self) -> &SyncList {
        SingleInner::receiver_waiters(self)
    }
    #[inline]
    fn sender_waiters(&self) -> &SyncList {
        SingleInner::sender_waiters(self)
    }
    #[inline]
    fn tx_close(&self) {
        SingleInner::tx_close(self);
    }
    #[inline]
    fn rx_close(&self) {
        SingleInner::rx_close(self);
    }
}

unsafe impl<T: Send, const CAP: usize> Send for SingleInner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for SingleInner<T, CAP> {}

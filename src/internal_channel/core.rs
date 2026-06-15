use super::{
    sync::{Slot, SyncList},
    traits::{InnerChannel, MultiProducer},
};
use crate::cache::Padding;
use std::{
    array::from_fn,
    hint,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
    time,
};

/// Waiting phases push_fetch_add (escalation spin → yield → sleep)
/// Spin budget ≈ cost of the yield stage (~0.5 µs): 64 × ~20 ns ≈ 1.3 µs.
/// Covers the “CAS head → seq.store” window of the consumer (instruction units)
/// and pop happening right now.
pub const SENDER_SPIN_COUNT: u32 = 64;
/// The Yield phase covers a short plug (~tens-hundreds of µs polite
/// waiting at yield ~0.5-5 µs under load), then the plug is prolonged.
const YIELD_UNTIL: u32 = 256;
/// Consumer's protracted plug: we sleep in quanta, DO NOT burn the core, which
/// needed by the consumer himself. Price up to 20 µs of excess latency per
/// waking up after an already long period of inactivity.
const SLEEP: std::time::Duration = time::Duration::from_micros(20);


#[inline]
fn notify_one_waiter(list: &SyncList) {
    // Dekker: pair to fence(SeqCst) in push_blocking /fetch_add(SeqCst) in
    // push_async_slot. Release store data + Acquire load counter NOT
    // form a fence (on x86 SeqCst load regular mov): the producer could
    // read count==0 before its store becomes visible to the consumer,
    // consumer reread the old seq and park. Lost wakeup,
    // caught Miri (weak memory) on a spinning producer with a full channel.
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
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

// SeqInner (X86/ARM aarch64)

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

        #[inline]
        pub fn push_fetch_add(&self, value: T) -> Result<(), T> {
            if self.rx_closed.load(Ordering::Acquire) {
                return Err(value);
            }
            let pos = self.tail.fetch_add(1, Ordering::Relaxed);
            let slot = &self.slots[pos & (CAP - 1)];
            let mut waits = 0u32;
            loop {
                let seq = slot.sequence.load(Ordering::Acquire);
                if seq == pos { break; }
                if self.rx_closed.load(Ordering::Acquire) {
                    //WITHOUT seal and WITHOUT waiting. The old seal was waiting for seq==pos
                    //i.e. freeing the slot by the consumer, who, when
                    //rx_closed no longer exists: send() on full channel
                    //+ drop receiver froze forever. Hole in seq chain
                    //after closing it is harmless: there are no consumers, the rest
                    //producers leave using their rx_closed checks, and Drop
                    //distinguishes states by seq (see impl Drop below) slot
                    //saves with unconsumed predecessor data
                    //its label p_old+1 and will be dropped correctly.
                    return Err(value);
                }
                //Escalate: spin(hotpath, wait=ns) → yield
                //(short silence) → sleep (long silence of the consumer:
                //do not burn the core, which it itself needs otherwise everything will
                //blocked producers eat up the CPU of the one they are waiting for).
                //Park is not possible: notify_senders wakes up ONE arbitrary
                //waiting, and we are waiting for our specific position woke up
                //if it weren't for the owner, the owner would continue to sleep.
                waits += 1;
                if waits < SENDER_SPIN_COUNT {
                    hint::spin_loop();
                } else if waits < YIELD_UNTIL {
                    thread::yield_now();
                } else {
                    thread::sleep(SLEEP);
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

impl<T: Send + 'static, const CAP: usize> InnerChannel<T, CAP> for SeqInner<T, CAP> {
    #[inline]
    fn push(&self, v: T) -> Result<(), T> {
        self.push_inner(v)
    }
    #[inline]
    fn push_blocking(&self, v: T) -> Result<(), T> {
        #[cfg(target_os = "macos")]
        {
            self.push_fetch_add(v)
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.push_fetch_add(v)
        }
    }
    #[inline]
    fn pop(&self) -> Option<T> {
        self.pop_inner()
    }

    fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        if buf.is_empty() || self.rx_closed.load(Ordering::Acquire) {
            return 0;
        }
        let mask = CAP - 1;
        let mut tail = self.tail.load(Ordering::Relaxed);
        let (pos, k) = loop {
            let head = self.head.load(Ordering::Acquire);
            let used = tail.wrapping_sub(head);
            // The tail snapshot could have gone bad: another producer moved tail,
            // the consumer sent head FOR our snapshot → used "negative"
            // (wrap in huge usize). Without guard CAP used panics in debug
            // (caught by Miri) and gives garbage free in release. Let's re-read it.
            if used > CAP {
                tail = self.tail.load(Ordering::Relaxed);
                continue;
            }
            if used == CAP {
                return 0;
            }
            let k = buf.len().min(CAP - used);
            match self.tail.compare_exchange_weak(
                tail,
                tail + k,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break (tail, k),
                Err(t) => tail = t,
            }
        };
        for i in 0..k {
            let p = pos + i;
            let slot = &self.slots[p & mask];
            while slot.sequence.load(Ordering::Acquire) != p {
                if self.rx_closed.load(Ordering::Acquire) {
                    // Phase B has not started no slots are written,
                    // buf is not touched. Without seal (see push_fetch_add).
                    return 0;
                }
                // Window “CAS head → seq.store” for the consumer. yield_now, as in
                // push_fetch_add: gives the scheduler (and Miri) a switch point;
                // pure spin_loop here livelock under Miri with preemption rate=0.
                thread::yield_now();
            }
        }
        for (i, value) in buf.drain(..k).enumerate() {
            let p = pos + i;
            let slot = &self.slots[p & mask];
            unsafe { (*slot.data.get()).write(value) };
            slot.sequence.store(p + 1, Ordering::Release);
        }
        k
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

impl<T, const CAP: usize> Drop for SeqInner<T, CAP> {
    fn drop(&mut self) {
        // &mut self ⇒ no competitors. Slot states at rest
        // seq == pos free (not recorded), skip;
        // seq == pos+1 RECORDED, data is alive -drop;
        // seq == pos+CAP consumed or sealed (without data), skip.
        // Without this Drop, undelivered Ts leaked (caught by Miri leak-checker).
        let mask = CAP - 1;
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in head..tail {
            let slot = &self.slots[pos & mask];
            if slot.sequence.load(Ordering::Relaxed) == pos + 1 {
                unsafe { (*slot.data.get()).assume_init_drop() };
            }
        }
    }
}

impl<T, const CAP: usize> MultiProducer for SeqInner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Send for SeqInner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for SeqInner<T, CAP> {}

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
        if buf.is_empty() {
            return 0;
        }
        let mask = CAP - 1;
        let tail = self.tail.load(Ordering::Relaxed);
        // The readiness of the slot is determined ONLY by the seq protocol: Acquire load
        // seq==p pairs with the consumer's Release store in pop and gives
        // happens before it reads data. head is unusable here:
        // pop publishes head via Relaxed Acquire load of Relaxed store
        // does not provide synchronization (data race, caught by Miri).
        let mut k = 0usize;
        while k < buf.len() && k < CAP {
            let p = tail + k;
            if self.slots[p & mask].sequence.load(Ordering::Acquire) != p {
                break; // the slot is not free yet, we truncate the batch
            }
            k += 1;
        }
        if k == 0 {
            return 0;
        }
        for (i, value) in buf.drain(..k).enumerate() {
            let p = tail + i;
            let slot = &self.slots[p & mask];
            unsafe { (*slot.data.get()).write(value) };
            slot.sequence.store(p + 1, Ordering::Release);
        }
        self.tail.store(tail + k, Ordering::Relaxed); // one publication per batch
        k
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
    
    #[cfg(target_os = "linux")]
    #[inline]
    fn yield_before_park(&self) -> bool {
        false
    }
}

impl<T, const CAP: usize> Drop for SingleInner<T, CAP> {
    fn drop(&mut self) {
        let mask = CAP - 1;
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in head..tail {
            let slot = &self.slots[pos & mask];
            if slot.sequence.load(Ordering::Relaxed) == pos + 1 {
                unsafe { (*slot.data.get()).assume_init_drop() };
            }
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for SingleInner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for SingleInner<T, CAP> {}

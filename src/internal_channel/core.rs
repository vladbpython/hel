use super::{
    sync::{Slot, SyncList},
    traits::{InnerChannel, MultiConsumer, MultiProducer},
};
use crate::{
    cache::Padding,
    shim,
};
use shim::loom::{
    AtomicBool, AtomicUsize, Ordering
};

#[cfg(not(loom))]
use std::hint;
#[cfg(not(loom))]
use std::time;
use std::{
    mem::MaybeUninit,
    ptr::addr_of_mut,
    sync::Arc,
    thread,
};

/// Waiting phases push_fetch_add (escalation spin → yield → sleep)
/// Spin budget ≈ cost of the yield stage (~0.5 µs): 64 × ~20 ns ≈ 1.3 µs.
/// Covers the “CAS head → seq.store” window of the consumer (instruction units)
/// and pop happening right now.
pub const SENDER_SPIN_COUNT: u32 = 64;
/// The Yield phase covers a short plug (~tens-hundreds of µs polite
/// waiting at yield ~0.5-5 µs under load), then the plug is prolonged.
#[cfg(not(loom))]
const YIELD_UNTIL: u32 = 256;
/// Consumer's protracted plug: we sleep in quanta, DO NOT burn the core, which
/// needed by the consumer himself. Price up to 20 µs of excess latency per
/// waking up after an already long period of inactivity.
#[cfg(not(loom))]
const SLEEP: std::time::Duration = time::Duration::from_micros(20);

#[inline]
fn notify_one_waiter(list: &SyncList) {
    // Dekker: pair to fence(SeqCst) in push_blocking /fetch_add(SeqCst) in
    // push_async_slot. Release store data + Acquire load counter NOT
    // form a fence (on x86 SeqCst load regular mov): the producer could
    // read count==0 before its store becomes visible to the consumer,
    // consumer reread the old seq and park. Lost wakeup,
    // caught Miri (weak memory) on a spinning producer with a full channel.
    shim::loom::fence(shim::loom::Ordering::SeqCst);
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
    pub fn new() -> Arc<Self> {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        assert!(
            CAP >= 2,
            "sequence ring requires CAP >= 2: at CAP=1 the 'free' (seq == pos) and 'consumed' (seq == pos + CAP) conditions collide, allowing silent overwrite (found by loom algo_ring_1p1c_wraparound_exact)"
        );
        let mut uninit: Arc<MaybeUninit<Self>> = Arc::new_uninit();
        // get_mut gives &mut MaybeUninit<Self> (Arc is unique, just created)
        let slot_ptr = Arc::get_mut(&mut uninit).unwrap();
        let ptr = slot_ptr.as_mut_ptr(); // *mut Self в heap
        unsafe {
            // initialize the fields DIRECTLY in the heap
            // slots one at a time, placement in a heap array
            let slots_ptr = addr_of_mut!((*ptr).slots) as *mut Slot<T>;
            for i in 0..CAP {
                slots_ptr.add(i).write(Slot::new(i));
            }
            // adding fields
            addr_of_mut!((*ptr).tail).write(Padding(AtomicUsize::new(0)));
            addr_of_mut!((*ptr).head).write(Padding(AtomicUsize::new(0)));
            addr_of_mut!((*ptr).send_waiters).write(SyncList::new());
            addr_of_mut!((*ptr).recv_waiters).write(SyncList::new());
            addr_of_mut!((*ptr).senders).write(AtomicUsize::new(1));
            addr_of_mut!((*ptr).receivers).write(AtomicUsize::new(1));
            addr_of_mut!((*ptr).tx_closed).write(AtomicBool::new(false));
            addr_of_mut!((*ptr).rx_closed).write(AtomicBool::new(false));
            // everything is initialized → assume_init
            uninit.assume_init()
        }
    }

    #[inline]
    fn push_inner(&self, value: T) -> Result<(), T> {
        let mask = CAP - 1;
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[tail & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(tail) as isize;
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    tail,
                    tail.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.data.with_mut(|p| unsafe { (*p).write(value) });
                        slot.sequence.store(tail.wrapping_add(1), Ordering::Release);
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
            if seq == pos {
                break;
            }
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
            #[cfg(loom)]
            {
                let _ = waits;
                shim::loom::yield_now(); // every wait step is a loom branch
            }
            #[cfg(not(loom))]
            if waits < SENDER_SPIN_COUNT {
                hint::spin_loop();
            } else if waits < YIELD_UNTIL {
                thread::yield_now();
            } else {
                thread::sleep(SLEEP);
            }
        }
        slot.data.with_mut(|p| unsafe { (*p).write(value) });
        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    #[inline]
    fn pop_inner(&self) -> Option<T> {
        let mask = CAP - 1;
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[head & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(head.wrapping_add(1)) as isize;
            let next_seq = head.wrapping_add(CAP);
            if diff == 0 {
                match self.head.compare_exchange_weak(
                    head,
                    head.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let v = slot.data.with_mut(|p| unsafe { (*p).assume_init_read() });
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

    #[inline]
    pub fn queued(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head).min(CAP)
    }
}

impl<T: Send + 'static, const CAP: usize> InnerChannel<T, CAP> for SeqInner<T, CAP> {
    #[inline]
    fn push(&self, v: T) -> Result<(), T> {
        self.push_inner(v)
    }
    #[inline]
    fn push_blocking(&self, v: T) -> Result<(), T> {
        self.push_fetch_add(v)
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
            // The tail snapshot could have gone bad: another producer moved tail,
            // the consumer sent head FOR our snapshot → used "negative"
            // (wrap in huge usize). Without guard CAP used panics in debug
            // (caught by Miri) and gives garbage free in release. Let's re-read it.
            let dist = tail.wrapping_sub(head) as isize;
            if dist < 0 {
                tail = self.tail.load(Ordering::Relaxed);
                continue;
            }

            let used = tail.wrapping_sub(head);
            if used >= CAP {
                return 0;
            }

            let k = buf.len().min(CAP - used);
            match self.tail.compare_exchange_weak(
                tail,
                tail.wrapping_add(k),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break (tail, k),
                Err(t) => tail = t,
            }
        };
        for i in 0..k {
            let p = pos.wrapping_add(i);
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
                shim::loom::yield_now();
            }
        }
        for (i, value) in buf.drain(..k).enumerate() {
            let p = pos.wrapping_add(i);
            let slot = &self.slots[p & mask];
            slot.data.with_mut(|p| unsafe { (*p).write(value) });
            slot.sequence.store(p.wrapping_add(1), Ordering::Release);
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
    #[inline]
    fn queued(&self) -> usize {
        self.queued()
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
        let count = tail.wrapping_sub(head);
        for n in 0..count {
            let pos = head.wrapping_add(n);
            let slot = &self.slots[pos & mask];
            if slot.sequence.load(Ordering::Relaxed) == pos.wrapping_add(1) {
                slot.data.with_mut(|p| unsafe { (*p).assume_init_drop() });
            }
        }
    }
}

impl<T, const CAP: usize> MultiConsumer for SeqInner<T, CAP> {}
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
    pub fn new() -> Arc<Self> {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        assert!(
            CAP >= 2,
            "sequence ring requires CAP >= 2: at CAP=1 the 'free' (seq == pos) and 'consumed' (seq == pos + CAP) conditions collide, allowing silent overwrite (found by loom algo_ring_1p1c_wraparound_exact)"
        );
        let mut uninit: Arc<MaybeUninit<Self>> = Arc::new_uninit();
        let ptr = Arc::get_mut(&mut uninit).unwrap().as_mut_ptr();
        unsafe {
            let slots_ptr = addr_of_mut!((*ptr).slots) as *mut Slot<T>;
            for i in 0..CAP {
                slots_ptr.add(i).write(Slot::new(i));
            }
            addr_of_mut!((*ptr).tail).write(Padding(AtomicUsize::new(0)));
            addr_of_mut!((*ptr).head).write(Padding(AtomicUsize::new(0)));
            addr_of_mut!((*ptr).send_waiters).write(SyncList::new());
            addr_of_mut!((*ptr).recv_waiters).write(SyncList::new());
            addr_of_mut!((*ptr).tx_closed).write(AtomicBool::new(false));
            addr_of_mut!((*ptr).rx_closed).write(AtomicBool::new(false));
            uninit.assume_init()
        }
    }

    #[inline]
    pub fn push(&self, v: T) -> Result<(), T> {
        let mask = CAP - 1;
        let tail = self.tail.load(Ordering::Relaxed);
        let slot = &self.slots[tail & mask];
        if slot.sequence.load(Ordering::Acquire) == tail {
            slot.data.with_mut(|p| unsafe { (*p).write(v) });
            slot.sequence.store(tail.wrapping_add(1), Ordering::Release);
            self.tail.store(tail.wrapping_add(1), Ordering::Relaxed);
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
        let next_seq = head.wrapping_add(CAP);
        if slot.sequence.load(Ordering::Acquire) == head.wrapping_add(1) {
            let v = slot.data.with_mut(|p| unsafe { (*p).assume_init_read() });
            slot.sequence.store(next_seq, Ordering::Release);
            self.head.store(head.wrapping_add(1), Ordering::Relaxed);
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

    fn push_batch(&self, buf: &mut Vec<T>) -> usize {
        if buf.is_empty() || self.rx_closed.load(Ordering::Acquire) {
            return 0;
        }
        let mask = CAP - 1;
        let pos = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        let used = pos.wrapping_sub(head);
        if used >= CAP {
            return 0;
        }
        let k = buf.len().min(CAP - used);
        for i in 0..k {
            let p = pos.wrapping_add(i);
            let slot = &self.slots[p & mask];
            while slot.sequence.load(Ordering::Acquire) != p {
                if self.rx_closed.load(Ordering::Acquire) {
                    // Action B has not started no slots are written, buf is not touched.
                    // Without seal (see push_fetch_add).
                    return 0;
                }
                // Window CAS head -> seq.store for the consumer. yield_now, as in
                // push_fetch_add: gives the scheduler a switch point.
                shim::loom::yield_now();
            }
        }
        for (i, value) in buf.drain(..k).enumerate() {
            let p = pos.wrapping_add(i);
            let slot = &self.slots[p & mask];
            slot.data.with_mut(|p| unsafe { (*p).write(value) });
            slot.sequence.store(p.wrapping_add(1), Ordering::Release);
        }
        self.tail.store(pos.wrapping_add(k), Ordering::Relaxed);
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

    #[inline]
    pub fn queued(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head)
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

    #[cfg(target_os = "macos")]
    #[inline]
    fn yield_before_park(&self) -> bool {
        false
    }
    #[inline]
    fn queued(&self) -> usize {
        self.queued()
    }
}

impl<T, const CAP: usize> Drop for SingleInner<T, CAP> {
    fn drop(&mut self) {
        let mask = CAP - 1;
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        let count = tail.wrapping_sub(head);
        for n in 0..count {
            let pos = head.wrapping_add(n);
            let slot = &self.slots[pos & mask];
            if slot.sequence.load(Ordering::Relaxed) == pos.wrapping_add(1) {
                slot.data.with_mut(|p| unsafe { (*p).assume_init_drop() });
            }
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for SingleInner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for SingleInner<T, CAP> {}

#[cfg(test)]
mod core_init_tests {
    use super::*;
    use std::{sync::atomic::Ordering, time::{Duration,Instant}};

    // Basic: fields are correct
    #[test]
    fn seq_basic() {
        let inner: Arc<SeqInner<u64, 16>> = SeqInner::new();
        assert_eq!(inner.tail.load(Ordering::Relaxed), 0);
        assert_eq!(inner.head.load(Ordering::Relaxed), 0);
        assert_eq!(inner.senders.load(Ordering::Relaxed), 1);
        assert_eq!(inner.receivers.load(Ordering::Relaxed), 1);
        assert!(!inner.tx_closed.load(Ordering::Relaxed));
        assert!(!inner.rx_closed.load(Ordering::Relaxed));
    }

    // Slots are initialized: sequence[i] == i
    #[test]
    fn seq_slots_initialized() {
        let inner: Arc<SeqInner<u64, 16>> = SeqInner::new();
        for i in 0..16 {
            assert_eq!(
                inner.slots[i].sequence.load(Ordering::Relaxed),
                i,
                "slot {i} sequence is incorrect, placement is broken"
            );
        }
    }

    // Functional: push/pop
    #[test]
    fn seq_push_pop() {
        let inner: Arc<SeqInner<u64, 16>> = SeqInner::new();
        assert!(inner.push(42).is_ok());
        assert!(inner.push(43).is_ok());
        assert_eq!(inner.pop(), Some(42));
        assert_eq!(inner.pop(), Some(43));
        assert_eq!(inner.pop(), None);
    }

    // Drop with unread string, Miri will check for release
    #[test]
    fn seq_drop_string() {
        let inner: Arc<SeqInner<String, 8>> = SeqInner::new();
        let _ = inner.push("hello".to_string());
        let _ = inner.push("world".to_string());
        assert_eq!(inner.pop(), Some("hello".to_string()));
        // "world" remains → Drop must drop, otherwise Miri: leak
        drop(inner);
    }

    // Average CAP under Miri still approx (256)
    #[test]
    fn seq_larger_cap() {
        let inner: Arc<SeqInner<u64, 256>> = SeqInner::new();
        assert_eq!(inner.slots[255].sequence.load(Ordering::Relaxed), 255);
    }

    // HUGE CAP, check NO stack overflow (heap).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn seq_huge_cap() {
        let inner: Arc<SeqInner<u64, 2_097_152>> = SeqInner::new();
        assert_eq!(inner.slots[0].sequence.load(Ordering::Relaxed), 0);
        assert_eq!(
            inner.slots[2_097_151].sequence.load(Ordering::Relaxed),
            2_097_151
        );
        assert!(inner.push(42).is_ok());
        assert_eq!(inner.pop(), Some(42));
    }

    #[test]
    fn single_basic() {
        let inner: Arc<SingleInner<u64, 16>> = SingleInner::new();
        assert_eq!(inner.tail.load(Ordering::Relaxed), 0);
        assert_eq!(inner.head.load(Ordering::Relaxed), 0);
        assert!(!inner.is_tx_closed());
        assert!(!inner.is_rx_closed());
    }

    #[test]
    fn single_slots_initialized() {
        let inner: Arc<SingleInner<u64, 16>> = SingleInner::new();
        for i in 0..16 {
            assert_eq!(
                inner.slots[i].sequence.load(Ordering::Relaxed),
                i,
                "slot {i} sequence is incorrect, placement is broken"
            );
        }
    }

    #[test]
    fn single_push_pop() {
        let inner: Arc<SingleInner<u64, 16>> = SingleInner::new();
        assert!(inner.push(42).is_ok());
        assert!(inner.push(43).is_ok());
        assert_eq!(inner.pop(), Some(42));
        assert_eq!(inner.pop(), Some(43));
        assert_eq!(inner.pop(), None);
    }

    #[test]
    fn single_push_full() {
        // CAP=2: fill, push should return Err when full
        let inner: Arc<SingleInner<u64, 2>> = SingleInner::new();
        assert!(inner.push(1).is_ok());
        assert!(inner.push(2).is_ok());
        // channel is full (CAP=2) → push will return Err
        assert_eq!(inner.push(3), Err(3));
        // release and push again
        assert_eq!(inner.pop(), Some(1));
        assert!(inner.push(3).is_ok());
    }

    #[test]
    fn single_drop_string() {
        // String in slots, Miri will check that Drop drops unread
        let inner: Arc<SingleInner<String, 8>> = SingleInner::new();
        let _ = inner.push("hello".to_string());
        let _ = inner.push("world".to_string());
        // pop one, the second REMAINS in the channel → Drop should drop it
        assert_eq!(inner.pop(), Some("hello".to_string()));
        drop(inner); // Miri: "world" (unread) dropped? Otherwise leaked
    }

    #[test]
    fn single_larger_cap() {
        let inner: Arc<SingleInner<u64, 256>> = SingleInner::new();
        assert_eq!(inner.slots[255].sequence.load(Ordering::Relaxed), 255);
        // функционально
        assert!(inner.push(100).is_ok());
        assert_eq!(inner.pop(), Some(100));
    }

    #[test]
    fn single_batch() {
        let inner: Arc<SingleInner<u64, 16>> = SingleInner::new();
        let mut buf = vec![1u64, 2, 3, 4];
        let pushed = inner.push_batch(&mut buf);
        assert_eq!(pushed, 4);
        assert!(buf.is_empty());

        let mut out = Vec::new();
        let (n, _closed) = inner.pop_batch(&mut out, 10);
        assert_eq!(n, 4);
        assert_eq!(out, vec![1, 2, 3, 4]); // FIFO order
    }

    #[test]
    #[should_panic(expected = "CAP >= 2")]
    fn seq_cap_one_rejected() {
        let _ = SeqInner::<u64, 1>::new();
    }

    #[test]
    #[should_panic(expected = "CAP >= 2")]
    fn single_cap_one_rejected() {
        let _ = SingleInner::<u64, 1>::new();
    }

    #[test]
    fn miri_two_holes_drop() {
        let inner: Arc<SeqInner<String, 2>> = SeqInner::new();
        inner.push_fetch_add("a".to_string()).unwrap();
        inner.push_fetch_add("b".to_string()).unwrap(); // full
        let mut blocked = Vec::new();
        for v in ["c", "d"] {
            let i = inner.clone();
            let v = v.to_string();
            blocked.push(thread::spawn(move || i.push_fetch_add(v)));
        }
        thread::yield_now();
        inner.rx_close();
        inner.notify_all_on_rx_close();
        for b in blocked {
            assert!(
                b.join().unwrap().is_err(),
                "rx closed push must return the value"
            );
        }
        drop(inner); // Drop: reclaim a,b; skip both abort holes
    }

    #[cfg(not(miri))]
    #[test]
    fn seq_push_batch_overreserved_does_not_spin() {
        let inner: Arc<SeqInner<u64, 2>> = SeqInner::new();
        inner.push_fetch_add(1).unwrap();
        inner.push_fetch_add(2).unwrap(); // full -> tail = head + CAP = 2
        // a blocked fetch_add sender would have bumped tail past head + CAP:
        inner.tail.fetch_add(1, Ordering::Relaxed); // tail = 3 = head(0) + CAP(2) + 1
        let done = Arc::new(AtomicBool::new(false));
        let inner2 = inner.clone();
        let done2 = done.clone();
        let h = thread::spawn(move || {
            let mut buf = vec![9u64];
            let n = inner2.push_batch(&mut buf);
            done2.store(true, Ordering::Release);
            n
        });
        thread::sleep(Duration::from_millis(300));
        assert!(
            done.load(Ordering::Acquire),
            "SeqInner::push_batch busy spins on over reservation instead of returning"
        );
        assert_eq!(
            h.join().unwrap(),
            0,
            "over reserved channel must accept nothing"
        );
    }

    #[test]
    fn miri_drops() {
        #[derive(Debug)]
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let inner: Arc<SeqInner<Tracked, 2>> = SeqInner::new();
        inner.push_fetch_add(Tracked(drops.clone())).unwrap(); // a
        inner.push_fetch_add(Tracked(drops.clone())).unwrap(); // b -> full
        let mut blocked = Vec::new();
        for _ in 0..2 {
            let i = inner.clone();
            let d = drops.clone();
            // c and d: fetch_add reserves a slot past head + CAP, then aborts.
            blocked.push(thread::spawn(move || i.push_fetch_add(Tracked(d))));
        }
        thread::yield_now();
        inner.rx_close();
        inner.notify_all_on_rx_close();
        for b in blocked {
            // the aborted push returns its value (Err), dropped here, not by the ring.
            assert!(
                b.join().unwrap().is_err(),
                "rx closed push must return the value"
            );
        }
        // a and b are still in the ring, only the two returned values dropped.
        assert_eq!(
            drops.load(Ordering::Relaxed),
            2,
            "returned aborted values must drop exactly once"
        );
        drop(inner); // ring Drop reclaims a, b and must SKIP both abort holes.
        assert_eq!(
            drops.load(Ordering::Relaxed),
            4,
            "ring must drop exactly a and b, skipping the abort holes"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn queued_never_reports_empty_while_busy() {
        const CAP_GUARD: usize = 1 << 20; // far above CAP, far below a wrapped value
        let inner: Arc<SeqInner<u64, 64>> = SeqInner::new();
        let stop = Arc::new(AtomicBool::new(false));

        let (p, s) = (inner.clone(), stop.clone());
        let prod = thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                let _ = p.push(1);
            }
        });
        let (c, s) = (inner.clone(), stop.clone());
        let cons = thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                let _ = c.pop();
            }
        });

        // The channel is hammered from both sides, so it is essentially never empty,
        // huge `queued()` would mean the subtraction underflowed.
        let mut fake = 0u64;
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_millis(300) {
            if inner.queued() > CAP_GUARD {
                fake += 1;
            }
        }
        stop.store(true, Ordering::Relaxed);
        prod.join().unwrap();
        cons.join().unwrap();
        assert_eq!(fake, 0, "queued() underflowed and returned a wrapped value");
    }

    #[cfg(not(miri))]
    #[test]
    fn queued_is_capped_by_cap_with_blocked_producers() {
        const CAP: usize = 4;
        let inner: Arc<SeqInner<u64, CAP>> = SeqInner::new();
        for i in 0..CAP as u64 {
            inner.push(i).unwrap(); // ring full: tail - head == CAP
        }
        // Two producers block inside push_fetch_add, each having already advanced `tail`,
        // the raw difference is now CAP + 2.
        let blocked: Vec<_> = (0..2)
            .map(|_| {
                let p = inner.clone();
                thread::spawn(move || p.push_fetch_add(99).unwrap())
            })
            .collect();
        // Wait until both reservations landed, then check the cap.
        let t0 = Instant::now();
        while inner.tail.load(Ordering::Relaxed) < CAP + 2 {
            assert!(t0.elapsed().as_secs() < 5, "producers never reserved");
            thread::yield_now();
        }
        assert_eq!(inner.queued(), CAP, "queued() must not exceed the capacity");
        // Free two slots so both blocked producers finish, then drain fully.
        for _ in 0..2 {
            inner.pop().unwrap();
        }
        for b in blocked {
            b.join().unwrap();
        }
        assert_eq!(inner.queued(), CAP, "full again after the blocked sends landed");
        for _ in 0..CAP {
            inner.pop().unwrap();
        }
        assert_eq!(inner.queued(), 0);
    }
}

#[cfg(test)]
mod core_wrap_tests {
    use super::*;

    fn warp_seq<T, const CAP: usize>(inner: &SeqInner<T, CAP>, start: usize) {
        inner.head.store(start, Ordering::Relaxed);
        inner.tail.store(start, Ordering::Relaxed);
        for i in 0..CAP {
            let pos = start.wrapping_add(i);
            inner.slots[pos & (CAP - 1)].sequence.store(pos, Ordering::Relaxed);
        }
    }

    fn warp_single<T, const CAP: usize>(inner: &SingleInner<T, CAP>, start: usize) {
        inner.head.store(start, Ordering::Relaxed);
        inner.tail.store(start, Ordering::Relaxed);
        for i in 0..CAP {
            let pos = start.wrapping_add(i);
            inner.slots[pos & (CAP - 1)].sequence.store(pos, Ordering::Relaxed);
        }
    }

    #[test]
    fn seq_fifo_across_wrap() {
        let inner: Arc<SeqInner<u64, 4>> = SeqInner::new();
        warp_seq(&inner, usize::MAX - 2);
        let mut got = Vec::new();
        for v in 1..=10u64 {
            loop {
                match inner.push(v) {
                    Ok(()) => break,
                    Err(_) => got.push(inner.pop().unwrap()),
                }
            }
        }
        while let Some(v) = inner.pop() {
            got.push(v);
        }
        assert_eq!(got, (1..=10).collect::<Vec<u64>>(), "FIFO broken across the wrap");
    }

    #[test]
    fn single_fifo_across_wrap() {
        let inner: Arc<SingleInner<u64, 4>> = SingleInner::new();
        warp_single(&inner, usize::MAX - 2);
        let mut got = Vec::new();
        for v in 1..=10u64 {
            loop {
                match inner.push(v) {
                    Ok(()) => break,
                    Err(_) => got.push(inner.pop().unwrap()),
                }
            }
        }
        while let Some(v) = inner.pop() {
            got.push(v);
        }
        assert_eq!(got, (1..=10).collect::<Vec<u64>>(), "FIFO broken across the wrap");
    }

    #[test]
    fn drop_reclaims_across_wrap() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let inner: Arc<SeqInner<Tracked, 2>> = SeqInner::new();
            warp_seq(&inner, usize::MAX - 1);
            assert!(inner.push(Tracked(drops.clone())).is_ok());
            assert!(inner.push(Tracked(drops.clone())).is_ok()); // tail wraps to 1
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2, "SeqInner leaked at the wrap");

        let drops2 = Arc::new(AtomicUsize::new(0));
        {
            let inner: Arc<SingleInner<Tracked, 2>> = SingleInner::new();
            warp_single(&inner, usize::MAX - 1);
            assert!(inner.push(Tracked(drops2.clone())).is_ok());
            assert!(inner.push(Tracked(drops2.clone())).is_ok());
        }
        assert_eq!(drops2.load(Ordering::Relaxed), 2, "SingleInner leaked at the wrap");
    }

    #[test]
    fn queued_across_wrap() {
        let inner: Arc<SeqInner<u64, 4>> = SeqInner::new();
        warp_seq(&inner, usize::MAX - 1);
        inner.push(1).unwrap();
        inner.push(2).unwrap();
        assert_eq!(inner.queued(), 2, "queued() lied at the wrap");
        assert!(!inner.is_empty());
    }

    #[test]
    fn push_batch_fifo_across_wrap() {
        let inner: Arc<SingleInner<u64, 4>> = SingleInner::new();
        warp_single(&inner, usize::MAX - 1);
        let mut buf = vec![1u64, 2, 3, 4];
        assert_eq!(InnerChannel::push_batch(&*inner, &mut buf), 4);
        let mut out = Vec::new();
        let (n, _) = inner.pop_batch(&mut out, 8);
        assert_eq!(n, 4);
        assert_eq!(out, vec![1, 2, 3, 4]);
    }

    #[cfg(not(miri))]
    #[test]
    fn seq_push_batch_overreserved_at_wrap_returns() {
        let inner: Arc<SeqInner<u64, 2>> = SeqInner::new();
        warp_seq(&inner, usize::MAX - 5);
        inner.push(1).unwrap();
        inner.push(2).unwrap(); // full
        inner.tail.fetch_add(5, Ordering::Relaxed); // reservation wraps past zero
        let done = Arc::new(AtomicBool::new(false));
        let (i2, d2) = (inner.clone(), done.clone());
        let h = thread::spawn(move || {
            let mut b = vec![9u64];
            let n = i2.push_batch(&mut b);
            d2.store(true, Ordering::Release);
            n
        });
        thread::sleep(std::time::Duration::from_millis(300));
        assert!(done.load(Ordering::Acquire), "push_batch livelocked at the wrap");
        assert_eq!(h.join().unwrap(), 0, "over-reserved ring must accept nothing");
    }
}

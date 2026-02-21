use super::{
    cache::Padding,
    sync::SyncList,
};
use std::{
    array::from_fn,
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering
        },
    }
};

struct Slot<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>
}

impl <T> Slot<T> {

    const fn new(seq: usize) -> Self {
        Self { 
            sequence: AtomicUsize::new(seq), 
            data: UnsafeCell::new(MaybeUninit::uninit())
        }
    }
}


// Поддерживает MPMC
pub struct Inner<T, const CAP: usize> {
    slots: [Slot<T>; CAP],
    tail: Padding<AtomicUsize>,
    head: Padding<AtomicUsize>,
    send_waiters: SyncList,
    receiver_waiters: SyncList,
    // Счётчики ждущих — пропускаем SyncList::mutex на hot path
    send_wait_counter: Padding<AtomicUsize>,
    receiver_wait_counter: Padding<AtomicUsize>,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    tx_closed: AtomicBool,
    rx_closed: AtomicBool,
}

impl<T, const CAP: usize> Inner<T,CAP> {

    pub fn new() -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        Self { 
            slots: from_fn(|n| Slot::new(n)),
            tail: Padding(AtomicUsize::new(0)),
            head: Padding(AtomicUsize::new(0)), 
            send_waiters: SyncList::new(), 
            receiver_waiters: SyncList::new(), 
            send_wait_counter: Padding(AtomicUsize::new(0)), 
            receiver_wait_counter: Padding(AtomicUsize::new(0)), 
            senders: AtomicUsize::new(1), 
            receivers: AtomicUsize::new(1), 
            tx_closed: AtomicBool::new(false), 
            rx_closed: AtomicBool::new(false) 
        }
    }
    
    #[inline]
    pub fn push(&self, value: T) -> Result<(),T> {
        let mask = CAP - 1;
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop{
            let slot = &self.slots[tail & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - tail as isize;
            let next = tail + 1;
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    tail, 
                    next, 
                    Ordering::AcqRel, 
                    Ordering::Relaxed
                ) {
                    Ok(_) => {
                        unsafe {
                            (*slot.data.get()).write(value);
                        }
                        let _ = slot.sequence.store(next, Ordering::Release);
                        return Ok(());
                    }
                    Err(t) => tail = t,
                } 
            } else if diff < 0 {
                return Err(value); // Очередь полна
            } else {
                tail = self.tail.load(Ordering::Acquire);
            }
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mask = CAP - 1;
        let mut head = self.head.load(Ordering::Relaxed);
        loop{
            let slot = &self.slots[head & mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (head + 1) as isize;
            let next_head = head + 1;
            let next_seq = head + CAP;
            if diff == 0 {
                match self.head.compare_exchange_weak(
                    head, 
                    next_head, 
                    Ordering::AcqRel, 
                    Ordering::Relaxed
                ) 
                {
                    Ok(_) => {
                        let value  = unsafe {
                            (*slot.data.get()).assume_init_read()
                        };
                        slot.sequence.store(next_seq, Ordering::Release);
                        return Some(value);
                    },
                    Err(h) => head = h,
                }
            } else if diff < 0 {
                return None // Пусто
            } else {
                head = self.head.load(Ordering::Acquire);
            }
        }
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
    pub fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }

    #[inline]
    pub fn notify_receivers(&self) {
        if self.receiver_wait_counter.load(Ordering::SeqCst) > 0 {
            self.receiver_waiters.wake_one();
        }
    }

    #[inline]
    pub fn notify_senders(&self) {
        if self.send_wait_counter.load(Ordering::SeqCst) > 0 {
            self.send_waiters.wake_one();
        }
    }

    pub fn notify_all_on_tx_close(&self) {
        self.receiver_waiters.wake_all();
    }

    pub fn notify_all_on_rx_close(&self) {
        self.send_waiters.wake_all();
    } 

    pub fn receiver_add(&self,ordering: Ordering) -> usize {
        self.receivers.fetch_add(1, ordering)
    }

    pub fn receiver_wait_counter_add(&self,ordering: Ordering) -> usize {
        self.receiver_wait_counter.fetch_add(1, ordering)
    }

    pub fn receiver_wait_counter_sub(&self, ordering: Ordering) -> usize {
        self.receiver_wait_counter.fetch_sub(1, ordering)
    }

    pub fn receiver_waiters(&self) -> &SyncList{
        &self.receiver_waiters
    }

    pub fn receiver_sub(&self, ordering: Ordering) -> usize {
        self.receivers.fetch_sub(1, ordering)
    }  

    pub fn sender_add(&self, ordering: Ordering) -> usize {
        self.senders.fetch_add(1, ordering)
    }

    pub fn sender_wait_counter_add(&self, ordering: Ordering) -> usize {
        self.send_wait_counter.fetch_add(1, ordering)
    }

    pub fn sender_wait_counter_sub(&self, ordering: Ordering) -> usize {
        self.send_wait_counter.fetch_sub(1, ordering)
    }

    pub fn sender_waiters(&self) -> &SyncList{
        &self.send_waiters
    }

    pub fn sender_sub(&self, ordering: Ordering) -> usize {
        self.senders.fetch_sub(1, ordering)
    }

    pub fn tx_close(&self) {
        self.tx_closed.store(true, Ordering::Release);
    }

    pub fn rx_close(&self) {
        self.rx_closed.store(true, Ordering::Release);
    }

}

unsafe impl<T: Send, const CAP: usize> Send for Inner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for Inner<T, CAP> {}


// Поддерживает только SCSP
// Отличия от MPMC:
// - push: нет CAS на tail — только relaxed store (единственный producer)
// - pop:  нет CAS на head — только relaxed store (единственный consumer)
pub struct SingleInner<T, const CAP: usize>{
    slots: [Slot<T>; CAP],
    tail: Padding<AtomicUsize>,
    head: Padding<AtomicUsize>,
    send_waiters: SyncList,
    receiver_waiters: SyncList,
    // Счётчики ждущих — пропускаем SyncList::mutex на hot path
    send_wait_counter: Padding<AtomicUsize>,
    receiver_wait_counter: Padding<AtomicUsize>,
    tx_closed: AtomicBool,
    rx_closed: AtomicBool,
}

impl <T,const CAP: usize> SingleInner<T,CAP> {
    
    pub fn new() -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        Self { 
            slots: from_fn(|n| Slot::new(n)),
            tail: Padding(AtomicUsize::new(0)),
            head: Padding(AtomicUsize::new(0)), 
            send_waiters: SyncList::new(), 
            receiver_waiters: SyncList::new(), 
            send_wait_counter: Padding(AtomicUsize::new(0)), 
            receiver_wait_counter: Padding(AtomicUsize::new(0)),
            tx_closed: AtomicBool::new(false), 
            rx_closed: AtomicBool::new(false) 
        }
    }

    #[inline]
    pub fn push(&self, value: T) -> Result<(), T> {
        let mask = CAP - 1;
        let tail = self.tail.load(Ordering::Relaxed);
        let slot = &self.slots[tail & mask];
        let seq  = slot.sequence.load(Ordering::Acquire);
        let diff = seq as isize - tail as isize;
        let next = tail + 1;
        if diff == 0 {
            // Слот свободен
            unsafe { (*slot.data.get()).write(value) };
            slot.sequence.store(next, Ordering::Release);
            self.tail.store(next, Ordering::Relaxed);
            Ok(())
        } else {
            // diff < 0: Full
            Err(value)
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mask = CAP - 1;
        let head = self.head.load(Ordering::Relaxed); // единственный consumer — нет гонки
        let slot = &self.slots[head & mask];
        let seq  = slot.sequence.load(Ordering::Acquire);
        let diff = seq as isize - (head + 1) as isize;
        let next_head = head + 1;
        let next_seq = head + CAP;
        if diff == 0 {
            let value = unsafe { (*slot.data.get()).assume_init_read() };
            slot.sequence.store(next_seq, Ordering::Release);
            self.head.store(next_head, Ordering::Relaxed);
            Some(value)
        } else {
            // diff < 0: Empty
            None
        }
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
    pub fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }

    #[inline]
    pub fn notify_receivers(&self) {
        if self.receiver_wait_counter.load(Ordering::SeqCst) > 0 {
            self.receiver_waiters.wake_one();
        }
    }

    #[inline]
    pub fn notify_senders(&self) {
        if self.send_wait_counter.load(Ordering::SeqCst) > 0 {
            self.send_waiters.wake_one();
        }
    }

    pub fn notify_all_on_tx_close(&self) {
        self.receiver_waiters.wake_all();
    }

    pub fn notify_all_on_rx_close(&self) {
        self.send_waiters.wake_all();
    }

    pub fn receiver_wait_counter_add(&self,ordering: Ordering) -> usize {
        self.receiver_wait_counter.fetch_add(1, ordering)
    }

    pub fn receiver_wait_counter_sub(&self, ordering: Ordering) -> usize {
        self.receiver_wait_counter.fetch_sub(1, ordering)
    }

    pub fn receiver_waiters(&self) -> &SyncList{
        &self.receiver_waiters
    }

    pub fn sender_wait_counter_add(&self, ordering: Ordering) -> usize {
        self.send_wait_counter.fetch_add(1, ordering)
    }

    pub fn sender_wait_counter_sub(&self, ordering: Ordering) -> usize {
        self.send_wait_counter.fetch_sub(1, ordering)
    }

    pub fn sender_waiters(&self) -> &SyncList{
        &self.send_waiters
    }

    pub fn tx_close(&self) {
        self.tx_closed.store(true, Ordering::Release);
    }

    pub fn rx_close(&self) {
        self.rx_closed.store(true, Ordering::Release);
    }

}

unsafe impl<T: Send, const CAP: usize> Send for SingleInner<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for SingleInner<T, CAP> {}
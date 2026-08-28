use crate::shim::loom::{
    AtomicBool, AtomicUsize, AtomicWaker, Lock, Mutex, Ordering, PLMutex, Thread, UnsafeCell,
    fence, thread_current,
};

use std::{
    collections::VecDeque,
    marker::{PhantomData, PhantomPinned},
    mem::MaybeUninit,
    ptr::null_mut,
    sync::Arc,
    task::Waker,
};

pub struct Slot<T> {
    pub(crate) sequence: AtomicUsize,
    pub(crate) data: UnsafeCell<MaybeUninit<T>>,
}
impl<T> Slot<T> {
    #[cfg(not(loom))]
    pub const fn new(seq: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(seq),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
    #[cfg(loom)]
    pub fn new(seq: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(seq),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

pub struct AsyncSlot {
    pub(crate) waker: AtomicWaker,
    pub(crate) in_queue: AtomicBool,
    pub(crate) cancelled: AtomicBool,
    counted: AtomicBool,
}
impl AsyncSlot {
    fn new(waker: Waker) -> Arc<Self> {
        let s = Arc::new(Self {
            waker: AtomicWaker::new(),
            in_queue: AtomicBool::new(true),
            cancelled: AtomicBool::new(false),
            counted: AtomicBool::new(false),
        });
        s.waker.register(&waker);
        s
    }
}

pub enum SyncKind {
    Blocking(Thread),
}
impl SyncKind {
    fn wake(self) {
        match self {
            Self::Blocking(t) => t.unpark(),
        }
    }
}

pub struct SyncNode {
    kind: Option<SyncKind>,
    prev: *mut SyncNode,
    next: *mut SyncNode,
    in_list: bool,
    _pin: PhantomPinned,
}
impl SyncNode {
    pub fn new() -> Self {
        Self {
            kind: None,
            prev: null_mut(),
            next: null_mut(),
            in_list: false,
            _pin: PhantomPinned,
        }
    }
    pub fn new_blocking() -> Self {
        let mut s = Self::new();
        s.kind = Some(SyncKind::Blocking(thread_current()));
        s
    }
    pub fn is_in_list(&self) -> bool {
        self.in_list
    }
    pub fn set_in_list(&mut self, v: bool) {
        self.in_list = v;
    }
}
unsafe impl Send for SyncNode {}

struct SyncRawList {
    head: *mut SyncNode,
    tail: *mut SyncNode,
}
impl SyncRawList {
    const fn new() -> Self {
        Self {
            head: null_mut(),
            tail: null_mut(),
        }
    }
    fn push_back(&mut self, node: *mut SyncNode) {
        unsafe {
            debug_assert!(!(*node).is_in_list());
            (*node).next = null_mut();
            (*node).prev = self.tail;
            if self.tail.is_null() {
                self.head = node;
            } else {
                (*self.tail).next = node;
            }
            self.tail = node;
            (*node).set_in_list(true);
        }
    }
    fn remove(&mut self, node: *mut SyncNode) {
        unsafe {
            if !(*node).is_in_list() {
                return;
            }
            let prev = (*node).prev;
            let next = (*node).next;
            if prev.is_null() {
                self.head = next;
            } else {
                (*prev).next = next;
            }
            if next.is_null() {
                self.tail = prev;
            } else {
                (*next).prev = prev;
            }
            (*node).set_in_list(false);
            (*node).prev = null_mut();
            (*node).next = null_mut();
        }
    }
    fn pop_front(&mut self) -> Option<SyncKind> {
        unsafe {
            if self.head.is_null() {
                return None;
            }
            let node = self.head;
            self.remove(node);
            (*node).kind.take()
        }
    }
}
unsafe impl Send for SyncRawList {}

pub struct SyncGuard<'a> {
    list: &'a SyncList,
    node: *mut SyncNode,
    _node: PhantomData<&'a mut SyncNode>,
}

impl<'a> SyncGuard<'a> {
    fn new(list: &'a SyncList, node: &'a mut SyncNode) -> Self {
        let node: *mut SyncNode = node;
        list.push_blocking(node);
        Self {
            list,
            node,
            _node: PhantomData,
        }
    }
}

impl Drop for SyncGuard<'_> {
    fn drop(&mut self) {
        self.list.remove(self.node);
    }
}

/// Unified waiter list: parking_lot::Mutex for async, std::Mutex for blocking.
/// This is the best achieved version for unified blocking + async architecture.
pub struct SyncList {
    blocking: Mutex<SyncRawList>,
    blocking_count: AtomicUsize,
    async_waiters: PLMutex<VecDeque<Arc<AsyncSlot>>>,
    async_count: AtomicUsize,
    cancelled_in_queue: AtomicUsize,
}

impl SyncList {
    pub fn new() -> Self {
        Self {
            blocking: Mutex::new(SyncRawList::new()),
            blocking_count: AtomicUsize::new(0),
            async_waiters: PLMutex::new(VecDeque::new()),
            async_count: AtomicUsize::new(0),
            cancelled_in_queue: AtomicUsize::new(0),
        }
    }

    pub fn has_waiters(&self) -> bool {
        self.async_count.load(Ordering::Acquire) > 0
            || self.blocking_count.load(Ordering::Acquire) > 0
    }

    pub fn push_blocking(&self, node: *mut SyncNode) {
        {
            let mut list = self.blocking.lock_();
            list.push_back(node);
            self.blocking_count.fetch_add(1, Ordering::Relaxed);
        }
        fence(Ordering::SeqCst); // Dekker read
    }

    pub fn remove(&self, node: *mut SyncNode) {
        let mut list = self.blocking.lock_();
        let was_in = unsafe { (*node).is_in_list() };
        list.remove(node);
        if was_in {
            self.blocking_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Links node into the blocking list and unlinks it when the guard drops.
    pub fn sync_guard<'a>(&'a self, node: &'a mut SyncNode) -> SyncGuard<'a> {
        SyncGuard::new(self, node)
    }

    /// count.fetch_add(SeqCst) = Dekker read fence for async.
    pub fn push_async_slot(&self, waker: Waker) -> Arc<AsyncSlot> {
        let slot = AsyncSlot::new(waker);
        let for_queue = Arc::clone(&slot);
        {
            self.async_waiters.lock_().push_back(for_queue);
        }
        self.async_count.fetch_add(1, Ordering::SeqCst);
        slot
    }

    #[inline]
    pub fn cancel_async_slot(&self, slot: &Arc<AsyncSlot>) {
        slot.cancelled.store(true, Ordering::Release);
        slot.waker.take();
        // Lazy sweep: pop already cancelled slots from the FRONT only.
        // O(k) for k dead heads, no mid queue removal, FIFO intact.
        let mut guard = self.async_waiters.lock_();
        if slot.in_queue.load(Ordering::Acquire) && !slot.counted.swap(true, Ordering::AcqRel) {
            self.cancelled_in_queue.fetch_add(1, Ordering::Relaxed);
        }
        while let Some(head) = guard.front() {
            if head.cancelled.load(Ordering::Acquire) {
                let s = guard.pop_front().unwrap();
                s.in_queue.store(false, Ordering::Release);
                self.async_count.fetch_sub(1, Ordering::Relaxed);
                if s.counted.swap(false, Ordering::AcqRel) {
                    self.cancelled_in_queue.fetch_sub(1, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }

        let dead = self.cancelled_in_queue.load(Ordering::Relaxed);
        if dead > 0 && dead * 2 >= guard.len() {
            guard.retain(|s| {
                if s.cancelled.load(Ordering::Acquire) {
                    s.in_queue.store(false, Ordering::Release);
                    self.async_count.fetch_sub(1, Ordering::Relaxed);
                    if s.counted.swap(false, Ordering::AcqRel) {
                        self.cancelled_in_queue.fetch_sub(1, Ordering::Relaxed);
                    }
                    false
                } else {
                    true
                }
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_async_queue_len(&self) -> usize {
        self.async_waiters.lock_().len()
    }

    /// Called after SeqCst load in Inner::notify*.
    /// `async_hint` and `blocking_hint` are pre loaded counters (SeqCst/Acquire).
    /// Avoids redundant Acquire loads inside.
    #[inline]
    pub fn notify_one_if(&self, async_hint: usize, blocking_hint: usize) {
        if async_hint > 0
            && let Some(w) = self.pop_async()
        {
            w.wake();
            return;
        }
        if blocking_hint > 0 {
            let kind = {
                let mut list = self.blocking.lock_();
                let k = list.pop_front();
                if k.is_some() {
                    self.blocking_count.fetch_sub(1, Ordering::Relaxed);
                }
                k
            };
            if let Some(k) = kind {
                k.wake();
            }
        }
    }

    /// Wake up to `n` waiters (async first, then blocking).
    pub fn notify_n(&self, n: usize) {
        for _ in 0..n {
            let a = self.async_count.load(Ordering::Acquire);
            let b = self.blocking_count.load(Ordering::Acquire);
            if a == 0 && b == 0 {
                return;
            }
            self.notify_one_if(a, b);
        }
    }

    /// Plain notify_one for compatibility (Drop futures, etc.)
    pub fn notify_one(&self) {
        let a = self.async_count.load(Ordering::Acquire);
        let b = self.blocking_count.load(Ordering::Acquire);
        self.notify_one_if(a, b);
    }

    #[inline]
    pub fn async_count_seqcst(&self) -> usize {
        self.async_count.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn blocking_count_acquire(&self) -> usize {
        self.blocking_count.load(Ordering::Acquire)
    }

    fn pop_async(&self) -> Option<Waker> {
        loop {
            let slot = {
                let mut guard = self.async_waiters.lock_();
                let s = guard.pop_front()?;
                s.in_queue.store(false, Ordering::Release);
                self.async_count.fetch_sub(1, Ordering::Relaxed);
                s
            }; // PLMutex released before wake()
            if slot.cancelled.load(Ordering::Acquire) {
                if slot.counted.swap(false, Ordering::AcqRel) {
                    self.cancelled_in_queue.fetch_sub(1, Ordering::Relaxed);
                }
                continue;
            }
            // The waker can be gone even though `cancelled` read false a moment
            // ago: `cancel_async_slot` sets the flag and takes the waker, and we
            // may have read the flag before its store landed. An empty slot means
            // the same thing as a cancelled one, so skip it and try the next.
            // Returning None here would strand every waiter still queued behind
            // this slot: `notify_one_if` treats None as "no async waiters" and
            // moves on to the blocking list. `wake_all` already does this.
            if let Some(w) = slot.waker.take() {
                return Some(w);
            }
        }
    }

    pub fn wake_all(&self) {
        let mut kinds = Vec::new();
        {
            let mut list = self.blocking.lock_();
            while let Some(k) = list.pop_front() {
                self.blocking_count.fetch_sub(1, Ordering::Relaxed);
                kinds.push(k);
            }
        }
        for k in kinds {
            k.wake();
        }
        let mut wakers = Vec::new();
        {
            let mut q = self.async_waiters.lock_();
            while let Some(slot) = q.pop_front() {
                slot.in_queue.store(false, Ordering::Release);
                self.async_count.fetch_sub(1, Ordering::Relaxed);
                if slot.counted.swap(false, Ordering::AcqRel) {
                    self.cancelled_in_queue.fetch_sub(1, Ordering::Relaxed);
                }
                if !slot.cancelled.load(Ordering::Acquire)
                    && let Some(w) = slot.waker.take()
                {
                    wakers.push(w);
                }
            }
        }
        for w in wakers {
            w.wake();
        }
    }

    pub fn wake_one(&self) {
        self.notify_one();
    }
}

// Loom models: exhaustive schedule exploration for the cancel/notify pair.
// Run: RUSTFLAGS="--cfg loom" cargo test --release --lib loom_tests
#[cfg(all(loom, test))]
mod loom_tests {
    use super::*;
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize as StdAtomicUsize, Ordering as O};
    use std::task::{Wake, Waker};

    // Counts wake() calls; counters are read only after join(), so plain
    // std atomics are fine - they are not part of the explored synchronization.
    struct CountingWake(StdAtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: StdArc<Self>) {
            self.0.fetch_add(1, O::Relaxed);
        }
        fn wake_by_ref(self: &StdArc<Self>) {
            self.0.fetch_add(1, O::Relaxed);
        }
    }

    fn counting() -> (StdArc<CountingWake>, Waker) {
        let w = StdArc::new(CountingWake(StdAtomicUsize::new(0)));
        let waker = Waker::from(StdArc::clone(&w));
        (w, waker)
    }

    /// Regression for the lost wakeup bug: `notify_one` racing a cancel of
    /// the OTHER slot must still wake someone — a live waiter (b) is queued
    /// for the whole schedule. The pre fix `pop_async` (returning
    /// `slot.waker.take()` directly) has a schedule where the popped dying
    /// slot reads `cancelled == false` but its waker is already taken:
    /// pop_async returns None, notify_one_if gives up, zero wakes.
    #[test]
    fn notify_never_starves_live_waiter() {
        loom::model(|| {
            let list = StdArc::new(SyncList::new());
            let (wa, waker_a) = counting();
            let (wb, waker_b) = counting();
            let a = list.push_async_slot(waker_a);
            let _b = list.push_async_slot(waker_b);

            let l = StdArc::clone(&list);
            let t = loom::thread::spawn(move || {
                l.cancel_async_slot(&a);
            });

            list.notify_one();
            t.join().unwrap();

            assert!(
                wa.0.load(O::Relaxed) + wb.0.load(O::Relaxed) >= 1,
                "notify was swallowed by a dying slot while a live waiter was queued"
            );
        });
    }

    /// Lazy-sweep bookkeeping: two racing cancels must leave `async_count`
    /// equal to the actual queue length (every tombstone popped and
    /// decremented at most once, under the same mutex).
    #[test]
    fn concurrent_cancels_keep_count_consistent() {
        loom::model(|| {
            let list = StdArc::new(SyncList::new());
            let (_wa, waker_a) = counting();
            let (_wb, waker_b) = counting();
            let a = list.push_async_slot(waker_a);
            let b = list.push_async_slot(waker_b);

            let l1 = StdArc::clone(&list);
            let l2 = StdArc::clone(&list);
            let t1 = loom::thread::spawn(move || l1.cancel_async_slot(&a));
            let t2 = loom::thread::spawn(move || l2.cancel_async_slot(&b));
            t1.join().unwrap();
            t2.join().unwrap();

            let count = list.async_count.load(Ordering::SeqCst);
            let qlen = list.async_waiters.lock_().len();
            assert_eq!(
                count, qlen,
                "async_count drifted from real queue length after racing cancels"
            );
            assert!(qlen <= 2, "queue grew beyond the slots ever pushed");
        });
    }
}

#[cfg(all(not(loom), test))]
mod sync_guard_tests {
    use super::*;
    use futures::task::noop_waker;
    use std::panic;

    #[test]
    fn unwind_while_parked_unlinks_the_node() {
        let list = SyncList::new();

        let r = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let mut node = SyncNode::new_blocking();
            let _parked = list.sync_guard(&mut node);
            assert!(list.has_waiters(), "node should be linked while parked");
            panic!("unwind through the wait");
        }));
        assert!(r.is_err(), "the probe must actually panic");

        assert!(
            !list.has_waiters(),
            "node stayed linked after the unwind: the list holds a dangling pointer"
        );
        assert_eq!(list.blocking_count_acquire(), 0, "waiter count leaked");
        // A pop now would follow that stale pointer if it were still linked.
        assert!(list.blocking.lock_().pop_front().is_none());
    }

    /// The ordinary path still unlinks exactly once.
    #[test]
    fn normal_exit_unlinks_the_node() {
        let list = SyncList::new();
        {
            let mut node = SyncNode::new_blocking();
            let _parked = list.sync_guard(&mut node);
            assert_eq!(list.blocking_count_acquire(), 1);
        }
        assert_eq!(list.blocking_count_acquire(), 0);
        assert!(!list.has_waiters());
    }

    #[test]
    fn cancelled_slots_behind_a_live_waiter_are_swept() {
        let list = SyncList::new();
        let live = list.push_async_slot(noop_waker());
        let dead: Vec<_> = (0..64)
            .map(|_| list.push_async_slot(noop_waker()))
            .collect();
        for s in &dead {
            list.cancel_async_slot(s);
        }
        // Ratio only trigger: after every cancel at most the live waiter
        // remains is nothing accumulates at all.
        assert_eq!(
            list.debug_async_queue_len(),
            1,
            "tombstones piled up behind the live head"
        );
        list.cancel_async_slot(&live);
        assert_eq!(list.debug_async_queue_len(), 0);
    }
}

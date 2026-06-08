use atomic_waker::AtomicWaker;
use parking_lot::Mutex as PLMutex;
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    marker::PhantomPinned,
    mem::MaybeUninit,
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence},
    sync::{Arc, Mutex},
    task::Waker,
    thread::{self, Thread},
};

pub struct Slot<T> {
    pub(crate) sequence: AtomicUsize,
    pub(crate) data: UnsafeCell<MaybeUninit<T>>,
}
impl<T> Slot<T> {
    pub const fn new(seq: usize) -> Self {
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
}
impl AsyncSlot {
    fn new(waker: Waker) -> Arc<Self> {
        let s = Arc::new(Self {
            waker: AtomicWaker::new(),
            in_queue: AtomicBool::new(true),
            cancelled: AtomicBool::new(false),
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
        s.kind = Some(SyncKind::Blocking(thread::current()));
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

/// Unified waiter list: parking_lot::Mutex for async, std::Mutex for blocking.
/// This is the best achieved version for unified blocking + async architecture.
pub struct SyncList {
    blocking: Mutex<SyncRawList>,
    blocking_count: AtomicUsize,
    async_waiters: PLMutex<VecDeque<Arc<AsyncSlot>>>,
    async_count: AtomicUsize,
}

impl SyncList {
    pub fn new() -> Self {
        Self {
            blocking: Mutex::new(SyncRawList::new()),
            blocking_count: AtomicUsize::new(0),
            async_waiters: PLMutex::new(VecDeque::new()),
            async_count: AtomicUsize::new(0),
        }
    }

    pub fn has_waiters(&self) -> bool {
        self.async_count.load(Ordering::Acquire) > 0
            || self.blocking_count.load(Ordering::Acquire) > 0
    }

    pub fn push_blocking(&self, node: *mut SyncNode) {
        {
            let mut list = self.blocking.lock().unwrap_or_else(|e| e.into_inner());
            list.push_back(node);
            self.blocking_count.fetch_add(1, Ordering::Relaxed);
        }
        fence(Ordering::SeqCst); // Dekker read
    }

    pub fn remove(&self, node: *mut SyncNode) {
        let mut list = self.blocking.lock().unwrap_or_else(|e| e.into_inner());
        let was_in = unsafe { (*node).is_in_list() };
        list.remove(node);
        if was_in {
            self.blocking_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// count.fetch_add(SeqCst) = Dekker read fence for async.
    pub fn push_async_slot(&self, waker: Waker) -> Arc<AsyncSlot> {
        let slot = AsyncSlot::new(waker);
        let for_queue = Arc::clone(&slot);
        {
            self.async_waiters.lock().push_back(for_queue);
        }
        self.async_count.fetch_add(1, Ordering::SeqCst);
        slot
    }

    #[inline]
    pub fn cancel_async_slot(slot: &Arc<AsyncSlot>) {
        slot.cancelled.store(true, Ordering::Release);
        slot.waker.take();
    }

    /// Called after SeqCst load in Inner::notify*.
    /// `async_hint` and `blocking_hint` are pre loaded counters (SeqCst/Acquire).
    /// Avoids redundant Acquire loads inside.
    #[inline]
    pub fn notify_one_if(&self, async_hint: usize, blocking_hint: usize) {
        if async_hint > 0 {
            if let Some(w) = self.pop_async() {
                w.wake();
                return;
            }
        }
        if blocking_hint > 0 {
            let kind = {
                let mut list = self.blocking.lock().unwrap_or_else(|e| e.into_inner());
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
                let mut q = self.async_waiters.lock();
                let s = q.pop_front()?;
                s.in_queue.store(false, Ordering::Release);
                self.async_count.fetch_sub(1, Ordering::Relaxed);
                s
            }; // PLMutex released before wake()
            if slot.cancelled.load(Ordering::Acquire) {
                continue;
            }
            return slot.waker.take();
        }
    }

    pub fn wake_all(&self) {
        let mut kinds = Vec::new();
        {
            let mut list = self.blocking.lock().unwrap_or_else(|e| e.into_inner());
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
            let mut q = self.async_waiters.lock();
            while let Some(slot) = q.pop_front() {
                slot.in_queue.store(false, Ordering::Release);
                self.async_count.fetch_sub(1, Ordering::Relaxed);
                if !slot.cancelled.load(Ordering::Acquire) {
                    if let Some(w) = slot.waker.take() {
                        wakers.push(w);
                    }
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

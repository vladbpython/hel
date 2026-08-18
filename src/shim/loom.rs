use core::ops::DerefMut;

// atomics + fence

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, fence};
#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, fence};

pub(crate) use std::sync::atomic::Ordering;

// mutex aliases

#[cfg(not(loom))]
pub(crate) type Mutex<T> = std::sync::Mutex<T>;
#[cfg(not(loom))]
pub(crate) type PLMutex<T> = parking_lot::Mutex<T>;

#[cfg(loom)]
pub(crate) type Mutex<T> = loom::sync::Mutex<T>;
#[cfg(loom)]
pub(crate) type PLMutex<T> = loom::sync::Mutex<T>;

// unified lock
// The name is NOT `lock`: otherwise the inherent method of the mutex wins on the calling side and the trait is not involved at all.

pub(crate) trait Lock<T> {
    fn lock_(&self) -> impl DerefMut<Target = T> + '_;
}

#[cfg(not(loom))]
impl<T> Lock<T> for std::sync::Mutex<T> {
    #[inline]
    fn lock_(&self) -> impl DerefMut<Target = T> + '_ {
        self.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(not(loom))]
impl<T> Lock<T> for parking_lot::Mutex<T> {
    #[inline]
    fn lock_(&self) -> impl DerefMut<Target = T> + '_ {
        self.lock()
    }
}

#[cfg(loom)]
impl<T> Lock<T> for loom::sync::Mutex<T> {
    #[inline]
    fn lock_(&self) -> impl DerefMut<Target = T> + '_ {
        self.lock().unwrap()
    }
}

// AtomicWaker

#[cfg(not(loom))]
pub use atomic_waker::AtomicWaker;

#[cfg(loom)]
pub struct AtomicWaker {
    inner: loom::sync::Mutex<Option<std::task::Waker>>,
}

#[cfg(loom)]
impl AtomicWaker {
    pub fn new() -> Self {
        Self {
            inner: loom::sync::Mutex::new(None),
        }
    }
    pub fn register(&self, waker: &std::task::Waker) {
        *self.inner.lock().unwrap() = Some(waker.clone());
    }
    pub fn take(&self) -> Option<std::task::Waker> {
        self.inner.lock().unwrap().take()
    }
}

#[cfg(loom)]
pub(crate) fn yield_now() {
    loom::thread::yield_now();
}
#[cfg(not(loom))]
pub(crate) fn yield_now() {
    std::thread::yield_now();
}

#[cfg(loom)]
pub(crate) use loom::thread::{Thread, current as thread_current, park};
#[cfg(not(loom))]
pub(crate) use std::thread::{Thread, current as thread_current, park, park_timeout};
#[cfg(loom)]
pub(crate) fn park_timeout(_d: std::time::Duration) {
    park();
}

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;
 
#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);
 
#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    #[inline(always)]
    pub(crate) const fn new(data: T) -> UnsafeCell<T> {
        UnsafeCell(std::cell::UnsafeCell::new(data))
    }
    #[inline(always)]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

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
    state: loom::sync::atomic::AtomicUsize,
    waker: loom::cell::UnsafeCell<Option<std::task::Waker>>,
}

#[cfg(loom)]
impl AtomicWaker {
    const WAITING: usize = 0;
    const REGISTERING: usize = 0b01;
    const WAKING: usize = 0b10;

    pub fn new() -> Self {
        Self {
            state: loom::sync::atomic::AtomicUsize::new(Self::WAITING),
            waker: loom::cell::UnsafeCell::new(None),
        }
    }

    pub fn register(&self, waker: &std::task::Waker) {
        match self.state.compare_exchange(
            Self::WAITING,
            Self::REGISTERING,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.waker.with_mut(|w| unsafe { *w = Some(waker.clone()) });
                // Try to release the registration; failure means a taker set waking
                // while we held the cell - wakeup is ours to deliver:
                // take the fresh waker and wake it ourselves.
                match self.state.compare_exchange(
                    Self::REGISTERING,
                    Self::WAITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {}
                    Err(actual) => {
                        debug_assert_eq!(actual, Self::REGISTERING | Self::WAKING);
                        let w = self.waker.with_mut(|w| unsafe { (*w).take() }).unwrap();
                        self.state.swap(Self::WAITING, Ordering::AcqRel);
                        w.wake();
                    }
                }
            }
            // A take() is mid-flight: equivalent to being woken right after
            // registering, so deliver a wakeup to the NEW waker ourselves.
            // The SeqCst fence mirrors the real crate; without it the model
            // is WEAKER than production (false positives only, but noisy).
            Err(actual) if actual == Self::WAKING => {
                waker.wake_by_ref();
                fence(Ordering::SeqCst);
            }
            // Concurrent register: the protocol drops this registration.
            Err(_) => {}
        }
    }

    pub fn take(&self) -> Option<std::task::Waker> {
        match self.state.fetch_or(Self::WAKING, Ordering::AcqRel) {
            Self::WAITING => {
                let w = self.waker.with_mut(|w| unsafe { (*w).take() });
                self.state.fetch_and(!Self::WAKING, Ordering::Release);
                w
            }
            // Registration in flight (it will self wake) or another taker
            // holds the waking lock: nothing for us to deliver.
            _ => None,
        }
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

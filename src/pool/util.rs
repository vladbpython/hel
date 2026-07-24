use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A yield that works on any runtime. It returns Pending once, after asking to
/// be woken right away, which hands the runtime thread back to other tasks, and
/// then returns Ready. The async idle loop uses this instead of a blocking
/// thread::yield_now, so one idle worker does not keep a whole runtime thread to
/// itself when there are more consumers than runtime threads.
#[derive(Default)]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

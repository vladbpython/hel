use std::{
    hint::spin_loop,
    time::{Duration, Instant},
    thread,
};

#[inline(always)]
pub (crate) fn backoff(wait: &mut u32) {
    let spin_loops = 6; // cost of one yield_now: past about 126 pauses the winner is off-core and spinning cannot help
    *wait = wait.saturating_add(1);
    if *wait <= spin_loops{
        for _ in 0..(1u32 << *wait) {
            spin_loop();
        }
    } else {
        #[cfg(loom)]
        spin_loop();
        #[cfg(not(loom))]
        thread::yield_now();
    }
}

#[inline]
pub fn deadline_after(d: Duration) -> Instant {
    let now = Instant::now();
    let mut d = d;
    loop {
        if let Some(at) = now.checked_add(d) {
            return at;
        }
        d /= 2;
    }
}

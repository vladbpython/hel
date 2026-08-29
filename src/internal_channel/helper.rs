use std::time::{Duration, Instant};

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

use std::time::{Duration, Instant};

#[inline]
pub fn deadline_after(d: Duration) -> Option<Instant> {
    std::time::Instant::now().checked_add(d)
}

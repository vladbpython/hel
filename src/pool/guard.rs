use super::instance::{NONE, State};
use std::sync::atomic::Ordering;

pub struct OwnerGuard<'a> {
    state: &'a State,
    id: usize,
}

impl<'a> OwnerGuard<'a> {
    pub fn new(state: &'a State, id: usize) -> Self {
        Self { state, id }
    }
}

impl Drop for OwnerGuard<'_> {
    fn drop(&mut self) {
        // Runs on normal exit AND on unwind: release every shard this worker still owns,
        // so a handler panic cannot unused a shard.
        for shard in 0..self.state.shards() {
            let _ = self.state.owner(shard).compare_exchange(
                self.id,
                NONE,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
    }
}

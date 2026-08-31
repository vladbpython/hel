use super::instance::{NONE, State};
use std::sync::atomic::Ordering;

/// Shard's slice of `stats::Pool`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Shard {
    /// Items waiting in the shard's ring
    pub queued: usize,
    /// Worker that owns the shard right now, if any.
    pub owner: Option<usize>,
    /// Worker the monitor wants to own it
    pub desired: usize,
}

fn worker_id(raw: usize) -> Option<usize> {
    if raw == NONE { None } else { Some(raw) }
}

/// Worker's slice of `stats::Pool`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Worker {
    /// Inside user code for one item right now
    pub busy: bool,
    /// Marked by the stall monitor: busy and no completed item within `stall_takeover` budget.
    /// Always `false` when the feature is off.
    pub stalled: bool,
    /// Completed items heartbeat. Busy worker whose beats stop moving is exactly what the stall monitor looks for.
    pub beats: u64,
    /// Shard whose dequeued batch sits in this worker's buffer, if any.
    pub current_shard: Option<usize>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Pool {
    /// Total items completed by the handlers
    pub processed: u64,
    /// Handler/dead letter/Drop panics caught by the workers
    pub handler_panics: u64,
    /// Shards moved off a stalled owner by `stall_takeover`
    pub takeovers: u64,
    /// Current worker count the monitor aims for.
    pub active: usize,
    /// Peak `active` reached over the pool's lifetime
    pub max_active: usize,
    /// Stop flag: set by shutdown, cancellation, or completed autodrain
    pub stopped: bool,
    /// Every receiver has been dropped: a clean stop, or an async runtime
    /// tearing the worker tasks down without the pool ever being stopped -
    /// `stopped` alone cannot tell the second story. Once true,
    /// `queued` values are frozen history: whatever the mirror last
    /// saw before the channels died.
    pub channels_closed: bool,
    /// Worker spawns the OS refused during lazy scale up. The
    /// monitor retries next tick instead of panicking; a non zero value
    /// here is the only sign the pool wanted to grow and could not.
    pub spawn_failures: u64,
    /// Per worker that ever existed (index = worker id, length is at least
    /// `max_active` and always covers every `ShardStats::owner` and
    /// `ShardStats::desired`).
    /// Deliberately not truncated to `active`: worker
    /// outside the current window still holds its shards until it releases
    /// them - a stalled one forever - so `owner` may point past `active`,
    /// and the one worker an incident cares about most is exactly the
    /// retired-but-stuck one. The row count follows the paek, so a pool
    /// that once scaled to the ceiling keeps paying for the full list.
    pub workers: Vec<Worker>,
    /// Per shard (index = shard).
    pub shards: Vec<Shard>,
}

/// Use for debugging
pub(crate) fn snapshot(state: &State) -> Pool {
    let max_active = state.max_active();
    let shards: Vec<Shard> = (0..state.shards())
        .map(|s| Shard {
            queued: state.depth(s),
            owner: worker_id(state.owner(s).load(Ordering::Acquire)),
            desired: state.desired(s),
        })
        .collect();

    let rows = shards
        .iter()
        .flat_map(|s| [s.owner.map_or(0, |o| o + 1), s.desired + 1])
        .fold(max_active, usize::max);
    let workers = (0..rows)
        .map(|w| Worker {
            busy: state.worker_busy(w),
            stalled: state.worker_stalled(w),
            beats: state.beat_of(w),
            current_shard: worker_id(state.current_shard_of(w)),
        })
        .collect();

    Pool {
        processed: state.processed(),
        handler_panics: state.handler_panics(),
        takeovers: state.takeovers(),
        active: state.active(),
        max_active,
        stopped: state.is_stopped(),
        channels_closed: state.channels_closed(),
        spawn_failures: state.spawn_failures(),
        workers,
        shards,
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn snapshot_rows_cover_desired_targets() {
        let state = State::new(2, 1); // one worker, peak = 1
        state.set_desired(1, 7); // monitor points shard 1 at a future worker
        let s = snapshot(&state);
        assert!(
            s.shards[1].desired < s.workers.len(),
            "desired {} must be indexable into workers ({})",
            s.shards[1].desired,
            s.workers.len()
        );
    }

    #[test]
    fn snapshot_keeps_rows_for_workers_beyond_active() {
        let state = State::new(2, 2); // two workers exist
        state.owner(1).store(1, Ordering::Release); // worker 1 owns shard 1
        state.set_active(1); // scale down: worker 1 leaves the active window
        let s = snapshot(&state);
        assert_eq!(s.active, 1);
        assert_eq!(s.workers.len(), 2, "peak workers must keep their rows");
        let owner = s.shards[1].owner.expect("shard 1 has an owner");
        assert!(
            owner < s.workers.len(),
            "owner {owner} must be indexable into workers ({})",
            s.workers.len()
        );
        assert_eq!(owner, 1);
    }

    #[test]
    fn spawn_failures_reach_the_snapshot() {
        let state = State::new(1, 1);
        assert_eq!(snapshot(&state).spawn_failures, 0);
        state.note_spawn_failure();
        state.note_spawn_failure();
        assert_eq!(snapshot(&state).spawn_failures, 2);
    }
}

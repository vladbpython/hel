use crate::internal_channel::{receiver::Receiver, traits::InnerChannel};
use std::{
    hint,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

pub const IDLE_SPIN: u32 = 128;
pub const IDLE_YIELD: u32 = 256;
pub const IDLE_SLEEP: Duration = Duration::from_micros(50);
pub const NONE: usize = usize::MAX;

/// Pool configuration.
/// Scaling is driven by channel fill ratio (queue depth), not by guessed
/// throughput/latency. Sync vs async is chosen you call;
/// batch vs per item by the handler wrapper, so there are no mode fields to mix up.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Never fewer than this many active consumers.
    pub min_consumers: usize,
    /// Never more than this. For CPU work ≈ cores; for I/O it can be large.
    pub max_consumers: usize,
    /// Grow when average queue fill exceeds this fraction (0.0..=1.0).
    pub scale_up_fill: f64,
    /// Shrink when average queue fill drops below this fraction.
    pub scale_down_fill: f64,
    /// How often the monitor reevaluates load.
    pub sample_interval: Duration,
    /// Max items pulled from one shard per pass.
    pub batch_size: usize,
}

impl Config {
    /// Config with sensible defaults (fill thresholds 0.80/0.20).
    pub fn new(min: usize, max: usize) -> Self {
        Self {
            min_consumers: min.max(1),
            max_consumers: max.max(min).max(1),
            scale_up_fill: 0.80,
            scale_down_fill: 0.20,
            sample_interval: Duration::from_millis(100),
            batch_size: 64,
        }
    }

    pub fn scale_up_fill(mut self, value: f64) -> Self {
        self.scale_up_fill = value;
        self
    }

    pub fn scale_down_fill(mut self, value: f64) -> Self {
        self.scale_down_fill = value;
        self
    }

    pub fn sample_interval(mut self, value: Duration) -> Self {
        self.sample_interval = value;
        self
    }

    pub fn batch_size(mut self, value: usize) -> Self {
        self.batch_size = value;
        self
    }

    #[inline]
    fn decide(&self, current: usize, fill: f64) -> usize {
        if fill > self.scale_up_fill {
            // UP: jump to target
            let target = (fill * self.max_consumers as f64).ceil() as usize;
            target.clamp(current, self.max_consumers)
        } else if fill < self.scale_down_fill {
            // DOWN: STRICTLY one at a time, NOT a jump!
            current.saturating_sub(1).max(self.min_consumers)
        } else {
            current
        }
    }
}

/// Handle to a running pool
/// Handle to the pool's shared state: shard ownership, active count, stop flag,
/// metrics, and per shard close tracking for autodrain.
pub struct State {
    owner: Vec<AtomicUsize>,   // owner[shard] = worker id, or NONE
    active: AtomicUsize,       // current active consumers
    shutdown: AtomicBool,      // stop flag (set by cancel/shutdown or autodrain)
    processed: AtomicU64,      // total items processed
    shards: usize,             // shard count (immutable)
    closed: Vec<AtomicBool>,   // closed[shard] = shard empty AND senders dropped
    closed_count: AtomicUsize, // how many shards are closed
}

impl State {
    pub fn new(shards: usize, min: usize) -> Arc<Self> {
        Arc::new(Self {
            owner: (0..shards).map(|_| AtomicUsize::new(NONE)).collect(),
            active: AtomicUsize::new(min.max(1)),
            shutdown: AtomicBool::new(false),
            processed: AtomicU64::new(0),
            shards,
            closed: (0..shards).map(|_| AtomicBool::new(false)).collect(),
            closed_count: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn processed_add(&self, n: u64) -> u64 {
        self.processed.fetch_add(n, Ordering::Relaxed)
    }

    #[inline]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn set_active(&self, n: usize) {
        self.active.store(n, Ordering::Release);
    }

    #[inline]
    pub fn shards(&self) -> usize {
        self.shards
    }

    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Forced cancellation (signal). Sets stop; workers will finalize current batch and exit.
    /// Elements remaining in the queues are NOT processed (this is the meaning of cancellation).
    /// The current batch is NOT torn (safe).
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// The owner of the shard records its closure (empty + senders gone).
    /// When ALL shards are closed -> auto-shutdown (drainage completed).
    #[inline]
    pub(crate) fn mark_closed(&self, shard: usize) {
        // CAS false -> true: count the shard exactly once
        if self.closed[shard]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let prev = self.closed_count.fetch_add(1, Ordering::AcqRel);
            if prev + 1 == self.shards {
                // all shards are drained and closed -> the pool itself stops
                self.shutdown.store(true, Ordering::Release);
            }
        }
    }

    // claim_or_release is used.
    #[inline]
    pub(crate) fn owner(&self, shard: usize) -> &AtomicUsize {
        &self.owner[shard]
    }
}

// desired_owner: shard belongs to the worker (with % active).
#[inline]
pub(crate) fn desired_owner(shard: usize, active: usize) -> usize {
    shard % active.max(1)
}

/// One ownership step for worker id on shard. true = owns, drains.
/// Release then acquire -> shard reads ≤1 worker -> per shard FIFO upon resize.
#[inline]
pub(crate) fn claim_or_release(state: &State, id: usize, shard: usize, active: usize) -> bool {
    let want = desired_owner(shard, active) == id;
    let cur = state.owner(shard).load(Ordering::Acquire);
    if want {
        if cur == id {
            return true;
        } else if cur == NONE {
            return state
                .owner(shard)
                .compare_exchange(NONE, id, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok();
        }
    } else if cur == id {
        state.owner(shard).store(NONE, Ordering::Release);
    }
    false
}

//  metrics

#[inline]
fn fill_ratio<T, const CAP: usize, I>(receivers: &[Receiver<T, CAP, I>]) -> f64
where
    T: Send + 'static,
    I: InnerChannel<T, CAP>,
{
    let n = receivers.len();
    if n == 0 {
        return 0.0;
    }
    // each queued is limited by CAP (the queue is no longer than the capacity);
    // The saturating sum eliminates overflow even with garbage queued.
    let total: usize = receivers
        .iter()
        .map(|r| r.queued().min(CAP)) // clamp to container
        .fold(0usize, |acc, q| acc.saturating_add(q)); // without overflow
    total as f64 / (n * CAP) as f64
}

#[inline]
pub fn monitor<T, const CAP: usize, I>(
    cfg: &Config,
    state: &State,
    receivers: &[Receiver<T, CAP, I>],
) where
    T: Send + 'static,
    I: InnerChannel<T, CAP>,
{
    let fill = fill_ratio(receivers);
    let cur = state.active();
    let want = cfg.decide(cur, fill);
    if want != cur {
        state.set_active(want);
    }
}

/// Handle spin/yield phases; return true if the caller should sleep.
#[inline]
pub(crate) fn idle_backoff_step(streak: u32) -> bool {
    if streak < IDLE_SPIN {
        hint::spin_loop();
        false
    } else if streak < IDLE_YIELD {
        thread::yield_now();
        false
    } else {
        true
    }
}

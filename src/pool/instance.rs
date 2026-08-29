use crate::internal_channel::{receiver::Receiver, traits::InnerChannel};
use crate::shim::loom::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::{sync::Arc, time::Duration};

pub const IDLE_SPIN: u32 = 128;
pub const IDLE_YIELD: u32 = 256;
pub const IDLE_SLEEP: Duration = Duration::from_micros(50);
pub const NONE: usize = usize::MAX;

/// Pool configuration.
/// Scaling is driven by channel fill ratio (queue depth), not by guessed
/// throughput/latency. Sync vs async is chosen you call;
/// batch vs per item by the handler wrapper, so there are no mode fields to mix up.
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Never fewer than this many active consumers.
    pub min_consumers: usize,
    /// Never more than this. Value above the shard count is silently capped to it:
    /// hard has exactly one owner at a time, so an extra worker could never receive work anyway.
    pub max_consumers: usize,
    /// Grow when average queue fill exceeds this fraction (0.0..=1.0).
    pub scale_up_fill: f64,
    /// Shrink when average queue fill drops below this fraction.
    pub scale_down_fill: f64,
    /// How often the monitor reevaluates load.
    pub sample_interval: Duration,
    /// Max items pulled from one shard per pass.
    pub batch_size: usize,
    /// How full one shard must get before we treat it as a hotspot and give it
    /// its own consumer. This is separate from scale_up_fill, which looks at the
    /// average over all shards. One full shard among many empty ones barely
    /// changes the average, so we need a per shard threshold to notice it.
    pub hotspot_fill: f64,
    /// How much lighter another consumer must be before we move a shard to it,
    /// measured as a fraction of CAP. It stops placement from jumping around
    /// when loads only wiggle a little. 0.0 means move on any gain, which reacts
    /// fastest but thrashes most. A larger value keeps placement steady and tail
    /// latency calmer, at the cost of reacting to a new hotspot a bit slower.
    pub rebalance_margin: f64,
    /// Turns load aware placement on or off. When false the pool falls back to
    /// the plain shard % active mapping with no hotspot isolation.
    pub hotspot_isolation: bool,
    pub stall_takeover: Option<Duration>,
}

impl Config {
    /// Config with sensible defaults (fill thresholds 0.80/0.20).
    pub fn new(min: usize, max: usize) -> Self {
        let max = max.max(1);
        Self {
            min_consumers: min.clamp(1, max),
            max_consumers: max,
            scale_up_fill: 0.80,
            scale_down_fill: 0.20,
            sample_interval: Duration::from_millis(100),
            batch_size: 64,
            hotspot_fill: 0.80,
            rebalance_margin: 0.15,
            hotspot_isolation: true,
            stall_takeover: None,
        }
    }

    pub(crate) fn init(mut self) -> Self {
        self.max_consumers = self.max_consumers.max(1);
        self.min_consumers = self.min_consumers.clamp(1, self.max_consumers);
        self.batch_size = self.batch_size.max(1);
        self.sample_interval = self.sample_interval.max(Duration::from_millis(1));
        if let Some(d) = self.stall_takeover {
            self.stall_takeover = Some(d.max(self.sample_interval));
        }
        self
    }

    pub fn min_consumers(mut self, value: usize) -> Self {
        self.min_consumers = value;
        self
    }

    pub fn max_consumers(mut self, value: usize) -> Self {
        self.max_consumers = value;
        self
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

    pub fn hotspot_fill(mut self, value: f64) -> Self {
        self.hotspot_fill = value;
        self
    }

    pub fn rebalance_margin(mut self, value: f64) -> Self {
        self.rebalance_margin = value;
        self
    }

    pub fn hotspot_isolation(mut self, value: bool) -> Self {
        self.hotspot_isolation = value;
        self
    }

    pub fn stall_takeover(mut self, d: Duration) -> Self {
        self.stall_takeover = Some(d);
        self
    }

    #[inline]
    fn decide(&self, current: usize, fill: f64) -> usize {
        if fill > self.scale_up_fill {
            // UP: jump to target
            let target = (fill * self.max_consumers as f64).ceil() as usize;
            target
                .min(self.max_consumers)
                .max(current.min(self.max_consumers))
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
    desired: Vec<AtomicUsize>, // desired[shard] = worker id that should own it, picked by load
    active: AtomicUsize,       // current active consumers
    max_active: AtomicUsize,   // peak active reached
    shutdown: AtomicBool,      // stop flag (set by cancel/shutdown or autodrain)
    processed: AtomicU64,      // total items processed
    shards: usize,             // shard count (immutable)
    closed: Vec<AtomicBool>,   // closed[shard] = shard empty AND senders dropped
    closed_count: AtomicUsize, // how many shards are closed
    handler_panics: AtomicU64, // panics caught in worker loops (cold path only)
    beats: Vec<AtomicU64>,     // beats[worker] += 1 per completed item (stall detection)
    stalled: Vec<AtomicBool>,  // stalled[worker]: over the stall budget, not placement capacity
    busy: Vec<AtomicBool>, // busy[worker]: inside user code for one item (handler/dead_letter/drop)
    cur_shard: Vec<AtomicUsize>, // cur_shard[worker]: shard whose batch sits in the worker's buffer
}

impl State {
    pub fn new(shards: usize, min: usize) -> Arc<Self> {
        let active = min.max(1);
        Arc::new(Self {
            owner: (0..shards).map(|_| AtomicUsize::new(NONE)).collect(),
            // Start from the plain shard % active mapping, so the pool behaves
            // exactly like before until the monitor sees a hotspot and moves
            // shards around.
            desired: (0..shards)
                .map(|s| AtomicUsize::new(desired_owner(s, active)))
                .collect(),
            active: AtomicUsize::new(active),
            max_active: AtomicUsize::new(active),
            shutdown: AtomicBool::new(false),
            processed: AtomicU64::new(0),
            shards,
            closed: (0..shards).map(|_| AtomicBool::new(false)).collect(),
            closed_count: AtomicUsize::new(0),
            handler_panics: AtomicU64::new(0),
            // One per potential worker: ids are capped by the shard count.
            beats: (0..shards).map(|_| AtomicU64::new(0)).collect(),
            stalled: (0..shards).map(|_| AtomicBool::new(false)).collect(),
            busy: (0..shards).map(|_| AtomicBool::new(false)).collect(),
            cur_shard: (0..shards).map(|_| AtomicUsize::new(NONE)).collect(),
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
        if n > self.max_active.load(Ordering::Relaxed) {
            self.max_active.store(n, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn shards(&self) -> usize {
        self.shards
    }

    #[inline]
    pub fn handler_panics(&self) -> u64 {
        self.handler_panics.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn note_handler_panic(&self) -> u64 {
        self.handler_panics.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn beat(&self, id: usize) {
        if let Some(b) = self.beats.get(id) {
            b.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn beat_of(&self, id: usize) -> u64 {
        self.beats.get(id).map_or(0, |b| b.load(Ordering::Relaxed))
    }

    /// Worker `id` is inside user code (handler, dead_letter, or the item's Drop) for one item.
    /// Together with an unmoving beat this is the stall signal:
    /// idle worker is `busy == false` and is never treated as stalled, and a slow worker clears the moment the item ends.
    #[inline]
    pub(crate) fn set_worker_busy(&self, id: usize, v: bool) {
        if let Some(b) = self.busy.get(id) {
            b.store(v, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn worker_busy(&self, id: usize) -> bool {
        self.busy.get(id).is_some_and(|b| b.load(Ordering::Relaxed))
    }

    /// The shard whose batch currently sits in worker `id`'s buffer, or none.
    /// The takeover must not steal it while it holds items:
    /// worker's buffer only ever holds one shard's batch,
    /// so every other shard is stealable with zero data in flight and zero order at risk.
    #[inline]
    pub(crate) fn set_current_shard(&self, id: usize, shard: usize) {
        if let Some(s) = self.cur_shard.get(id) {
            s.store(shard, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn current_shard(&self, id: usize) -> usize {
        self.cur_shard.get(id).map_or(NONE, |s| s.load(Ordering::Relaxed))
    }

    #[inline]
    pub(crate) fn set_worker_stalled(&self, id: usize, v: bool) {
        if let Some(s) = self.stalled.get(id) {
            s.store(v, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn worker_stalled(&self, id: usize) -> bool {
        self.stalled.get(id).is_some_and(|s| s.load(Ordering::Relaxed))
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

    /// The worker that should own this shard, from the monitor's last placement.
    #[inline]
    pub(crate) fn desired(&self, shard: usize) -> usize {
        self.desired[shard].load(Ordering::Acquire)
    }

    /// Set the wanted owner of a shard. Called by the monitor. The Release store
    /// pairs with the Acquire load in desired and claim_or_release, so a worker
    /// that sees the new owner also sees everything the monitor did before it.
    #[inline]
    pub(crate) fn set_desired(&self, shard: usize, id: usize) {
        self.desired[shard].store(id, Ordering::Release);
    }
}

// desired_owner: shard belongs to the worker (with % active).
#[inline]
pub(crate) fn desired_owner(shard: usize, active: usize) -> usize {
    shard % active.max(1)
}

/// One ownership step for worker `id` on `shard`, given the wanted `target`
/// owner. Returns true if this worker now owns the shard and should drain it.
/// Only one worker can own a shard at a time, because that is settled by the
/// owner CAS below and not by how target was picked. So the order inside a shard
/// stays correct whether target comes from the plain modulo rule or the load
/// table.
#[inline]
pub(crate) fn claim_or_release_to(state: &State, id: usize, shard: usize, target: usize) -> bool {
    let want = target == id;
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
        let _ = state
            .owner(shard)
            .compare_exchange(id, NONE, Ordering::AcqRel, Ordering::Relaxed);
    }
    false
}

/// The path the worker loop uses. The wanted owner comes from the monitor's
/// placement table, which starts as the plain shard % active mapping until the
/// first rebalance. See [`claim_or_release_to`].
#[inline]
pub(crate) fn claim_or_release(state: &State, id: usize, shard: usize) -> bool {
    claim_or_release_to(state, id, shard, state.desired(shard))
}

//  metrics

/// Queue depth of every shard, capped at CAP. Read once and used both for the
/// average fill (how many consumers to run) and for placement (which shard goes
/// to which worker).
#[inline]
fn shard_loads<T, const CAP: usize, I>(receivers: &[Receiver<T, CAP, I>]) -> Vec<usize>
where
    T: Send + 'static,
    I: InnerChannel<T, CAP>,
{
    receivers.iter().map(|r| r.queued().min(CAP)).collect()
}

/// Spreads the shards over the active workers by load and writes the result
/// into desired[shard]. The busiest shards are placed first, each onto the
/// worker that has the least work so far.
///
/// Every shard stays with the worker that owns it now (or its shard % active
/// home if that owner has been retired), and moves to another worker only when
/// that worker has at least `margin` fewer items. So an even load keeps the
/// current mapping and does not thrash, a hot shard still pushes its neighbours
/// off its worker because the load gap is far bigger than the margin, and small
/// load changes below the margin leave the owners alone, which keeps tail
/// latency steady.
///
/// Only worker ids below `active` ever get a shard, so a retired worker sees it
/// is no longer wanted, releases its shards, and goes idle.
#[inline]
fn rebalance(state: &State, loads: &[usize], active: usize, margin: usize) {
    let shards = loads.len();
    let active = active.max(1);

    // Busiest shard first. The sort is stable, so equal loads keep their shard
    // order and the all equal case is deterministic and lands on the home
    // worker.
    let mut order: Vec<usize> = (0..shards).collect();
    order.sort_by(|&a, &b| loads[b].cmp(&loads[a]));

    let mut worker_load = vec![0usize; active];
    for &s in &order {
        // Keep the current owner for stickiness. If it has been retired or
        // is STALLED (over the stall-takeover budget), fall back: a stalled
        // worker must never be handed work it cannot pick up.
        let anchor = {
            let cur = state.desired(s);
            if cur < active && !state.worker_stalled(cur) {
                cur
            } else {
                let home = s % active;
                if !state.worker_stalled(home) {
                    home
                } else {
                    // First live worker; if EVERY active worker is stalled,
                    // keep the home - the takeover pass raises `active` and
                    // brings up a fresh one.
                    (0..active).find(|&w| !state.worker_stalled(w)).unwrap_or(home)
                }
            }
        };
        let mut best = anchor;
        let mut best_load = worker_load[anchor];
        for (w, &l) in worker_load.iter().enumerate() {
            if state.worker_stalled(w) {
                continue;
            }
            // Move only when another worker is lighter by more than the margin.
            if l + margin < best_load {
                best = w;
                best_load = l;
            }
        }
        worker_load[best] += loads[s];
        state.set_desired(s, best);
    }
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
    let loads = shard_loads(receivers);
    let n = loads.len();
    if n == 0 {
        return;
    }

    // The average fill over all shards tells us how many workers we want.
    let total: usize = loads.iter().fold(0usize, |acc, q| acc.saturating_add(*q));
    let fill = total as f64 / (n * CAP) as f64;
    let cur = state.active();
    let mut want = cfg.decide(cur, fill);

    // Hotspot handling off: just scale by the average fill and use the plain
    // shard % active mapping, like the pool did before.
    if !cfg.hotspot_isolation {
        if want != cur {
            state.set_active(want);
        }
        let active = want.max(1);
        for s in 0..n {
            state.set_desired(s, s % active);
        }
        return;
    }

    // A shard that is almost full is a hotspot. The average fill hides it,
    // because one full shard among many empty ones averages to about zero, so we
    // check each shard on its own against hotspot_fill. Every hot shard wants a
    // consumer of its own plus one more to drain the rest, so we ask for at
    // least hot + 1 workers. More workers help only because placement then pins
    // the hot shard to a worker of its own. Without placement this floor would
    // do nothing.
    let hot = loads
        .iter()
        .filter(|&&q| q as f64 > cfg.hotspot_fill * CAP as f64)
        .count();
    if hot > 0 {
        want = want.max((hot + 1).min(cfg.max_consumers));
    }

    // Stalled workers are not capacity (audit 2, N2): a scale-down that
    // counted them used to hand every shard back to the stuck owner and
    // oscillate against the takeover pass. Grow `want` by the number of
    // stalled workers inside the window so the live ones remain enough.
    let stalled_inside = (0..want).filter(|&w| state.worker_stalled(w)).count();
    want = want.saturating_add(stalled_inside).min(cfg.max_consumers);

    if want != cur {
        state.set_active(want);
    }

    // Place the shards onto the wanted workers by load. The margin is a dead
    // zone in items that stops placement from thrashing on small load changes.
    let margin = (cfg.rebalance_margin * CAP as f64) as usize;
    rebalance(state, &loads, want, margin);
}

#[inline]
pub(crate) fn stall_takeover_pass<T, const CAP: usize, I>(
    cfg: &Config,
    state: &State,
    receivers: &[Receiver<T, CAP, I>],
    prev: &mut [u64],
    stall: &mut [u32],
    spawned: usize,
) -> bool
where
    T: Send + 'static,
    I: InnerChannel<T, CAP>,
{
    let Some(d) = cfg.stall_takeover else {
        return false;
    };
    if !I::MULTI_CONSUMER {
        return false;
    }
    let ticks = stall_ticks(d, cfg.sample_interval);

    for (w, p) in prev.iter_mut().enumerate().take(spawned) {
        let b = state.beat_of(w);
        if b != *p || !state.worker_busy(w) {
            *p = b;
            stall[w] = 0;
            state.set_worker_stalled(w, false);
        } else {
            stall[w] = stall[w].saturating_add(1);
            if stall[w] >= ticks {
                state.set_worker_stalled(w, true);
            }
        }
    }

    let is_stalled = |w: usize| w < spawned && stall.get(w).is_some_and(|&s| s >= ticks);
    let mut took = false;
    for (s, r) in receivers.iter().enumerate() {
        let o = state.owner(s).load(Ordering::Acquire);
        if o == NONE || !is_stalled(o) {
            continue;
        }
        if state.current_shard(o) == s && r.queued() > 0 {
            continue;
        }
        // A live target: the first active worker that is not stalled. If all
        // of them are, ask for one more (the lazy spawner brings it up).
        let active = state.active();
        let target = (0..active).find(|&w| w != o && !is_stalled(w)).or_else(|| {
            let want = active.saturating_add(1).min(cfg.max_consumers);
            if want > active {
                state.set_active(want);
                Some(want - 1)
            } else {
                None
            }
        });
        let Some(target) = target else { continue };
        // CAS, not a store: the owner may be releasing it right now itself.
        if state
            .owner(s)
            .compare_exchange(o, NONE, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            state.set_desired(s, target);
            took = true;
        }
    }
    took
}


#[inline]
pub(crate) fn stall_ticks(d: Duration, interval: Duration) -> u32 {
    (d.as_nanos() / interval.as_nanos().max(1))
        .clamp(1, u32::MAX as u128) as u32
}

/// What an idle worker should do when it found no work this pass. The caller
/// picks the real action, so the sync path can block with thread::yield_now or
/// thread::sleep, while the async path gives the runtime thread back on its own.
/// A blocking thread::yield_now inside an async task does not hand the thread to
/// other tasks, so it would starve them when there are more consumers than
/// runtime threads.
pub(crate) enum IdlePhase {
    Spin,
    Yield,
    Sleep,
}

#[inline]
pub(crate) fn idle_phase(streak: u32) -> IdlePhase {
    if streak < IDLE_SPIN {
        IdlePhase::Spin
    } else if streak < IDLE_YIELD {
        IdlePhase::Yield
    } else {
        IdlePhase::Sleep
    }
}

#[cfg(all(test, not(loom)))]
mod placement_tests {
    use super::*;

    fn desired_vec(state: &State) -> Vec<usize> {
        (0..state.shards()).map(|s| state.desired(s)).collect()
    }

    #[test]
    fn init_floors_the_sample_interval() {
        let mut cfg = Config::new(1, 1);
        cfg.sample_interval = Duration::ZERO;
        assert!(cfg.init().sample_interval >= Duration::from_millis(1));
        
        let ok = Config::new(2, 8).sample_interval(Duration::from_millis(100)).init();
        assert_eq!(ok.sample_interval, Duration::from_millis(100));
    }

    /// An even or idle load must reproduce the plain shard % active mapping
    /// exactly, so there is no churn and no change in behaviour until a hotspot
    /// shows up.
    #[test]
    fn uniform_load_matches_modulo() {
        let state = State::new(8, 4);
        for &loads in &[[0usize; 8], [5usize; 8]] {
            rebalance(&state, &loads, 4, 0);
            let got = desired_vec(&state);
            let want: Vec<usize> = (0..8).map(|s| s % 4).collect();
            assert_eq!(got, want, "uniform load drifted from modulo placement");
        }
    }

    /// A single hot shard must end up alone on its own worker, and every other
    /// shard that shared that worker's home must move away.
    #[test]
    fn hot_shard_gets_isolated() {
        let state = State::new(4, 2);
        // shard 0 is hot; homes under active=2 are [0,1,0,1].
        let loads = [100usize, 0, 0, 0];
        rebalance(&state, &loads, 2, 0);
        let d = desired_vec(&state);

        // Hot shard keeps a worker to itself.
        let hot_worker = d[0];
        assert_eq!(
            d.iter().filter(|&&w| w == hot_worker).count(),
            1,
            "hot shard must not share its worker: {d:?}"
        );
        // Shard 2 shared shard 0's home (both % 2 == 0) and must have moved off.
        assert_ne!(
            d[2], hot_worker,
            "cold shard 2 did not migrate off the hotspot"
        );
    }

    /// Scaling down to `active` must never leave a wanted owner at or above
    /// active, or a retired worker would be asked to own a shard forever.
    #[test]
    fn placement_never_targets_retired_worker() {
        let state = State::new(16, 8);
        let loads = [3usize; 16];
        rebalance(&state, &loads, 3, 0); // shrink to 3 workers
        for (s, w) in desired_vec(&state).into_iter().enumerate() {
            assert!(w < 3, "shard {s} targets retired worker {w}");
        }
    }

    /// The margin keeps placement sticky. A load gap under the margin must not
    /// move a shard's owner, but a gap over the margin still does.
    #[test]
    fn rebalance_margin_damps_churn() {
        // Both shards currently on worker 0, worker 1 is idle.
        let state = State::new(2, 2);
        state.set_desired(0, 0);
        state.set_desired(1, 0);
        // No margin: the idle worker 1 takes the lighter shard S1.
        rebalance(&state, &[10, 8], 2, 0);
        assert_eq!(
            desired_vec(&state),
            vec![0, 1],
            "no margin: idle worker takes S1"
        );

        // Reset. With a margin wider than the 8 item gap, S1 stays put.
        state.set_desired(0, 0);
        state.set_desired(1, 0);
        rebalance(&state, &[10, 8], 2, 20);
        assert_eq!(
            desired_vec(&state),
            vec![0, 0],
            "margin: sub-threshold gap must not reshuffle owners"
        );
    }

    // Old monitor (scales by average fill) versus new monitor (per shard signal
    // plus placement).
    // Run with: cargo test --features pool old_vs_new_on_one_hot_shard -- --nocapture

    const CAP: usize = 1024;

    /// The old monitor. It scales the consumer count by the average fill and
    /// places shards with shard % active. Returns the active count and how many
    /// shards the hot shard's owner must also drain.
    fn old_monitor(cfg: &Config, cur: usize, loads: &[usize]) -> (usize, usize) {
        let n = loads.len();
        let total: usize = loads.iter().sum();
        let fill = total as f64 / (n * CAP) as f64;
        let active = cfg.decide(cur, fill).max(1);
        let hot_owner = 0 % active; // hot shard is index 0
        let co_tenants = (0..n).filter(|s| s % active == hot_owner).count();
        (active, co_tenants)
    }

    /// The new monitor. It uses the per shard hotspot signal for the count and
    /// load aware placement. Returns the active count and how many shards the
    /// hot shard's owner drains.
    fn new_monitor(cfg: &Config, cur: usize, loads: &[usize]) -> (usize, usize) {
        let n = loads.len();
        let total: usize = loads.iter().sum();
        let fill = total as f64 / (n * CAP) as f64;
        let hot = loads
            .iter()
            .filter(|&&q| q as f64 > cfg.hotspot_fill * CAP as f64)
            .count();
        let mut active = cfg.decide(cur, fill);
        if hot > 0 {
            active = active.max((hot + 1).min(cfg.max_consumers));
        }
        let state = State::new(n, 1);
        rebalance(&state, loads, active, 0);
        let hot_owner = state.desired(0);
        let co_tenants = (0..n).filter(|s| state.desired(*s) == hot_owner).count();
        (active, co_tenants)
    }

    #[test]
    fn old_vs_new_on_one_hot_shard() {
        // 8 shards, exactly one full (hot), the rest idle.
        let mut loads = [0usize; 8];
        loads[0] = CAP;

        eprintln!("\none hot shard out of 8 (CAP={CAP}):");
        eprintln!("scenario         | active | shards on hot owner");
        eprintln!("-----------------+--------+--------------------");

        // Scenario A: min=2, so both keep the same consumer count. The only
        // difference is placement, which shows the win is not about count.
        let cfg = Config::new(2, 8);
        let (ao, co) = old_monitor(&cfg, 2, &loads);
        let (an, cn) = new_monitor(&cfg, 2, &loads);
        eprintln!(
            "old (min=2)      |   {ao}    |   {co}  (hot + {} cold)",
            co - 1
        );
        eprintln!("new (min=2)      |   {an}    |   {cn}  (hot isolated)");
        assert_eq!(ao, an, "same consumer count in this scenario");
        assert!(
            co > 1,
            "old: hot shard shares its consumer with cold shards"
        );
        assert_eq!(cn, 1, "new: hot shard is alone on its consumer");

        // Scenario B: min=1, so the old pool cannot isolate at all, because one
        // worker owns everything. The new hotspot floor pulls in the second
        // worker that placement needs.
        let cfg = Config::new(1, 8);
        let (ao, co) = old_monitor(&cfg, 1, &loads);
        let (an, cn) = new_monitor(&cfg, 1, &loads);
        eprintln!("old (min=1)      |   {ao}    |   {co}  (no isolation)");
        eprintln!("new (min=1)      |   {an}    |   {cn}  (floor hot+1 isolates)");
        assert_eq!(ao, 1, "old: aggregate fill stayed below 0.80, no scale-up");
        assert_eq!(
            co, 8,
            "old: the sole worker drains every shard incl. the hot one"
        );
        assert_eq!(an, 2, "new: skew floor bumps active to hot+1");
        assert_eq!(cn, 1, "new: hot shard isolated");
        eprintln!();
    }

    #[test]
    fn init_repairs_broken_fields() {
        let mut cfg = Config::new(1, 4);
        cfg.min_consumers = 10;
        cfg.max_consumers = 0;
        cfg.batch_size = 0;
        let n = cfg.init();
        assert_eq!(n.max_consumers, 1, "zero max becomes one worker");
        assert_eq!(n.min_consumers, 1, "min pulled into 1..=max");
        assert_eq!(n.batch_size, 1, "zero batch pulls at least one item");

        // A well formed config passes through untouched.
        let ok = Config::new(2, 8).batch_size(32).init();
        assert_eq!(
            (ok.min_consumers, ok.max_consumers, ok.batch_size),
            (2, 8, 32),
            "valid config must not be altered"
        );
    }

    #[test]
    fn decide_with_current_above_max_does_not_panic() {
        let cfg = Config::new(1, 4);
        assert_eq!(cfg.decide(10, 0.9), 4, "scale back toward max, not panic");
        assert_eq!(cfg.decide(2, 0.9), 4, "scale up to target");
        assert_eq!(cfg.decide(3, 0.5), 3, "hold inside the band");
        assert_eq!(cfg.decide(3, 0.1), 2, "scale down one step");
    }

    #[test]
    fn stall_ticks_never_truncates_to_zero() {
        let interval = Duration::from_millis(1);
        assert_eq!(stall_ticks(Duration::from_millis(100), interval), 100);
        assert_eq!(stall_ticks(Duration::ZERO, interval), 1, "floor is one tick");
        let huge = interval * u32::MAX; // 2^32 - 1 ticks: still exact
        assert_eq!(stall_ticks(huge, interval), u32::MAX);
        let wrap = Duration::from_nanos(1_000_000u64 * 4_294_967_296); // 2^32 ticks
        assert_eq!(
            stall_ticks(wrap, interval),
            u32::MAX,
            "2^32 ticks must clamp, not wrap to zero"
        );
    }
}

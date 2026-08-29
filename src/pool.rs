pub mod errors;
pub(crate) mod guard;
pub mod handler;
pub mod instance;
pub(crate) mod loom_tests;
pub mod signal;
pub mod sync;
pub mod traits;
pub(crate) mod util;

use crate::{
    helper::panic::PanicReason,
    internal_channel::{receiver::Receiver, traits::InnerChannel},
};
use futures_util::FutureExt;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

const MONITOR_TICK: Duration = Duration::from_millis(10);

#[allow(clippy::too_many_arguments)]
fn spawn_async_worker<AR, T, const CAP: usize, I, H, D>(
    async_runtime: &AR,
    id: usize,
    cfg: instance::Config,
    shards: usize,
    state: Arc<instance::State>,
    receivers: Arc<Vec<Receiver<T, CAP, I>>>,
    handler: Arc<H>,
    dead_letter: Arc<D>,
) -> AR::JoinHandle
where
    AR: traits::AsyncRuntime,
    T: Send + 'static,
    I: InnerChannel<T, CAP> + Send + Sync + 'static,
    Receiver<T, CAP, I>: Send + Sync,
    H: traits::AsyncSlotHandler<T>,
    D: Fn(T, PanicReason) + Send + Sync + 'static,
{
    let ar = async_runtime.clone();
    async_runtime.spawn(async move {
        let _guard = guard::OwnerGuard::new(&state, id);
        let mut buf: Vec<T> = Vec::with_capacity(cfg.batch_size);
        let mut idle_streak: u32 = 0;
        while !state.is_stopped() {
            let mut done = false;
            for shard in 0..shards {
                if !instance::claim_or_release(&state, id, shard) {
                    continue;
                }
                let (n, dc) = receivers[shard].try_recv_batch(&mut buf, cfg.batch_size);
                if n > 0 {
                    done = true;
                    assert_eq!(
                        buf.len(),
                        n,
                        "worker buffer must hold exactly the dequeued batch"
                    );
                    state.set_current_shard(id, shard);
                    let mut batch = guard::CommitBatchGuard::new(&mut buf, &*dead_letter);
                    while let Some(item) = batch.next() {
                        let mut held = guard::CommitGuard::new(item, &*dead_letter);
                        let slot = held.slot();
                        state.set_worker_busy(id, true);
                        let r = AssertUnwindSafe(async { handler.handle(slot).await })
                            .catch_unwind()
                            .await;
                        let mut slot = held.disarm();
                        match r {
                            Ok(()) => {
                                // slot (taken or not) drops here: the item is committed/consumed.
                                _ = state.processed_add(1);
                            }
                            Err(err) => {
                                _ = state.note_handler_panic();
                                if let Some(poison) = slot.take() {
                                    // panic before take(): item is ours, hand it back zero loss.
                                    // A panicking  sink must not kill the worker.
                                    let _ = catch_unwind(AssertUnwindSafe(|| {
                                        dead_letter(poison, PanicReason(err))
                                    }));
                                }
                                // panic after take(): handler owned it.
                            }
                        }
                        // The item's own Drop is user code too: a panicking
                        // Drop must not unwind the worker (it killed the task and its shards forever).
                        let _ = catch_unwind(AssertUnwindSafe(move || drop(slot)));
                        // Stall-takeover heartbeat: one completed item.
                        state.beat(id);
                        state.set_worker_busy(id, false);
                    }
                    state.set_current_shard(id, instance::NONE);
                }
                if dc {
                    state.mark_closed(shard);
                }
            }
            if done {
                idle_streak = 0;
            } else {
                idle_streak = idle_streak.saturating_add(1);
                if id >= state.active() {
                    ar.sleep(MONITOR_TICK).await;
                } else {
                    match instance::idle_phase(idle_streak) {
                        instance::IdlePhase::Spin => std::hint::spin_loop(),
                        // Give the runtime thread back to other tasks instead
                        // of blocking it. See YieldNow.
                        instance::IdlePhase::Yield => util::YieldNow::default().await,
                        instance::IdlePhase::Sleep => ar.sleep(instance::IDLE_SLEEP).await,
                    }
                }
            }
        }
    })
}

/// The worker owns each item until the handler commits (`slot.take()`).
/// On a handler panic:
/// - item still in slot  -> delivered to `dead_letter` (zero loss),
/// - item already taken  -> consumed by contract, counted via `handler_panics` (the handler owned it at the panic point).
pub fn async_pool_slot<AR, T, const CAP: usize, I, H, D>(
    async_runtime: AR,
    cfg: instance::Config,
    receivers: Vec<Receiver<T, CAP, I>>,
    handler: H,
    dead_letter: D,
) -> Result<sync::AsyncPool<AR>,errors::PoolError>
where
    AR: traits::AsyncRuntime,
    T: Send + 'static,
    I: InnerChannel<T, CAP> + Send + Sync + 'static,
    Receiver<T, CAP, I>: Send + Sync,
    H: traits::AsyncSlotHandler<T>,
    D: Fn(T, PanicReason) + Send + Sync + 'static,
{
    let cfg = cfg.init();
    if receivers.is_empty(){
        return Err(errors::PoolError::ReceiverEmpty)
    }
    let shards = receivers.len();
    let cfg = {
        let mut cfg = cfg;
        cfg.max_consumers = cfg.max_consumers.min(shards);
        cfg.min_consumers = cfg.min_consumers.min(cfg.max_consumers);
        cfg
    };
    if cfg.stall_takeover.is_some() && cfg.max_consumers < 2 {
        return Err(
            errors::PoolError::Config(errors::ConfigError::StallTakeoverNeedsSpareWorker 
                { 
                    effective_max: cfg.max_consumers 
                }
            )
        );
    }
    let state = instance::State::new(shards, cfg.min_consumers);
    let receivers = Arc::new(receivers);
    let handler = Arc::new(handler);
    let dead_letter: Arc<D> = Arc::new(dead_letter);
    let workers = Arc::new(Mutex::new(Vec::with_capacity(
        cfg.min_consumers.saturating_add(1),
    )));

    for id in 0..cfg.min_consumers {
        let h = spawn_async_worker(
            &async_runtime,
            id,
            cfg,
            shards,
            state.clone(),
            receivers.clone(),
            handler.clone(),
            dead_letter.clone(),
        );
        sync::lock_workers(&workers).push(h);
    }

    // monitor worker (same as async_pool)
    {
        let state = state.clone();
        let receivers = receivers.clone();
        let handler = handler.clone();
        let dead_letter = dead_letter.clone();
        let ar = async_runtime.clone();
        let workers_store = workers.clone();
        let h = async_runtime.spawn(async move {
            let mut spawned = cfg.min_consumers;
            let mut beat_prev = vec![0u64; cfg.max_consumers];
            let mut stall_ticks = vec![0u32; cfg.max_consumers];
            while !state.is_stopped() {
                if sleep_interruptible_async(&ar, &state, cfg.sample_interval).await {
                    break;
                }
                instance::monitor(&cfg, &state, &receivers);
                let _ = instance::stall_takeover_pass(
                    &cfg,
                    &state,
                    &receivers,
                    &mut beat_prev,
                    &mut stall_ticks,
                    spawned,
                );
                // Top up to the new active target, `spawned` only grows,
                // so later scale down keeps the tasks (they park at the monitor tick cadence)
                // and re scale up is free.
                let want = state.active().min(cfg.max_consumers);
                while spawned < want && !state.is_stopped() {
                    let h = spawn_async_worker(
                        &ar,
                        spawned,
                        cfg,
                        shards,
                        state.clone(),
                        receivers.clone(),
                        handler.clone(),
                        dead_letter.clone(),
                    );
                    sync::lock_workers(&workers_store).push(h);
                    spawned += 1;
                }
            }
        });
        sync::lock_workers(&workers).push(h);
    }

    Ok(sync::AsyncPool::new(state, workers))
}

/// Interruptible async sleep: the same via AsyncRuntime::sleep (runtime parks the task).
async fn sleep_interruptible_async<AR: traits::AsyncRuntime>(
    async_runtime: &AR,
    state: &instance::State,
    total: Duration,
) -> bool {
    let mut slept = Duration::ZERO;
    while slept < total {
        if state.is_stopped() {
            return true;
        }
        let quant = MONITOR_TICK.min(total - slept);
        async_runtime.sleep(quant).await;
        slept += quant;
    }
    state.is_stopped()
}

fn spawn_sync_worker<T, const CAP: usize, I, H, D>(
    id: usize,
    cfg: instance::Config,
    shards: usize,
    state: Arc<instance::State>,
    receivers: Arc<Vec<Receiver<T, CAP, I>>>,
    handler: Arc<H>,
    dead_letter: Arc<D>,
) -> thread::JoinHandle<()>
where
    T: Send + 'static,
    I: InnerChannel<T, CAP> + Send + Sync + 'static,
    Receiver<T, CAP, I>: Send + Sync,
    H: traits::SyncSlotHandler<T>,
    D: Fn(T, PanicReason) + Send + Sync + 'static,
{
    thread::spawn(move || {
        let _guard = guard::OwnerGuard::new(&state, id);
        let mut buf: Vec<T> = Vec::with_capacity(cfg.batch_size);
        let mut idle_streak: u32 = 0;
        while !state.is_stopped() {
            let mut done = false;
            for shard in 0..shards {
                if !instance::claim_or_release(&state, id, shard) {
                    continue;
                }
                let (n, dc) = receivers[shard].try_recv_batch(&mut buf, cfg.batch_size);
                if n > 0 {
                    done = true;
                    state.set_current_shard(id, shard);
                    for item in buf.drain(..n) {
                        let mut slot = Some(item);
                        state.set_worker_busy(id, true);
                        let r = catch_unwind(AssertUnwindSafe(|| handler.handle(&mut slot)));
                        match r {
                            Ok(()) => {
                                // slot (taken or not) drops here: the item is committed/consumed.
                                _ = state.processed_add(1);
                            }
                            Err(err) => {
                                _ = state.note_handler_panic();
                                if let Some(poison) = slot.take() {
                                    // panic before take(): item is ours, hand it back zero loss.
                                    // A panicking sink must not kill the worker.
                                    let _ = catch_unwind(AssertUnwindSafe(|| {
                                        dead_letter(poison, PanicReason(err))
                                    }));
                                }
                                // panic after take(): handler owned it.
                            }
                        }
                        let _ = catch_unwind(AssertUnwindSafe(move || drop(slot)));
                        // Stall-takeover heartbeat: one completed item.
                        state.beat(id);
                        state.set_worker_busy(id, false);
                    }
                    state.set_current_shard(id, instance::NONE);
                }
                if dc {
                    state.mark_closed(shard);
                }
            }
            if done {
                idle_streak = 0;
            } else {
                idle_streak = idle_streak.saturating_add(1);
                if id >= state.active() {
                    thread::sleep(MONITOR_TICK);
                } else {
                    match instance::idle_phase(idle_streak) {
                        instance::IdlePhase::Spin => std::hint::spin_loop(),
                        instance::IdlePhase::Yield => thread::yield_now(),
                        instance::IdlePhase::Sleep => thread::sleep(instance::IDLE_SLEEP),
                    }
                }
            }
        }
    })
}

/// Sync twin of [`async_pool_slot`]: zero loss pool over the slot-based handler contract.
/// Same failure hierarchy:
/// - handler panic before `take()` -> item delivered to `dead_letter`,
/// - handler panic after `take()` -> consumed by contract, counted,
/// - `dead_letter` panic -> item dropped but counted, worker survives (bottom of the hierarchy: nobody left to hand it to).
///   Batching is preserved on the receiver side;
///   items are fed to the handler one at a time through the slot.
pub fn sync_pool_slot<T, const CAP: usize, I, H, D>(
    cfg: instance::Config,
    receivers: Vec<Receiver<T, CAP, I>>,
    handler: H,
    dead_letter: D,
) -> Result<sync::SyncPool,errors::PoolError>
where
    T: Send + 'static,
    I: InnerChannel<T, CAP> + Send + Sync + 'static,
    Receiver<T, CAP, I>: Send + Sync,
    H: traits::SyncSlotHandler<T>,
    D: Fn(T, PanicReason) + Send + Sync + 'static,
{
    let cfg = cfg.init();
    if receivers.is_empty(){
        return Err(errors::PoolError::ReceiverEmpty)
    }
    let shards = receivers.len();
    let cfg = {
        let mut cfg = cfg;
        cfg.max_consumers = cfg.max_consumers.min(shards);
        cfg.min_consumers = cfg.min_consumers.min(cfg.max_consumers);
        cfg
    };
    if cfg.stall_takeover.is_some() && cfg.max_consumers < 2 {
        return Err(
            errors::PoolError::Config(errors::ConfigError::StallTakeoverNeedsSpareWorker 
                { 
                    effective_max: cfg.max_consumers 
                }
            )
        );
    }
    let state = instance::State::new(shards, cfg.min_consumers);
    let receivers = Arc::new(receivers);
    let handler = Arc::new(handler);
    let dead_letter = Arc::new(dead_letter);
    let workers = Arc::new(Mutex::new(Vec::with_capacity(
        cfg.min_consumers.saturating_add(1),
    )));

    for id in 0..cfg.min_consumers {
        let h = spawn_sync_worker(
            id,
            cfg,
            shards,
            state.clone(),
            receivers.clone(),
            handler.clone(),
            dead_letter.clone(),
        );
        sync::lock_workers(&workers).push(h);
    }

    {
        let state = state.clone();
        let receivers = receivers.clone();
        let handler = handler.clone();
        let dead_letter = dead_letter.clone();
        let workers_store = workers.clone();
        let h = thread::spawn(move || {
            let mut spawned = cfg.min_consumers;
            let mut beat_prev = vec![0u64; cfg.max_consumers];
            let mut stall_ticks = vec![0u32; cfg.max_consumers];
            while !state.is_stopped() {
                if sleep_interruptible_sync(&state, cfg.sample_interval) {
                    break;
                }
                instance::monitor(&cfg, &state, &receivers);
                let _ = instance::stall_takeover_pass(
                    &cfg,
                    &state,
                    &receivers,
                    &mut beat_prev,
                    &mut stall_ticks,
                    spawned,
                );
                // Top up to the new active target, `spawned` only grows,
                // so later scale down keeps the tasks (they park at the monitor tick cadence)
                // and re scale up is free.
                let want = state.active().min(cfg.max_consumers);
                while spawned < want && !state.is_stopped() {
                    let h = spawn_sync_worker(
                        spawned,
                        cfg,
                        shards,
                        state.clone(),
                        receivers.clone(),
                        handler.clone(),
                        dead_letter.clone(),
                    );
                    sync::lock_workers(&workers_store).push(h);
                    spawned += 1;
                }
            }
        });
        sync::lock_workers(&workers).push(h);
    }

    Ok(sync::SyncPool::new(state, workers))
}

/// Interruptible sleep: sleeps total, but wakes up, check is_stopped every MONITOR_TICK.
/// Returns true if it is time to exit (stopped).
fn sleep_interruptible_sync(state: &instance::State, total: Duration) -> bool {
    let mut slept = Duration::ZERO;
    while slept < total {
        if state.is_stopped() {
            return true; // shutdown has arrived -> we leave immediately
        }
        let quant = MONITOR_TICK.min(total - slept);
        std::thread::sleep(quant);
        slept += quant;
    }
    state.is_stopped()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{
        mpmc::{ShardGroupCase, round_robin, shard_group, shard_key},
        nearest_power_of_two,
    };
    use std::{
        mem,
        collections::HashMap,
        sync::{
            Arc, Mutex, OnceLock,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration,Instant},
        thread,
    };

    const CAP: usize = nearest_power_of_two(16);

    #[cfg(miri)]
    const SCALE: u64 = 1;
    #[cfg(not(miri))]
    const SCALE: u64 = 100;

    // SYNC: all elements are processed exactly once (round_robin)
    #[test]
    fn sync_processed_once() {
        let (tx, rx) = round_robin::<u64, CAP>(2);
        let count = Arc::new(AtomicU64::new(0));
        let sum = Arc::new(AtomicU64::new(0));

        let c = count.clone();
        let s = sum.clone();
        let pool = sync_pool_slot(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                c.fetch_add(1, Ordering::Relaxed);
                s.fetch_add(*v, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        let per = 10 * SCALE;
        let producers: Vec<_> = (0..2u64)
            .map(|_| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for i in 0..per {
                        tx.send(i).unwrap();
                    }
                })
            })
            .collect();
        for p in producers {
            p.join().unwrap();
        }
        drop(tx); //senders are closed -> mark_closed -> autoshutdown

        pool.wait_stopping(); // waiting for autodrainage

        let expected_count = 2 * per;
        let expected_sum = 2 * (0..per).sum::<u64>();
        assert_eq!(
            count.load(Ordering::Relaxed),
            expected_count,
            "loss/duplicates"
        );
        assert_eq!(sum.load(Ordering::Relaxed), expected_sum, "sum");
    }

    // SYNC: per key FIFO for resize (shard_key)
    #[test]
    fn sync_order_under_resize() {
        const KEYS: usize = 4;
        let per_key = 8 * SCALE;

        let (tx, rx) = shard_key::<(u64, u64), CAP>(4);
        let last: Arc<Vec<AtomicU64>> = Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());
        let no_fifo_counter = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        let last_c = last.clone();
        let no_fifo_counter_c = no_fifo_counter.clone();
        let proc_c = processed.clone();
        let pool = sync_pool_slot(
            instance::Config::new(1, 4)
                .batch_size(4)
                .sample_interval(Duration::from_millis(2)),
            rx.into_receivers(),
            handler::PerItem(move |(k, seq): &(u64, u64)| {
                std::hint::black_box(
                    (0..2000u64).fold(*seq, |a, x| a.wrapping_mul(x).wrapping_add(1)),
                );
                let prev = last_c[*k as usize].swap(*seq, Ordering::Relaxed);
                if *seq != 0 && *seq <= prev {
                    no_fifo_counter_c.fetch_add(1, Ordering::Relaxed);
                }
                proc_c.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        let producers: Vec<_> = (0..KEYS)
            .map(|k| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let k = k as u64;
                    let mut buf = Vec::with_capacity(4);
                    for seq in 0..per_key {
                        buf.push((k, seq));
                        if buf.len() == 4 {
                            while tx.send_batch(&mut buf, |(k, _)| key_str(*k)).is_err() {
                                std::thread::yield_now();
                            }
                        }
                    }
                    while !buf.is_empty() {
                        if tx.send_batch(&mut buf, |(k, _)| key_str(*k)).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        drop(tx);
        // Prove a resize actually happened: the pool's peak worker count must
        // exceed the min, else "under resize" was never exercised (one owner the
        // whole run -> per-key order trivially preserved, no handoff).
        let max_active = pool.max_active();
        pool.wait_stopping();

        // Under Miri the item count is tiny and the timing-driven scaling is not
        // reliably reachable; the FIFO/loss checks below still run.
        if !cfg!(miri) {
            assert!(
                max_active > 1,
                "resize never happened active stayed at {max_active}, per key FIFO was not tested across a handoff"
            );
        }
        let expected = KEYS as u64 * per_key;
        assert_eq!(
            processed.load(Ordering::Relaxed),
            expected,
            "loss/duplicates"
        );
        assert_eq!(
            no_fifo_counter.load(Ordering::Relaxed),
            0,
            "broken FIFO on resize"
        );
    }

    // SYNC: shard_group
    #[test]
    fn group_processed_once() {
        let (tx, rx) = shard_group::<(String, u64), CAP>(ShardGroupCase::Groups {
            groups: &[&["a", "b"], &["c", "d"]],
        });
        let count = Arc::new(AtomicU64::new(0));

        let c = count.clone();
        let pool = sync_pool_slot(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |_: &(String, u64)| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        let per = 8 * SCALE;
        let keys = ["a", "b", "c", "d"];
        let producers: Vec<_> = keys
            .iter()
            .map(|&sym| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let mut buf = Vec::with_capacity(4);
                    for i in 0..per {
                        buf.push((sym.to_string(), i));
                        if buf.len() == 4 {
                            while !buf.is_empty() {
                                let _ = tx.send_batch(&mut buf, |(s, _)| s.as_str()).unwrap();
                                if !buf.is_empty() {
                                    std::thread::yield_now();
                                }
                            }
                        }
                    }
                    while !buf.is_empty() {
                        let _ = tx.send_batch(&mut buf, |(s, _)| s.as_str()).unwrap();
                        if !buf.is_empty() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        for p in producers {
            p.join().unwrap();
        }
        drop(tx);

        pool.wait_stopping();

        let expected = keys.len() as u64 * per;
        assert_eq!(count.load(Ordering::Relaxed), expected, "loss/duplicates");
    }

    // SYNC: forced stop (stop_and_wait)
    // Checks for cancellation: some elements may NOT be processed.
    // Guarantee: the processed ones are correct, the pool ends cleanly.
    #[test]
    fn sync_stop_and_wait() {
        let (tx, rx) = round_robin::<u64, CAP>(2);
        let count = Arc::new(AtomicU64::new(0));

        let c = count.clone();
        let pool = sync_pool_slot(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |_: &u64| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        // fill the elements (do not drop tx, the pool will NOT end on its own)
        for i in 0..(100 * SCALE) {
            let _ = tx.send(i).unwrap();
        }

        // force stop: workers will finish the current batch and exit
        pool.stop_and_wait();

        // processed SOMETHING (0..=100*SCALE), the exact number is non deterministic (depends on how much time was left before stop).
        // We check: no more than filled and the pool has ended (not frozen).
        let done = count.load(Ordering::Relaxed);
        assert!(done <= 100 * SCALE, "processed more than what was poured?!");
        drop(tx);
    }

    // SYNC: cancellation via signal (get_signal_stop)
    #[test]
    fn sync_signal_stop() {
        let (tx, rx) = round_robin::<u64, CAP>(2);
        let count = Arc::new(AtomicU64::new(0));

        let c = count.clone();
        let pool = sync_pool_slot(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |_: &u64| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        // cancellation signal
        let stop = pool.get_signal_stop();

        for i in 0..(100 * SCALE) {
            let _ = tx.send(i).unwrap();
        }

        // "signal" from another thread
        let stopper = std::thread::spawn(move || {
            stop.stop();
        });
        stopper.join().unwrap();

        pool.wait_stopping(); // will end on stopping (not on disconnect)

        let done = count.load(Ordering::Relaxed);
        assert!(done <= 100 * SCALE);
        drop(tx);
    }

    fn key_str(k: u64) -> &'static str {
        static CACHE: OnceLock<Mutex<HashMap<u64, &'static str>>> = OnceLock::new();
        let m = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        let mut g = m.lock().unwrap();
        g.entry(k)
            .or_insert_with(|| Box::leak(format!("k{k}").into_boxed_str()))
    }

    // ASYNC

    // TokioRuntime adapter

    pub(super) struct TokioJoinHandle(tokio::task::JoinHandle<()>);
    impl traits::AsyncJoinHandle for TokioJoinHandle {
        async fn join(self) {
            let _ = self.0.await; // JoinError (panic/cancel) ignored
        }
    }

    #[derive(Clone, Copy, Default)]
    pub(super) struct TokioRuntime;
    impl traits::AsyncRuntime for TokioRuntime {
        type JoinHandle = TokioJoinHandle;
        fn spawn<F>(&self, fut: F) -> TokioJoinHandle
        where
            F: Future<Output = ()> + Send + 'static,
        {
            TokioJoinHandle(tokio::spawn(fut))
        }
        fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send {
            tokio::time::sleep(dur)
        }
    }

    // ASYNC: all elements processed, per item, round_robin
    #[cfg(not(miri))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_processed_once() {
        let (tx, rx) = round_robin::<u64, CAP>(4);
        let sum = Arc::new(AtomicU64::new(0));
        let count = Arc::new(AtomicU64::new(0));
        let s = sum.clone();
        let c = count.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            instance::Config::new(1, 4),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                let s = s.clone();
                let c = c.clone();
                let v = *v;
                async move {
                    s.fetch_add(v, Ordering::Relaxed);
                    c.fetch_add(1, Ordering::Relaxed);
                }
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        // producers in blocking streams (send synchronous)
        let producers: Vec<_> = (0..8)
            .map(|_| {
                let tx = tx.clone();
                tokio::task::spawn(async move {
                    for i in 0..1000u64 {
                        tx.send_async(i).await.unwrap();
                    }
                })
            })
            .collect();
        for p in producers {
            p.await.unwrap();
        }
        drop(tx); // senders closed -> autodrainage
        pool.wait_stopping().await; // wait for auto completion
        let expected_count = 8 * 1000u64;
        let expected_sum = 8 * (0..1000u64).sum::<u64>();
        assert_eq!(
            count.load(Ordering::Relaxed),
            expected_count,
            "loss/duplicates"
        );
        assert_eq!(sum.load(Ordering::Relaxed), expected_sum, "sum");
    }

    // ASYNC: batch handler
    #[cfg(not(miri))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_batch_handler() {
        let (tx, rx) = round_robin::<u64, CAP>(4);
        let sum = Arc::new(AtomicU64::new(0));

        let s = sum.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            instance::Config::new(2, 4),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                let s = s.clone();
                let v = *v;
                async move {
                    s.fetch_add(v, Ordering::Relaxed);
                }
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        for i in 0..10_000u64 {
            tx.send_async(i).await.unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        let expected = (0..10_000u64).sum::<u64>();
        assert_eq!(sum.load(Ordering::Relaxed), expected);
    }

    // ASYNC: per key FIFO for resize, shard_key
    #[cfg(not(miri))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_order_under_resize() {
        const KEYS: usize = 8;
        const PER_KEY: u64 = 2000;

        let (tx, rx) = shard_key::<(u64, u64), CAP>(8);
        let last: Arc<Vec<AtomicU64>> = Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());
        let no_fifo_counter = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        let last_c = last.clone();
        let no_fifo_counter_c = no_fifo_counter.clone();
        let proc_c = processed.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            instance::Config::new(1, 8)
                .batch_size(16)
                .sample_interval(Duration::from_millis(2)),
            rx.into_receivers(),
            handler::PerItem(move |&(k, seq): &(u64, u64)| {
                let last = last_c.clone();
                let n_f_c = no_fifo_counter_c.clone();
                let proc = proc_c.clone();
                async move {
                    std::hint::black_box(
                        (0..2000u64).fold(seq, |a, x| a.wrapping_mul(x).wrapping_add(1)),
                    );
                    let prev = last[k as usize].swap(seq, Ordering::Relaxed);
                    if seq != 0 && seq <= prev {
                        n_f_c.fetch_add(1, Ordering::Relaxed);
                    }
                    proc.fetch_add(1, Ordering::Relaxed);
                }
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        let producers: Vec<_> = (0..KEYS)
            .map(|k| {
                let tx = tx.clone();
                tokio::task::spawn(async move {
                    let k = k as u64;
                    let mut buf = Vec::with_capacity(16);
                    for seq in 0..PER_KEY {
                        buf.push((k, seq));
                        if buf.len() == 16 {
                            while tx
                                .send_batch_async(&mut buf, |(k, _)| key_str(*k))
                                .await
                                .is_err()
                            {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    while !buf.is_empty() {
                        if tx
                            .send_batch_async(&mut buf, |(k, _)| key_str(*k))
                            .await
                            .is_err()
                        {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        for p in producers {
            p.await.unwrap();
        }
        drop(tx);
        // Prove a resize actually happened, see the sync test for rationale.
        let max_active = pool.max_active();
        pool.wait_stopping().await;

        assert!(
            max_active > 1,
            "resize never happened active stayed at {max_active}, per key FIFO was not tested across a handoff"
        );
        let expected = KEYS as u64 * PER_KEY;
        assert_eq!(
            processed.load(Ordering::Relaxed),
            expected,
            "loss/duplicates"
        );
        assert_eq!(
            no_fifo_counter.load(Ordering::Relaxed),
            0,
            "broken FIFO on resize"
        );
    }

    // ASYNC: shard_group, per key FIFO for resize
    #[cfg(not(miri))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_group_order_under_resize() {
        const KEYS: usize = 16;
        const PER_KEY: u64 = 2000;
        let (tx, rx) = shard_group::<(String, u64), CAP>(ShardGroupCase::Groups {
            groups: &[
                &["k0", "k1"],
                &["k2", "k3"],
                &["k4", "k5"],
                &["k6", "k7"],
                &["k8", "k9"],
                &["k10", "k11"],
                &["k12", "k13"],
                &["k14", "k15"],
            ],
        });

        let last: Arc<Vec<AtomicU64>> = Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());
        let no_fifo_counter = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        let last_c = last.clone();
        let no_fifo_counter_c = no_fifo_counter.clone();
        let proc_c = processed.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            instance::Config::new(1, 8)
                .batch_size(16)
                .sample_interval(Duration::from_millis(2)),
            rx.into_receivers(),
            handler::PerItem(move |kv: &(String, u64)| {
                let last = last_c.clone();
                let n_f_c = no_fifo_counter_c.clone();
                let proc = proc_c.clone();
                let (idx, seq): (usize, u64) = (kv.0[1..].parse().unwrap(), kv.1);
                async move {
                    std::hint::black_box(
                        (0..2000u64).fold(seq, |a, x| a.wrapping_mul(x).wrapping_add(1)),
                    );
                    let prev = last[idx].swap(seq, Ordering::Relaxed);
                    if seq != 0 && seq <= prev {
                        n_f_c.fetch_add(1, Ordering::Relaxed);
                    }
                    proc.fetch_add(1, Ordering::Relaxed);
                }
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        let producers: Vec<_> = (0..KEYS)
            .map(|k| {
                let tx = tx.clone();
                tokio::task::spawn(async move {
                    let sym = format!("k{k}");
                    let mut buf = Vec::with_capacity(16);
                    for seq in 0..PER_KEY {
                        buf.push((sym.clone(), seq));
                        if buf.len() == 16 {
                            while !buf.is_empty() {
                                let _ = tx
                                    .send_batch_async(&mut buf, |(s, _)| s.as_str())
                                    .await
                                    .unwrap();
                                if !buf.is_empty() {
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    }
                    while !buf.is_empty() {
                        let _ = tx
                            .send_batch_async(&mut buf, |(s, _)| s.as_str())
                            .await
                            .unwrap();
                        if !buf.is_empty() {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        for p in producers {
            p.await.unwrap();
        }
        drop(tx);
        // Prove a resize actually happened, see the sync test for rationale.
        let max_active = pool.max_active();
        pool.wait_stopping().await;

        assert!(
            max_active > 1,
            "resize never happened active stayed at {max_active}, per key FIFO was not tested across a handoff"
        );
        let expected = KEYS as u64 * PER_KEY;
        assert_eq!(
            processed.load(Ordering::Relaxed),
            expected,
            "loss/duplicates"
        );
        assert_eq!(
            no_fifo_counter.load(Ordering::Relaxed),
            0,
            "per key FIFO for shard_group is broken during resize"
        );
    }

    // ASYNC: shard_group batch handler
    #[cfg(not(miri))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_group_batch_handler() {
        let (tx, rx) = shard_group::<(String, u64), CAP>(ShardGroupCase::Groups {
            groups: &[&["AAA", "BBB"], &["CCC", "DDD"]],
        });
        let sum = Arc::new(AtomicU64::new(0));

        let s = sum.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            instance::Config::new(2, 2),
            rx.into_receivers(),
            handler::PerItem(move |kv: &(String, u64)| {
                let s = s.clone();
                let v = kv.1;
                async move {
                    s.fetch_add(v, Ordering::Relaxed);
                }
            }),
            |_poison, _panic_info| {},
        ).unwrap();

        const PER_KEY: u64 = 2500;
        let keys = ["AAA", "BBB", "CCC", "DDD"];
        let producers: Vec<_> = keys
            .iter()
            .map(|&sym| {
                let tx = tx.clone();
                tokio::task::spawn(async move {
                    let mut buf = Vec::with_capacity(16);
                    for i in 0..PER_KEY {
                        buf.push((sym.to_string(), i));
                        if buf.len() == 16 {
                            while !buf.is_empty() {
                                let _ = tx
                                    .send_batch_async(&mut buf, |(s, _)| s.as_str())
                                    .await
                                    .unwrap();
                                if !buf.is_empty() {
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    }
                    while !buf.is_empty() {
                        let _ = tx
                            .send_batch_async(&mut buf, |(s, _)| s.as_str())
                            .await
                            .unwrap();
                        if !buf.is_empty() {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        for p in producers {
            p.await.unwrap();
        }
        drop(tx);

        pool.wait_stopping().await;

        let expected = keys.len() as u64 * (0..PER_KEY).sum::<u64>();
        assert_eq!(sum.load(Ordering::Relaxed), expected);
    }

    #[cfg(not(miri))]
    #[test]
    fn stall_takeover_rescues_shards_from_a_stuck_owner() {
        let (tx, rx) = shard_key::<(u64, u64), 64>(4);
        // One key per shard, by probing.
        let mut keys: Vec<Option<String>> = vec![None; 4];
        let mut i = 0u32;
        while keys.iter().any(|k| k.is_none()) {
            let k = format!("k{i}");
            i += 1;
            let s = tx.shard_for(&k);
            if keys[s].is_none() {
                keys[s] = Some(k);
            }
        }
        let keys: Vec<String> = keys.into_iter().map(Option::unwrap).collect();

        // batch_size(1): the poison is taken alone, the items behind it stay
        // in the ring of the stuck shard - exactly the state the takeover
        // must refuse to steal (non empty current shard).
        let cfg = instance::Config::new(1, 4)
            .sample_interval(Duration::from_millis(1))
            .batch_size(1)
            .stall_takeover(Duration::from_millis(20));
        let processed = Arc::new(AtomicU64::new(0));
        let inversions = Arc::new(AtomicU64::new(0));
        let last: Arc<Vec<AtomicU64>> = Arc::new((0..4).map(|_| AtomicU64::new(0)).collect());
        let (p, inv, l) = (processed.clone(), inversions.clone(), last.clone());
        let pool = sync_pool_slot(
            cfg,
            rx.into_receivers(),
            handler::PerItem(move |&(key, seq): &(u64, u64)| {
                if seq == u64::MAX {
                    loop {
                        thread::sleep(Duration::from_secs(3600));
                    }
                }
                let prev = l[key as usize].swap(seq, Ordering::Relaxed);
                if seq != 0 && seq <= prev {
                    inv.fetch_add(1, Ordering::Relaxed);
                }
                p.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        mem::forget(pool); // stuck worker: joining would hang

        // Poison key 0 plus a tail behind it, all queued before the worker
        // claims: the worker takes the poison alone (batch_size = 1) and
        // tail stays in the frozen shard's ring - it must not be stolen, lost, or reordered.
        tx.try_send(&keys[0], (0, u64::MAX)).unwrap();
        for seq in 1..=5u64 {
            tx.try_send(&keys[0], (0, seq)).unwrap();
        }
        thread::sleep(Duration::from_millis(30));
        // Load on the three healthy keys.
        let mut sent = 0u64;
        for seq in 1..=50u64 {
            for key in 1..4u64 {
                if tx.try_send(&keys[key as usize], (key, seq)).is_ok() {
                    sent += 1;
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while processed.load(Ordering::Relaxed) < sent {
            assert!(
                Instant::now() < deadline,
                "stall takeover never rescued the healthy shards: {}/{}",
                processed.load(Ordering::Relaxed),
                sent
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(inversions.load(Ordering::Relaxed), 0, "per-key FIFO broke");
        // The stuck shard stays frozen: nothing behind the poison leaks out.
        thread::sleep(Duration::from_millis(200));
        assert_eq!(
            processed.load(Ordering::Relaxed),
            sent,
            "items behind the poison escaped the frozen shard out of order"
        );
        mem::forget(tx);
    }

    /// default stays strict ownership: per-shard FIFO is never traded silently,
    /// so without an explicit stall budget the freeze from forever blocking handler is the documented behavior.
    #[cfg(not(miri))]
    #[test]
    fn no_takeover_by_default_strict_ownership_holds() {
        let (tx, rx) = round_robin::<u64, 64>(4);
        let cfg = instance::Config::new(1, 4).sample_interval(Duration::from_millis(1));
        let processed = Arc::new(AtomicU64::new(0));
        let p = processed.clone();
        let pool = sync_pool_slot(
            cfg,
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                if *v == u64::MAX {
                    loop {
                        thread::sleep(Duration::from_secs(3600));
                    }
                }
                p.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        mem::forget(pool); // stuck worker: joining would hang
        tx.try_send(u64::MAX).unwrap();
        thread::sleep(Duration::from_millis(30));
        for i in 0..50u64 {
            let _ = tx.try_send(i);
        }
        thread::sleep(Duration::from_millis(300));
        assert_eq!(
            processed.load(Ordering::Relaxed),
            0,
            "no stall budget configured: ownership must stay strict"
        );
        mem::forget(tx);
    }

    #[cfg(not(miri))]
    #[test]
    fn lazy_spawn_starts_at_min_and_grows_under_load() {
        let (tx, rx) = round_robin::<u64, CAP>(4);
        let cfg = instance::Config::new(1, 4).sample_interval(Duration::from_millis(1));
        let pool = sync_pool_slot(
            cfg,
            rx.into_receivers(),
            handler::PerItem(move |_v: &u64| {
                std::thread::sleep(Duration::from_micros(500));
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        assert_eq!(
            pool.worker_handles(),
            2,
            "min=1: one worker plus the monitor; max is a ceiling, not a bill"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut i = 0u64;
        while pool.worker_handles() < 3 {
            let _ = tx.try_send(i);
            i += 1;
            assert!(
                std::time::Instant::now() < deadline,
                "monitor never spawned beyond min under saturation"
            );
            if i % 64 == 0 {
                std::thread::yield_now();
            }
        }
        drop(tx);
        pool.wait_stopping();
    }

    #[cfg(not(miri))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lazy_spawn_async_starts_at_min_and_grows_under_load() {
        use super::tests::TokioRuntime;
        let (tx, rx) = round_robin::<u64, CAP>(4);
        let cfg = instance::Config::new(1, 4).sample_interval(Duration::from_millis(1));
        let pool = async_pool_slot(
            TokioRuntime,
            cfg,
            rx.into_receivers(),
            handler::PerItem(move |_v: &u64| async {
                tokio::time::sleep(Duration::from_micros(500)).await;
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        assert_eq!(
            pool.worker_handles(),
            2,
            "min=1: one worker task plus the monitor"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut i = 0u64;
        while pool.worker_handles() < 3 {
            let _ = tx.try_send(i);
            i += 1;
            assert!(
                std::time::Instant::now() < deadline,
                "monitor never spawned beyond min under saturation"
            );
            if i % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
        drop(tx);
        pool.wait_stopping().await;
    }
}

#[cfg(test)]
mod panic_safety_tests {
    use super::*;
    use crate::channel::mpmc::{ShardGroupCase, round_robin, shard_group, shard_key};
    use std::{
        collections::HashSet,
        panic,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU8, AtomicU64, Ordering},
        },
        thread,
        time,
    };

    const CAP: usize = 64;
    #[cfg(miri)]
    const N: u64 = 700;
    #[cfg(not(miri))]
    const N: u64 = 10_000;
    // 1..=N, multiples of 7 panic
    const POISON: u64 = N / 7;

    struct TesterPoison {
        ok: Vec<AtomicU8>,   // ok[v] = times value v was processed
        dead: Vec<AtomicU8>, // dead[v] = times value v was dead
    }
    impl TesterPoison {
        fn new(n: u64) -> Arc<Self> {
            Arc::new(Self {
                ok: (0..=n).map(|_| AtomicU8::new(0)).collect(),
                dead: (0..=n).map(|_| AtomicU8::new(0)).collect(),
            })
        }
        fn processed(&self, v: u64) {
            self.ok[v as usize].fetch_add(1, Ordering::Relaxed);
        }
        fn dead_lettered(&self, v: u64) {
            self.dead[v as usize].fetch_add(1, Ordering::Relaxed);
        }
        fn assert_exactly_once(&self, n: u64, poison: impl Fn(u64) -> bool) {
            for v in 1..=n {
                let ok = self.ok[v as usize].load(Ordering::Relaxed);
                let dead = self.dead[v as usize].load(Ordering::Relaxed);
                if poison(v) {
                    assert_eq!(
                        (ok, dead),
                        (0, 1),
                        "poison {v}: want dead once, got processed={ok} dead={dead}"
                    );
                } else {
                    assert_eq!(
                        (ok, dead),
                        (1, 0),
                        "value {v}: want processed once, got processed={ok} dead={dead}"
                    );
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "want processed once")]
    fn tester_poison_catches_loss_and_dup() {
        let t = TesterPoison::new(5);
        for v in 1..=5 {
            if v == 3 {
                continue; // value 3 lost
            }
            t.processed(v);
        }
        t.processed(5); // value 5 processed twice
        t.assert_exactly_once(5, |v| v % 7 == 0);
    }

    #[test]
    fn owner_guard_releases_all_shards_on_panic() {
        let state = instance::State::new(2, 1);
        let s = state.clone();
        let prev = panic::take_hook();
        panic::set_hook(Box::new(|_| {})); // silence the expected panic
        let joined = thread::spawn(move || {
            let _guard = super::guard::OwnerGuard::new(&s, 0);
            for shard in 0..2 {
                let _ = instance::claim_or_release_to(&s, 0, shard, 0);
            }
            panic!("worker 0 crashed"); // unwind through OwnerGuard::drop
        })
        .join();
        panic::set_hook(prev);

        assert!(joined.is_err(), "the worker must have panicked");
        for shard in 0..2 {
            assert_eq!(
                state.owner(shard).load(Ordering::Relaxed),
                instance::NONE,
                "shard {shard} not released after the owner thread unwound"
            );
            assert!(
                instance::claim_or_release_to(&state, 1, shard, 1),
                "survivor cannot claim shard {shard} after crash"
            );
        }
    }

    /// Sync slot API + PerItem: zero loss.
    /// Every item is either processed exactly once or handed to the dead letter sink exactly once,
    /// and the sink receives precisely the poison items.
    #[test]
    fn sync_slot_ref_zero_loss() {
        let (tx, rx) = round_robin::<u64, CAP>(2);
        let tester = TesterPoison::new(N);
        let (tp, td) = (tester.clone(), tester.clone());
        let pool = sync_pool_slot(
            instance::Config::new(2, 2),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                if *v % 7 == 0 {
                    panic!("babah on {v}");
                }
                tp.processed(*v);
            }),
            move |poison: u64, _panic_info| {
                td.dead_lettered(poison);
            },
        ).unwrap();
        let producer = std::thread::spawn(move || {
            for i in 1..=N {
                tx.send(i).unwrap();
            }
        });
        producer.join().unwrap();
        pool.wait_stopping();

        tester.assert_exactly_once(N, |v| v % 7 == 0);
    }

    /// A panicking dead letter sink must not kill the worker.
    /// Handler panics on multiples of 7 and the sink itself always panics;
    /// the pool must still process every non poison item and stop cleanly.
    #[test]
    fn sync_slot_panicking_sink_does_not_kill_worker() {
        let (tx, rx) = round_robin::<u64, CAP>(2);
        let ok = Arc::new(AtomicU64::new(0));
        let c = ok.clone();
        let pool = sync_pool_slot(
            instance::Config::new(2, 2),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                if *v % 7 == 0 {
                    panic!("babah on {v}");
                }
                c.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison: u64, _panic_info| panic!("sink is broken too"),
        ).unwrap();
        let producer = std::thread::spawn(move || {
            for i in 1..=N {
                tx.send(i).unwrap();
            }
        });
        producer.join().unwrap();
        pool.wait_stopping(); // must not hang

        assert_eq!(ok.load(Ordering::Relaxed), N - POISON);
    }

    /// Poison accounting on the async slot pool: every non poison item processed exactly once,
    /// receivers exactly the poison items, panics counted.
    #[cfg(not(miri))]
    #[test]
    fn async_slot_panics_counted_exactly() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = round_robin::<u64, CAP>(2);
            let tester = TesterPoison::new(N);
            let (tp, td) = (tester.clone(), tester.clone());
            let pool = async_pool_slot(
                tests::TokioRuntime,
                instance::Config::new(2, 2),
                rx.into_receivers(),
                handler::PerItem(move |v: &u64| {
                    let tp = tp.clone();
                    let v = *v;
                    async move {
                        if v % 7 == 0 {
                            panic!("babah on {v}");
                        }
                        tp.processed(v);
                    }
                }),
                move |poison: u64, _panic_info| {
                    td.dead_lettered(poison);
                },
            ).unwrap();
            let sender = tokio::task::spawn(async move {
                for i in 1..=N {
                    tx.send_async(i).await.unwrap();
                }
            });
            sender.await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            let panics = pool.handler_panics();
            pool.wait_stopping().await;
            tester.assert_exactly_once(N, |v| v % 7 == 0);
            assert!(panics >= POISON);
        });
    }

    /// Keyed routing + sync slot pool: zero loss per key. Poison values
    /// land in dl with the exact per key sum proves dead lettering does not cross contaminate shards.
    #[test]
    fn sync_key_slot_zero_loss() {
        let (tx, rx) = shard_key::<u64, CAP>(4);
        let tester = TesterPoison::new(N);
        let (tp, td) = (tester.clone(), tester.clone());
        let pool = sync_pool_slot(
            instance::Config::new(2, 4),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                if *v % 7 == 0 {
                    panic!("babah on {v}");
                }
                tp.processed(*v);
            }),
            move |poison: u64, _panic_info| {
                td.dead_lettered(poison);
            },
        ).unwrap();
        const KEYS: [&str; 4] = ["AAA", "BBB", "CCC", "DDD"];
        let producer = std::thread::spawn(move || {
            for i in 1..=N {
                let key = KEYS[(i % 4) as usize];
                tx.send(key, i).unwrap();
            }
        });
        producer.join().unwrap();
        pool.wait_stopping();

        tester.assert_exactly_once(N, |v| v % 7 == 0);
    }

    /// Keyed routing + async slot pool: zero loss, panics counted.
    #[cfg(not(miri))]
    #[test]
    fn async_key_slot_zero_loss() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = shard_key::<u64, CAP>(4);
            let tester = TesterPoison::new(N);
            let (tp, td) = (tester.clone(), tester.clone());
            let pool = async_pool_slot(
                tests::TokioRuntime,
                instance::Config::new(2, 4),
                rx.into_receivers(),
                handler::PerItem(move |v: &u64| {
                    let tp = tp.clone();
                    let v = *v;
                    async move {
                        if v % 7 == 0 {
                            panic!("babah on {v}");
                        }
                        tp.processed(v);
                    }
                }),
                move |poison: u64, _panic_info| {
                    td.dead_lettered(poison);
                },
            ).unwrap();
            const KEYS: [&str; 4] = ["AAA", "BBB", "CCC", "DDD"];
            let sender = tokio::task::spawn(async move {
                for i in 1..=N {
                    let key = KEYS[(i % 4) as usize];
                    tx.send_async(key, i).await.unwrap();
                }
            });
            sender.await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            let panics = pool.handler_panics();
            pool.wait_stopping().await;

            tester.assert_exactly_once(N, |v| v % 7 == 0);
            assert!(panics >= POISON);
        });
    }

    /// Group routing + sync slot pool: zero loss.
    /// Values are tagged by symbol so dl contents are verifiable per group.
    #[test]
    fn sync_group_slot_zero_loss() {
        let groups: &[&[&str]] = &[&["AAA", "BBB"], &["CCC", "DDD"]];
        let (tx, rx) = shard_group::<u64, CAP>(ShardGroupCase::Groups { groups });
        let tester = TesterPoison::new(N);
        let (tp, td) = (tester.clone(), tester.clone());
        let pool = sync_pool_slot(
            instance::Config::new(2, 2),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                if *v % 7 == 0 {
                    panic!("babah on {v}");
                }
                tp.processed(*v);
            }),
            move |poison: u64, _panic_info| {
                td.dead_lettered(poison);
            },
        ).unwrap();
        const SYMS: [&str; 4] = ["AAA", "BBB", "CCC", "DDD"];
        let handles: Vec<_> = SYMS.iter().map(|s| tx.handle(s).unwrap()).collect();
        let producer = std::thread::spawn(move || {
            for i in 1..=N {
                let h = handles[(i % 4) as usize];
                tx.send(h, i).unwrap();
            }
        });
        producer.join().unwrap();
        pool.wait_stopping();

        tester.assert_exactly_once(N, |v| v % 7 == 0);
    }

    /// Group routing + async slot pool: zero loss, panics counted.
    #[cfg(not(miri))]
    #[test]
    fn async_group_slot_zero_loss() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let groups: &[&[&str]] = &[&["AAA", "BBB"], &["CCC", "DDD"]];
            let (tx, rx) = shard_group::<u64, CAP>(ShardGroupCase::Groups { groups });
            let tester = TesterPoison::new(N);
            let (tp, td) = (tester.clone(), tester.clone());
            let pool = async_pool_slot(
                tests::TokioRuntime,
                instance::Config::new(2, 2),
                rx.into_receivers(),
                handler::PerItem(move |v: &u64| {
                    let tp = tp.clone();
                    let v = *v;
                    async move {
                        if v % 7 == 0 {
                            panic!("babah on {v}");
                        }
                        tp.processed(v);
                    }
                }),
                move |poison: u64, _panic_info| {
                    td.dead_lettered(poison);
                },
            ).unwrap();
            const SYMS: [&str; 4] = ["AAA", "BBB", "CCC", "DDD"];
            let handles: Vec<_> = SYMS.iter().map(|s| tx.handle(s).unwrap()).collect();
            let sender = tokio::task::spawn(async move {
                for i in 1..=N {
                    let h = handles[(i % 4) as usize];
                    tx.send_async(h, i).await.unwrap();
                }
            });
            sender.await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            let panics = pool.handler_panics();
            pool.wait_stopping().await;

            tester.assert_exactly_once(N, |v| v % 7 == 0);
            assert!(panics >= POISON);
        });
    }

    /// `AsyncSlotHandler::handle` returns `impl Future`, not `async fn`,
    /// so an implementor may run code before the future exists.
    /// That code must be covered by the same panic net as the future.
    struct PanicsBeforeTheFuture;
    impl traits::AsyncSlotHandler<u64> for PanicsBeforeTheFuture {
        fn handle(&self, slot: &mut Option<u64>) -> impl Future<Output = ()> + Send {
            let v = slot.as_ref().copied().unwrap_or(0);
            if v % 2 == 0 {
                panic!("synchronous panic on {v}");
            }
            let _ = slot.take();
            async move {}
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn sync_panic_in_handle_is_caught_and_dead_lettered() {
        const N: u64 = 20;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = round_robin::<u64, 64>(1);
            let dead = Arc::new(AtomicU64::new(0));
            let d = dead.clone();

            let pool = async_pool_slot(
                tests::TokioRuntime,
                instance::Config::new(1, 1),
                rx.into_receivers(),
                PanicsBeforeTheFuture,
                move |_poison: u64, _p| {
                    d.fetch_add(1, Ordering::Relaxed);
                },
            ).unwrap();
            for i in 1..=N {
                while tx.try_send(i).is_err() {
                    tokio::task::yield_now().await;
                }
            }
            drop(tx);
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;

            let processed = pool.processed();
            let panics = pool.handler_panics();
            let dl = dead.load(Ordering::Relaxed);
            pool.stop_and_wait().await;

            // Every even item panics before its future exists.
            assert_eq!(panics, N / 2, "synchronous panics were not counted");
            assert_eq!(dl, N / 2, "items lost instead of dead-lettered");
            assert_eq!(
                processed,
                N / 2,
                "worker died instead of surviving the panic: odd items were stranded"
            );
        });
    }

    /// Per the slot contract the handler takes the item only at its commit point,
    /// so while it awaits, the item still sits in the worker's slot.
    /// Cancelling the worker task there used to drop the frame and the item with it,
    /// nothing counted, nothing delivered.
    /// Now the item comes back through `dead_letter`, flagged as a cancellation rather than a panic.
    struct CommitsAfterAwait;
    impl traits::AsyncSlotHandler<u64> for CommitsAfterAwait {
        fn handle(&self, slot: &mut Option<u64>) -> impl std::future::Future<Output = ()> + Send {
            async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let _ = slot.take(); // commit point, never reached here
            }
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn cancelled_worker_hands_the_item_back() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dead = Arc::new(AtomicU64::new(0));
        let cancelled = Arc::new(AtomicU64::new(0));
        let (d, c) = (dead.clone(), cancelled.clone());

        rt.block_on(async move {
            let (tx, rx) = round_robin::<u64, 64>(1);
            let _pool = async_pool_slot(
                tests::TokioRuntime,
                instance::Config::new(1, 1),
                rx.into_receivers(),
                CommitsAfterAwait,
                move |poison: u64, reason: PanicReason| {
                    assert_eq!(poison, 42, "a different item came back");
                    if reason.is_cancelled() {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                    d.fetch_add(1, Ordering::Relaxed);
                },
            );
            tx.try_send(42).unwrap();
            // Let the worker pick it up and park inside the handler.
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        // Cancel every worker task while the handler is mid await.
        rt.shutdown_timeout(Duration::from_millis(100));

        assert_eq!(
            dead.load(Ordering::Relaxed),
            1,
            "the inflight item was lost on cancellation"
        );
        assert_eq!(
            cancelled.load(Ordering::Relaxed),
            1,
            "reason should say cancelled, not panic"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn cancelled_worker_hands_back_the_whole_batch() {
        const N: u64 = 8;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dead: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let cancelled = Arc::new(AtomicU64::new(0));
        let (d, c) = (dead.clone(), cancelled.clone());

        rt.block_on(async move {
            let (tx, rx) = round_robin::<u64, 64>(1);
            for i in 0..N {
                tx.try_send(i).unwrap();
            }
            let _pool = async_pool_slot(
                tests::TokioRuntime,
                instance::Config::new(1, 1),
                rx.into_receivers(),
                CommitsAfterAwait,
                move |poison: u64, reason: PanicReason| {
                    if reason.is_cancelled() {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                    d.lock().unwrap().push(poison);
                },
            ).unwrap();
            // let the worker take the batch and park inside the first handler.
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        // cancel the worker while it awaits with N - 1 items still in its buffer.
        rt.shutdown_timeout(Duration::from_millis(100));
        let got = dead.lock().unwrap().clone();
        let mut sorted = got.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..N).collect::<Vec<_>>(),
            "the batch tail was dropped on cancellation"
        );
        assert_eq!(
            cancelled.load(Ordering::Relaxed),
            N,
            "reason should say cancelled, not panic"
        );
        assert_eq!(
            got,
            (0..N).collect::<Vec<_>>(),
            "batch handed back out of order"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn broken_config_fields_are_repaired() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (std_tx, std_rx) = round_robin::<u64, CAP>(6);
            let mut cfg = instance::Config::new(1, 2).batch_size(0);
            cfg.min_consumers = 10;
            let count = Arc::new(AtomicU64::new(0));
            let c = count.clone();
            let pool = sync_pool_slot(
                cfg,
                std_rx.into_receivers(),
                handler::PerItem(move |_v: &u64| {
                    c.fetch_add(1, Ordering::Relaxed);
                }),
                |_poison, _panic_info| {},
            ).unwrap();
            for i in 0..600u64 {
                std_tx.send(i).unwrap();
            }
            drop(std_tx); // autodrain: the pool stops once every shard drains and closes
            pool.wait_stopping();
            done_tx.send(count.load(Ordering::Relaxed)).unwrap();
        });
        let processed = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("pool wedged: unserviced shards or a zero batch_size");
        assert_eq!(processed, 600, "loss on a repaired config");
    }

    #[cfg(not(miri))]
    #[test]
    fn idle_workers_are_never_marked_stalled() {
        let (tx, rx) = shard_key::<(u64, u64), 64>(4);
        let mut keys: Vec<Option<String>> = vec![None; 4];
        let mut i = 0u32;
        while keys.iter().any(|k| k.is_none()) {
            let k = format!("k{i}");
            i += 1;
            let s = tx.shard_for(&k);
            if keys[s].is_none() {
                keys[s] = Some(k);
            }
        }
        let keys: Vec<String> = keys.into_iter().map(Option::unwrap).collect();

        let cfg = instance::Config::new(4, 4)
            .sample_interval(Duration::from_millis(1))
            .stall_takeover(Duration::from_millis(20));
        let threads: Arc<Mutex<HashSet<std::thread::ThreadId>>> =
            Arc::new(Mutex::new(HashSet::new()));
        let done = Arc::new(AtomicU64::new(0));
        let (t, d) = (threads.clone(), done.clone());
        let pool = sync_pool_slot(
            cfg,
            rx.into_receivers(),
            handler::PerItem(move |_v: &(u64, u64)| {
                thread::sleep(Duration::from_millis(1));
                t.lock().unwrap().insert(thread::current().id());
                d.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        for seq in 0..40u64 {
            let _ = tx.try_send(&keys[0], (0, seq));
            thread::sleep(Duration::from_millis(10));
        }
        // Burst over every key: with the latch only the trickle worker would
        // still be fed, and the whole burst would run on one thread.
        threads.lock().unwrap().clear();
        let before = done.load(Ordering::Relaxed);
        for seq in 0..50u64 {
            for key in 0..4u64 {
                while tx.try_send(&keys[key as usize], (key, seq + 100)).is_err() {
                    thread::yield_now();
                }
            }
        }
        let deadline = time::Instant::now() + Duration::from_secs(10);
        while done.load(Ordering::Relaxed) < before + 200 {
            assert!(time::Instant::now() < deadline, "burst never drained");
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            threads.lock().unwrap().len() >= 2,
            "idle workers were latched as stalled: the burst ran on one thread"
        );
        drop(tx);
        pool.wait_stopping();
    }

    #[cfg(not(miri))]
    #[test]
    fn autodrain_completes_despite_a_stuck_worker() {
        let (tx, rx) = shard_key::<(u64, u64), 64>(2);
        let mut keys: Vec<Option<String>> = vec![None; 2];
        let mut i = 0u32;
        while keys.iter().any(|k| k.is_none()) {
            let k = format!("k{i}");
            i += 1;
            let s = tx.shard_for(&k);
            if keys[s].is_none() {
                keys[s] = Some(k);
            }
        }
        let keys: Vec<String> = keys.into_iter().map(Option::unwrap).collect();
        let cfg = instance::Config::new(1, 2)
            .sample_interval(Duration::from_millis(1))
            .stall_takeover(Duration::from_millis(20));
        let processed = Arc::new(AtomicU64::new(0));
        let p = processed.clone();
        let pool = sync_pool_slot(
            cfg,
            rx.into_receivers(),
            handler::PerItem(move |&(_, seq): &(u64, u64)| {
                if seq == u64::MAX {
                    loop {
                        thread::sleep(Duration::from_secs(3600));
                    }
                }
                p.fetch_add(1, Ordering::Relaxed);
            }),
            |_poison, _panic_info| {},
        ).unwrap();
        // Poison is shard 0's only item: once taken, the shard is empty,
        // so the takeover may steal it and observe the disconnect.
        tx.try_send(&keys[0], (0, u64::MAX)).unwrap();
        thread::sleep(Duration::from_millis(30));
        for seq in 1..=20u64 {
            tx.try_send(&keys[1], (1, seq)).unwrap();
        }
        drop(tx); // all senders gone -> every drained shard can close
        let deadline = time::Instant::now() + Duration::from_secs(5);
        let mut drained = false;
        while std::time::Instant::now() < deadline {
            if pool.is_stopped() {
                drained = true;
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let got = processed.load(Ordering::Relaxed);
        // The shutdown path that can never hang - taken vefore any assert,
        // so a failure unwinds past an already detached pool instead of join hanging on the stuck worker in Drop.
        let t0 = time::Instant::now();
        pool.stop_and_detach();
        assert!(t0.elapsed() < Duration::from_secs(1), "stop_and_detach must not join");
        assert!(
            drained,
            "autodrain never completed: the stuck owner's empty shard was not closed \
             (processed {got})"
        );
        assert_eq!(got, 20);
    }
}

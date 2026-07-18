pub(crate) mod guard;
pub mod handler;
pub mod instance;
pub mod signal;
pub mod sync;
pub mod traits;

use crate::internal_channel::{receiver::Receiver, traits::InnerChannel};
use std::{sync::Arc, time::Duration};

const MONITOR_TICK: Duration = Duration::from_millis(10);

pub fn async_pool<AR, T, const CAP: usize, I, H>(
    async_runtime: AR,
    cfg: instance::Config,
    receivers: Vec<Receiver<T, CAP, I>>,
    handler: H,
) -> sync::AsyncPool<AR>
where
    AR: traits::AsyncRuntime,
    T: Send + 'static,
    I: InnerChannel<T, CAP> + Send + Sync + 'static,
    Receiver<T, CAP, I>: Send + Sync,
    H: traits::AsyncHandler<T>,
{
    let shards = receivers.len();
    let state = instance::State::new(shards, cfg.min_consumers);
    let receivers = Arc::new(receivers);
    let handler = Arc::new(handler);
    let mut workers = Vec::with_capacity(cfg.max_consumers + 1);

    for id in 0..cfg.max_consumers {
        let state = state.clone();
        let receivers = receivers.clone();
        let handler = handler.clone();
        let ar = async_runtime.clone();
        let h = ar.clone().spawn(async move {
            let _guard = guard::OwnerGuard::new(&state, id);
            let mut buf: Vec<T> = Vec::with_capacity(cfg.batch_size);
            let mut idle_streak: u32 = 0;
            while !state.is_stopped() {
                let active = state.active();
                let mut done = false;
                for shard in 0..shards {
                    if !instance::claim_or_release(&state, id, shard, active) {
                        continue;
                    }
                    let (n, dc) = receivers[shard].try_recv_batch(&mut buf, cfg.batch_size);
                    if n > 0 {
                        done = true;
                        let batch: Vec<T> = buf.drain(..n).collect();
                        handler.handle(batch).await;
                        _ = state.processed_add(n as u64);
                    } else if dc {
                        state.mark_closed(shard); // shard empty + closed -> autodrainage
                    }
                }
                if done {
                    idle_streak = 0;
                } else {
                    idle_streak = idle_streak.saturating_add(1);
                    if instance::idle_backoff_step(idle_streak) {
                        ar.sleep(instance::IDLE_SLEEP).await;
                    }
                }
            }
        });
        workers.push(h);
    }

    // Interruptible sleep -> wait_stopping waits ≤ MONITOR_TICK.
    {
        let state = state.clone();
        let receivers = receivers.clone();
        let ar = async_runtime.clone();
        let h = ar.clone().spawn(async move {
            while !state.is_stopped() {
                if sleep_interruptible_async(&ar, &state, cfg.sample_interval).await {
                    break; // stopped during sleep -> exit immediately
                }
                instance::monitor(&cfg, &state, &receivers);
            }
        });
        workers.push(h);
    }

    sync::AsyncPool::new(state, workers)
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

pub fn sync_pool<T, const CAP: usize, I, H>(
    cfg: instance::Config,
    receivers: Vec<Receiver<T, CAP, I>>,
    handler: H,
) -> sync::SyncPool
where
    T: Send + 'static,
    I: InnerChannel<T, CAP> + Send + Sync + 'static,
    Receiver<T, CAP, I>: Send + Sync,
    H: traits::SyncHandler<T>,
{
    use std::thread;
    let shards = receivers.len();
    let state = instance::State::new(shards, cfg.min_consumers);
    let receivers = Arc::new(receivers);
    let handler = Arc::new(handler);
    let mut workers = Vec::with_capacity(cfg.max_consumers + 1);

    for id in 0..cfg.max_consumers {
        let state = state.clone();
        let receivers = receivers.clone();
        let handler = handler.clone();
        let h = thread::spawn(move || {
            let _guard = guard::OwnerGuard::new(&state, id);
            let mut buf: Vec<T> = Vec::with_capacity(cfg.batch_size);
            let mut idle_streak: u32 = 0;
            while !state.is_stopped() {
                let active = state.active();
                let mut done = false;
                for shard in 0..shards {
                    if !instance::claim_or_release(&state, id, shard, active) {
                        continue;
                    }
                    let (n, dc) = receivers[shard].try_recv_batch(&mut buf, cfg.batch_size);
                    if n > 0 {
                        done = true;
                        handler.handle(&mut buf, n);
                        _ = state.processed_add(n as u64);
                    } else if dc {
                        state.mark_closed(shard); // shard is empty + closed -> autodrainage
                    }
                }
                if done {
                    idle_streak = 0;
                } else {
                    idle_streak = idle_streak.saturating_add(1);
                    if instance::idle_backoff_step(idle_streak) {
                        thread::sleep(instance::IDLE_SLEEP);
                    }
                }
            }
        });
        workers.push(h);
    }

    // Interruptible sleep -> wait_stopping waits ≤ MONITOR_TICK
    {
        let state = state.clone();
        let receivers = receivers.clone();
        let h = thread::spawn(move || {
            while !state.is_stopped() {
                if sleep_interruptible_sync(&state, cfg.sample_interval) {
                    break; // stopped during sleep -> exit immediately
                }
                instance::monitor(&cfg, &state, &receivers);
            }
        });
        workers.push(h);
    }

    sync::SyncPool::new(state, workers)
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
        collections::HashMap,
        sync::{
            Arc, Mutex, OnceLock,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
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
        let pool = sync_pool(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |v: &u64| {
                c.fetch_add(1, Ordering::Relaxed);
                s.fetch_add(*v, Ordering::Relaxed);
            }),
        );

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
        let violations = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        let last_c = last.clone();
        let viol_c = violations.clone();
        let proc_c = processed.clone();
        let pool = sync_pool(
            instance::Config::new(1, 4).batch_size(4),
            rx.into_receivers(),
            handler::PerItem(move |(k, seq): &(u64, u64)| {
                let prev = last_c[*k as usize].swap(*seq, Ordering::Relaxed);
                if *seq != 0 && *seq <= prev {
                    viol_c.fetch_add(1, Ordering::Relaxed);
                }
                proc_c.fetch_add(1, Ordering::Relaxed);
            }),
        );

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

        pool.wait_stopping();

        let expected = KEYS as u64 * per_key;
        assert_eq!(
            processed.load(Ordering::Relaxed),
            expected,
            "loss/duplicates"
        );
        assert_eq!(
            violations.load(Ordering::Relaxed),
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
        let pool = sync_pool(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |_: &(String, u64)| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
        );

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
        let pool = sync_pool(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |_: &u64| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
        );

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
        let pool = sync_pool(
            instance::Config::new(1, 2),
            rx.into_receivers(),
            handler::PerItem(move |_: &u64| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
        );

        // cancellation signal
        let stop = pool.get_singal_stop();

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

    struct TokioJoinHandle(tokio::task::JoinHandle<()>);
    impl traits::AsyncJoinHandle for TokioJoinHandle {
        async fn join(self) {
            let _ = self.0.await; // JoinError (panic/cancel) ignored
        }
    }

    #[derive(Clone, Copy, Default)]
    struct TokioRuntime;
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
        let pool = async_pool(
            TokioRuntime,
            instance::Config::new(1, 4),
            rx.into_receivers(),
            handler::PerItem(move |v: u64| {
                let s = s.clone();
                let c = c.clone();
                async move {
                    s.fetch_add(v, Ordering::Relaxed);
                    c.fetch_add(1, Ordering::Relaxed);
                }
            }),
        );
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
        let pool = async_pool(
            TokioRuntime,
            instance::Config::new(2, 4),
            rx.into_receivers(),
            handler::Batch(move |batch: &[u64]| {
                let s = s.clone();
                let part: u64 = batch.iter().sum();
                async move {
                    s.fetch_add(part, Ordering::Relaxed);
                }
            }),
        );

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
        let violations = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        let last_c = last.clone();
        let viol_c = violations.clone();
        let proc_c = processed.clone();
        let pool = async_pool(
            TokioRuntime,
            instance::Config::new(1, 8).batch_size(16),
            rx.into_receivers(),
            handler::PerItem(move |(k, seq): (u64, u64)| {
                let last = last_c.clone();
                let viol = viol_c.clone();
                let proc = proc_c.clone();
                async move {
                    let prev = last[k as usize].swap(seq, Ordering::Relaxed);
                    if seq != 0 && seq <= prev {
                        viol.fetch_add(1, Ordering::Relaxed);
                    }
                    proc.fetch_add(1, Ordering::Relaxed);
                }
            }),
        );

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

        pool.wait_stopping().await;

        let expected = KEYS as u64 * PER_KEY;
        assert_eq!(
            processed.load(Ordering::Relaxed),
            expected,
            "loss/duplicates"
        );
        assert_eq!(
            violations.load(Ordering::Relaxed),
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
        let violations = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        let last_c = last.clone();
        let viol_c = violations.clone();
        let proc_c = processed.clone();
        let pool = async_pool(
            TokioRuntime,
            instance::Config::new(1, 8).batch_size(16),
            rx.into_receivers(),
            handler::PerItem(move |(k, seq): (String, u64)| {
                let last = last_c.clone();
                let viol = viol_c.clone();
                let proc = proc_c.clone();
                async move {
                    let idx: usize = k[1..].parse().unwrap();
                    let prev = last[idx].swap(seq, Ordering::Relaxed);
                    if seq != 0 && seq <= prev {
                        viol.fetch_add(1, Ordering::Relaxed);
                    }
                    proc.fetch_add(1, Ordering::Relaxed);
                }
            }),
        );

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

        pool.wait_stopping().await;

        let expected = KEYS as u64 * PER_KEY;
        assert_eq!(
            processed.load(Ordering::Relaxed),
            expected,
            "loss/duplicates"
        );
        assert_eq!(
            violations.load(Ordering::Relaxed),
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
        let pool = async_pool(
            TokioRuntime,
            instance::Config::new(2, 2),
            rx.into_receivers(),
            handler::Batch(move |batch: &[(String, u64)]| {
                let s = s.clone();
                let part: u64 = batch.iter().map(|(_, v)| *v).sum();
                async move {
                    s.fetch_add(part, Ordering::Relaxed);
                }
            }),
        );

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
}

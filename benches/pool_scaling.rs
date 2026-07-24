use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use hel::channel::{
    mpmc::{ShardGroupCase, SymbolHandle, round_robin, shard_group, shard_key},
    nearest_power_of_two,
};
use hel::pool::{
    async_pool_slot,
    handler::PerItem,
    instance::Config,
    sync_pool_slot,
    traits::{AsyncJoinHandle, AsyncRuntime},
};
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const CAP: usize = nearest_power_of_two(8192);

#[inline]
fn cpu_work(seed: u64, iters: u32) -> u64 {
    let mut acc = seed;
    for _ in 0..iters {
        acc = acc
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
    }
    acc
}

// TokioRuntime adapter (AsyncRuntime for tokio)
#[derive(Clone, Copy, Default)]
struct TokioRuntime;

impl AsyncRuntime for TokioRuntime {
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

struct TokioJoinHandle(tokio::task::JoinHandle<()>);

impl AsyncJoinHandle for TokioJoinHandle {
    async fn join(self) {
        let _ = self.0.await;
    }
}

fn run_sync(shards: usize, consumers: usize, work: u32, n: u64) -> Duration {
    let (tx, rx) = round_robin::<u64, CAP>(shards);
    let processed = Arc::new(AtomicU64::new(0));

    let p = processed.clone();
    let pool = sync_pool_slot(
        Config::new(consumers, consumers).batch_size(64),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            black_box(cpu_work(*v, work));
            p.fetch_add(1, Ordering::Relaxed);
        }),
        |_poison: u64, _panic_info| {},
    );

    thread::sleep(Duration::from_millis(5)); // warming up workers

    let n_producers = 2;
    let per = n / n_producers as u64;
    let start = Instant::now();
    let producers: Vec<_> = (0..n_producers)
        .map(|pi| {
            let tx = tx.clone();
            thread::spawn(move || {
                let base = pi as u64 * per;
                for i in 0..per {
                    tx.send(base + i).unwrap(); // blocking
                }
            })
        })
        .collect();
    for pr in producers {
        pr.join().unwrap();
    }
    drop(tx); // senders closed -> autodrainage
    pool.wait_stopping(); // WAITING FOR FULL PROCESSING
    let elapsed = start.elapsed(); // STOP AFTER treatment: sending + drainage
    elapsed
}

// ASYNC run
// Chases inside rt.block_on. Producers -regular streams (send synchronous), pool async tasks tokio.
// We take time from the start of sending until complete processing.
fn run_async(rt: &Runtime, shards: usize, consumers: usize, work: u32, n: u64) -> Duration {
    rt.block_on(async move {
        let (tx, rx) = round_robin::<u64, CAP>(shards);
        let processed = Arc::new(AtomicU64::new(0));

        let p = processed.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(consumers, consumers).batch_size(64),
            rx.into_receivers(),
            PerItem(move |v: &u64| {
                let p = p.clone();
                let v = *v;
                async move {
                    black_box(cpu_work(v, work));
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }),
            |_poison: u64, _panic_info| {},
        );

        tokio::time::sleep(Duration::from_millis(5)).await; // warming up workers

        let n_producers = 2;
        let per = n / n_producers as u64;
        let start = Instant::now();

        // producers regular streams (send is synchronous)
        let producers: Vec<_> = (0..n_producers)
            .map(|pi| {
                let tx = tx.clone();
                thread::spawn(move || {
                    let base = pi as u64 * per;
                    for i in 0..per {
                        tx.send(base + i).unwrap(); // blocking
                    }
                })
            })
            .collect();
        for pr in producers {
            pr.join().unwrap();
        }
        drop(tx); // senders closed -> autodrainage
        pool.wait_stopping().await;
        let elapsed = start.elapsed();
        elapsed
    })
}

// Keys spread wide enough that every shard gets hit.
fn make_keys(shards: usize) -> Vec<String> {
    let count = (shards * 8).max(64);
    (0..count).map(|k| format!("k{k}")).collect()
}

// SYNC shard_key run
fn run_sync_key(shards: usize, consumers: usize, work: u32, n: u64) -> Duration {
    let (tx, rx) = shard_key::<u64, CAP>(shards);
    let processed = Arc::new(AtomicU64::new(0));
    let p = processed.clone();
    let pool = sync_pool_slot(
        Config::new(consumers, consumers).batch_size(64),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            black_box(cpu_work(*v, work));
            p.fetch_add(1, Ordering::Relaxed);
        }),
        |_poison: u64, _panic_info| {},
    );
    thread::sleep(Duration::from_millis(5));

    let keys = Arc::new(make_keys(shards));
    let n_producers = 2;
    let per = n / n_producers as u64;
    let start = Instant::now();
    let producers: Vec<_> = (0..n_producers)
        .map(|pi| {
            let tx = tx.clone();
            let keys = keys.clone();
            thread::spawn(move || {
                let base = pi as u64 * per;
                for i in 0..per {
                    let idx = ((base + i) as usize) % keys.len();
                    tx.send(&keys[idx], base + i).unwrap();
                }
            })
        })
        .collect();
    for pr in producers {
        pr.join().unwrap();
    }
    drop(tx);
    pool.wait_stopping();
    start.elapsed()
}

// SYNC shard_group run (one symbol per group, so groups == shards)
fn run_sync_group(shards: usize, consumers: usize, work: u32, n: u64) -> Duration {
    let names: Vec<String> = (0..shards).map(|g| format!("g{g}")).collect();
    let groups_vec: Vec<Vec<&str>> = names.iter().map(|s| vec![s.as_str()]).collect();
    let groups_ref: Vec<&[&str]> = groups_vec.iter().map(|g| g.as_slice()).collect();
    let (tx, rx) = shard_group::<u64, CAP>(ShardGroupCase::Groups {
        groups: groups_ref.as_slice(),
    });
    let handles: Arc<Vec<SymbolHandle>> =
        Arc::new((0..shards).map(|g| tx.handle(&names[g]).unwrap()).collect());

    let processed = Arc::new(AtomicU64::new(0));
    let p = processed.clone();
    let pool = sync_pool_slot(
        Config::new(consumers, consumers).batch_size(64),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            black_box(cpu_work(*v, work));
            p.fetch_add(1, Ordering::Relaxed);
        }),
        |_poison: u64, _panic_info| {},
    );
    thread::sleep(Duration::from_millis(5));

    let n_producers = 2;
    let per = n / n_producers as u64;
    let start = Instant::now();
    let producers: Vec<_> = (0..n_producers)
        .map(|pi| {
            let tx = tx.clone();
            let handles = handles.clone();
            thread::spawn(move || {
                let base = pi as u64 * per;
                for i in 0..per {
                    let h = handles[((base + i) as usize) % handles.len()];
                    tx.send(h, base + i).unwrap();
                }
            })
        })
        .collect();
    for pr in producers {
        pr.join().unwrap();
    }
    drop(tx);
    pool.wait_stopping();
    start.elapsed()
}

// ASYNC shard_key run
fn run_async_key(rt: &Runtime, shards: usize, consumers: usize, work: u32, n: u64) -> Duration {
    rt.block_on(async move {
        let (tx, rx) = shard_key::<u64, CAP>(shards);
        let processed = Arc::new(AtomicU64::new(0));
        let p = processed.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(consumers, consumers).batch_size(64),
            rx.into_receivers(),
            PerItem(move |v: &u64| {
                let p = p.clone();
                let v = *v;
                async move {
                    black_box(cpu_work(v, work));
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }),
            |_poison: u64, _panic_info| {},
        );
        tokio::time::sleep(Duration::from_millis(5)).await;

        let keys = Arc::new(make_keys(shards));
        let n_producers = 2;
        let per = n / n_producers as u64;
        let start = Instant::now();
        let producers: Vec<_> = (0..n_producers)
            .map(|pi| {
                let tx = tx.clone();
                let keys = keys.clone();
                thread::spawn(move || {
                    let base = pi as u64 * per;
                    for i in 0..per {
                        let idx = ((base + i) as usize) % keys.len();
                        tx.send(&keys[idx], base + i).unwrap();
                    }
                })
            })
            .collect();
        for pr in producers {
            pr.join().unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        start.elapsed()
    })
}

// ASYNC shard_group run
fn run_async_group(rt: &Runtime, shards: usize, consumers: usize, work: u32, n: u64) -> Duration {
    rt.block_on(async move {
        let names: Vec<String> = (0..shards).map(|g| format!("g{g}")).collect();
        let groups_vec: Vec<Vec<&str>> = names.iter().map(|s| vec![s.as_str()]).collect();
        let groups_ref: Vec<&[&str]> = groups_vec.iter().map(|g| g.as_slice()).collect();
        let (tx, rx) = shard_group::<u64, CAP>(ShardGroupCase::Groups {
            groups: groups_ref.as_slice(),
        });
        let handles: Arc<Vec<SymbolHandle>> =
            Arc::new((0..shards).map(|g| tx.handle(&names[g]).unwrap()).collect());

        let processed = Arc::new(AtomicU64::new(0));
        let p = processed.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(consumers, consumers).batch_size(64),
            rx.into_receivers(),
            PerItem(move |v: &u64| {
                let p = p.clone();
                let v = *v;
                async move {
                    black_box(cpu_work(v, work));
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }),
            |_poison: u64, _panic_info| {},
        );
        tokio::time::sleep(Duration::from_millis(5)).await;

        let n_producers = 2;
        let per = n / n_producers as u64;
        let start = Instant::now();
        let producers: Vec<_> = (0..n_producers)
            .map(|pi| {
                let tx = tx.clone();
                let handles = handles.clone();
                thread::spawn(move || {
                    let base = pi as u64 * per;
                    for i in 0..per {
                        let h = handles[((base + i) as usize) % handles.len()];
                        tx.send(h, base + i).unwrap();
                    }
                })
            })
            .collect();
        for pr in producers {
            pr.join().unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        start.elapsed()
    })
}

// Throughput as the shard count grows, at a fixed consumer count. Checks that
// per shard scanning and placement stay flat from a few shards up to many.
const SHARD_SWEEP: [usize; 4] = [8, 64, 256, 1024];

fn bench_shard_scaling_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_scaling_sync");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    let n = 50_000u64;
    group.throughput(Throughput::Elements(n));
    let work = 500;
    let consumers = 8;

    for shards in SHARD_SWEEP {
        group.bench_with_input(BenchmarkId::new("rr", shards), &shards, |b, &s| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_sync(s, consumers, work, n);
                }
                total
            });
        });
        group.bench_with_input(BenchmarkId::new("key", shards), &shards, |b, &s| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_sync_key(s, consumers, work, n);
                }
                total
            });
        });
        group.bench_with_input(BenchmarkId::new("group", shards), &shards, |b, &s| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_sync_group(s, consumers, work, n);
                }
                total
            });
        });
    }
    group.finish();
}

fn bench_shard_scaling_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_scaling_async");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    let n = 50_000u64;
    group.throughput(Throughput::Elements(n));
    let work = 500;
    let consumers = 8;
    let rt = Runtime::new().unwrap();

    for shards in SHARD_SWEEP {
        group.bench_with_input(BenchmarkId::new("rr", shards), &shards, |b, &s| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_async(&rt, s, consumers, work, n);
                }
                total
            });
        });
        group.bench_with_input(BenchmarkId::new("key", shards), &shards, |b, &s| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_async_key(&rt, s, consumers, work, n);
                }
                total
            });
        });
        group.bench_with_input(BenchmarkId::new("group", shards), &shards, |b, &s| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_async_group(&rt, s, consumers, work, n);
                }
                total
            });
        });
    }
    group.finish();
}

// Zero loss under injected handler panics.
//
// The handler panics on one item out of every `fail_k`. Because `PerItem` reads
// the item by reference and never takes it, a panic before the commit point
// leaves the item with the worker, which hands it to the dead letter sink. So
// every item ends up in exactly one place: processed or dead lettered.
//
// Each run checks two things after the pool drains: the counts add up to N (no
// item lost, none handled twice) and the sum of the item values in both buckets
// equals the known total (a lost item lowers it, a duplicate raises it, so this
// catches both even if the counts happened to cancel out). Values are the
// distinct integers 0..N. The reported throughput counts all N items, processed
// plus recovered.
//
// Running this benchmark therefore also stress-tests the zero loss guarantee
// across thousands of iterations under real concurrency: any violation panics
// the run.

// Silence only the injected handler panics (the pool catches them, but the
// default hook still prints each one). Anything else, including a zero loss
// assert failure, is printed as usual so a real problem stays visible.
fn silence_injected_panics() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            let injected = info
                .payload()
                .downcast_ref::<&str>()
                .is_some_and(|s| *s == "injected fault");
            if !injected {
                eprintln!("{info}");
            }
        }));
    });
}

fn assert_zero_loss(n: u64, ok_count: u64, ok_sum: u64, dead_count: u64, dead_sum: u64) {
    assert_eq!(
        ok_count + dead_count,
        n,
        "loss or double handling: processed {ok_count} + dead lettered {dead_count} != {n}"
    );
    let expected_sum = n * (n - 1) / 2;
    assert_eq!(
        ok_sum + dead_sum,
        expected_sum,
        "checksum mismatch: an item was lost or duplicated"
    );
}

// Returns (elapsed, processed, dead_lettered).
fn run_sync_faults(
    shards: usize,
    consumers: usize,
    work: u32,
    n: u64,
    fail_k: u64,
) -> (Duration, u64, u64) {
    let (tx, rx) = round_robin::<u64, CAP>(shards);
    let ok_count = Arc::new(AtomicU64::new(0));
    let ok_sum = Arc::new(AtomicU64::new(0));
    let dead_count = Arc::new(AtomicU64::new(0));
    let dead_sum = Arc::new(AtomicU64::new(0));

    let (oc, os) = (ok_count.clone(), ok_sum.clone());
    let (dc, ds) = (dead_count.clone(), dead_sum.clone());
    let pool = sync_pool_slot(
        Config::new(consumers, consumers).batch_size(64),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            if *v % fail_k == 0 {
                panic!("injected fault");
            }
            black_box(cpu_work(*v, work));
            oc.fetch_add(1, Ordering::Relaxed);
            os.fetch_add(*v, Ordering::Relaxed);
        }),
        move |poison: u64, _reason| {
            dc.fetch_add(1, Ordering::Relaxed);
            ds.fetch_add(poison, Ordering::Relaxed);
        },
    );
    thread::sleep(Duration::from_millis(5));

    let n_producers = 2;
    let per = n / n_producers as u64;
    let start = Instant::now();
    let producers: Vec<_> = (0..n_producers)
        .map(|pi| {
            let tx = tx.clone();
            thread::spawn(move || {
                let base = pi as u64 * per;
                for i in 0..per {
                    tx.send(base + i).unwrap();
                }
            })
        })
        .collect();
    for pr in producers {
        pr.join().unwrap();
    }
    drop(tx);
    pool.wait_stopping();
    let elapsed = start.elapsed();

    let (p, d) = (
        ok_count.load(Ordering::Relaxed),
        dead_count.load(Ordering::Relaxed),
    );
    assert_zero_loss(
        n,
        p,
        ok_sum.load(Ordering::Relaxed),
        d,
        dead_sum.load(Ordering::Relaxed),
    );
    (elapsed, p, d)
}

// Returns (elapsed, processed, dead_lettered).
fn run_async_faults(
    rt: &Runtime,
    shards: usize,
    consumers: usize,
    work: u32,
    n: u64,
    fail_k: u64,
) -> (Duration, u64, u64) {
    rt.block_on(async move {
        let (tx, rx) = round_robin::<u64, CAP>(shards);
        let ok_count = Arc::new(AtomicU64::new(0));
        let ok_sum = Arc::new(AtomicU64::new(0));
        let dead_count = Arc::new(AtomicU64::new(0));
        let dead_sum = Arc::new(AtomicU64::new(0));

        let (oc, os) = (ok_count.clone(), ok_sum.clone());
        let (dc, ds) = (dead_count.clone(), dead_sum.clone());
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(consumers, consumers).batch_size(64),
            rx.into_receivers(),
            PerItem(move |v: &u64| {
                let (oc, os) = (oc.clone(), os.clone());
                let v = *v;
                async move {
                    if v % fail_k == 0 {
                        panic!("injected fault");
                    }
                    black_box(cpu_work(v, work));
                    oc.fetch_add(1, Ordering::Relaxed);
                    os.fetch_add(v, Ordering::Relaxed);
                }
            }),
            move |poison: u64, _reason| {
                dc.fetch_add(1, Ordering::Relaxed);
                ds.fetch_add(poison, Ordering::Relaxed);
            },
        );
        tokio::time::sleep(Duration::from_millis(5)).await;

        let n_producers = 2;
        let per = n / n_producers as u64;
        let start = Instant::now();
        let producers: Vec<_> = (0..n_producers)
            .map(|pi| {
                let tx = tx.clone();
                thread::spawn(move || {
                    let base = pi as u64 * per;
                    for i in 0..per {
                        tx.send(base + i).unwrap();
                    }
                })
            })
            .collect();
        for pr in producers {
            pr.join().unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        let elapsed = start.elapsed();

        let (p, d) = (
            ok_count.load(Ordering::Relaxed),
            dead_count.load(Ordering::Relaxed),
        );
        assert_zero_loss(
            n,
            p,
            ok_sum.load(Ordering::Relaxed),
            d,
            dead_sum.load(Ordering::Relaxed),
        );
        (elapsed, p, d)
    })
}

// One panic in every K items: rare (1000), heavy (10), brutal (3).
const FAULT_SWEEP: [usize; 3] = [1000, 10, 3];

fn bench_zero_loss_faults_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_loss_faults_sync");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    silence_injected_panics();
    let n = 50_000u64;
    group.throughput(Throughput::Elements(n));
    let (work, consumers, shards) = (200u32, 8usize, 16usize);

    for k in FAULT_SWEEP {
        let (_d, p, dead) = run_sync_faults(shards, consumers, work, n, k as u64);
        eprintln!(
            "[zero loss] sync, 1 panic in {k}: processed {p}, dead lettered {dead}, total {}/{n}, no loss no dupes",
            p + dead
        );
        group.bench_with_input(BenchmarkId::new("1_in", k), &k, |b, &k| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_sync_faults(shards, consumers, work, n, k as u64).0;
                }
                total
            });
        });
    }
    group.finish();
}

fn bench_zero_loss_faults_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_loss_faults_async");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    silence_injected_panics();
    let n = 50_000u64;
    group.throughput(Throughput::Elements(n));
    let (work, consumers, shards) = (200u32, 8usize, 16usize);
    let rt = Runtime::new().unwrap();

    for k in FAULT_SWEEP {
        let (_d, p, dead) = run_async_faults(&rt, shards, consumers, work, n, k as u64);
        eprintln!(
            "[zero loss] async, 1 panic in {k}: processed {p}, dead lettered {dead}, total {}/{n}, no loss no dupes",
            p + dead
        );
        group.bench_with_input(BenchmarkId::new("1_in", k), &k, |b, &k| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_async_faults(&rt, shards, consumers, work, n, k as u64).0;
                }
                total
            });
        });
    }
    group.finish();
}

// SYNC scaling
fn bench_sync_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_scaling");
    group.sample_size(15);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(2));

    let n = 50_000u64;
    group.throughput(Throughput::Elements(n));
    let work = 5000;

    for consumers in [1usize, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(consumers),
            &consumers,
            |b, &c| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_sync(8, c, work, n);
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// ASYNC scaling
fn bench_async_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_scaling");
    group.sample_size(15);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(2));

    let n = 50_000u64;
    group.throughput(Throughput::Elements(n));
    let work = 5000;

    // ONE runtime for the whole group (do not measure its start).
    // Worker_threads >= max consumers, so that worker tasks actually run in parallel.
    let rt = Runtime::new().unwrap();

    for consumers in [1usize, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(consumers),
            &consumers,
            |b, &c| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_async(&rt, 8, c, work, n);
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sync_scaling,
    bench_async_scaling,
    bench_shard_scaling_sync,
    bench_shard_scaling_async,
    bench_zero_loss_faults_sync,
    bench_zero_loss_faults_async
);
criterion_main!(benches);

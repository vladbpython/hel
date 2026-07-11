use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use hel::channel::{mpmc::round_robin, nearest_power_of_two};
use hel::pool::{
    async_pool,
    handler::PerItem,
    instance::Config,
    sync_pool,
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
    let pool = sync_pool(
        Config::new(consumers, consumers).batch_size(64),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            black_box(cpu_work(*v, work));
            p.fetch_add(1, Ordering::Relaxed);
        }),
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
        let pool = async_pool(
            TokioRuntime,
            Config::new(consumers, consumers).batch_size(64),
            rx.into_receivers(),
            PerItem(move |v: u64| {
                let p = p.clone();
                async move {
                    black_box(cpu_work(v, work));
                    p.fetch_add(1, Ordering::Relaxed);
                }
            }),
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

criterion_group!(benches, bench_sync_scaling, bench_async_scaling);
criterion_main!(benches);

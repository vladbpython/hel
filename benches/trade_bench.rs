use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hel::channel::mpmc::{round_robin, shard_key};
use hel::channel::spsc::SpscShard;
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Barrier as AsyncBarrier;

/// Tokio workers = number of P cores (M2 Max: 8P+4E → 8; i9 2019: 8).
/// Oversubscription is placed in a separate group trade_async_oversub.
const WORKERS: usize = 8;

// Binance WebSocket Trade &'static str, Copy, zero heap allocation
#[derive(Clone, Copy, Debug)]
pub struct Trade {
    pub e: &'static str,
    pub event_time: u64,
    pub s: &'static str,
    pub t: u64,
    pub p: &'static str,
    pub q: &'static str,
    pub trade_time: u64,
    pub m: bool,
    pub _m: bool,
}

const SYMBOLS: &[(&str, &str, &str)] = &[
    ("BTCUSDT", "43251.82000000", "0.12500000"),
    ("ETHUSDT", "2241.50000000", "1.05000000"),
    ("BNBBTC", "0.00823400", "150.00000000"),
    ("SOLUSDT", "98.45000000", "25.00000000"),
    ("XRPUSDT", "0.52310000", "1000.00000000"),
    ("ADAUSDT", "0.38200000", "500.00000000"),
    ("DOGEUSDT", "0.08234000", "10000.00000000"),
    ("MATICUSDT", "0.72100000", "800.00000000"),
    ("LTCUSDT", "71.25000000", "5.00000000"),
    ("DOTUSDT", "6.82000000", "50.00000000"),
    ("LINKUSDT", "14.53000000", "30.00000000"),
    ("AVAXUSDT", "35.21000000", "10.00000000"),
];

#[inline(always)]
fn make_trade(i: u64, producer: u64) -> Trade {
    let idx = ((producer * 1_000_000 + i) as usize) % SYMBOLS.len();
    let (sym, price, qty) = SYMBOLS[idx];
    Trade {
        e: "trade",
        event_time: 1672515782136 + i,
        s: sym,
        t: i,
        p: price,
        q: qty,
        trade_time: 1672515782136 + i,
        m: i.is_multiple_of(2),
        _m: true,
    }
}

#[inline(always)]
fn bench_hash(key: &str) -> usize {
    key.bytes().fold(14695981039346656037u64, |h, b| {
        (h ^ b as u64).wrapping_mul(1099511628211)
    }) as usize
}

// POOLS allocated ONCE per group, out of measurement, reused
// between iterations via Arc (Trade: Copy, no mutations).
// checksum = Σ t for all elements to verify delivery.

#[derive(Clone)]
struct Pools {
    data: Arc<Vec<Vec<Trade>>>,
    checksum: u64,
}

fn make_pools(producers: u64, n: u64) -> Pools {
    let items_per = n / producers;
    let data: Vec<Vec<Trade>> = (0..producers)
        .map(|p| (0..items_per).map(|i| make_trade(i, p)).collect())
        .collect();
    let checksum = data.iter().flatten().map(|t| t.t).sum();
    Pools {
        data: Arc::new(data),
        checksum,
    }
}

/// Один уникальный символ на пул — для shard_key с 1 продюсером на шард.
fn make_unique_sym_pools(num_pools: usize, n: u64) -> Pools {
    let items_per = n / num_pools as u64;
    let data: Vec<Vec<Trade>> = (0..num_pools)
        .map(|p| {
            let (sym, price, qty) = SYMBOLS[p % SYMBOLS.len()];
            (0..items_per)
                .map(|i| Trade {
                    e: "trade",
                    event_time: 1672515782136 + i,
                    s: sym,
                    t: i,
                    p: price,
                    q: qty,
                    trade_time: 1672515782136 + i,
                    m: i % 2 == 0,
                    _m: true,
                })
                .collect()
        })
        .collect();
    let checksum = data.iter().flatten().map(|t| t.t).sum();
    Pools {
        data: Arc::new(data),
        checksum,
    }
}

fn new_rt(workers: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap()
}

/// iter_custom binding for async runs: summarizes the Duration of single runs.
fn time_async(b: &mut criterion::Bencher, mut run_once: impl FnMut() -> Duration) {
    b.iter_custom(|iters| (0..iters).map(|_| run_once()).sum());
}

// SYNC double barrier: only the transfer is timed, not the allocation/join.
// spawn_fn returns (handles, total, expected) for checksum checking.

type SyncSpawn = (Vec<thread::JoinHandle<()>>, Arc<AtomicU64>, u64);

fn run_sync_bench(
    b: &mut criterion::Bencher,
    n: u64,
    n_threads: usize,
    spawn_fn: impl Fn(Arc<Barrier>, Arc<Barrier>, u64) -> SyncSpawn,
) {
    b.iter_custom(|iters| {
        let mut elapsed = Duration::ZERO;
        for _ in 0..iters {
            let sb = Arc::new(Barrier::new(n_threads + 1));
            let eb = Arc::new(Barrier::new(n_threads + 1));
            let (handles, total, expected) = spawn_fn(sb.clone(), eb.clone(), n);
            sb.wait();
            let t0 = Instant::now();
            eb.wait();
            elapsed += t0.elapsed();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(total.load(Relaxed), expected, "message loss detected");
        }
        elapsed
    });
}

macro_rules! sync_consumer {
    ($rx:ident, $total:ident, $sb:ident, $eb:ident) => {{
        let total = $total.clone();
        let sb2 = $sb.clone();
        let eb2 = $eb.clone();
        thread::spawn(move || {
            sb2.wait();
            let mut cnt = 0u64;
            while let Ok(t) = $rx.recv() {
                cnt += t.t;
            }
            total.fetch_add(cnt, Relaxed);
            eb2.wait();
        })
    }};
}

fn hel_key_sync(
    sb: Arc<Barrier>,
    eb: Arc<Barrier>,
    n: u64,
    producers: u64,
    num_shards: usize,
) -> SyncSpawn {
    let (tx, rx) = shard_key::<Trade, 1024>(num_shards);
    let items_per = n / producers;
    let expected = producers * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for r in rx.into_receivers() {
        handles.push(sync_consumer!(r, total, sb, eb));
    }
    for p in 0..producers {
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, p)).collect();
        let tx2 = tx.clone();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            for &t in &items {
                tx2.send(t.s, t).unwrap();
            }
            drop(tx2);
            eb2.wait();
            drop(items);
        }));
    }
    drop(tx);
    (handles, total, expected)
}

fn hel_rr_sync(
    sb: Arc<Barrier>,
    eb: Arc<Barrier>,
    n: u64,
    producers: u64,
    num_shards: usize,
) -> SyncSpawn {
    let (tx, rx) = round_robin::<Trade, 1024>(num_shards);
    let items_per = n / producers;
    let expected = producers * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for r in rx.into_receivers() {
        handles.push(sync_consumer!(r, total, sb, eb));
    }
    for p in 0..producers {
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, p)).collect();
        let tx2 = tx.clone();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            for &t in &items {
                tx2.send(t).unwrap();
            }
            drop(tx2);
            eb2.wait();
            drop(items);
        }));
    }
    drop(tx);
    (handles, total, expected)
}

fn hel_spsc_sync(sb: Arc<Barrier>, eb: Arc<Barrier>, n: u64, num_shards: usize) -> SyncSpawn {
    let ch = SpscShard::<Trade, 1024>::new(num_shards);
    let items_per = n / num_shards as u64;
    let expected = num_shards as u64 * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for (_, tx, rx) in ch.into_pairs() {
        handles.push(sync_consumer!(rx, total, sb, eb));
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, 0)).collect();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            for &t in &items {
                tx.send(t).unwrap();
            }
            drop(tx);
            eb2.wait();
            drop(items);
        }));
    }
    (handles, total, expected)
}

fn cb_key_sync(
    sb: Arc<Barrier>,
    eb: Arc<Barrier>,
    n: u64,
    producers: u64,
    num_shards: usize,
) -> SyncSpawn {
    let mask = num_shards - 1;
    let channels: Vec<_> = (0..num_shards)
        .map(|_| crossbeam_channel::bounded::<Trade>(1024))
        .collect();
    let txs: Arc<[_]> = channels
        .iter()
        .map(|(t, _)| t.clone())
        .collect::<Vec<_>>()
        .into();
    let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
    let items_per = n / producers;
    let expected = producers * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for rx in rxs {
        handles.push(sync_consumer!(rx, total, sb, eb));
    }
    for p in 0..producers {
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, p)).collect();
        let txs2 = txs.clone();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            for &t in &items {
                let shard = bench_hash(t.s) & mask;
                txs2[shard].send(t).unwrap();
            }
            drop(txs2);
            eb2.wait();
            drop(items);
        }));
    }
    (handles, total, expected)
}

fn cb_rr_sync(
    sb: Arc<Barrier>,
    eb: Arc<Barrier>,
    n: u64,
    producers: u64,
    num_shards: usize,
) -> SyncSpawn {
    let channels: Vec<_> = (0..num_shards)
        .map(|_| crossbeam_channel::bounded::<Trade>(1024))
        .collect();
    let txs: Arc<[_]> = channels
        .iter()
        .map(|(t, _)| t.clone())
        .collect::<Vec<_>>()
        .into();
    let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
    let items_per = n / producers;
    let expected = producers * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for rx in rxs {
        handles.push(sync_consumer!(rx, total, sb, eb));
    }
    for p in 0..producers {
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, p)).collect();
        let txs2 = txs.clone();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            let mut cursor = p as usize;
            for &t in &items {
                let shard = cursor % txs2.len();
                cursor += 1;
                txs2[shard].send(t).unwrap();
            }
            drop(txs2);
            eb2.wait();
            drop(items);
        }));
    }
    (handles, total, expected)
}

fn flume_key_sync(
    sb: Arc<Barrier>,
    eb: Arc<Barrier>,
    n: u64,
    producers: u64,
    num_shards: usize,
) -> SyncSpawn {
    let mask = num_shards - 1;
    let channels: Vec<_> = (0..num_shards)
        .map(|_| flume::bounded::<Trade>(1024))
        .collect();
    let txs: Arc<[_]> = channels
        .iter()
        .map(|(t, _)| t.clone())
        .collect::<Vec<_>>()
        .into();
    let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
    let items_per = n / producers;
    let expected = producers * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for rx in rxs {
        handles.push(sync_consumer!(rx, total, sb, eb));
    }
    for p in 0..producers {
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, p)).collect();
        let txs2 = txs.clone();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            for &t in &items {
                let shard = bench_hash(t.s) & mask;
                txs2[shard].send(t).unwrap();
            }
            drop(txs2);
            eb2.wait();
            drop(items);
        }));
    }
    (handles, total, expected)
}

fn flume_rr_sync(
    sb: Arc<Barrier>,
    eb: Arc<Barrier>,
    n: u64,
    producers: u64,
    num_shards: usize,
) -> SyncSpawn {
    let channels: Vec<_> = (0..num_shards)
        .map(|_| flume::bounded::<Trade>(1024))
        .collect();
    let txs: Arc<[_]> = channels
        .iter()
        .map(|(t, _)| t.clone())
        .collect::<Vec<_>>()
        .into();
    let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
    let items_per = n / producers;
    let expected = producers * (items_per * (items_per - 1) / 2);
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for rx in rxs {
        handles.push(sync_consumer!(rx, total, sb, eb));
    }
    for p in 0..producers {
        let items: Vec<Trade> = (0..items_per).map(|i| make_trade(i, p)).collect();
        let txs2 = txs.clone();
        let sb2 = sb.clone();
        let eb2 = eb.clone();
        handles.push(thread::spawn(move || {
            sb2.wait();
            let mut cursor = p as usize;
            for &t in &items {
                let shard = cursor % txs2.len();
                cursor += 1;
                txs2[shard].send(t).unwrap();
            }
            drop(txs2);
            eb2.wait();
            drop(items);
        }));
    }
    (handles, total, expected)
}

// ASYNC mirror sync methodology:
// outside of measurement: runtime, pools (once per group);
// inside block_on, but OUTSIDE the timer: creating channels, tokio::spawn,
// all tasks are parked on the starting AsyncBarrier;
// timer: from barrier.wait() of the main task to join all handles;
// after the timer: assert checksum.
// Each run_* function returns the Duration of one run; time_async
// sums them into iter_custom.

fn run_hel_key<const CAP: usize>(rt: &Runtime, pools: &Pools, num_shards: usize) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let (tx, rx) = shard_key::<Trade, CAP>(num_shards);
        let receivers = rx.into_receivers();
        let bar = Arc::new(AsyncBarrier::new(receivers.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = receivers
            .into_iter()
            .map(|r| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut s = 0u64;
                    while let Ok(v) = r.recv_async().await {
                        s += v.t;
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let tx = tx.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    for &t in &data[p] {
                        tx.send_async(t.s, t).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(total.load(Relaxed), expected, "message loss detected");
        elapsed
    })
}

fn run_hel_rr<const CAP: usize>(rt: &Runtime, pools: &Pools, num_shards: usize) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let (tx, rx) = round_robin::<Trade, CAP>(num_shards);
        let receivers = rx.into_receivers();
        let bar = Arc::new(AsyncBarrier::new(receivers.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = receivers
            .into_iter()
            .map(|r| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut s = 0u64;
                    while let Ok(v) = r.recv_async().await {
                        s += v.t;
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let tx = tx.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    for &t in &data[p] {
                        tx.send_async(t).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(total.load(Relaxed), expected, "message loss detected");
        elapsed
    })
}

fn run_hel_spsc<const CAP: usize>(rt: &Runtime, pools: &Pools) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    let num_shards = data.len();
    rt.block_on(async move {
        let ch = SpscShard::<Trade, CAP>::new(num_shards);
        let bar = Arc::new(AsyncBarrier::new(num_shards * 2 + 1));
        let total = Arc::new(AtomicU64::new(0));
        let pairs: Vec<_> = ch
            .into_pairs()
            .enumerate()
            .map(|(p, (_, tx, rx))| {
                let t = total.clone();
                let data = data.clone();
                let bc = bar.clone();
                let bp = bar.clone();
                let c = tokio::spawn(async move {
                    bc.wait().await;
                    let mut s = 0u64;
                    while let Ok(v) = rx.recv_async().await {
                        s += v.t;
                    }
                    t.fetch_add(s, Relaxed);
                });
                let pr = tokio::spawn(async move {
                    bp.wait().await;
                    for &t in &data[p] {
                        tx.send_async(t).await.unwrap();
                    }
                });
                (pr, c)
            })
            .collect();
        bar.wait().await;
        let t0 = Instant::now();
        for (pr, c) in pairs {
            pr.await.unwrap();
            c.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(total.load(Relaxed), expected, "message loss detected");
        elapsed
    })
}

fn run_flume_rr(rt: &Runtime, pools: &Pools, num_shards: usize, cap: usize) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let channels: Vec<_> = (0..num_shards)
            .map(|_| flume::bounded::<Trade>(cap))
            .collect();
        let txs: Arc<Vec<_>> = Arc::new(channels.iter().map(|(t, _)| t.clone()).collect());
        let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
        let bar = Arc::new(AsyncBarrier::new(rxs.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rxs
            .into_iter()
            .map(|rx| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut s = 0u64;
                    while let Ok(v) = rx.recv_async().await {
                        s += v.t;
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let txs = txs.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut cursor = p;
                    for &t in &data[p] {
                        let shard = cursor % txs.len();
                        cursor += 1;
                        txs[shard].send_async(t).await.unwrap();
                    }
                })
            })
            .collect();
        drop(txs);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(total.load(Relaxed), expected, "message loss detected");
        elapsed
    })
}

fn run_flume_key(rt: &Runtime, pools: &Pools, num_shards: usize, cap: usize) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    let mask = num_shards - 1;
    rt.block_on(async move {
        let channels: Vec<_> = (0..num_shards)
            .map(|_| flume::bounded::<Trade>(cap))
            .collect();
        let txs: Arc<Vec<_>> = Arc::new(channels.iter().map(|(t, _)| t.clone()).collect());
        let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
        let bar = Arc::new(AsyncBarrier::new(rxs.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rxs
            .into_iter()
            .map(|rx| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut s = 0u64;
                    while let Ok(v) = rx.recv_async().await {
                        s += v.t;
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let txs = txs.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    for &t in &data[p] {
                        let shard = bench_hash(t.s) & mask;
                        txs[shard].send_async(t).await.unwrap();
                    }
                })
            })
            .collect();
        drop(txs);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(total.load(Relaxed), expected, "message loss detected");
        elapsed
    })
}

// async batch
// Sending via send_batch_async: inside try fast path, with a full shard
// await exactly one element, then repeat in batch. Checksum assert catches
// losses/duplicates (history: try_send_batch_keyed lost groups before fix).

const BATCH: usize = 64;

fn run_hel_key_batch<const CAP: usize>(rt: &Runtime, pools: &Pools, num_shards: usize) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let (tx, rx) = shard_key::<Trade, CAP>(num_shards);
        let receivers = rx.into_receivers();
        let bar = Arc::new(AsyncBarrier::new(receivers.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = receivers
            .into_iter()
            .map(|r| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut rbuf: Vec<Trade> = Vec::with_capacity(256);
                    let mut s = 0u64;
                    loop {
                        let (n, dc) = r.recv_batch_async(&mut rbuf, 256).await;
                        if n == 0 && dc {
                            break;
                        }
                        for v in rbuf.drain(..) {
                            s += v.t;
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let tx = tx.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut buf: Vec<Trade> = Vec::with_capacity(BATCH);
                    for &t in &data[p] {
                        buf.push(t);
                        if buf.len() == BATCH {
                            tx.send_batch_async(&mut buf, |t| t.s).await.unwrap();
                        }
                    }
                    if !buf.is_empty() {
                        tx.send_batch_async(&mut buf, |t| t.s).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            total.load(Relaxed),
            expected,
            "message loss/duplication detected"
        );
        elapsed
    })
}

fn run_hel_rr_batch<const CAP: usize>(rt: &Runtime, pools: &Pools, num_shards: usize) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let (tx, rx) = round_robin::<Trade, CAP>(num_shards);
        let receivers = rx.into_receivers();
        let bar = Arc::new(AsyncBarrier::new(receivers.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = receivers
            .into_iter()
            .map(|r| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut rbuf: Vec<Trade> = Vec::with_capacity(256);
                    let mut s = 0u64;
                    loop {
                        let (n, dc) = r.recv_batch_async(&mut rbuf, 256).await;
                        if n == 0 && dc {
                            break;
                        }
                        for v in rbuf.drain(..) {
                            s += v.t;
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let tx = tx.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut buf: Vec<Trade> = Vec::with_capacity(BATCH);
                    for &t in &data[p] {
                        buf.push(t);
                        if buf.len() == BATCH {
                            tx.send_batch_async(&mut buf).await.unwrap();
                        }
                    }
                    if !buf.is_empty() {
                        tx.send_batch_async(&mut buf).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            total.load(Relaxed),
            expected,
            "message loss/duplication detected"
        );
        elapsed
    })
}

fn run_hel_spsc_batch<const CAP: usize>(rt: &Runtime, pools: &Pools) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    let num_shards = data.len();
    rt.block_on(async move {
        let ch = SpscShard::<Trade, CAP>::new(num_shards);
        let bar = Arc::new(AsyncBarrier::new(num_shards * 2 + 1));
        let total = Arc::new(AtomicU64::new(0));
        let pairs: Vec<_> = ch
            .into_pairs()
            .enumerate()
            .map(|(p, (_, tx, rx))| {
                let t = total.clone();
                let data = data.clone();
                let bc = bar.clone();
                let bp = bar.clone();
                let c = tokio::spawn(async move {
                    bc.wait().await;
                    let mut rbuf: Vec<Trade> = Vec::with_capacity(256);
                    let mut s = 0u64;
                    loop {
                        let (n, dc) = rx.recv_batch_async(&mut rbuf, 256).await;
                        if n == 0 && dc {
                            break;
                        }
                        for v in rbuf.drain(..) {
                            s += v.t;
                        }
                    }
                    t.fetch_add(s, Relaxed);
                });
                let pr = tokio::spawn(async move {
                    bp.wait().await;
                    let mut buf: Vec<Trade> = Vec::with_capacity(BATCH);
                    for &t in &data[p] {
                        buf.push(t);
                        if buf.len() == BATCH {
                            tx.send_batch_async(&mut buf).await;
                        }
                    }
                    tx.send_batch_async(&mut buf).await;
                });
                (pr, c)
            })
            .collect();
        bar.wait().await;
        let t0 = Instant::now();
        for (pr, c) in pairs {
            pr.await.unwrap();
            c.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            total.load(Relaxed),
            expected,
            "message loss/duplication detected"
        );
        elapsed
    })
}

// async prebatch PURE channel throughput
// Batches are cut into the task BEFORE the starting barrier: producer inside the timer
// does only try_send_batch(_keyed) on ready Vec<Trade>. No pushes
// no length checks, no allocations in the hot loop the API ceiling is measured.
// Price: ~80 MB of batch allocations for each iteration in the setup phase
// (try_send_batch drains the buffer, batches are disposable)  wall clock of the run
// will grow, does not affect the measurement.

fn run_hel_key_prebatch<const CAP: usize>(
    rt: &Runtime,
    pools: &Pools,
    num_shards: usize,
) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let (tx, rx) = shard_key::<Trade, CAP>(num_shards);
        let receivers = rx.into_receivers();
        let bar = Arc::new(AsyncBarrier::new(receivers.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = receivers
            .into_iter()
            .map(|r| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut rbuf: Vec<Trade> = Vec::with_capacity(256);
                    let mut s = 0u64;
                    loop {
                        let (n, dc) = r.recv_batch_async(&mut rbuf, 256).await;
                        if n == 0 && dc {
                            break;
                        }
                        for v in rbuf.drain(..) {
                            s += v.t;
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let tx = tx.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    let mut batches: Vec<Vec<Trade>> =
                        data[p].chunks(BATCH).map(|c| c.to_vec()).collect();
                    bar.wait().await;
                    for batch in &mut batches {
                        tx.send_batch_async(batch, |t| t.s).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            total.load(Relaxed),
            expected,
            "message loss/duplication detected"
        );
        elapsed
    })
}

fn run_hel_rr_prebatch<const CAP: usize>(
    rt: &Runtime,
    pools: &Pools,
    num_shards: usize,
) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    rt.block_on(async move {
        let (tx, rx) = round_robin::<Trade, CAP>(num_shards);
        let receivers = rx.into_receivers();
        let bar = Arc::new(AsyncBarrier::new(receivers.len() + data.len() + 1));
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = receivers
            .into_iter()
            .map(|r| {
                let t = total.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    bar.wait().await;
                    let mut rbuf: Vec<Trade> = Vec::with_capacity(256);
                    let mut s = 0u64;
                    loop {
                        let (n, dc) = r.recv_batch_async(&mut rbuf, 256).await;
                        if n == 0 && dc {
                            break;
                        }
                        for v in rbuf.drain(..) {
                            s += v.t;
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let ps: Vec<_> = (0..data.len())
            .map(|p| {
                let tx = tx.clone();
                let data = data.clone();
                let bar = bar.clone();
                tokio::spawn(async move {
                    let mut batches: Vec<Vec<Trade>> =
                        data[p].chunks(BATCH).map(|c| c.to_vec()).collect();
                    bar.wait().await;
                    for batch in &mut batches {
                        tx.send_batch_async(batch).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        bar.wait().await;
        let t0 = Instant::now();
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            total.load(Relaxed),
            expected,
            "message loss/duplication detected"
        );
        elapsed
    })
}

fn run_hel_spsc_prebatch<const CAP: usize>(rt: &Runtime, pools: &Pools) -> Duration {
    let data = pools.data.clone();
    let expected = pools.checksum;
    let num_shards = data.len();
    rt.block_on(async move {
        let ch = SpscShard::<Trade, CAP>::new(num_shards);
        let bar = Arc::new(AsyncBarrier::new(num_shards * 2 + 1));
        let total = Arc::new(AtomicU64::new(0));
        let pairs: Vec<_> = ch
            .into_pairs()
            .enumerate()
            .map(|(p, (_, tx, rx))| {
                let t = total.clone();
                let data = data.clone();
                let bc = bar.clone();
                let bp = bar.clone();
                let c = tokio::spawn(async move {
                    bc.wait().await;
                    let mut rbuf: Vec<Trade> = Vec::with_capacity(256);
                    let mut s = 0u64;
                    loop {
                        let (n, dc) = rx.recv_batch_async(&mut rbuf, 256).await;
                        if n == 0 && dc {
                            break;
                        }
                        for v in rbuf.drain(..) {
                            s += v.t;
                        }
                    }
                    t.fetch_add(s, Relaxed);
                });
                let pr = tokio::spawn(async move {
                    let mut batches: Vec<Vec<Trade>> =
                        data[p].chunks(BATCH).map(|c| c.to_vec()).collect();
                    bp.wait().await;
                    for batch in &mut batches {
                        tx.send_batch_async(batch).await;
                    }
                });
                (pr, c)
            })
            .collect();
        bar.wait().await;
        let t0 = Instant::now();
        for (pr, c) in pairs {
            pr.await.unwrap();
            c.await.unwrap();
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            total.load(Relaxed),
            expected,
            "message loss/duplication detected"
        );
        elapsed
    })
}

// BENCHMARK GROUPS

const N: u64 = 1_000_000;

fn bench_sync(c: &mut Criterion) {
    let mut g = c.benchmark_group("trade_sync");
    g.throughput(Throughput::Elements(N));
    g.measurement_time(Duration::from_secs(15));

    macro_rules! b {
        ($name:expr, $p:expr, $s:expr, $fn:expr) => {
            g.bench_function($name, |b| {
                run_sync_bench(b, N, $p + $s, |sb, eb, n| $fn(sb, eb, n, $p as u64, $s));
            });
        };
        (spsc $name:expr, $s:expr) => {
            g.bench_function($name, |b| {
                run_sync_bench(b, N, $s * 2, |sb, eb, n| hel_spsc_sync(sb, eb, n, $s));
            });
        };
    }

    b!("hel_key_4p_4s", 4, 4, hel_key_sync);
    b!("hel_rr_4p_4s", 4, 4, hel_rr_sync);
    b!(spsc "hel_spsc_4s", 4);
    b!("cb_key_4p_4s", 4, 4, cb_key_sync);
    b!("cb_rr_4p_4s", 4, 4, cb_rr_sync);
    b!("flume_key_4p_4s", 4, 4, flume_key_sync);
    b!("flume_rr_4p_4s", 4, 4, flume_rr_sync);

    b!("hel_key_8p_8s", 8, 8, hel_key_sync);
    b!("hel_rr_8p_8s", 8, 8, hel_rr_sync);
    b!(spsc "hel_spsc_8s", 8);
    b!("cb_key_8p_8s", 8, 8, cb_key_sync);
    b!("cb_rr_8p_8s", 8, 8, cb_rr_sync);
    b!("flume_key_8p_8s", 8, 8, flume_key_sync);
    b!("flume_rr_8p_8s", 8, 8, flume_rr_sync);

    b!("hel_key_12p_8s", 12, 8, hel_key_sync);
    b!("hel_rr_12p_8s", 12, 8, hel_rr_sync);
    b!("cb_key_12p_8s", 12, 8, cb_key_sync);
    b!("cb_rr_12p_8s", 12, 8, cb_rr_sync);
    b!("flume_key_12p_8s", 12, 8, flume_key_sync);
    b!("flume_rr_12p_8s", 12, 8, flume_rr_sync);

    b!("hel_key_12p_16s", 12, 16, hel_key_sync);
    b!("hel_rr_12p_16s", 12, 16, hel_rr_sync);
    b!(spsc "hel_spsc_16s", 16);
    b!("cb_key_12p_16s", 12, 16, cb_key_sync);
    b!("cb_rr_12p_16s", 12, 16, cb_rr_sync);
    b!("flume_key_12p_16s", 12, 16, flume_key_sync);
    b!("flume_rr_12p_16s", 12, 16, flume_rr_sync);

    g.finish();
}

fn bench_async_sharded(c: &mut Criterion) {
    let mut g = c.benchmark_group("trade_async_sharded");
    g.throughput(Throughput::Elements(N));
    g.measurement_time(Duration::from_secs(15));

    let rt = new_rt(WORKERS);
    let p4 = make_pools(4, N);
    let p8 = make_pools(8, N);
    let p12 = make_pools(12, N);

    g.bench_function("hel_key_4p_4s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &p4, 4))
    });
    g.bench_function("hel_rr_4p_4s", |b| {
        time_async(b, || run_hel_rr::<128>(&rt, &p4, 4))
    });
    g.bench_function("hel_key_8p_4s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &p8, 4))
    });
    g.bench_function("hel_key_8p_8s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &p8, 8))
    });
    g.bench_function("hel_rr_8p_4s", |b| {
        time_async(b, || run_hel_rr::<128>(&rt, &p8, 4))
    });
    g.bench_function("hel_rr_8p_8s", |b| {
        time_async(b, || run_hel_rr::<128>(&rt, &p8, 8))
    });
    g.bench_function("hel_key_12p_4s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &p12, 4))
    });
    g.bench_function("hel_key_12p_8s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &p12, 8))
    });
    g.bench_function("hel_rr_12p_4s", |b| {
        time_async(b, || run_hel_rr::<128>(&rt, &p12, 4))
    });
    g.bench_function("hel_rr_12p_8s", |b| {
        time_async(b, || run_hel_rr::<128>(&rt, &p12, 8))
    });
    g.bench_function("flume_rr_4p_4s", |b| {
        time_async(b, || run_flume_rr(&rt, &p4, 4, 128))
    });
    g.bench_function("flume_rr_8p_8s", |b| {
        time_async(b, || run_flume_rr(&rt, &p8, 8, 128))
    });
    g.bench_function("flume_rr_12p_8s", |b| {
        time_async(b, || run_flume_rr(&rt, &p12, 8, 128))
    });
    g.bench_function("flume_key_4p_4s", |b| {
        time_async(b, || run_flume_key(&rt, &p4, 4, 128))
    });
    g.bench_function("flume_key_8p_8s", |b| {
        time_async(b, || run_flume_key(&rt, &p8, 8, 128))
    });
    g.bench_function("flume_key_12p_8s", |b| {
        time_async(b, || run_flume_key(&rt, &p12, 8, 128))
    });

    g.finish();
}

fn bench_async_spsc_scale(c: &mut Criterion) {
    let mut g = c.benchmark_group("trade_async_spsc_scale");
    g.throughput(Throughput::Elements(N));
    g.measurement_time(Duration::from_secs(15));

    let rt = new_rt(WORKERS);
    let sp2 = make_pools(2, N);
    let sp4 = make_pools(4, N);
    let sp8 = make_pools(8, N);
    let sp16 = make_pools(16, N);
    let up2 = make_unique_sym_pools(2, N);
    let up4 = make_unique_sym_pools(4, N);
    let up8 = make_unique_sym_pools(8, N);
    let up16 = make_unique_sym_pools(16, N);

    g.bench_function("hel_spsc_scale_2s", |b| {
        time_async(b, || run_hel_spsc::<128>(&rt, &sp2))
    });
    g.bench_function("hel_spsc_scale_4s", |b| {
        time_async(b, || run_hel_spsc::<128>(&rt, &sp4))
    });
    g.bench_function("hel_spsc_scale_8s", |b| {
        time_async(b, || run_hel_spsc::<128>(&rt, &sp8))
    });
    g.bench_function("hel_spsc_scale_16s", |b| {
        time_async(b, || run_hel_spsc::<128>(&rt, &sp16))
    });
    g.bench_function("hel_mpmc_scale_2s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &up2, 2))
    });
    g.bench_function("hel_mpmc_scale_4s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &up4, 4))
    });
    g.bench_function("hel_mpmc_scale_8s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &up8, 8))
    });
    g.bench_function("hel_mpmc_scale_16s", |b| {
        time_async(b, || run_hel_key::<128>(&rt, &up16, 16))
    });

    g.finish();
}

fn bench_batch(c: &mut Criterion) {
    let mut g = c.benchmark_group("trade_batch");
    g.throughput(Throughput::Elements(N));
    g.measurement_time(Duration::from_secs(15));

    let rt = new_rt(WORKERS);
    let p4 = make_pools(4, N);
    let p8 = make_pools(8, N);

    // CAP = 1024 for both options: we measure the effect of batching, not the buffer size.
    g.bench_function("hel_key_4p_4s_elem", |b| {
        time_async(b, || run_hel_key::<1024>(&rt, &p4, 4))
    });
    g.bench_function("hel_key_4p_4s_batch", |b| {
        time_async(b, || run_hel_key_batch::<1024>(&rt, &p4, 4))
    });
    g.bench_function("hel_rr_4p_4s_elem", |b| {
        time_async(b, || run_hel_rr::<1024>(&rt, &p4, 4))
    });
    g.bench_function("hel_rr_4p_4s_batch", |b| {
        time_async(b, || run_hel_rr_batch::<1024>(&rt, &p4, 4))
    });
    g.bench_function("hel_spsc_4s_elem", |b| {
        time_async(b, || run_hel_spsc::<1024>(&rt, &p4))
    });
    g.bench_function("hel_spsc_4s_batch", |b| {
        time_async(b, || run_hel_spsc_batch::<1024>(&rt, &p4))
    });
    g.bench_function("hel_key_8p_8s_elem", |b| {
        time_async(b, || run_hel_key::<1024>(&rt, &p8, 8))
    });
    g.bench_function("hel_key_8p_8s_batch", |b| {
        time_async(b, || run_hel_key_batch::<1024>(&rt, &p8, 8))
    });
    g.bench_function("hel_rr_8p_8s_elem", |b| {
        time_async(b, || run_hel_rr::<1024>(&rt, &p8, 8))
    });
    g.bench_function("hel_rr_8p_8s_batch", |b| {
        time_async(b, || run_hel_rr_batch::<1024>(&rt, &p8, 8))
    });
    g.bench_function("hel_spsc_8s_elem", |b| {
        time_async(b, || run_hel_spsc::<1024>(&rt, &p8))
    });
    g.bench_function("hel_spsc_8s_batch", |b| {
        time_async(b, || run_hel_spsc_batch::<1024>(&rt, &p8))
    });

    g.bench_function("hel_key_4p_4s_prebatch", |b| {
        time_async(b, || run_hel_key_prebatch::<1024>(&rt, &p4, 4))
    });
    g.bench_function("hel_rr_4p_4s_prebatch", |b| {
        time_async(b, || run_hel_rr_prebatch::<1024>(&rt, &p4, 4))
    });
    g.bench_function("hel_spsc_4s_prebatch", |b| {
        time_async(b, || run_hel_spsc_prebatch::<1024>(&rt, &p4))
    });
    g.bench_function("hel_key_8p_8s_prebatch", |b| {
        time_async(b, || run_hel_key_prebatch::<1024>(&rt, &p8, 8))
    });
    g.bench_function("hel_rr_8p_8s_prebatch", |b| {
        time_async(b, || run_hel_rr_prebatch::<1024>(&rt, &p8, 8))
    });
    g.bench_function("hel_spsc_8s_prebatch", |b| {
        time_async(b, || run_hel_spsc_prebatch::<1024>(&rt, &p8))
    });

    g.finish();
}

/// Separate stress group: oversubscription (24 workers for 8 cores).
/// Shows degradation under displacement, cannot be compared with the main group.
fn bench_async_oversub(c: &mut Criterion) {
    let mut g = c.benchmark_group("trade_async_oversub");
    g.throughput(Throughput::Elements(N));
    g.measurement_time(Duration::from_secs(15));

    let rt24 = new_rt(24);
    let p12 = make_pools(12, N);

    g.bench_function("hel_key_12p_8s_w24", |b| {
        time_async(b, || run_hel_key::<128>(&rt24, &p12, 8))
    });
    g.bench_function("hel_rr_12p_8s_w24", |b| {
        time_async(b, || run_hel_rr::<128>(&rt24, &p12, 8))
    });
    g.bench_function("flume_rr_12p_8s_w24", |b| {
        time_async(b, || run_flume_rr(&rt24, &p12, 8, 128))
    });
    g.bench_function("flume_key_12p_8s_w24", |b| {
        time_async(b, || run_flume_key(&rt24, &p12, 8, 128))
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_sync,
    bench_async_sharded,
    bench_async_spsc_scale,
    bench_batch,
    bench_async_oversub
);
criterion_main!(benches);

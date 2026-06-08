use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hel::channel::{
    errors::*,
    mpmc::{round_robin, shard_key},
    spsc::{SpscShard, shard_spsc},
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    thread,
    time::Duration,
};
use tokio::runtime::Builder;

const SYMBOLS: &[&str] = &[
    "AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "AMD", "INTC", "NFLX", "BTC", "ETH",
];

/// Sharded ByKey: N producers → hash(key) → S shards → dedicated consumer per shard
fn sharded_bykey(n: u64, producers: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, rx) = shard_key::<u64, 128>(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .map(|r| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let items_per = n / producers;
        let ps: Vec<_> = (0..producers)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..items_per {
                        let sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                        tx.send_async(sym, i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// Sharded RoundRobin: N producers → RR → S shards → dedicated consumer per shard
fn sharded_rr(n: u64, producers: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, rx) = round_robin::<u64, 128>(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .map(|r| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let items_per = n / producers;
        let ps: Vec<_> = (0..producers)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..items_per {
                        let _sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                        tx.send_async(i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// Sharded fanin: N producers → hash → S shards → ONE consumer via recv_any.
/// We measure overhead fanin vs dedicated consumer per shard.
fn sharded_fanin(n: u64, producers: u64, num_shards: usize, workers: usize) {
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, mut rx) = shard_key::<u64, 128>(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let t = total.clone();
        let c = tokio::spawn(async move {
            let mut s = 0u64;
            loop {
                match rx.recv_any().await {
                    Ok((_, v)) => s += v,
                    Err(_) => break,
                }
            }
            t.fetch_add(s, Relaxed);
        });
        let items_per = n / producers;
        let ps: Vec<_> = (0..producers)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..items_per {
                        let sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                        tx.send_async(sym, i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for h in ps {
            h.await.unwrap();
        }
        c.await.unwrap();
    });
}

/// Flume round robin: N producers → N bounded channels → N consumers (async)
fn flume_sharded_rr(n: u64, producers: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        // N separate channels, one per shard
        let channels: Vec<_> = (0..num_shards)
            .map(|_| flume::bounded::<u64>(128))
            .collect();
        let txs: Vec<_> = channels.iter().map(|(t, _)| t.clone()).collect();
        let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();

        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rxs
            .into_iter()
            .map(|rx| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    while let Ok(v) = rx.recv_async().await {
                        s += v;
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();

        let items_per = n / producers;
        let ps: Vec<_> = (0..producers)
            .map(|p| {
                let txs = txs.clone();
                tokio::spawn(async move {
                    for i in 0..items_per {
                        let shard = ((p * items_per + i) as usize) % txs.len();
                        txs[shard].send_async(i).await.unwrap();
                    }
                })
            })
            .collect();

        drop(txs);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// Flume by key: routing by hash(key) like hel sharded_key
fn flume_sharded_key(n: u64, producers: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let channels: Vec<_> = (0..num_shards)
            .map(|_| flume::bounded::<u64>(128))
            .collect();
        let txs: Vec<_> = channels.iter().map(|(t, _)| t.clone()).collect();
        let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();

        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rxs
            .into_iter()
            .map(|rx| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    while let Ok(v) = rx.recv_async().await {
                        s += v;
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();

        let items_per = n / producers;
        let ps: Vec<_> = (0..producers)
            .map(|p| {
                let txs = txs.clone();
                tokio::spawn(async move {
                    for i in 0..items_per {
                        let sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                        // hash by symbol name as in hel sharded_key
                        let shard = sym.len() % txs.len();
                        txs[shard].send_async(i).await.unwrap();
                    }
                })
            })
            .collect();

        drop(txs);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// Sharded fanin: N producers → hash → S shards → ONE consumer via recv_any

fn bench_sharded(c: &mut Criterion) {
    let mut g = c.benchmark_group("sharded_vs_mpmc");
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));

    //4p: baseline
    g.bench_function("sharded_bk_4p_4s", |b| {
        b.iter(|| sharded_bykey(1_000_000, 4, 4, 8))
    });
    g.bench_function("sharded_rr_4p_4s", |b| {
        b.iter(|| sharded_rr(1_000_000, 4, 4, 8))
    });

    //8p
    g.bench_function("sharded_bk_8p_4s", |b| {
        b.iter(|| sharded_bykey(1_000_000, 8, 4, 16))
    });
    g.bench_function("sharded_bk_8p_8s", |b| {
        b.iter(|| sharded_bykey(1_000_000, 8, 8, 16))
    });
    g.bench_function("sharded_rr_8p_4s", |b| {
        b.iter(|| sharded_rr(1_000_000, 8, 4, 16))
    });
    g.bench_function("sharded_rr_8p_8s", |b| {
        b.iter(|| sharded_rr(1_000_000, 8, 8, 16))
    });

    //12p
    g.bench_function("sharded_bk_12p_4s", |b| {
        b.iter(|| sharded_bykey(1_000_000, 12, 4, 24))
    });
    g.bench_function("sharded_bk_12p_8s", |b| {
        b.iter(|| sharded_bykey(1_000_000, 12, 8, 24))
    });
    g.bench_function("sharded_rr_12p_4s", |b| {
        b.iter(|| sharded_rr(1_000_000, 12, 4, 24))
    });
    g.bench_function("sharded_rr_12p_8s", |b| {
        b.iter(|| sharded_rr(1_000_000, 12, 8, 24))
    });

    //Fan in
    g.bench_function("sharded_fanin_12p_4s", |b| {
        b.iter(|| sharded_fanin(1_000_000, 12, 4, 13))
    });
    g.bench_function("sharded_fanin_12p_8s", |b| {
        b.iter(|| sharded_fanin(1_000_000, 12, 8, 13))
    });

    g.bench_function("flume_rr_4p_4s", |b| {
        b.iter(|| flume_sharded_rr(1_000_000, 4, 4, 8))
    });
    g.bench_function("flume_rr_8p_4s", |b| {
        b.iter(|| flume_sharded_rr(1_000_000, 8, 4, 16))
    });
    g.bench_function("flume_rr_8p_8s", |b| {
        b.iter(|| flume_sharded_rr(1_000_000, 8, 8, 16))
    });
    g.bench_function("flume_rr_12p_4s", |b| {
        b.iter(|| flume_sharded_rr(1_000_000, 12, 4, 24))
    });
    g.bench_function("flume_rr_12p_8s", |b| {
        b.iter(|| flume_sharded_rr(1_000_000, 12, 8, 24))
    });
    g.bench_function("flume_key_4p_4s", |b| {
        b.iter(|| flume_sharded_key(1_000_000, 4, 4, 8))
    });
    g.bench_function("flume_key_8p_8s", |b| {
        b.iter(|| flume_sharded_key(1_000_000, 8, 8, 16))
    });
    g.bench_function("flume_key_12p_8s", |b| {
        b.iter(|| flume_sharded_key(1_000_000, 12, 8, 24))
    });

    g.finish();
}

// Fair comparison: we also shard flume manually (N independent bounded + hash routing).
// So we eliminate the advantage of the architecture and compare only throughput channels.
// Sharded SPSC benchmarks
/// Sharded SPSC: N independent SPSC channels, 1 producer per shard.
fn sharded_spsc_bench(n: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let ch = SpscShard::<u64, 128>::new(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let items_per = n / num_shards as u64;
        let pairs: Vec<_> = ch
            .into_pairs()
            .map(|(_, tx, rx)| {
                let t = total.clone();
                let c = tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match rx.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                });
                let p = tokio::spawn(async move {
                    for i in 0..items_per {
                        tx.send_async(i).await.unwrap();
                    }
                });
                (p, c)
            })
            .collect();
        for (p, c) in pairs {
            p.await.unwrap();
            c.await.unwrap();
        }
    });
}

/// Sharded MPMC with 1 producer per shard a fair comparison with sharded SPSC.
/// Unique symbols for fair 1p/shard comparison.
/// Each character is hashed into a unique shard at 4 or 8 shards.
/// 4 shards: AAPL→3, GOOG→1, META→0, NVDA→2
/// 8 shards: AAPL→3, GOOG→1, AMZN→7, META→0, NVDA→6, AMD→5, ETH→4, ...
const UNIQUE_SYMBOLS_2S: &[&str] = &["AAPL", "META"];
const UNIQUE_SYMBOLS_4S: &[&str] = &["META", "GOOG", "NVDA", "AAPL"];
const UNIQUE_SYMBOLS_8S: &[&str] = &["META", "GOOG", "AAPL", "AMZN", "ETH", "AMD", "NVDA", "LTC"];
const UNIQUE_SYMBOLS_16S: &[&str] = &[
    "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AN", "AO", "AP",
];

/// Fair 1p/shard: each producer writes only one unique character
/// → only one shard → no cross shard contention.
fn sharded_bykey_1p_per_shard(n: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, rx) = shard_key::<u64, 128>(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let items_per = n / num_shards as u64;
        let syms: &[&str] = match num_shards {
            n if n <= 2 => UNIQUE_SYMBOLS_2S,
            n if n <= 4 => UNIQUE_SYMBOLS_4S,
            n if n <= 8 => UNIQUE_SYMBOLS_8S,
            _ => UNIQUE_SYMBOLS_16S,
        };
        let cs: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .map(|r| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        // Each producer writes only HIS symbol → only one shard
        let ps: Vec<_> = (0..num_shards)
            .map(|p| {
                let tx = tx.clone();
                let sym = syms[p % syms.len()];
                tokio::spawn(async move {
                    for i in 0..items_per {
                        tx.send_async(sym, i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// flume sharded SPSC: N independent flume::bounded channels, 1 producer per shard.
/// Fair comparison with hel SpscSharded same architecture, different implementations.
fn flume_spsc_sharded(n: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let pairs: Vec<_> = (0..num_shards)
            .map(|_| flume::bounded::<u64>(1024))
            .collect();
        let total = Arc::new(AtomicU64::new(0));
        let items_per = n / num_shards as u64;
        let handles: Vec<_> = pairs
            .into_iter()
            .map(|(tx, rx)| {
                let t = total.clone();
                let c = tokio::spawn(async move {
                    let mut s = 0u64;
                    while let Ok(v) = rx.recv_async().await {
                        s += v;
                    }
                    t.fetch_add(s, Relaxed);
                });
                let p = tokio::spawn(async move {
                    for i in 0..items_per {
                        tx.send_async(i).await.unwrap();
                    }
                });
                (p, c)
            })
            .collect();
        for (p, c) in handles {
            p.await.unwrap();
            c.await.unwrap();
        }
    });
}

fn bench_sharded_spsc(c: &mut Criterion) {
    let mut g = c.benchmark_group("sharded_spsc");
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));

    //one SPSC
    g.bench_function("spsc_single_1p1c", |b| {
        b.iter(|| {
            // sharded_spsc with 1 shard = pure SPSC without hash routing overhead
            let mut ch = shard_spsc::<u64, 128>(1);
            let (tx, rx) = ch.take_pair(0).unwrap();
            let rt = Builder::new_multi_thread()
                .worker_threads(2)
                .build()
                .unwrap();
            rt.block_on(async move {
                let p = tokio::spawn(async move {
                    for i in 0..1_000_000u64 {
                        tx.send_async(i).await.unwrap();
                    }
                });
                let c = tokio::spawn(async move {
                    let mut _s = 0u64;
                    loop {
                        match rx.recv_async().await {
                            Ok(v) => _s += v,
                            Err(_) => break,
                        }
                    }
                });
                p.await.unwrap();
                c.await.unwrap();
            });
        })
    });

    // 4 shards: SPSC vs MPMC(ByKey 1p/shard) vs flume
    g.bench_function("spsc_4s", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 4, 8))
    });
    g.bench_function("mpmc_bk_4s_1p_each", |b| {
        b.iter(|| sharded_bykey_1p_per_shard(1_000_000, 4, 8))
    });
    g.bench_function("flume_4s", |b| {
        b.iter(|| flume_spsc_sharded(1_000_000, 4, 8))
    });

    // 8 shards
    g.bench_function("spsc_8s", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 8, 16))
    });
    g.bench_function("mpmc_bk_8s_1p_each", |b| {
        b.iter(|| sharded_bykey_1p_per_shard(1_000_000, 8, 16))
    });
    g.bench_function("flume_8s", |b| {
        b.iter(|| flume_spsc_sharded(1_000_000, 8, 16))
    });

    // Scaling: throughput × N shards
    // SPSC: no CAS overhead → should grow linearly to bottleneck scheduler
    // MPMC ByKey: low CAS overhead even at 1p/shard
    g.bench_function("spsc_scale_2s", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 2, 4))
    });
    g.bench_function("spsc_scale_4s", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 4, 8))
    });
    g.bench_function("spsc_scale_8s", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 8, 16))
    });
    g.bench_function("spsc_scale_16s", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 16, 32))
    });
    g.bench_function("mpmc_bk_scale_2s", |b| {
        b.iter(|| sharded_bykey_1p_per_shard(1_000_000, 2, 4))
    });
    g.bench_function("mpmc_bk_scale_4s", |b| {
        b.iter(|| sharded_bykey_1p_per_shard(1_000_000, 4, 8))
    });
    g.bench_function("mpmc_bk_scale_8s", |b| {
        b.iter(|| sharded_bykey_1p_per_shard(1_000_000, 8, 16))
    });
    g.bench_function("mpmc_bk_scale_16s", |b| {
        b.iter(|| sharded_bykey_1p_per_shard(1_000_000, 16, 32))
    });

    g.finish();
}

/// Sharded batch: send_batch_keyed grouped by shards, one lock per shard.
fn sharded_key_batch(n: u64, producers: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, rx) = shard_key::<u64, 1024>(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .map(|r| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let items_per = n / producers;
        let batch_size = 64usize;
        let ps: Vec<_> = (0..producers)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u64> = Vec::with_capacity(batch_size);
                    for i in 0..items_per {
                        buf.push(i);
                        if buf.len() == batch_size {
                            // group batch by key one lock per shard
                            // try_send_batch_keyed → Result, flush unsent async
                            if tx
                                .try_send_batch_keyed(&mut buf, |v| {
                                    SYMBOLS[((p * items_per + v) as usize) % SYMBOLS.len()]
                                })
                                .is_err()
                            {
                                for v in buf.drain(..) {
                                    tx.send_async(
                                        SYMBOLS[(p as usize + v as usize) % SYMBOLS.len()],
                                        v,
                                    )
                                    .await
                                    .unwrap();
                                }
                            }
                        }
                    }
                    // flush the remainder
                    for v in buf {
                        tx.send_async(SYMBOLS[(p as usize + v as usize) % SYMBOLS.len()], v)
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// Sharded RR batch: try_send_batch to the next shard.
fn sharded_rr_batch(n: u64, producers: u64, num_shards: usize, workers: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, rx) = round_robin::<u64, 1024>(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let cs: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .map(|r| {
                let t = total.clone();
                tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                })
            })
            .collect();
        let items_per = n / producers;
        let batch_size = 64usize;
        let ps: Vec<_> = (0..producers)
            .map(|_| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u64> = Vec::with_capacity(batch_size);
                    for i in 0..items_per {
                        buf.push(i);
                        if buf.len() == batch_size {
                            // try_send_batch → Result, flush unsent async
                            if tx.try_send_batch(&mut buf).is_err() {
                                for v in buf.drain(..) {
                                    tx.send_async(v).await.unwrap();
                                }
                            }
                        }
                    }
                    for v in buf {
                        tx.send_async(v).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for h in ps {
            h.await.unwrap();
        }
        for h in cs {
            h.await.unwrap();
        }
    });
}

/// Sharded SPSC batch: SingleSender push_batch.
fn sharded_spsc_batch_bench(n: u64, num_shards: usize, workers: usize) {
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    rt.block_on(async move {
        let ch = SpscShard::<u64, 1024>::new(num_shards);
        let total = Arc::new(AtomicU64::new(0));
        let items_per = n / num_shards as u64;
        let batch_size = 64usize;
        let pairs: Vec<_> = ch
            .into_pairs()
            .map(|(_, tx, rx)| {
                let t = total.clone();
                let c = tokio::spawn(async move {
                    let mut s = 0u64;
                    loop {
                        match rx.recv_async().await {
                            Ok(v) => s += v,
                            Err(_) => break,
                        }
                    }
                    t.fetch_add(s, Relaxed);
                });
                let p = tokio::spawn(async move {
                    let mut buf: Vec<u64> = Vec::with_capacity(batch_size);
                    for i in 0..items_per {
                        buf.push(i);
                        if buf.len() == batch_size {
                            if tx.try_send_batch(&mut buf).is_err() {
                                for v in buf.drain(..) {
                                    tx.send_async(v).await.unwrap();
                                }
                            }
                        }
                    }
                    for v in buf {
                        tx.send_async(v).await.unwrap();
                    }
                });
                (p, c)
            })
            .collect();
        for (p, c) in pairs {
            p.await.unwrap();
            c.await.unwrap();
        }
    });
}

fn bench_batch_send(c: &mut Criterion) {
    let mut g = c.benchmark_group("batch_send");
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));

    // Baseline: per element vs batch
    g.bench_function("sharded_key_4p_4s_elem", |b| {
        b.iter(|| sharded_bykey(1_000_000, 4, 4, 8))
    });
    g.bench_function("sharded_key_4p_4s_batch", |b| {
        b.iter(|| sharded_key_batch(1_000_000, 4, 4, 8))
    });
    g.bench_function("sharded_rr_4p_4s_elem", |b| {
        b.iter(|| sharded_rr(1_000_000, 4, 4, 8))
    });
    g.bench_function("sharded_rr_4p_4s_batch", |b| {
        b.iter(|| sharded_rr_batch(1_000_000, 4, 4, 8))
    });
    g.bench_function("sharded_spsc_4s_elem", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 4, 8))
    });
    g.bench_function("sharded_spsc_4s_batch", |b| {
        b.iter(|| sharded_spsc_batch_bench(1_000_000, 4, 8))
    });

    // 8 shards
    g.bench_function("sharded_key_8p_8s_elem", |b| {
        b.iter(|| sharded_bykey(1_000_000, 8, 8, 16))
    });
    g.bench_function("sharded_key_8p_8s_batch", |b| {
        b.iter(|| sharded_key_batch(1_000_000, 8, 8, 16))
    });
    g.bench_function("sharded_rr_8p_8s_elem", |b| {
        b.iter(|| sharded_rr(1_000_000, 8, 8, 16))
    });
    g.bench_function("sharded_rr_8p_8s_batch", |b| {
        b.iter(|| sharded_rr_batch(1_000_000, 8, 8, 16))
    });
    g.bench_function("sharded_spsc_8s_elem", |b| {
        b.iter(|| sharded_spsc_bench(1_000_000, 8, 16))
    });
    g.bench_function("sharded_spsc_8s_batch", |b| {
        b.iter(|| sharded_spsc_batch_bench(1_000_000, 8, 16))
    });

    g.finish();
}

// ShardedKey with short characters for comparison.

/// hel ShardedKey sync: N OS threads producers → hash(key) → S shards → N consumers.
fn sync_hel_sharded_bk(n: u64, producers: u64, num_shards: usize) {
    let (tx, rx) = shard_key::<u64, 1024>(num_shards);
    let total = Arc::new(AtomicU64::new(0));
    let items_per = n / producers;
    let cs: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .map(|r| {
            let t = total.clone();
            thread::spawn(move || {
                let mut s = 0u64;
                loop {
                    match r.recv() {
                        Ok(v) => s += v,
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                t.fetch_add(s, Relaxed);
            })
        })
        .collect();
    let ps: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..items_per {
                    let sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                    tx.send(sym, i).unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    for h in ps {
        h.join().unwrap();
    }
    for h in cs {
        h.join().unwrap();
    }
}

/// hel Sharded RoundRobin sync.
fn sync_hel_sharded_rr(n: u64, producers: u64, num_shards: usize) {
    let (tx, rx) = round_robin::<u64, 1024>(num_shards);
    let total = Arc::new(AtomicU64::new(0));
    let items_per = n / producers;
    let cs: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .map(|r| {
            let t = total.clone();
            thread::spawn(move || {
                let mut s = 0u64;
                loop {
                    match r.recv() {
                        Ok(v) => s += v,
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                t.fetch_add(s, Relaxed);
            })
        })
        .collect();
    let ps: Vec<_> = (0..producers)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..items_per {
                    tx.send(i).unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    for h in ps {
        h.join().unwrap();
    }
    for h in cs {
        h.join().unwrap();
    }
}

/// hel SpscSharded sync: N independent SPSC channels, 1 thread per shard.
fn sync_hel_sharded_spsc(n: u64, num_shards: usize) {
    let ch = SpscShard::<u64, 1024>::new(num_shards);
    let total = Arc::new(AtomicU64::new(0));
    let items_per = n / num_shards as u64;
    let handles: Vec<_> = ch
        .into_pairs()
        .map(|(_, tx, rx)| {
            let t = total.clone();
            let c = thread::spawn(move || {
                let mut s = 0u64;
                loop {
                    match rx.recv() {
                        Ok(v) => s += v,
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                t.fetch_add(s, Relaxed);
            });
            let p = thread::spawn(move || {
                for i in 0..items_per {
                    tx.send(i).unwrap();
                }
            });
            (p, c)
        })
        .collect();
    for (p, c) in handles {
        p.join().unwrap();
        c.join().unwrap();
    }
}

/// flume sharded sync: N independent flume::bounded channels.
fn sync_flume_sharded(n: u64, producers: u64, num_shards: usize) {
    let (txs_orig, rxs): (Vec<_>, Vec<_>) =
        (0..num_shards).map(|_| flume::bounded::<u64>(1024)).unzip();
    let total = Arc::new(AtomicU64::new(0));
    let items_per = n / producers;
    let mask = num_shards - 1;
    let cs: Vec<_> = rxs
        .into_iter()
        .map(|rx| {
            let t = total.clone();
            thread::spawn(move || {
                let mut s = 0u64;
                while let Ok(v) = rx.recv() {
                    s += v;
                }
                t.fetch_add(s, Relaxed);
            })
        })
        .collect();
    let ps: Vec<_> = (0..producers)
        .map(|p| {
            let txs = txs_orig.clone(); // clones for producer
            thread::spawn(move || {
                for i in 0..items_per {
                    let sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                    let mut h = DefaultHasher::new();
                    sym.hash(&mut h);
                    let shard = h.finish() as usize & mask;
                    txs[shard].send(i).unwrap();
                }
            })
        })
        .collect();
    drop(txs_orig); // drop the originals consumers will see Disconnected when producer threads complete
    for h in ps {
        h.join().unwrap();
    }
    for h in cs {
        h.join().unwrap();
    }
}

/// crossbeam sharded sync: N independent crossbeam::bounded channels.
fn sync_crossbeam_sharded(n: u64, producers: u64, num_shards: usize) {
    let (txs_orig, rxs): (Vec<_>, Vec<_>) = (0..num_shards)
        .map(|_| crossbeam_channel::bounded::<u64>(1024))
        .unzip();
    let total = Arc::new(AtomicU64::new(0));
    let items_per = n / producers;
    let mask = num_shards - 1;
    let cs: Vec<_> = rxs
        .into_iter()
        .map(|rx| {
            let t = total.clone();
            thread::spawn(move || {
                let mut s = 0u64;
                while let Ok(v) = rx.recv() {
                    s += v;
                }
                t.fetch_add(s, Relaxed);
            })
        })
        .collect();
    let ps: Vec<_> = (0..producers)
        .map(|p| {
            let txs = txs_orig.clone();
            thread::spawn(move || {
                for i in 0..items_per {
                    let sym = SYMBOLS[((p * items_per + i) as usize) % SYMBOLS.len()];
                    let mut h = DefaultHasher::new();
                    sym.hash(&mut h);
                    let shard = h.finish() as usize & mask;
                    txs[shard].send(i).unwrap();
                }
            })
        })
        .collect();
    drop(txs_orig); // originals are dropped -consumers will see Disconnected
    for h in ps {
        h.join().unwrap();
    }
    for h in cs {
        h.join().unwrap();
    }
}

/// Flume round robin sync: N producers → N shards (round-robin) → N consumers
fn sync_flume_sharded_rr(n: u64, producers: u64, num_shards: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering::*},
    };
    let channels: Vec<_> = (0..num_shards)
        .map(|_| flume::bounded::<u64>(1024))
        .collect();
    let txs: Arc<Vec<_>> = Arc::new(channels.iter().map(|(t, _)| t.clone()).collect());
    let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
    let total = Arc::new(AtomicU64::new(0));

    let cs: Vec<_> = rxs
        .into_iter()
        .map(|rx| {
            let t = total.clone();
            std::thread::spawn(move || {
                let mut s = 0u64;
                while let Ok(v) = rx.recv() {
                    s += v;
                }
                t.fetch_add(s, Relaxed);
            })
        })
        .collect();

    let items_per = n / producers;
    let counter = Arc::new(AtomicUsize::new(0));
    let ps: Vec<_> = (0..producers)
        .map(|_p| {
            let txs = txs.clone();
            let counter = counter.clone();
            std::thread::spawn(move || {
                for i in 0..items_per {
                    let shard = counter.fetch_add(1, Relaxed) % txs.len();
                    txs[shard].send(i).unwrap();
                }
            })
        })
        .collect();

    for h in ps {
        h.join().unwrap();
    }
    drop(txs);
    for h in cs {
        h.join().unwrap();
    }
}

/// Crossbeam round robin sync: N producers → N shards → N consumers
fn sync_crossbeam_sharded_rr(n: u64, producers: u64, num_shards: usize) {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering::*},
    };
    let channels: Vec<_> = (0..num_shards)
        .map(|_| crossbeam_channel::bounded::<u64>(1024))
        .collect();
    let txs: Arc<Vec<_>> = Arc::new(channels.iter().map(|(t, _)| t.clone()).collect());
    let rxs: Vec<_> = channels.into_iter().map(|(_, r)| r).collect();
    let total = Arc::new(AtomicU64::new(0));

    let cs: Vec<_> = rxs
        .into_iter()
        .map(|rx| {
            let t = total.clone();
            std::thread::spawn(move || {
                let mut s = 0u64;
                while let Ok(v) = rx.recv() {
                    s += v;
                }
                t.fetch_add(s, Relaxed);
            })
        })
        .collect();

    let items_per = n / producers;
    let counter = Arc::new(AtomicUsize::new(0));
    let ps: Vec<_> = (0..producers)
        .map(|_p| {
            let txs = txs.clone();
            let counter = counter.clone();
            std::thread::spawn(move || {
                for i in 0..items_per {
                    let shard = counter.fetch_add(1, Relaxed) % txs.len();
                    txs[shard].send(i).unwrap();
                }
            })
        })
        .collect();

    for h in ps {
        h.join().unwrap();
    }
    drop(txs);
    for h in cs {
        h.join().unwrap();
    }
}

fn bench_sync_sharded_compare(c: &mut Criterion) {
    let mut g = c.benchmark_group("sync_sharded_compare");
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));

    // 4p/4s
    g.bench_function("hel_bk_4p_4s", |b| {
        b.iter(|| sync_hel_sharded_bk(1_000_000, 4, 4))
    });
    g.bench_function("hel_rr_4p_4s", |b| {
        b.iter(|| sync_hel_sharded_rr(1_000_000, 4, 4))
    });
    g.bench_function("hel_spsc_4s", |b| {
        b.iter(|| sync_hel_sharded_spsc(1_000_000, 4))
    });
    g.bench_function("flume_4p_4s", |b| {
        b.iter(|| sync_flume_sharded(1_000_000, 4, 4))
    });
    g.bench_function("flume_rr_4p_4s", |b| {
        b.iter(|| sync_flume_sharded_rr(1_000_000, 4, 4))
    });
    g.bench_function("crossbeam_4p_4s", |b| {
        b.iter(|| sync_crossbeam_sharded(1_000_000, 4, 4))
    });
    g.bench_function("crossbeam_rr_4p_4s", |b| {
        b.iter(|| sync_crossbeam_sharded_rr(1_000_000, 4, 4))
    });

    // 8p/8s
    g.bench_function("hel_bk_8p_8s", |b| {
        b.iter(|| sync_hel_sharded_bk(1_000_000, 8, 8))
    });
    g.bench_function("hel_rr_8p_8s", |b| {
        b.iter(|| sync_hel_sharded_rr(1_000_000, 8, 8))
    });
    g.bench_function("hel_spsc_8s", |b| {
        b.iter(|| sync_hel_sharded_spsc(1_000_000, 8))
    });
    g.bench_function("flume_8p_8s", |b| {
        b.iter(|| sync_flume_sharded(1_000_000, 8, 8))
    });
    g.bench_function("flume_rr_8p_8s", |b| {
        b.iter(|| sync_flume_sharded_rr(1_000_000, 8, 8))
    });
    g.bench_function("crossbeam_8p_8s", |b| {
        b.iter(|| sync_crossbeam_sharded(1_000_000, 8, 8))
    });
    g.bench_function("crossbeam_rr_8p_8s", |b| {
        b.iter(|| sync_crossbeam_sharded_rr(1_000_000, 8, 8))
    });

    // 12p/8s
    g.bench_function("hel_bk_12p_8s", |b| {
        b.iter(|| sync_hel_sharded_bk(1_000_000, 12, 8))
    });
    g.bench_function("hel_rr_12p_8s", |b| {
        b.iter(|| sync_hel_sharded_rr(1_000_000, 12, 8))
    });
    g.bench_function("flume_12p_8s", |b| {
        b.iter(|| sync_flume_sharded(1_000_000, 12, 8))
    });
    g.bench_function("flume_rr_12p_8s", |b| {
        b.iter(|| sync_flume_sharded_rr(1_000_000, 12, 8))
    });
    g.bench_function("crossbeam_12p_8s", |b| {
        b.iter(|| sync_crossbeam_sharded(1_000_000, 12, 8))
    });
    g.bench_function("crossbeam_rr_12p_8s", |b| {
        b.iter(|| sync_crossbeam_sharded_rr(1_000_000, 12, 8))
    });

    // 12p/12s → replace with 12p/16s (nearest power of two ≥ 12)
    g.bench_function("hel_bk_12p_16s", |b| {
        b.iter(|| sync_hel_sharded_bk(1_000_000, 12, 16))
    });
    g.bench_function("hel_rr_12p_16s", |b| {
        b.iter(|| sync_hel_sharded_rr(1_000_000, 12, 16))
    });
    g.bench_function("hel_spsc_16s", |b| {
        b.iter(|| sync_hel_sharded_spsc(1_000_000, 16))
    });
    g.bench_function("flume_12p_16s", |b| {
        b.iter(|| sync_flume_sharded(1_000_000, 12, 16))
    });
    g.bench_function("flume_rr_12p_16s", |b| {
        b.iter(|| sync_flume_sharded_rr(1_000_000, 12, 16))
    });
    g.bench_function("crossbeam_12p_16s", |b| {
        b.iter(|| sync_crossbeam_sharded(1_000_000, 12, 16))
    });
    g.bench_function("crossbeam_rr_12p_16s", |b| {
        b.iter(|| sync_crossbeam_sharded_rr(1_000_000, 12, 16))
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_sync_sharded_compare,
    bench_sharded,
    bench_sharded_spsc,
    bench_batch_send
);
criterion_main!(benches);

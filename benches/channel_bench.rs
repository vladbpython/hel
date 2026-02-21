use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::{hint::black_box, time::{Duration, Instant}};
use hel::{
    channel::{bounded, scsp_bounded},
    errors::RecvError,
};

fn percentile(mut s: Vec<u128>, p: f64) -> u128 {
    s.sort_unstable();
    s[((s.len() as f64 * p / 100.0) as usize).min(s.len()-1)]
}

fn print_lat(label: &str, s: Vec<u128>) {
    println!("{label:42} p50={:>5}ns  p99={:>5}ns  p999={:>6}ns  max={:>7}ns",
        percentile(s.clone(), 50.0), percentile(s.clone(), 99.0),
        percentile(s.clone(), 99.9), *s.iter().max().unwrap_or(&0));
}

fn lat_mpmc(n: u64) -> Vec<u128> {
    let (tx, rx) = bounded::<u64, 1024>();
    (0..n).map(|i| { let t = Instant::now(); tx.try_send(black_box(i)).ok(); rx.try_recv().ok(); t.elapsed().as_nanos() }).collect()
}

fn lat_spsc(n: u64) -> Vec<u128> {
    let (tx, rx) = scsp_bounded::<u64, 1024>();
    (0..n).map(|i| { let t = Instant::now(); tx.try_send(black_box(i)).ok(); rx.try_recv().ok(); t.elapsed().as_nanos() }).collect()
}

fn lat_flume(n: u64) -> Vec<u128> {
    let (tx, rx) = flume::bounded::<u64>(1024);
    (0..n).map(|i| { let t = Instant::now(); tx.try_send(black_box(i)).ok(); rx.try_recv().ok(); t.elapsed().as_nanos() }).collect()
}

fn blocking_mpmc_ours(n: u64) {
    use std::{sync::{atomic::{AtomicU64, Ordering::Relaxed}, Arc}, thread};
    let (tx, rx) = bounded::<u64, 64>();
    const P: u64 = 4; const C: u64 = 4;
    let total = Arc::new(AtomicU64::new(0));
    let ps: Vec<_> = (0..P).map(|_| { let tx=tx.clone(); thread::spawn(move || { for i in 0..n/P { tx.send(i).unwrap(); } }) }).collect();
    drop(tx);
    let cs: Vec<_> = (0..C).map(|_| { let rx=rx.clone(); let t=total.clone(); thread::spawn(move || { let mut s=0u64; loop { match rx.recv() { Ok(v)=>s+=v, Err(RecvError::Disconnected)=>break, _=>unreachable!() } } t.fetch_add(s,Relaxed); }) }).collect();
    for h in ps { h.join().unwrap(); } for h in cs { h.join().unwrap(); }
}

fn blocking_spsc(n: u64) {
    use std::thread;
    let (tx, rx) = scsp_bounded::<u64, 64>();
    let h = thread::spawn(move || { for i in 0..n { tx.send(i).unwrap(); } });
    let mut s = 0u64;
    loop { match rx.recv() { Ok(v)=>s+=v, Err(RecvError::Disconnected)=>break, _=>unreachable!() } }
    let _ = s; h.join().unwrap();
}

fn blocking_mpmc_flume(n: u64) {
    use std::{sync::{atomic::{AtomicU64, Ordering::Relaxed}, Arc}, thread};
    let (tx,rx) = flume::bounded::<u64>(64);
    const P:u64=4; const C:u64=4;
    let t = Arc::new(AtomicU64::new(0));
    let ps: Vec<_> = (0..P).map(|_| { let tx=tx.clone(); thread::spawn(move || { for i in 0..n/P { tx.send(i).unwrap(); } }) }).collect();
    drop(tx);
    let cs: Vec<_> = (0..C).map(|_| { let rx=rx.clone(); let t=t.clone(); thread::spawn(move || { let mut s=0u64; while let Ok(v)=rx.recv() { s+=v; } t.fetch_add(s,Relaxed); }) }).collect();
    for h in ps { h.join().unwrap(); } for h in cs { h.join().unwrap(); }
}

fn async_mpmc_hel(n: u64) {
    use std::sync::{atomic::{AtomicU64, Ordering::Relaxed}, Arc};
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(8).build().unwrap();
    rt.block_on(async move {
        let (tx, rx) = bounded::<u64, 128>();
        const P: u64=4; const C: u64=4;
        let total = Arc::new(AtomicU64::new(0));
        let ps: Vec<_> = (0..P).map(|_| { let tx=tx.clone(); tokio::spawn(async move { for i in 0..n/P { tx.send_async(i).await.unwrap(); } }) }).collect();
        drop(tx);
        let cs: Vec<_> = (0..C).map(|_| { let rx=rx.clone(); let t=total.clone(); tokio::spawn(async move { let mut s=0u64; loop { match rx.recv_async().await { Ok(v)=>s+=v, Err(RecvError::Disconnected)=>break, _=>unreachable!() } } t.fetch_add(s,Relaxed); }) }).collect();
        for h in ps { h.await.unwrap(); } for h in cs { h.await.unwrap(); }
    });
}

fn async_spsc(n: u64) {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap();
    rt.block_on(async move {
        let (tx, rx) = scsp_bounded::<u64, 128>();
        tokio::spawn(async move { for i in 0..n { tx.send_async(i).await.unwrap(); } });
        let mut s = 0u64;
        loop { match rx.recv_async().await { Ok(v)=>s+=v, Err(RecvError::Disconnected)=>break, _=>unreachable!() } }
        let _ = s;
    });
}

fn async_mpmc_flume(n: u64) {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(8).build().unwrap();
    rt.block_on(async move {
        let (tx,rx)=flume::bounded::<u64>(128);
        const P:u64=4; const C:u64=4;
        let ps: Vec<_> = (0..P).map(|_| { let tx=tx.clone(); tokio::spawn(async move { for i in 0..n/P { tx.send_async(i).await.unwrap(); } }) }).collect();
        drop(tx);
        let cs: Vec<_> = (0..C).map(|_| { let rx=rx.clone(); tokio::spawn(async move { while let Ok(_)=rx.recv_async().await {} }) }).collect();
        for h in ps { h.await.unwrap(); } for h in cs { h.await.unwrap(); }
    });
}

fn bench_batch_vs_tryrecv(n: u64) {
    use std::thread;
    // batch
    let (tx, rx) = bounded::<u64, 256>();
    let h = thread::spawn(move || { for i in 0..n { tx.send(i).unwrap(); } });
    let mut buf = Vec::with_capacity(64);
    loop {
        let (cnt, disc) = rx.recv_batch(&mut buf, 64);
        buf.clear();
        if disc && cnt == 0 { break; }
        if cnt == 0 { std::hint::spin_loop(); }
    }
    h.join().unwrap();
}

fn bench_latency(c: &mut Criterion) {
    println!("=== Latency distribution (100k samples) ===");
    print_lat("hel  mpmc hot_path", lat_mpmc(100_000));
    print_lat("hel  spsc hot_path", lat_spsc(100_000));
    print_lat("flume     hot_path", lat_flume(100_000));

    let mut g = c.benchmark_group("hot_path_throughput");
    g.throughput(Throughput::Elements(1_000_000));
    g.bench_function("hel_mpmc", |b| b.iter(|| { let (tx,rx)=bounded::<u64,1024>(); for i in 0..1_000_000u64 { tx.try_send(black_box(i)).ok(); rx.try_recv().ok(); } }));
    g.bench_function("hel_spsc", |b| b.iter(|| { let (tx,rx)=scsp_bounded::<u64,1024>(); for i in 0..1_000_000u64 { tx.try_send(black_box(i)).ok(); rx.try_recv().ok(); } }));
    g.bench_function("flume",     |b| b.iter(|| { let (tx,rx)=flume::bounded::<u64>(1024); for i in 0..1_000_000u64 { tx.try_send(black_box(i)).ok(); rx.try_recv().ok(); } }));
    g.finish();
}

fn bench_blocking(c: &mut Criterion) {
    let mut g = c.benchmark_group("blocking");
    g.sample_size(60);
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));
    g.bench_function("hel_mpmc_4p4c", |b| b.iter(|| blocking_mpmc_ours(1_000_000)));
    g.bench_function("hel_spsc_1p1c", |b| b.iter(|| blocking_spsc(1_000_000)));
    g.bench_function("flume_4p4c",     |b| b.iter(|| blocking_mpmc_flume(1_000_000)));
    g.finish();
}

fn bench_async(c: &mut Criterion) {
    let mut g = c.benchmark_group("async");
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));
    g.bench_function("hel_mpmc_4p4c", |b| b.iter(|| async_mpmc_hel(1_000_000)));
    g.bench_function("hel_spsc_1p1c", |b| b.iter(|| async_spsc(1_000_000)));
    g.bench_function("flume_4p4c",     |b| b.iter(|| async_mpmc_flume(1_000_000)));
    g.finish();
}

fn bench_batch(c: &mut Criterion) {
    let mut g = c.benchmark_group("batch_recv");
    g.throughput(Throughput::Elements(1_000_000));
    g.measurement_time(Duration::from_secs(15));
    g.bench_function("batch_64", |b| b.iter(|| bench_batch_vs_tryrecv(1_000_000)));
    g.finish();
}

criterion_group!(benches, bench_latency, bench_blocking, bench_async, bench_batch);
criterion_main!(benches);
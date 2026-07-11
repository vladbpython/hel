use hel::channel::mpmc::round_robin;
use hel::channel::nearest_power_of_two;
use hel::pool::{handler::PerItem, instance::Config, sync_pool};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const CAP: usize = nearest_power_of_two(4096);

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

// OVERCALL: how quickly active increases 1 -> max under sudden load
fn measure_rampup(shards: usize, max_consumers: usize, work: u32) {
    let (tx, rx) = round_robin::<u64, CAP>(shards);
    let processed = Arc::new(AtomicU64::new(0));

    let p = processed.clone();
    let pool = sync_pool(
        Config::new(1, max_consumers).batch_size(64), // start from 1, look at acceleration
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            black_box(cpu_work(*v, work));
            p.fetch_add(1, Ordering::Relaxed);
        }),
    );

    let stop = Arc::new(AtomicBool::new(false));

    // producers -> a constant FLOW of load (too much to clog the channel).
    // send blocking: when the channel is full, parks the thread
    // but check stop before entering to exit correctly.
    let producers: Vec<_> = (0..shards)
        .map(|pi| {
            let tx = tx.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                let mut i = pi as u64;
                while !stop.load(Ordering::Relaxed) {
                    // try_send + short check stop: blocking send could
                    // go to sleep forever when the channel is full if the consumers are standing.
                    // For controlled completion, try_send with stop check.
                    while tx.try_send(i).is_err() {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        thread::yield_now();
                    }
                    i += shards as u64;
                }
            })
        })
        .collect();

    // sample active every 10ms for 1.5s
    println!("--- ACCELERATION (min=1, max={max_consumers}, work={work}) ---");
    println!("{:>8} {:>8} {:>12}", "time_ms", "active", "processed");
    let start = Instant::now();
    let mut last_processed = 0u64;
    let mut time_to_max: Option<u128> = None;
    while start.elapsed() < Duration::from_millis(1500) {
        thread::sleep(Duration::from_millis(10));
        let ms = start.elapsed().as_millis();
        let active = pool.active();
        let proc = processed.load(Ordering::Relaxed);
        let delta = proc - last_processed;
        last_processed = proc;
        if ms % 50 < 11 {
            println!("{:>8} {:>8} {:>12} (+{delta})", ms, active, proc);
        }
        if active >= max_consumers && time_to_max.is_none() {
            time_to_max = Some(ms);
        }
    }
    match time_to_max {
        Some(ms) => println!(">>> reached active={max_consumers} for {ms}ms"),
        None => println!(">>> NOT reached active={max_consumers} for 1500ms (slow acceleration!)"),
    }

    stop.store(true, Ordering::Relaxed);
    for pr in producers {
        pr.join().unwrap();
    }
    drop(tx);
    // stop_and_wait: forced (load was still running)
    pool.stop_and_wait();
}

// OVERCALL OFF: how active drops after the load is removed
fn measure_rampdown(shards: usize, max_consumers: usize, work: u32) {
    let (tx, rx) = round_robin::<u64, CAP>(shards);
    let processed = Arc::new(AtomicU64::new(0));

    let p = processed.clone();
    let pool = sync_pool(
        Config::new(1, max_consumers).batch_size(64),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            black_box(cpu_work(*v, work));
            p.fetch_add(1, Ordering::Relaxed);
        }),
    );

    let stop = Arc::new(AtomicBool::new(false));

    // LOAD (fast flow) -> acceleration to max
    let producers: Vec<_> = (0..shards)
        .map(|pi| {
            let tx = tx.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                let mut i = pi as u64;
                while !stop.load(Ordering::Relaxed) {
                    while tx.try_send(i).is_err() {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        thread::yield_now();
                    }
                    i += shards as u64;
                }
            })
        })
        .collect();

    // waiting for acceleration to max
    thread::sleep(Duration::from_millis(500));
    let active_at_peak = pool.active();

    // REMOVE the load (stop producers), but tx is ALIVE (NOT drop!)
    // The channel is drained by workers, fill -> 0, the monitor is cooled by active max -> 1.
    stop.store(true, Ordering::Relaxed);
    for pr in producers {
        pr.join().unwrap();
    }
    // We keep tx alive until the end of the cooling measurement.

    println!("--- COOLING DOWN (load removed, tx LIVE, peak active={active_at_peak}) ---");
    println!("{:>8} {:>8}", "time_ms", "active");
    let start = Instant::now();
    let mut time_to_min: Option<u128> = None;
    while start.elapsed() < Duration::from_millis(1500) {
        thread::sleep(Duration::from_millis(50));
        let ms = start.elapsed().as_millis();
        let active = pool.active();
        println!("{:>8} {:>8}", ms, active);
        if active <= 1 && time_to_min.is_none() {
            time_to_min = Some(ms);
        }
    }
    match time_to_min {
        Some(ms) => println!(">>> cooled down to active=1 in {ms}ms after removing the load"),
        None => println!(">>> NOT cooled down to 1 in 1500ms"),
    }

    // NOW you can complete: drop(tx) -> auto drain (channel is already empty) wait_stopping will wait.
    // Or stop_and_wait is forced.
    drop(tx);
    pool.stop_and_wait();
}

fn main() {
    println!("Pool elasticity indicator (M2 MAX). work=5000 (hard), 8 shards, max=8.");
    println!("Overclocking: how quickly active increases under sudden load.");
    println!("Cooling down: how quickly it drops after removal.");

    measure_rampup(8, 8, 5000);
    measure_rampdown(8, 8, 5000);
}

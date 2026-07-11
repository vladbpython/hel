// Sync struct handler BATCH - heavy calculation over the ENTIRE slice.
// Trait: SyncHandler<T>::handle(&mut Vec<T>, n) work with &batch[..n], drain.
// Batch is beneficial for calculations OVER the ENTIRE array: sum, min/max, sliding aggregates -things that cannot be done effectively element by element

use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    pool::{instance::Config, sync_pool, traits::SyncHandler},
};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;

const CAP: usize = nearest_power_of_two(1024);

// statistics storage (general, atomic)
struct Stats {
    count: AtomicU64, // сколько обработано
    sum: AtomicU64,   // сумма
    max: AtomicU64,   // максимум
}

// Struct handler BATCH: heavy calculation over the entire slice
struct StatsBatch {
    stats: Arc<Stats>,
}

impl SyncHandler<u64> for StatsBatch {
    fn handle(&self, batch: &mut Vec<u64>, n: usize) {
        let slice = &batch[..n];

        // CALCULATION OVER THE ENTIRE CUT (what Batch is for):
        // sum, maximum, “heavy” convolution in one pass through the array.
        let mut sum = 0u64;
        let mut max = 0u64;
        for &v in slice {
            // imitation of heavy calculation per element
            let mixed = heavy_compute(v);
            sum = sum.wrapping_add(mixed);
            if mixed > max {
                max = mixed;
            }
        }

        // aggregates in one pass -> atoms (one update per batch, not per element)
        self.stats.count.fetch_add(n as u64, Relaxed);
        self.stats.sum.fetch_add(sum, Relaxed);
        self.stats.max.fetch_max(max, Relaxed);

        // MANDATORY: remove processed ones from buf (trait contract)
        batch.drain(..n);
    }
}

// imitation of heavy CPU calculations.
// black_box prevents the optimizer from throwing out the loop (otherwise the load will disappear).
fn heavy_compute(v: u64) -> u64 {
    black_box((0..1000).fold(v, |acc, _| acc.wrapping_mul(31).wrapping_add(1)))
}

fn main() {
    let (tx, rx) = round_robin::<u64, CAP>(4);
    let stats = Arc::new(Stats {
        count: AtomicU64::new(0),
        sum: AtomicU64::new(0),
        max: AtomicU64::new(0),
    });

    let pool = sync_pool(
        Config::new(1, 4).batch_size(128), // large pack for calculations over an array
        rx.into_receivers(),
        StatsBatch {
            stats: stats.clone(),
        },
    );

    const TOTAL: u64 = 100_000;
    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..(TOTAL / 4) {
                    tx.send(p * (TOTAL / 4) + i).unwrap();
                }
            })
        })
        .collect();
    for h in producers {
        h.join().unwrap();
    }
    drop(tx);
    pool.wait_stopping();

    println!("BATCH stats:");
    println!("count = {}", stats.count.load(Relaxed));
    println!("sum = {}", stats.sum.load(Relaxed));
    println!("max = {}", stats.max.load(Relaxed));
    assert_eq!(stats.count.load(Relaxed), TOTAL, "loss of elements");
    println!("OK: processed {TOTAL} (calculation by slice, aggregates on batch)");
}

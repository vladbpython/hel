// Sync struct-handler PER-ITEM - heavy calculation FOR EACH element.
// Trait: SyncHandler<T>::handle(&mut Vec<T>, n) —> drain(..n) one at a time.
// PerItem is beneficial when the calculation is INDEPENDENT on the element (there is no aggregate over the array):
// transformation, validation, heavy function from one value.

use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    pool::{instance::Config, sync_pool, traits::SyncHandler},
};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;

const CAP: usize = nearest_power_of_two(1024);

struct Accum {
    processed: AtomicU64,
    result_sum: AtomicU64, // the sum of the results of a heavy calculation
}

// Struct-handler PER ITEM: heavy calculation for each element
struct HeavyPerItem {
    accum: Arc<Accum>,
}

impl SyncHandler<u64> for HeavyPerItem {
    fn handle(&self, batch: &mut Vec<u64>, n: usize) {
        // &batch[..n]: read from the link WITHOUT owned move (heavy_transform only reads the value).
        for &item in &batch[..n] {
            //heavy INDEPENDENT calculation per element (no aggregate by array)
            let result = heavy_transform(item);
            self.accum.result_sum.fetch_add(result, Relaxed);
            self.accum.processed.fetch_add(1, Relaxed);
        }
        //cleanup AFTER processing (single shift, like Batch)
        batch.drain(..n);
    }
}

// heavy independent transformation of one meaning.
// black_box prevents the optimizer from throwing out the loop (otherwise the load will disappear).
fn heavy_transform(v: u64) -> u64 {
    black_box((0..2000).fold(v, |acc, _| acc.wrapping_mul(31).wrapping_add(1)))
}

fn main() {
    let (tx, rx) = round_robin::<u64, CAP>(4);
    let accum = Arc::new(Accum {
        processed: AtomicU64::new(0),
        result_sum: AtomicU64::new(0),
    });

    let pool = sync_pool(
        Config::new(1, 4).batch_size(64),
        rx.into_receivers(),
        HeavyPerItem {
            accum: accum.clone(),
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

    println!("PER ITEM:");
    println!("processed  = {}", accum.processed.load(Relaxed));
    println!("result_sum = {}", accum.result_sum.load(Relaxed));
    assert_eq!(accum.processed.load(Relaxed), TOTAL, "loss of elements");
    println!("OK: processed by {TOTAL} (heavy calculation per element, scale by workers)");
}

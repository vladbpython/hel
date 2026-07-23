// Ownership boundary:
// - panic BEFORE slot.take() -> item stays with the worker -> dead letter (zero loss);
// - panic AFTER slot.take()   -> the handler accepted ownership; the item is consumed by contract (counted via handler_panics).
// PerItem style processing is what the slot API is: one item per call.

use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    helper::panic::PanicReason,
    pool::{instance::Config, sync_pool_slot, traits::SyncSlotHandler},
};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;

const CAP: usize = nearest_power_of_two(1024);

struct Accum {
    processed: AtomicU64,
    result_sum: AtomicU64,
}

// Custom struct handler: heavy independent computation per element.
// Reads by reference  never takes ownership, so ANY panic here is recoverable:
// the item lands in the dead-letter sink, not in the void.
struct HeavyPerItem {
    accum: Arc<Accum>,
}

impl SyncSlotHandler<u64> for HeavyPerItem {
    fn handle(&self, slot: &mut Option<u64>) {
        if let Some(item) = slot.as_ref() {
            let result = heavy_transform(*item);
            self.accum.result_sum.fetch_add(result, Relaxed);
            self.accum.processed.fetch_add(1, Relaxed);
        }
        // No take(): the worker clears the slot on success. If we needed to KEEP the item (send it onward, store it),
        // we would take() explicitly accepting that a panic after that point consumes it.
    }
}

fn heavy_transform(v: u64) -> u64 {
    black_box((0..2000).fold(v, |acc, _| acc.wrapping_mul(31).wrapping_add(1)))
}

fn main() {
    let (tx, rx) = round_robin::<u64, CAP>(4);
    let accum = Arc::new(Accum {
        processed: AtomicU64::new(0),
        result_sum: AtomicU64::new(0),
    });

    let pool = sync_pool_slot(
        Config::new(1, 4).batch_size(64),
        rx.into_receivers(),
        HeavyPerItem {
            accum: accum.clone(),
        },
        // Dead-letter sink: receives the item and the panic cause.
        |poison: u64, panic_info: PanicReason| {
            eprintln!("dead-letter: item={poison} panic_info={panic_info:?}");
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

    println!("PER ITEM (slot):");
    println!("processed  = {}", accum.processed.load(Relaxed));
    println!("result_sum = {}", accum.result_sum.load(Relaxed));
    assert_eq!(accum.processed.load(Relaxed), TOTAL, "loss of elements");
    println!("OK: processed {TOTAL} (heavy per-element compute, scaled by workers)");
}

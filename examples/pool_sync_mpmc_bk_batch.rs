// shard_key + Batch (sync): a batch from a shard (symbols of this shard), the order is intact.
use hel::{
    channel::{mpmc::shard_key, nearest_power_of_two},
    pool::{handler::Batch, instance::Config, sync_pool},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;

const CAP: usize = nearest_power_of_two(1024);
const SYMBOLS: [&str; 16] = [
    "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC", "ETH",
    "ORCL", "UBER", "LYFT", "SNAP",
];

fn main() {
    let (tx, rx) = shard_key::<u64, CAP>(8);
    let sum = Arc::new(AtomicU64::new(0));

    let s = sum.clone();
    let pool = sync_pool(
        Config::new(2, 8).batch_size(64),
        rx.into_receivers(),
        Batch(move |batch: &[u64]| {
            s.fetch_add(batch.iter().sum::<u64>(), Relaxed);
        }),
    );

    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..1000u64 {
                    let sym = SYMBOLS[(p * 250 + i as usize) % SYMBOLS.len()];
                    tx.send(sym, i).unwrap();
                }
            })
        })
        .collect();
    for h in producers {
        h.join().unwrap();
    }
    drop(tx);
    pool.wait_stopping();
    println!("key_sync_batch: sum = {}", sum.load(Relaxed));
}

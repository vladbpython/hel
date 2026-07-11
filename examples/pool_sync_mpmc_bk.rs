// shard_key + PerItem (sync): symbols are registered into groups.
use hel::{
    channel::{mpmc::shard_key, nearest_power_of_two},
    pool::{handler::PerItem, instance::Config, sync_pool},
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
        Config::new(1, 8),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            s.fetch_add(*v, Relaxed);
        }),
    );

    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..1000u64 {
                    let sym = SYMBOLS[(p * 250 + i as usize) % SYMBOLS.len()];
                    // one symbol is always in one shard -> the order per symbol is preserved
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
    println!("key_sync_per_item: sum = {}", sum.load(Relaxed));
}

// shard_group + PerItem (sync): symbols are registered into groups.
use hel::{
    channel::{
        mpmc::{ShardGroupCase, shard_group},
        nearest_power_of_two,
    },
    pool::{handler::PerItem, instance::Config, sync_pool},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;

const CAP: usize = nearest_power_of_two(1024);

// группы по секторам (4 группы → 4 шарда)
const GROUPS: &[&[&str]] = &[
    &["AAPL", "MSFT", "GOOG", "ORCL", "INTC", "AMD", "NVDA"], // 0: tech
    &["TSLA", "UBER", "LYFT"],                                // 1: auto
    &["BTC", "ETH"],                                          // 2: crypto
    &["META", "SNAP", "NFLX", "AMZN"],                        // 3: media
];

const SYMBOLS: [&str; 16] = [
    "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC", "ETH",
    "ORCL", "UBER", "LYFT", "SNAP",
];

fn main() {
    let (tx, rx) = shard_group::<u64, CAP>(ShardGroupCase::Groups { groups: GROUPS });
    let sum = Arc::new(AtomicU64::new(0));
    let s = sum.clone();
    let pool = sync_pool(
        Config::new(1, 4), // 4 groups -> up to 4 workers
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            s.fetch_add(*v, Relaxed);
        }),
    );

    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                let handles: Vec<_> = SYMBOLS
                    .iter()
                    .map(|&s| tx.handle(s).expect("symbol must be registered"))
                    .collect();

                for i in 0..1000u64 {
                    let idx = (p * 250 + i as usize) % SYMBOLS.len();
                    let h = handles[idx];
                    tx.send(h, i).unwrap();
                }
            })
        })
        .collect();
    for h in producers {
        h.join().unwrap();
    }
    drop(tx);
    pool.wait_stopping();
    println!("group_sync_per_item: sum = {}", sum.load(Relaxed));
}

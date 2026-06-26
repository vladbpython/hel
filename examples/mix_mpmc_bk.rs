use hel::channel::{mpmc::shard_key, nearest_power_of_two};
use std::thread;
use tokio::runtime::Builder;

const CAPACITY: usize = nearest_power_of_two(256);

// Sync symbol producers → by-key routing → async consumers per shard.
// Real world: sync market data feed → async order processing per symbol.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let (tx, rx) = shard_key::<u64, CAPACITY>(4);
    let symbols = [
        "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC",
        "ETH", "ORCL", "UBER", "LYFT", "SNAP",
    ];

    // Async consumers one per shard, async processing pipeline
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            rt.spawn(async move {
                let mut count = 0u64;
                while r.recv_async().await.is_ok() {
                    count += 1;
                }
                println!("[async key shard {id}] messages = {count}");
            })
        })
        .collect();

    // Sync producers blocking threads simulating market data feed
    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..10_000u64 {
                    let sym = symbols[(p as usize * 2500 + i as usize) % symbols.len()];
                    // sync send same key always routes to same async consumer
                    tx.send(sym, i).unwrap();
                }
                println!("[sync producer {p}] done");
            })
        })
        .collect();

    drop(tx);
    for h in producers {
        h.join().unwrap();
    }

    rt.block_on(async {
        for h in consumers {
            h.await.unwrap();
        }
    });
}

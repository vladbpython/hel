use hel::channel::{errors::*, mpmc::shard_key, nearest_power_of_two};
use std::thread;

// N producers → hash(key) → S shards → S consumers.
// Guarantees ordering per key same key always goes to same shard.
// Optimal for symbol routing: trading, actors, sessions.

const CAPACITY: usize = nearest_power_of_two(256);

fn main() {
    let (tx, rx) = shard_key::<u64, CAPACITY>(8);

    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            thread::spawn(move || {
                let mut count = 0u64;
                loop {
                    match r.recv() {
                        Ok(v) => count += v,
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                println!("[shard {id}] total = {count}");
            })
        })
        .collect();

    let symbols = [
        "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC",
        "ETH", "ORCL", "UBER", "LYFT", "SNAP",
    ];

    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..1000u64 {
                    let sym = symbols[(p * 250 + i as usize) % symbols.len()];
                    // same symbol always routed to same shard → ordering preserved
                    tx.send(sym, i).unwrap();
                }
            })
        })
        .collect();

    drop(tx);
    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
}

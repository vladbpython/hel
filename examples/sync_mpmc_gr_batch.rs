use hel::channel::{
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use std::thread;

// sync batch for ShardGroup. EXPLICIT grouping (by sector).
// Producer for a sector sends packs of ITS symbol via keyed send_batch
// (all elements share one key → routed to that sector's shard).
// Element carries the symbol (String) so the keyed method can route by key.

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(256);

// (symbol, payload).
type Tick = (String, u64);

fn main() {
    let (tx, rx) = shard_group::<Tick, CAPACITY>(ShardGroupCase::Groups {
        groups: &[
            &["AAPL", "MSFT", "GOOG", "ORCL", "INTC", "AMD", "NVDA"], // 0: tech
            &["TSLA", "UBER", "LYFT"],                                // 1: auto
            &["BTC", "ETH"],                                          // 2: crypto
            &["META", "SNAP", "NFLX", "AMZN"],                        // 3: media
        ],
    });

    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .map(|r| {
            thread::spawn(move || {
                let mut total = 0u64;
                let mut buf: Vec<Tick> = Vec::with_capacity(BATCH);
                loop {
                    let (n, disconnected) = r.recv_batch(&mut buf, BATCH);
                    for (_, v) in buf.drain(..n) {
                        total += v; // payload
                    }
                    if disconnected {
                        break;
                    }
                }
                total
            })
        })
        .collect();

    let sectors = ["AAPL", "TSLA", "BTC", "META"]; // a symbol from each sector
    let producers: Vec<_> = sectors
        .iter()
        .map(|&sym| {
            let tx = tx.clone();
            thread::spawn(move || {
                debug_assert!(tx.shard_for(sym).is_some(), "symbol must be registered");

                let mut buf: Vec<Tick> = Vec::with_capacity(BATCH);
                for i in 0..100_000u64 {
                    buf.push((sym.to_string(), i));
                    if buf.len() == BATCH {
                        tx.send_batch(&mut buf, |(s, _)| s.as_str()).unwrap();
                    }
                }
                if !buf.is_empty() {
                    tx.send_batch(&mut buf, |(s, _)| s.as_str()).unwrap();
                }
            })
        })
        .collect();

    drop(tx);

    for p in producers {
        p.join().unwrap();
    }
    let total: u64 = consumers.into_iter().map(|h| h.join().unwrap()).sum();
    println!("batch total = {total}");
}

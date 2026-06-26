use hel::channel::{
    errors::*,
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use std::thread;

// N producers → handle (resolve ONCE) → S shards → S consumers.
// Unlike ShardKey (hash), the grouping is EXPLICIT: you decide which
// symbol in which shard. Here we group by SECTOR (tech /auto /crypto / media).
// One sector → one shard → one consumer thread per sector.

const CAPACITY: usize = nearest_power_of_two(256);

fn main() {
    // explicit grouping of symbols by sectors: group i → shard i
    let (tx, rx) = shard_group::<u64, CAPACITY>(ShardGroupCase::Groups {
        groups: &[
            &["AAPL", "MSFT", "GOOG", "ORCL", "INTC", "AMD", "NVDA"], // 0: tech
            &["TSLA", "UBER", "LYFT"],                                // 1: auto/mobility
            &["BTC", "ETH"],                                          // 2: crypto
            &["META", "SNAP", "NFLX", "AMZN"],                        // 3: media/consumer
        ],
    });

    //consumer thread for each shard. Everyone reads their own shard.
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
                let handles: Vec<_> = symbols
                    .iter()
                    .map(|&s| (s, tx.handle(s).expect("symbol must be registered")))
                    .collect();

                for i in 0..1000u64 {
                    let idx = (p * 250 + i as usize) % symbols.len();
                    let (_sym, h) = handles[idx];
                    tx.send(h, i).unwrap();
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

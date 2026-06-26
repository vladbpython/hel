use hel::channel::{
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use std::thread;

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(256);

fn main() {
    let (tx, rx) = shard_group::<u64, CAPACITY>(ShardGroupCase::Groups {
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
                let mut buf = Vec::with_capacity(BATCH);
                loop {
                    let (n, disconnected) = r.recv_batch(&mut buf, BATCH);
                    for v in buf.drain(..n) {
                        total += v;
                    }
                    if disconnected {
                        break;
                    }
                }
                total
            })
        })
        .collect();

    // producer for each sector: sends packets TO HIS shard according to the handle.
    // Take one symbol representative per sector to resolve the handle.
    let sectors = ["AAPL", "TSLA", "BTC", "META"]; // a symbol from each sector

    let producers: Vec<_> = sectors
        .iter()
        .map(|&sym| {
            let tx = tx.clone();
            thread::spawn(move || {
                // резолв хэндла сектора ОДИН раз
                let h = tx.handle(sym).expect("symbol must be registered");

                let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
                for i in 0..100_000u64 {
                    buf.push(i);
                    if buf.len() == BATCH {
                        // batch одного инструмента в его шард по хэндлу
                        tx.send_batch(h, &mut buf).unwrap();
                    }
                }
                if !buf.is_empty() {
                    tx.send_batch(h, &mut buf).unwrap();
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

// sync ShardGroup + batch via drain_batch_sink helper.
// EXPLICIT grouping (by sector). Each producer sends packs to HIS shard
// by handle (resolve once). Consumer receives the ENTIRE batch as an array and
// calculates the sum in one pass (the compiler can vectorize), rather than
// calls handler on each element.
use hel::{
    channel::{
        mpmc::{ShardGroupCase, shard_group},
        nearest_power_of_two,
    },
    helper::batch::drain_batch_sink,
};
use std::thread;

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(256);
const TOTAL: u64 = 100_000;

fn main() {
    // explicit grouping by sectors: group i → shard i
    let (tx, rx) = shard_group::<u64, CAPACITY>(ShardGroupCase::Groups {
        groups: &[
            &["AAPL", "MSFT", "GOOG", "ORCL", "INTC", "AMD", "NVDA"], // 0: tech
            &["TSLA", "UBER", "LYFT"],                                // 1: auto
            &["BTC", "ETH"],                                          // 2: crypto
            &["META", "SNAP", "NFLX", "AMZN"],                        // 3: media
        ],
    });

    // consumer thread for each shard. Batch array via sink.
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .map(|r| {
            thread::spawn(move || {
                // drain_batch_sink: sink takes ownership of the entire Vec<u64>, counts
                // over the array at once, returns an empty Vec (allocation is reused).
                drain_batch_sink(
                    BATCH,
                    move |buf: &mut Vec<u64>, m| r.recv_batch(buf, m),
                    |batch: Vec<u64>, acc: &mut u64| {
                        *acc += batch.iter().sum::<u64>(); // // calculation for the ENTIRE array
                        let mut b = batch;
                        b.clear();
                        b // return allocation
                    },
                    0u64,
                )
            })
        })
        .collect();

    // producer for each sector: sends packets TO HIS shard according to the handle.
    // one symbol representative per sector to resolve the handle.
    let sectors = ["AAPL", "TSLA", "BTC", "META"];

    let producers: Vec<_> = sectors
        .iter()
        .map(|&sym| {
            let tx = tx.clone();
            thread::spawn(move || {
                // resolve sector handle ONCE
                let h = tx.handle(sym).expect("symbol must be registered");

                let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
                for i in 0..TOTAL {
                    buf.push(i);
                    if buf.len() == BATCH {
                        // batch of one sector to its shard by handle
                        tx.send_batch(h, &mut buf).unwrap();
                    }
                }
                if !buf.is_empty() {
                    tx.send_batch(h, &mut buf).unwrap();
                }
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    drop(tx);

    let total: u64 = consumers.into_iter().map(|h| h.join().unwrap()).sum();
    // 4 producer sectors, each sends 0..TOTAL
    let expected = (0..TOTAL).sum::<u64>() * sectors.len() as u64;
    println!("batch total = {total} (expected {expected})");
    assert_eq!(total, expected, "lost or duplicated data");
    println!("OK");
}

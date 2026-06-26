use hel::channel::{
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use tokio::runtime::Builder;

// async batch for ShardGroup on tokio (8 worker threads).
// EXPLICIT grouping (by sector). Consumer drains batch through
// recv_batch_async. The producer for the sector sends packs TO HIS shard according to the handle
// via send_batch_async (resolve the handle once).

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(1024);

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();

    rt.block_on(async {
        // explicit grouping by sectors: group i → shard i
        let (tx, rx) = shard_group::<u64, CAPACITY>(ShardGroupCase::Groups {
            groups: &[
                &["AAPL", "MSFT", "GOOG", "ORCL", "INTC", "AMD", "NVDA"], // 0: tech
                &["TSLA", "UBER", "LYFT"],                                // 1: auto
                &["BTC", "ETH"],                                          // 2: crypto
                &["META", "SNAP", "NFLX", "AMZN"],                        // 3: media
            ],
        });

        // consumer task for each shard. Async batch reading.
        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                tokio::spawn(async move {
                    let mut total = 0u64;
                    let mut buf = Vec::with_capacity(BATCH);
                    loop {
                        let (n, dc) = r.recv_batch_async(&mut buf, BATCH).await;
                        for v in buf.drain(..n) {
                            total += v;
                        }
                        if dc {
                            break;
                        }
                    }
                    println!("[batch group shard {id}] total = {total}");
                })
            })
            .collect();

        let sectors = ["AAPL", "TSLA", "BTC", "META"];

        let producers: Vec<_> = sectors
            .iter()
            .map(|&sym| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let h = tx.handle(sym).expect("symbol must be registered");
                    let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
                    for i in 0..100_000u64 {
                        buf.push(i);
                        if buf.len() == BATCH {
                            tx.send_batch_async(h, &mut buf).await.unwrap();
                        }
                    }
                    if !buf.is_empty() {
                        tx.send_batch_async(h, &mut buf).await.unwrap();
                    }
                })
            })
            .collect();

        drop(tx);

        for h in producers {
            h.await.unwrap();
        }
        for h in consumers {
            h.await.unwrap();
        }
    });
}

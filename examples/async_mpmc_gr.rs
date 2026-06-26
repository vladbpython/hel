use hel::channel::{
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use tokio::runtime::Builder;
const CAPACITY: usize = nearest_power_of_two(256);

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();

    rt.block_on(async {
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
            .enumerate()
            .map(|(id, r)| {
                tokio::spawn(async move {
                    let mut total = 0u64;
                    while let Ok(v) = r.recv_async().await {
                        total += v;
                    }
                    println!("[group shard {id}] total = {total}");
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
                    for i in 0..1000u64 {
                        tx.send_async(h, i).await.unwrap();
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

use hel::channel::{
    errors::*,
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
const CAPACITY: usize = nearest_power_of_two(256);

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
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

        let token = CancellationToken::new();
        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                let token = token.clone();
                tokio::spawn(async move {
                    let mut total = 0u64;
                    loop {
                        tokio::select! {
                            _ = token.cancelled() => {
                                println!("[group shard {id}] cancelled, total = {total}");
                                break;
                            }
                            result = r.recv_async() => {
                                match result {
                                    Ok(v) => total += v,
                                    Err(AsyncRecvError::Disconnected) => {
                                        println!("[group shard {id}] disconnected, total = {total}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                })
            })
            .collect();

        let sectors = ["AAPL", "TSLA", "BTC", "META"];

        let producers: Vec<_> = sectors
            .iter()
            .enumerate()
            .map(|(p, &sym)| {
                let tx = tx.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    let h = tx.handle(sym).expect("symbol must be registered");
                    for i in 0..10_000u64 {
                        tokio::select! {
                            biased; // safe ordering, no random!
                            _ = token.cancelled() => break,
                            r = tx.send_async(h, (p as u64) * 10_000 + i) => {
                                if r.is_err() { break; } // Disconnected
                            }
                        }
                    }
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        drop(tx);

        for h in producers {
            h.await.unwrap();
        }
        for h in consumers {
            h.await.unwrap();
        }
    });
}

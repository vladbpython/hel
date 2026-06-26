use hel::channel::{
    errors::*,
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = nearest_power_of_two(256);
const PER_PRODUCER: u64 = 100_000;

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
        let sent = Arc::new(AtomicU64::new(0));

        // CONSUMER for each shard: pure drainage, without select/token.
        // Drains until it gets Disconnected (all tx are dropped).
        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(shard_id, r)| {
                tokio::spawn(async move {
                    let mut total = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(_) => total += 1,
                            Err(AsyncRecvError::Disconnected) => {
                                println!("[group shard {shard_id}] drained, count = {total}");
                                break;
                            }
                        }
                    }
                    total
                })
            })
            .collect();

        // PRODUCER на каждый сектор: шлёт по хэндлу с biased-select на отмену.
        let sectors = ["AAPL", "TSLA", "BTC", "META"];

        let producers: Vec<_> = sectors
            .iter()
            .map(|&sym| {
                let tx = tx.clone();
                let token_p = token.clone();
                let sent = sent.clone();
                tokio::spawn(async move {
                    let h = tx.handle(sym).expect("symbol must be registered");
                    let mut i = 0u64;
                    while i < PER_PRODUCER {
                        tokio::select! {
                            biased; // safe ordering, no random!
                            _ = token_p.cancelled() => break,
                            r = tx.send_async(h, i) => {
                                if r.is_err() { break; } // Disconnected
                                i += 1;
                            }
                        }
                    }
                    sent.fetch_add(i, Relaxed);
                })
            })
            .collect();

        drop(tx);

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel(); // signal to producers

        for h in producers {
            h.await.unwrap();
        }

        let mut recvd = 0u64;
        for h in consumers {
            recvd += h.await.unwrap();
        }

        let s = sent.load(Relaxed);
        println!("sent={s} recvd={recvd} lost={}", s as i64 - recvd as i64);
        assert_eq!(s, recvd, "ShardGroup graceful: zero-loss");
        println!("OK: zero-loss");
    });
}

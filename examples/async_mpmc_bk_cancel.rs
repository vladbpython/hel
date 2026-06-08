use hel::channel::{errors::*, mpmc::shard_key};
use std::time::Duration;
use tokio::runtime::Builder;
const CAPACITY: usize = 256;

// Shutdown via oneshot channel is a typical pattern for trading systems
// main loop receives a completion signal and drops senders.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = shard_key::<u64, CAPACITY>(4);
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);

        let symbols = ["AAPL", "MSFT", "GOOG", "AMZN"];

        // Consumers: recv per-symbol shard + watch shutdown
        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                let mut shutdown = shutdown_tx.subscribe();
                tokio::spawn(async move {
                    let mut count = 0u64;
                    loop {
                        tokio::select! {
                            biased; // safe ordering, no random!
                            Ok(_) = shutdown.changed() => {
                                if *shutdown.borrow() {
                                    println!("[key shard {id}] shutdown, count = {count}");
                                    break;
                                }
                            }
                            result = r.recv_async() => {
                                match result {
                                    Ok(_) => count += 1,
                                    Err(AsyncRecvError::Disconnected) => {
                                        println!("[key shard {id}] done, count = {count}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                })
            })
            .collect();

        // Producers
        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                let mut shutdown = shutdown_tx.subscribe();
                tokio::spawn(async move {
                    let mut i = 0u64;
                    loop {
                        let sym = symbols[i as usize % symbols.len()];
                        tokio::select! {
                            biased; // safe ordering, no random!
                            Ok(_) = shutdown.changed() => {
                                if *shutdown.borrow() { break; }
                            }
                            r = tx.send_async(sym, p * 10_000 + i) => {
                                if r.is_err() { break; }
                                i += 1;
                            }
                        }
                    }
                })
            })
            .collect();

        // Trigger shutdown after 50ms
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();

        drop(tx);
        for h in producers {
            h.await.unwrap();
        }
        for h in consumers {
            h.await.unwrap();
        }
    });
}

use hel::channel::{errors::*, mpmc::round_robin, nearest_power_of_two};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = nearest_power_of_two(256);

// Graceful shutdown: CancellationToken signals all tasks to stop.
// Producers drop tx → consumers get Disconnected.
// select! on the consumer side it allows recv_async to be interrupted before Disconnected.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = round_robin::<u64, CAPACITY>(4);
        let token = CancellationToken::new();

        // Consumers: recv_async or cancellation, whichever comes first
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
                            // Cancellation has priority
                            _ = token.cancelled() => {
                                println!("[rr shard {id}] cancelled, total = {total}");
                                break;
                            }
                            result = r.recv_async() => {
                                match result {
                                    Ok(v) => total += v,
                                    Err(AsyncRecvError::Disconnected) => {
                                        println!("[rr shard {id}] disconnected, total = {total}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                })
            })
            .collect();

        // Producers: send or cancellation
        let producers: Vec<_> = (0..8)
            .map(|p| {
                let tx = tx.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    for i in 0..10_000u64 {
                        tokio::select! {
                            biased; // safe ordering, no random!
                            _ = token.cancelled() => break,
                            r = tx.send_async(p * 10_000 + i) => {
                                if r.is_err() { break; } // Disconnected
                            }
                        }
                    }
                })
            })
            .collect();

        // Cancel after 50ms — simulates external shutdown signal
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

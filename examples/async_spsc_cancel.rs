use hel::channel::{errors::*, nearest_power_of_two, spsc::shard_spsc};
use std::time::Duration;
use tokio::{runtime::Builder, task::AbortHandle};
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = nearest_power_of_two(256);
// SPSC + cancellation: each pair is independent.
// Use AbortHandle to force abort a specific shard.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let ch = shard_spsc::<u64, CAPACITY>(4);
        let token = CancellationToken::new();

        let handles: Vec<(AbortHandle, AbortHandle)> = ch
            .into_pairs()
            .map(|(shard_id, tx, rx)| {
                let token_c = token.clone();
                let token_p = token.clone();

                let consumer = tokio::spawn(async move {
                    let mut total = 0u64;
                    loop {
                        tokio::select! {
                            _ = token_c.cancelled() => {
                                println!("[spsc shard {shard_id}] cancelled, total = {total}");
                                break;
                            }
                            result = rx.recv_async() => {
                                match result {
                                    Ok(v) => total += v,
                                    Err(AsyncRecvError::Disconnected) => {
                                        println!("[spsc shard {shard_id}] done, total = {total}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });

                let producer = tokio::spawn(async move {
                    for i in 0..100_000u64 {
                        tokio::select! {
                            biased; // safe ordering, no random!
                            _ = token_p.cancelled() => break,
                            r = tx.send_async(i) => {
                                if r.is_err() { break; }
                            }
                        }
                    }
                });

                (producer.abort_handle(), consumer.abort_handle())
            })
            .collect();

        // Cancel after 50ms
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();

        // Give tasks time to clean up
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Abort any still running tasks (safety net)
        for (p, c) in &handles {
            p.abort();
            c.abort();
        }
    });
}

use hel::channel::{errors::*, nearest_power_of_two, spsc::shard_spsc};
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
        let ch = shard_spsc::<u64, CAPACITY>(4);
        let token = CancellationToken::new();
        let sent = Arc::new(AtomicU64::new(0));

        let mut consumers = Vec::new();
        let mut producers = Vec::new();

        for (shard_id, tx, rx) in ch.into_pairs() {
            // CONSUMER: pure drain, no select/token
            let consumer = tokio::spawn(async move {
                let mut total = 0u64;
                loop {
                    match rx.recv_async().await {
                        Ok(_) => total += 1,
                        Err(AsyncRecvError::Disconnected) => {
                            println!("[spsc shard {shard_id}] drained, count = {total}");
                            break;
                        }
                    }
                }
                total
            });

            // PRODUCER: biased + cancel immediately
            let token_p = token.clone();
            let sent = sent.clone();
            let producer = tokio::spawn(async move {
                let mut i = 0u64;
                while i < PER_PRODUCER {
                    tokio::select! {
                        biased; // safe ordering, no random!
                        _ = token_p.cancelled() => break,
                        r = tx.send_async(i) => {
                            if r.is_err() { break; } // Disconnected
                            i += 1;
                        }
                    }
                }
                sent.fetch_add(i, Relaxed);
                // tx is dropped HERE (end of task) → channel is closed →
                // the consumer of this shard will receive Disconnected
            });

            consumers.push(consumer);
            producers.push(producer);
        }

        // Cancel after 50ms -imitation of an external stop signal
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel(); // signal to producers

        for h in producers {
            h.await.unwrap();
        } // producers left, tx dropped
        let mut recvd = 0u64; // consumers drain the remainder
        for h in consumers {
            recvd += h.await.unwrap();
        }

        let s = sent.load(Relaxed);
        println!("sent={s} recvd={recvd} lost={}", s as i64 - recvd as i64);
        assert_eq!(s, recvd, "SPSC graceful: zero-loss");
        println!("OK: zero-loss");
    });
}

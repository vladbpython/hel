use hel::channel::{errors::*, mpmc::round_robin, nearest_power_of_two};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = nearest_power_of_two(256);
const SHARDS: usize = 4;
const PRODUCERS: usize = 8; // MPMC: more producers than shards on purpose
const PER_PRODUCER: u64 = 100_000;

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = round_robin::<u64, CAPACITY>(SHARDS);
        let token = CancellationToken::new();
        let sent = Arc::new(AtomicU64::new(0));

        // CONSUMERS: one per shard, pure drain, no select/token.
        // rx.receiver(s) is the internal channel receiver; clones keep the
        // shard alive after the multi receiver is dropped.
        let mut consumers = Vec::new();
        for shard_id in 0..SHARDS {
            let rx_s = rx.receiver(shard_id).clone();
            consumers.push(tokio::spawn(async move {
                let mut total = 0u64;
                loop {
                    match rx_s.recv_async().await {
                        Ok(_) => total += 1,
                        Err(AsyncRecvError::Disconnected) => {
                            println!("[rr shard {shard_id}] drained, count = {total}");
                            break;
                        }
                    }
                }
                total
            }));
        }
        drop(rx); // per shard clones are the only receivers now

        // PRODUCERS: MPMC - every producer holds a clone of the SAME rr sender.
        let mut producers = Vec::new();
        for _ in 0..PRODUCERS {
            let tx = tx.clone();
            let token_p = token.clone();
            let sent = sent.clone();
            producers.push(tokio::spawn(async move {
                let mut i = 0u64;
                let mut slot: Option<u64> = None;
                while i < PER_PRODUCER {
                    // Refill ONLY if empty: a value cancelled mid send is still in the slot and must not be overwritten.
                    if slot.is_none() {
                        slot = Some(i);
                    }
                    tokio::select! {
                        biased; // deterministic ordering: check the stop signal first
                        _ = token_p.cancelled() => break,
                        r = tx.send_ref_async(&mut slot) => {
                            if r.is_err() { break; } // Disconnected
                            i += 1; // counts COMPLETED sends only
                        }
                    }
                }
                if let Some(v) = slot.take() {
                    println!("recovered unsent value {v} on cancellation");
                }
                sent.fetch_add(i, Relaxed);
            }));
        }
        drop(tx);

        // Imitation of an external stop signal.
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();

        for h in producers {
            h.await.unwrap();
        } // all sender clones dropped -> Disconnected on every shard
        let mut recvd = 0u64;
        for h in consumers {
            recvd += h.await.unwrap();
        }

        let s = sent.load(Relaxed);
        println!("sent={s} recvd={recvd} lost={}", s as i64 - recvd as i64);
        assert_eq!(s, recvd, "RR graceful: zero-loss");
        println!("OK: zero-loss");
    });
}

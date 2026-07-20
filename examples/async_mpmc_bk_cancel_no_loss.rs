use hel::channel::{errors::*, mpmc::shard_key, nearest_power_of_two};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
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
        let (tx, rx) = shard_key::<u64, CAPACITY>(4);
        let token = CancellationToken::new();
        let symbols = ["AAPL", "MSFT", "GOOG", "AMZN"];
        let sent = Arc::new(AtomicU64::new(0));

        // Consumers: by cancel they do not exit, but go to DRAIN!
        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                tokio::spawn(async move {
                    let mut count = 0u64;
                    loop {
                        // drain phase: without select, read to Disconnected
                        match r.recv_async().await {
                            Ok(_) => count += 1,
                            Err(AsyncRecvError::Disconnected) => {
                                println!("[key shard {id}] done, count = {count}");
                                break;
                            }
                        }
                    }
                    count
                })
            })
            .collect();

        // Producers: biased + cancel first branch → stop immediately
        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                let token = token.child_token();
                let sent = sent.clone();
                tokio::spawn(async move {
                    let mut i = 0u64;
                    let mut slot: Option<u64> = None;
                    loop {
                        let sym = symbols[i as usize % symbols.len()];
                        if slot.is_none() {
                            slot = Some(p * 10_000 + i);
                        }
                        tokio::select! {
                            biased; // safe ordering, no random!
                            _ = token.cancelled() => break,
                            r = tx.send_ref_async(sym, &mut slot) => {
                                if r.is_err() { break; }
                                i += 1;
                            }
                        }
                    }
                    if let Some(v) = slot.take() {
                        println!("[producer {p}] recovered unsent value {v} on cancellation");
                    }
                    sent.fetch_add(i, Relaxed);
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        for h in producers {
            h.await.unwrap();
        }
        drop(tx);
        let mut recvd = 0u64;
        for h in consumers {
            recvd += h.await.unwrap();
        }
        let s = sent.load(Relaxed);
        println!("sent={s} recvd={recvd} lost={}", s as i64 - recvd as i64);
        assert_eq!(s, recvd, "zero-loss with CancellationToken");
        println!("OK: zero-loss");
    });
}

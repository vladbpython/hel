use hel::channel::{errors::*, mpmc::round_robin};
use std::thread;
use tokio::runtime::Builder;

const CAPACITY: usize = 256;

// Sync OS threads produce → same channel ← async tokio tasks consume.
// Real world: blocking I/O producers (file, serial, legacy SDK)
// feeding async processing pipeline.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (tx, rx) = round_robin::<u64, CAPACITY>(4);

    // Async consumers inside tokio runtime
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            rt.spawn(async move {
                let mut total = 0u64;
                loop {
                    match r.recv_async().await {
                        Ok(v) => total += v,
                        Err(AsyncRecvError::Disconnected) => break,
                    }
                }
                println!("[async consumer shard {id}] total = {total}");
            })
        })
        .collect();

    // Sync producers plain OS threads, blocking send
    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..10_000u64 {
                    tx.send(p * 10_000 + i).unwrap();
                }
                println!("[sync producer {p}] done");
            })
        })
        .collect();

    drop(tx);
    for h in producers {
        h.join().unwrap();
    }

    // Block until async consumers finish
    rt.block_on(async {
        for h in consumers {
            h.await.unwrap();
        }
    });
}

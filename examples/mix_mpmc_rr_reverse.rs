use hel::channel::{errors::*, mpmc::round_robin, nearest_power_of_two};
use std::thread;
use tokio::runtime::Builder;

const CAPACITY: usize = nearest_power_of_two(256);

// Async tasks produce → same channel ← sync OS threads consume.
// Real world: async network receiver feeding sync CPU bound workers.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (tx, rx) = round_robin::<u64, CAPACITY>(4);

    // Sync consumers OS threads, blocking recv
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            thread::spawn(move || {
                let mut total = 0u64;
                loop {
                    match r.recv() {
                        Ok(v) => total += v,
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                println!("[sync consumer shard {id}] total = {total}");
            })
        })
        .collect();

    // Async producers tokio tasks
    rt.block_on(async {
        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..10_000u64 {
                        tx.send_async(p * 10_000 + i).await.unwrap();
                    }
                    println!("[async producer {p}] done");
                })
            })
            .collect();

        drop(tx);
        for h in producers {
            h.await.unwrap();
        }
    });

    for h in consumers {
        h.join().unwrap();
    }
}

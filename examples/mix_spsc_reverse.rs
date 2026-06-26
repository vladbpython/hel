use hel::channel::{errors::*, nearest_power_of_two, spsc::shard_spsc};
use std::thread;
use tokio::runtime::Builder;
const CAPACITY: usize = nearest_power_of_two(128);

// Async producer → SPSC → sync consumer per shard.
// Real world: async network downloader → sync file writer.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let ch = shard_spsc::<Vec<u8>, CAPACITY>(4);

    let handles: Vec<_> = ch
        .into_pairs()
        .map(|(shard_id, tx, rx)| {
            // Sync consumer blocking file/disk write
            let consumer = thread::spawn(move || {
                let mut bytes = 0usize;
                loop {
                    match rx.recv() {
                        Ok(chunk) => bytes += chunk.len(),
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                println!("[sync spsc consumer {shard_id}] wrote {bytes} bytes");
            });

            // Async producer network download simulation
            let producer = rt.spawn(async move {
                for i in 0..1_000u64 {
                    // simulate downloaded chunk
                    let chunk = vec![i as u8; 1024];
                    tx.send_async(chunk).await.unwrap();
                }
            });

            (producer, consumer)
        })
        .collect();

    rt.block_on(async {
        for (p, _) in &handles {
            p.abort();
        } // just wait, not abort
    });

    // Proper wait
    for (p, c) in handles {
        rt.block_on(p).ok();
        c.join().unwrap();
    }
}

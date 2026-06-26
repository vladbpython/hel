use hel::channel::{nearest_power_of_two, spsc::shard_spsc};
use std::thread;
use tokio::runtime::Builder;
const CAPACITY: usize = nearest_power_of_two(256);

// Sync producer → SPSC → async consumer per shard.
// Real world: blocking sensor/serial reader → async signal processing.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let ch = shard_spsc::<f64, CAPACITY>(4);

    let handles: Vec<_> = ch
        .into_pairs()
        .map(|(shard_id, tx, rx)| {
            // Async consumer processes data asynchronously
            let consumer = rt.spawn(async move {
                let mut sum = 0.0f64;
                let mut count = 0u64;
                while let Ok(v) = rx.recv_async().await {
                    sum += v;
                    count += 1;
                }
                println!("[async spsc {shard_id}] avg = {:.4}", sum / count as f64);
            });

            // Sync producer blocking sensor read simulation
            let producer = thread::spawn(move || {
                for i in 0..10_000u64 {
                    let sample = (i as f64) * 0.001;
                    tx.send(sample).unwrap();
                }
                println!("[sync spsc producer {shard_id}] done");
            });

            (producer, consumer)
        })
        .collect();

    for (p, c) in handles {
        p.join().unwrap();
        rt.block_on(c).unwrap();
    }
}

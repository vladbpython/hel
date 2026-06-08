use hel::channel::mpmc::round_robin;
use std::thread;

// Batch send: groups messages per shard one lock per shard.
// ~1.5–2× faster than per-element for large batches.
const BATCH: usize = 64;
const CAPACITY: usize = 256;

fn main() {
    let (tx, rx) = round_robin::<u64, CAPACITY>(4);

    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .map(|r| {
            thread::spawn(move || {
                let mut total = 0u64;
                let mut buf = Vec::with_capacity(BATCH);
                loop {
                    let (n, disconnected) = r.recv_batch(&mut buf, BATCH);
                    for v in buf.drain(..n) {
                        total += v;
                    }
                    if disconnected {
                        break;
                    }
                }
                total
            })
        })
        .collect();

    let producer = {
        let tx = tx.clone();
        thread::spawn(move || {
            let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
            for i in 0..100_000u64 {
                buf.push(i);
                if buf.len() == BATCH {
                    tx.send_batch(&mut buf).unwrap();
                }
            }
            if !buf.is_empty() {
                tx.send_batch(&mut buf).unwrap();
            }
        })
    };

    drop(tx);
    producer.join().unwrap();
    let total: u64 = consumers.into_iter().map(|h| h.join().unwrap()).sum();
    println!("batch total = {total}");
}

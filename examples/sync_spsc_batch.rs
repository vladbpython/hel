use hel::channel::{nearest_power_of_two, spsc::shard_spsc};
use std::thread;

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(1024);
// N independent SPSC channels 1 producer per shard, 1 consumer per shard.
// Fastest option when routing is known upfront.

fn main() {
    let ch = shard_spsc::<u64, CAPACITY>(4);

    let handles: Vec<_> = ch
        .into_pairs()
        .map(|(shard_id, tx, rx)| {
            let consumer = thread::spawn(move || {
                let mut total = 0u64;
                let mut buf = Vec::with_capacity(BATCH);
                loop {
                    let (n, dc) = rx.recv_batch(&mut buf, BATCH);
                    for v in buf.drain(..n) {
                        total += v;
                    }
                    if dc {
                        break;
                    }
                }
                println!("[spsc shard {shard_id}] batch total = {total}");
            });
            let producer = thread::spawn(move || {
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
            });
            (producer, consumer)
        })
        .collect();

    for (p, c) in handles {
        p.join().unwrap();
        c.join().unwrap();
    }
}

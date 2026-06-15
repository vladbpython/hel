use hel::channel::{
    errors::*, 
    nearest_power_of_two,
    spsc::shard_spsc
};
use std::thread;

const CAPACITY: usize = nearest_power_of_two(256);
// N independent SPSC channels 1 producer per shard, 1 consumer per shard.
// Fastest option when routing is known upfront.

fn main() {
    let ch = shard_spsc::<u64, CAPACITY>(4);

    let handles: Vec<_> = ch
        .into_pairs()
        .map(|(shard_id, tx, rx)| {
            let consumer = thread::spawn(move || {
                let mut sum = 0u64;
                loop {
                    match rx.recv() {
                        Ok(v) => sum += v,
                        Err(RecvError::Disconnected) => break,
                        Err(_) => unreachable!(),
                    }
                }
                println!("[spsc shard {shard_id}] sum = {sum}");
            });
            let producer = thread::spawn(move || {
                for i in 0..10_000u64 {
                    tx.send(i).unwrap();
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

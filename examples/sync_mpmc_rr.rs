use hel::channel::{errors::*, mpmc::round_robin};
use std::thread;

// N producers → round-robin → S shards → S consumers.
// Optimal for stateless workers: logs, HTTP requests, task queues.
const CAPACITY: usize = 256;

fn main() {
    let (tx, rx) = round_robin::<String, CAPACITY>(4);

    // Spawn consumers one per shard
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            thread::spawn(move || {
                loop {
                    match r.recv() {
                        Ok(msg) => println!("[shard {id}] got: {msg}"),
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
            })
        })
        .collect();

    // Spawn producers
    let producers: Vec<_> = (0..8)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    tx.send(format!("p{p}-msg{i}")).unwrap();
                }
            })
        })
        .collect();

    drop(tx); // signal disconnect when all clones drop
    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
}

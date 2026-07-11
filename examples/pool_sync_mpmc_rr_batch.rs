// round_robin + Batch (sync): in a batch (a unit over the entire slice).
use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    pool::{handler::Batch, instance::Config, sync_pool},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
const CAP: usize = nearest_power_of_two(1024);

fn main() {
    let (tx, rx) = round_robin::<u64, CAP>(4);
    let sum = Arc::new(AtomicU64::new(0));

    let s = sum.clone();
    let pool = sync_pool(
        Config::new(2, 4).batch_size(128),
        rx.into_receivers(),
        Batch(move |batch: &[u64]| {
            s.fetch_add(batch.iter().sum::<u64>(), Relaxed); // весь срез разом
        }),
    );

    for i in 0..10_000u64 {
        tx.send(i).unwrap();
    }

    drop(tx);
    pool.wait_stopping();
    println!("rr_sync_batch: sum = {}", sum.load(Relaxed));
}

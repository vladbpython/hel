// round_robin + PerItem (sync): uniform distribution, one element at a time.
use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    pool::{handler::PerItem, instance::Config, sync_pool},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
const CAP: usize = nearest_power_of_two(1024);

fn main() {
    let (tx, rx) = round_robin::<u64, CAP>(4);
    let sum = Arc::new(AtomicU64::new(0));
    let s = sum.clone();
    let pool = sync_pool(
        Config::new(1, 4),
        rx.into_receivers(),
        PerItem(move |v: &u64| {
            s.fetch_add(*v, Relaxed);
        }),
    );

    for i in 0..10_000u64 {
        tx.send(i).unwrap();
    }

    drop(tx);
    pool.wait_stopping();
    println!("rr_sync_per_item: sum = {}", sum.load(Relaxed));
}

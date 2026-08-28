#![cfg(feature = "pool")]
use hel::{
    channel::mpmc::round_robin,
    pool::{handler::PerItem, instance::Config, sync_pool_slot},
};
use std::{
    panic,
    sync::{
        Arc, Once,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};

#[test]
fn dropping_the_pool_handle_must_stop_workers() {
    let (tx, rx) = round_robin::<u64, 64>(1);
    let count = Arc::new(AtomicU64::new(0));
    let c = count.clone();
    let pool = sync_pool_slot(
        Config::new(1, 2),
        rx.into_receivers(),
        PerItem(move |_v: &u64| {
            c.fetch_add(1, Ordering::Relaxed);
        }),
        |_p: u64, _r| {},
    );
    drop(pool);
    assert!(tx.try_send(7).is_err(), "receivers must be gone after drop");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        count.load(Ordering::Relaxed),
        0,
        "workers are still alive after the handle was dropped"
    );
}

#[test]
#[should_panic(expected = "at least one shard receiver")]
fn azero_shards_is_rejected_loudly() {
    let _ = sync_pool_slot(
        Config::new(1, 1),
        Vec::<hel::channel::Receiver<u64, 8>>::new(),
        PerItem(|_v: &u64| {}),
        |_p: u64, _r| {},
    );
}

#[test]
fn panic_item_drop_does_not_kill_the_worker() {
    struct Boom(u64);
    impl Drop for Boom {
        fn drop(&mut self) {
            if self.0 == 7 {
                panic!("boom in drop");
            }
        }
    }

    {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let prev = panic::take_hook();
            panic::set_hook(Box::new(move |info| {
                let injected = info
                    .payload()
                    .downcast_ref::<&str>()
                    .is_some_and(|s| *s == "boom in drop");
                if !injected {
                    prev(info);
                }
            }));
        });
    }

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (tx, rx) = round_robin::<Boom, 64>(2);
        let count = Arc::new(AtomicU64::new(0));
        let c = count.clone();
        let pool = sync_pool_slot(
            Config::new(2, 2),
            rx.into_receivers(),
            PerItem(move |_v: &Boom| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            |_p: Boom, _r| {},
        );
        for i in 0..20u64 {
            tx.send(Boom(i)).unwrap();
        }
        drop(tx);
        pool.wait_stopping();
        done_tx.send(count.load(Ordering::Relaxed)).unwrap();
    });
    let processed = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("pool wedged, panic item Drop killed the worker and its shard");
    assert_eq!(processed, 20, "every item must still be handled");
}

#[test]
fn worker_count_is_capped_by_shard_count() {
    let (tx, rx) = round_robin::<u64, 64>(2);
    let pool = sync_pool_slot(
        Config::new(usize::MAX, usize::MAX),
        rx.into_receivers(),
        PerItem(|_v: &u64| {}),
        |_p: u64, _r| {},
    );
    assert!(
        pool.active() <= 2,
        "workers must never exceed the shard count"
    );
    for i in 0..100u64 {
        tx.send(i).unwrap();
    }
    drop(tx);
    pool.wait_stopping();
}

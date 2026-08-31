#![cfg(feature = "pool")]
use hel::{
    channel::mpmc::round_robin,
    pool::{errors, handler::PerItem, instance::Config, sync_pool_slot},
};
use std::{
    panic,
    sync::{
        Arc, Once,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
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
    )
    .unwrap();
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
fn azero_shards_is_rejected_loudly() {
    let err = sync_pool_slot(
        Config::new(1, 1),
        Vec::<hel::channel::Receiver<u64, 8>>::new(),
        PerItem(|_v: &u64| {}),
        |_p: u64, _r| {},
    )
    .err();
    assert!(err.is_some(), "UB");
    if let Some(err) = err {
        assert_eq!(err, errors::PoolError::ReceiverEmpty, "wrong error")
    } else {
    }
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
        )
        .unwrap();
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
    )
    .unwrap();
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

#[test]
fn depths_and_stats_expose_the_backed_up_shard() {
    let (tx, rx) = round_robin::<u64, 64>(1);
    for i in 0..10u64 {
        tx.try_send(i).unwrap();
    }
    let started = Arc::new(AtomicU64::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));
    let (s, r, d) = (started.clone(), release.clone(), done.clone());
    let pool = sync_pool_slot(
        Config::new(1, 1)
            .batch_size(1)
            .sample_interval(Duration::from_millis(1)),
        rx.into_receivers(),
        PerItem(move |_v: &u64| {
            s.fetch_add(1, Ordering::SeqCst);
            while !r.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
            d.fetch_add(1, Ordering::SeqCst);
        }),
        |_p: u64, _r| {},
    )
    .unwrap();
    // If any assert below fails while the gate is closed, the unwind drops
    // the pool, and SyncPool::Drop joins worker that is parked inside the gated handler - the test would hang instead of failing.
    // This guard opens the gate on any exit, so a failure stays a failure.
    struct Ungate(Arc<AtomicBool>);
    impl Drop for Ungate {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let _ungate = Ungate(release.clone());

    // The worker is inside the handler with item 1; batch_size(1) means exactly one item left the ring.
    let t = Instant::now();
    while started.load(Ordering::SeqCst) == 0 {
        assert!(t.elapsed() < Duration::from_secs(5), "worker never started");
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(pool.shards(), 1);
    // The depth mirror refreshes once per sample_interval (1ms here),
    // so give the monitor a tick to observe the post-claim state.
    let t = Instant::now();
    while pool.depths() != Some(vec![9]) {
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "mirror never showed 9 queued: {:?}",
            pool.depths()
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(pool.shard_queued(0), Some(9));
    assert_eq!(pool.shard_queued(1), None, "out of range must be None");

    let stats = pool.stats();
    assert_eq!(stats.workers.len(), 1);
    assert!(stats.workers[0].busy, "worker sits inside the handler");
    assert!(
        !stats.workers[0].stalled,
        "takeover is off, nobody is stalled"
    );
    assert_eq!(
        stats.workers[0].current_shard,
        Some(0),
        "the dequeued batch belongs to shard 0"
    );
    assert_eq!(stats.shards.len(), 1);
    assert_eq!(stats.shards[0].queued, 9);
    assert_eq!(stats.shards[0].owner, Some(0));
    assert_eq!(stats.processed, 0, "nothing completed while gated");
    assert_eq!(stats.takeovers, 0);
    assert!(!stats.stopped);

    // Release the gate: everything drains, the depth returns to zero.
    release.store(true, Ordering::Release);
    let t = Instant::now();
    while done.load(Ordering::SeqCst) < 10 {
        assert!(t.elapsed() < Duration::from_secs(5), "drain never finished");
        thread::sleep(Duration::from_millis(1));
    }
    let t = Instant::now();
    while pool.depths() != Some(vec![0]) {
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "drained shard must read empty: {:?}",
            pool.depths()
        );
        thread::sleep(Duration::from_millis(1));
    }
    let stats = pool.stats();
    assert_eq!(stats.processed, 10);
    assert_eq!(
        stats.workers[0].beats, 10,
        "one heartbeat per completed item"
    );
    assert!(
        !stats.channels_closed,
        "channels are alive while the pool runs"
    );
    drop(tx);
}

#[test]
fn stopped_pool_keeps_depths_while_receivers_live() {
    let (tx, rx) = round_robin::<u64, 64>(1);
    let gate = Arc::new(AtomicBool::new(false));
    let g = gate.clone();
    let pool = sync_pool_slot(
        Config::new(1, 1)
            .batch_size(1)
            .sample_interval(Duration::from_millis(1)),
        rx.into_receivers(),
        PerItem(move |_v: &u64| {
            while !g.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
        }),
        |_p: u64, _r| {},
    )
    .unwrap();
    struct Ungate(Arc<AtomicBool>);
    impl Drop for Ungate {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let _ungate = Ungate(gate.clone());

    // The worker takes this item and blocks inside the handler.
    tx.try_send(1).unwrap();
    let t = Instant::now();
    while !pool.stats().workers[0].busy {
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "worker never entered the handler"
        );
        thread::sleep(Duration::from_millis(1));
    }
    // Fill the ring behind the stuck worker, then stop. Nobody will ever
    // publish these depths: the monitor exits on the flag, the worker is
    // wedged.
    while tx.try_send(2).is_ok() {}
    pool.get_signal_stop().stop();

    let stats = pool.stats();
    assert!(stats.stopped, "stop must be visible");
    assert!(
        !stats.channels_closed,
        "the window needs the channels still open (worker holds its receivers)"
    );
    let depths = pool
        .depths()
        .expect("receivers alive: the mirror is still live (audit 8, Y3)");
    assert_eq!(
        depths[0], stats.shards[0].queued,
        "depths() and stats() must agree on the same instant's backlog"
    );
    assert_eq!(
        pool.shard_queued(0),
        Some(depths[0]),
        "the single-shard read must match the vector read"
    );
}

#[test]
fn stopping_the_pool_disconnects_senders_despite_the_live_handle() {
    let (tx, rx) = round_robin::<u64, 64>(1);
    let gate = Arc::new(AtomicBool::new(false));
    let g = gate.clone();
    let pool = sync_pool_slot(
        Config::new(1, 1)
            .batch_size(1)
            .sample_interval(Duration::from_millis(1)),
        rx.into_receivers(),
        PerItem(move |_v: &u64| {
            while !g.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
        }),
        |_p: u64, _r| {},
    )
    .unwrap();

    // Fill the ring while the worker is gated, then park one sender on it.
    while tx.try_send(1).is_ok() {}
    let tx2 = tx.clone();
    let blocked = thread::spawn(move || tx2.send(2));

    // Stop while the handle stays alive, then let the worker finish its item
    // and exit. Its receivers drop with it and the channel must close.
    pool.get_signal_stop().stop();
    gate.store(true, Ordering::Release);

    let t = Instant::now();
    loop {
        match tx.try_send(3) {
            Err(e) if e.err.is_disconnected() => break, // rx_close fired
            _ => {} // accepted or still Full: receivers not gone yet
        }
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "handle kept the receivers alive: senders never saw the close"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let sent = blocked.join().unwrap();
    assert!(
        sent.is_err(),
        "the parked sender must wake with Disconnected, not deliver into a dead ring"
    );
    assert_eq!(
        pool.depths(),
        None,
        "channels are gone: depths must say so instead of lying"
    );
    assert_eq!(pool.shard_queued(0), None);
    let stats = pool.stats();
    assert!(stats.stopped);
    assert!(
        stats.channels_closed,
        "the snapshot must say the channels are gone (audit 5, W3)"
    );
}

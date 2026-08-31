use hel::channel::mpmc::{ShardGroupCase, round_robin, shard_group, shard_key};
use std::{
    error::Error,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time as tokio_time;

#[test]
fn group_send_batch_timeout_overflow_must_not_destroy_batch() {
    let (tx, _rx) = shard_group::<(String, u64), 8>(ShardGroupCase::Groups {
        groups: &[&["A"], &["B"]],
    });
    let mut buf: Vec<(String, u64)> = vec![
        ("A".into(), 1),
        ("B".into(), 2),
        ("A".into(), 3),
        ("GHOST".into(), 4),
    ];
    // Duration::MAX must not panic at all now, deadline saturates and call behaves as "wait forever".
    // Ring is empty, so the 3 routable items go through and only the orphan comes back.
    let r = catch_unwind(AssertUnwindSafe(|| {
        tx.send_batch_timeout(&mut buf, Duration::MAX, |(k, _)| k.as_str())
    }));
    let sent = r
        .expect("huge Duration must not panic")
        .expect("send must succeed");
    assert_eq!(sent, 3, "three routable items");
    assert_eq!(buf.len(), 1, "only the GHOST stays in buf");
}

#[cfg(not(miri))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_recv_must_wake_every_parked_sender() {
    let (tx, mut rx) = round_robin::<u64, 4>(1);
    for i in 0..4u64 {
        tx.try_send(i).unwrap(); // ring full
    }
    let done = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for i in 0..3u64 {
        let tx = tx.clone();
        let d = done.clone();
        tasks.push(tokio::spawn(async move {
            tx.send_async(100 + i).await.unwrap();
            d.fetch_add(1, Ordering::Relaxed);
        }));
    }
    tokio_time::sleep(Duration::from_millis(100)).await; // parked

    let mut buf = Vec::new();
    let (n, _) = rx.try_recv_batch_any(&mut buf, 16);
    assert_eq!(n, 4, "the batch recv must free all 4 slots");

    tokio_time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        done.load(Ordering::Relaxed),
        3,
        "4 slots are free but parked senders were not all woken"
    );
    for t in tasks {
        t.abort();
    }
}

#[cfg(not(miri))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recv_any_must_not_eat_the_other_shards_notification() {
    let (tx, mut rx) = round_robin::<u64, 8>(2);
    let shard1 = rx.get_receiver(1).unwrap().clone();

    let got_b = Arc::new(AtomicU64::new(0));
    let gb = got_b.clone();

    // task A: recv_any, slots at both shards.
    let a = tokio::spawn(async move {
        let (_idx, v) = rx.recv_any().await.unwrap();
        v
    });
    tokio_time::sleep(Duration::from_millis(100)).await;

    // task B: regular receiver of shard 1 stands in FIFO behind A.
    let b = tokio::spawn(async move {
        let v = shard1.recv_async().await.unwrap();
        gb.store(v, Ordering::Relaxed);
    });
    tokio_time::sleep(Duration::from_millis(100)).await;

    // round_robin: первый send -> один шард, второй -> другой.
    tx.try_send(10).unwrap();
    tx.try_send(11).unwrap();

    let _ = tokio_time::timeout(Duration::from_millis(300), a)
        .await
        .expect("recv_any must complete");

    // B must receive an element of its shard.
    let r = tokio::time::timeout(Duration::from_millis(500), b).await;
    assert!(
        r.is_ok(),
        "LOST WAKEUP!!!!, an item sits in shard 1 but its receiver was never notified"
    );
}

#[cfg(not(miri))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recv_any_close_race_always_resolves() {
    for _ in 0..400 {
        let (tx, mut rx) = round_robin::<u64, 8>(2);
        let waiter = tokio::spawn(async move { rx.recv_any().await });
        let closer = tokio::spawn(async move { drop(tx) });
        closer.await.unwrap();
        let r = tokio_time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("recv_any parked forever after all senders were dropped")
            .unwrap();
        assert!(r.is_err(), "empty closed channel must report Disconnected");
    }
}

#[cfg(not(miri))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_send_must_wake_every_parked_receiver() {
    let (tx, rx) = round_robin::<u64, 8>(1);
    let done = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let r = rx.get_receiver(0).unwrap().clone();
        let d = done.clone();
        tasks.push(tokio::spawn(async move {
            r.recv_async().await.unwrap();
            d.fetch_add(1, Ordering::Relaxed);
        }));
    }
    tokio_time::sleep(Duration::from_millis(100)).await; // parked

    let mut buf: Vec<u64> = (0..4).collect();
    tx.try_send_batch(&mut buf).unwrap();
    assert!(buf.is_empty(), "the whole batch must fit");

    tokio_time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        done.load(Ordering::Relaxed),
        4,
        "4 items are queued but parked receivers were not all woken"
    );
    for t in tasks {
        t.abort();
    }
}

#[test]
fn errors_are_real_errors() {
    fn boxed() -> Result<(), Box<dyn Error>> {
        let (tx, rx) = round_robin::<u64, 8>(1);
        drop(rx);
        tx.try_send(1)?;
        Ok(())
    }
    let e = boxed().unwrap_err();
    let text = format!("{e}");
    assert!(
        text.contains("disconnected"),
        "human readable message, got: {text}"
    );
    let (tx, rx) = round_robin::<u64, 8>(1);
    drop(rx);
    let err = tx.try_send(41).unwrap_err();
    assert_eq!(err.err.into_inner(), 41, "payload must stay reachable");
}

#[test]
fn try_recv_any_reports_disconnect() {
    let (tx, mut rx) = round_robin::<u64, 8>(2);
    tx.try_send(1).unwrap();
    drop(tx);

    // While there is data -> Ok(Some(..)).
    let got = rx.try_recv_any().unwrap().expect("one item is queued");
    assert_eq!(got.1, 1);
    // There is no data and there are no senders -> Err, not an eternal Ok(None).
    assert!(rx.try_recv_any().is_err(), "must signal disconnect");

    let (tx2, mut rx2) = round_robin::<u64, 8>(2);
    tx2.try_send(5).unwrap();
    drop(tx2);
    let mut buf = Vec::new();
    let (n, dc) = rx2.try_recv_batch_any(&mut buf, 8);
    assert_eq!((n, dc), (1, false), "items first, no dc while draining");
    let (n, dc) = rx2.try_recv_batch_any(&mut buf, 8);
    assert_eq!((n, dc), (0, true), "empty + closed everywhere -> dc");
}

#[test]
#[should_panic(expected = "shard index 7 out of range")]
fn out_of_range_shard_panics() {
    let (_tx, mut rx) = round_robin::<u64, 8>(4);
    let _ = rx.try_recv(7);
}

#[test]
fn try_send_batch_attempts_every_shard() {
    let (tx, _rx) = shard_key::<(String, u64), 2>(4);
    let mut keys: Vec<Option<String>> = vec![None; 4];
    let mut i = 0u32;
    while keys.iter().any(|k| k.is_none()) {
        let k = format!("k{i}");
        i += 1;
        let s = tx.shard_for(&k);
        if keys[s].is_none() {
            keys[s] = Some(k);
        }
    }
    let keys: Vec<String> = keys.into_iter().map(Option::unwrap).collect();
    tx.try_send(&keys[0], (keys[0].clone(), 100)).unwrap();
    tx.try_send(&keys[0], (keys[0].clone(), 101)).unwrap();

    let mut buf: Vec<(String, u64)> = (0..4).map(|s| (keys[s].clone(), s as u64)).collect();
    let err = tx
        .try_send_batch(&mut buf, |(k, _)| k.as_str())
        .unwrap_err();
    assert_eq!(err.sent, 3, "the three free shards must be attempted");
    assert_eq!(err.shard, 0, "the full shard is the reported culprit");
    assert_eq!(buf.len(), 1, "only the full shard's item comes back");
    assert_eq!(buf[0].1, 0);
    assert_eq!(tx.shard_for(&err.key), 0, "error key points at the culprit");
}

#[test]
fn get_receiver_is_the_non_panicking_route() {
    let (tx, rx) = round_robin::<u64, 8>(4);
    tx.try_send(9).unwrap();
    assert!(
        rx.get_receiver(7).is_none(),
        "out of range -> None, not panic"
    );
    let r = rx.get_receiver(0).expect("shard 0 exists");
    let _ = r.try_recv();
}

#[test]
fn send_errors_convert_to_boxed_error_for_non_debug_payloads() {
    struct Opaque; // not Debug
    fn produces() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, rx) = round_robin::<Opaque, 2>(1);
        drop(rx);
        tx.try_send(Opaque)?; // must convert via `?`
        Ok(())
    }
    assert!(
        produces().is_err(),
        "disconnected send must surface as an error"
    );
}

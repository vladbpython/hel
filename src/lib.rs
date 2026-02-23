pub(crate) mod cache;
pub(crate) mod core;
pub mod errors;
pub mod result;
pub mod receiver;
pub mod sender;
pub (crate) mod sync;

pub mod channel{
    use super::{
        core::{Inner,SingleInner},
        receiver::{Receiver,SingleReceiver},
        sender::{Sender,SingleSender},
    };
    use std::sync::Arc;

    pub fn bounded<T,const CAP: usize>() -> (Sender<T, CAP>, Receiver<T, CAP>) {
        let inner = Arc::new(Inner::new());
        (Sender::new(inner.clone()), Receiver::new(inner))
    }

    pub fn scsp_bounded<T,const CAP: usize>() -> (SingleSender<T,CAP>, SingleReceiver<T,CAP>) {
        let inner = Arc::new(SingleInner::new());
        (SingleSender::new(inner.clone()), SingleReceiver::new(inner))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        channel::{bounded,scsp_bounded},
        errors::{
            RecvError,AsyncRecvError,
            SendError,
        },
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed as Rlx}
        },
        task::{Context,Poll},
        time::Duration,
        thread,
    };


    #[test]
    fn blocking_mpsc_full() {
        let (tx, rx) = bounded::<u64, 64>();
        let tx2 = tx.clone();
        let h1 = thread::spawn(move || { for i in 0..50_000u64 { tx.send(i).unwrap(); } });
        let h2 = thread::spawn(move || { for i in 50_000u64..100_000u64 { tx2.send(i).unwrap(); } });
        let mut sum = 0u64;
        let mut count = 0u64;
        while count < 100_000u64 {
            match rx.recv() {
                Ok(v) => { sum += v; count += 1; }
                Err(RecvError::Disconnected) => break,
                _ => unreachable!(),
            }
        }
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn blocking_mpmc() {
        let (tx, rx) = bounded::<u64, 64>();
        let tx2 = tx.clone();
        let rx2 = rx.clone();
        let total = Arc::new(AtomicU64::new(0));
        let total2 = total.clone();

        let h1 = thread::spawn(move || { for i in 0..50_000u64 { tx.send(i).unwrap(); } });
        let h2 = thread::spawn(move || { for i in 50_000u64..100_000u64 { tx2.send(i).unwrap(); } });

        let h3 = {
            let total = total.clone();
            thread::spawn(move || {
                let mut sum = 0u64;
                loop {
                    match rx.recv() {
                        Ok(v) => sum += v,
                        Err(RecvError::Disconnected) => break,
                        _ => unreachable!(),
                    }
                }
                total.fetch_add(sum, Rlx);
            })
        };
        let h4 = thread::spawn(move || {
            let mut sum = 0u64;
            loop {
                match rx2.recv() {
                    Ok(v) => sum += v,
                    Err(RecvError::Disconnected) => break,
                    _ => unreachable!(),
                }
            }
            total2.fetch_add(sum, Rlx);
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
        h4.join().unwrap();
        assert_eq!(total.load(Rlx), 4_999_950_000u64);
    }

    #[test]
    fn async_mpmc() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 128>();
            let tx2 = tx.clone();
            let rx2 = rx.clone();
            let total = Arc::new(AtomicU64::new(0));
            let total2 = total.clone();

            tokio::spawn(async move { for i in 0..50_000u64 { tx.send_async(i).await.unwrap(); } });
            tokio::spawn(async move { for i in 50_000u64..100_000u64 { tx2.send_async(i).await.unwrap(); } });

            let c1_total = total.clone();
            let c1 = tokio::spawn(async move {
                let mut sum = 0u64;
                loop {
                    match rx.recv_async().await {
                        Ok(v) => sum += v,
                        Err(AsyncRecvError::Disconnected) => break,
                    }
                }
                c1_total.fetch_add(sum, Rlx);
            });
            let c2 = tokio::spawn(async move {
                let mut sum = 0u64;
                loop {
                    match rx2.recv_async().await {
                        Ok(v) => sum += v,
                        Err(AsyncRecvError::Disconnected) => break,
                    }
                }
                total2.fetch_add(sum, Rlx);
            });
            c1.await.unwrap();
            c2.await.unwrap();
            assert_eq!(total.load(Rlx), 4_999_950_000u64);
        });
    }

    #[test]
    fn tx_close_wakes_all_receivers() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 4>();
            let rx2 = rx.clone();
            let h1 = tokio::spawn(async move {
                assert_eq!(rx.recv_async().await, Err(AsyncRecvError::Disconnected));
            });
            let h2 = tokio::spawn(async move {
                assert_eq!(rx2.recv_async().await, Err(AsyncRecvError::Disconnected));
            });
            tokio::task::yield_now().await;
            drop(tx);
            h1.await.unwrap();
            h2.await.unwrap();
        });
    }

    #[test]
    fn recv_future_drop_while_queued() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 4>();
            {
                let fut = rx.recv_async();
                tokio::pin!(fut);
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
            }
            tx.send_async(42u64).await.unwrap();
            assert_eq!(rx.recv_async().await.unwrap(), 42);
        });
    }

    #[test]
    fn blocking_tx_close_wakes_receivers() {
        let (tx, rx) = bounded::<u64, 4>();
        let rx2 = rx.clone();

        let h1 = thread::spawn(move || {
            assert_eq!(rx.recv(), Err(RecvError::Disconnected));
        });
        let h2 = thread::spawn(move || {
            assert_eq!(rx2.recv(), Err(RecvError::Disconnected));
        });
        thread::sleep(std::time::Duration::from_millis(10));
        drop(tx);
        h1.join().unwrap();
        h2.join().unwrap();
    }

    #[test]
    fn mixed_sync_send_async_recv() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, rx) = bounded::<u64, 64>();

        thread::spawn(move || {
            for i in 0..100_000u64 { tx.send(i).unwrap(); }
        });

        rt.block_on(async move {
            let mut sum = 0u64;
            loop {
                match rx.recv_async().await {
                    Ok(v) => sum += v,
                    Err(AsyncRecvError::Disconnected) => break,
                }
            }
            assert_eq!(sum, 4_999_950_000u64);
        });
    }

    #[test]
    fn spsc_blocking() {
        let (tx, rx) = scsp_bounded::<u64, 64>();
        let h = thread::spawn(move || {
            for i in 0..100_000u64 { tx.send(i).unwrap(); }
        });
        let mut sum = 0u64;
        loop {
            match rx.recv() {
                Ok(v) => sum += v,
                Err(RecvError::Disconnected) => break,
                _ => unreachable!(),
            }
        }
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn spsc_async() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = scsp_bounded::<u64, 128>();
            tokio::spawn(async move {
                for i in 0..100_000u64 { tx.send_async(i).await.unwrap(); }
            });
            let mut sum = 0u64;
            loop {
                match rx.recv_async().await {
                    Ok(v) => sum += v,
                    Err(AsyncRecvError::Disconnected) => break,
                }
            }
            assert_eq!(sum, 4_999_950_000u64);
        });
    }

    #[test]
    fn recv_batch_collects_all() {
        let (tx, rx) = bounded::<u64, 256>();
        // Заполняем буфер
        for i in 0..200u64 { tx.try_send(i).unwrap(); }

        let mut buf = Vec::new();
        let (n, _) = rx.recv_batch(&mut buf, 200);
        assert_eq!(n, 200);
        assert_eq!(buf.iter().sum::<u64>(), (0..200u64).sum::<u64>());
    }

    #[test]
    fn recv_batch_async_burst() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 256>();
            // Burst: 100 items сразу
            for i in 0..100u64 { tx.try_send(i).unwrap(); }
            drop(tx);

            let mut buf = Vec::new();
            let mut total = 0u64;
            loop {
                let (n, disconnected) = rx.recv_batch_async(&mut buf, 64).await;
                for v in buf.drain(..) { total += v; }
                if disconnected && n == 0 { break; }
            }
            assert_eq!(total, (0..100u64).sum::<u64>());
        });
    }

    #[test]
    fn spsc_batch() {
        let (tx, rx) = scsp_bounded::<u64, 256>();
        for i in 0..200u64 { tx.try_send(i).unwrap(); }
        let mut buf = Vec::new();
        let (n, _) = rx.recv_batch(&mut buf, 300);
        assert_eq!(n, 200);
        assert_eq!(buf.iter().sum::<u64>(), (0..200u64).sum::<u64>());
    }

    // ── Iterator tests ────────────────────────────────────────────────────────

    #[test]
    fn mpmc_iter_ref() {
        // for v in &rx — не потребляет receiver
        let (tx, rx) = bounded::<u64, 64>();
        let h = thread::spawn(move || { for i in 0..100_000u64 { tx.send(i).unwrap(); } });
        let sum: u64 = (&rx).into_iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn mpmc_into_iter() {
        // for v in rx — потребляет receiver
        let (tx, rx) = bounded::<u64, 64>();
        let h = thread::spawn(move || { for i in 0..100_000u64 { tx.send(i).unwrap(); } });
        let sum: u64 = rx.into_iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn mpmc_iter_method() {
        // rx.iter() — явный метод
        let (tx, rx) = bounded::<u64, 64>();
        let h = thread::spawn(move || { for i in 0..100_000u64 { tx.send(i).unwrap(); } });
        let sum: u64 = rx.iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn mpmc_stream() {
        use futures::StreamExt;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 128>();
            tokio::spawn(async move { for i in 0..100_000u64 { tx.send_async(i).await.unwrap(); } });
            let stream = std::pin::pin!(rx.stream());
            let sum: u64 = stream.fold(0u64, |acc, v| async move { acc + v }).await;
            assert_eq!(sum, 4_999_950_000u64);
        });
    }

    #[test]
    fn mpmc_stream_while_let() {
        use futures::StreamExt;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 128>();
            tokio::spawn(async move { for i in 0..100_000u64 { tx.send_async(i).await.unwrap(); } });
            let mut stream = std::pin::pin!(rx.stream());
            let mut sum = 0u64;
            while let Some(v) = stream.next().await { sum += v; }
            assert_eq!(sum, 4_999_950_000u64);
        });
    }

    #[test]
    fn spsc_iter_ref() {
        let (tx, rx) = scsp_bounded::<u64, 64>();
        let h = thread::spawn(move || { for i in 0..100_000u64 { tx.send(i).unwrap(); } });
        let sum: u64 = (&rx).into_iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn spsc_into_iter() {
        let (tx, rx) = scsp_bounded::<u64, 64>();
        let h = thread::spawn(move || { for i in 0..100_000u64 { tx.send(i).unwrap(); } });
        let sum: u64 = rx.into_iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn spsc_iter_method() {
        let (tx, rx) = scsp_bounded::<u64, 64>();
        let h = thread::spawn(move || { for i in 0..100_000u64 { tx.send(i).unwrap(); } });
        let sum: u64 = rx.iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn spsc_stream() {
        use futures::StreamExt;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = scsp_bounded::<u64, 128>();
            tokio::spawn(async move { for i in 0..100_000u64 { tx.send_async(i).await.unwrap(); } });
            let stream = std::pin::pin!(rx.stream());
            let sum: u64 = stream.fold(0u64, |acc, v| async move { acc + v }).await;
            assert_eq!(sum, 4_999_950_000u64);
        });
    }

    #[test]
    fn spsc_stream_while_let() {
        use futures::StreamExt;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = scsp_bounded::<u64, 128>();
            tokio::spawn(async move { for i in 0..100_000u64 { tx.send_async(i).await.unwrap(); } });
            let mut stream = std::pin::pin!(rx.stream());
            let mut sum = 0u64;
            while let Some(v) = stream.next().await { sum += v; }
            assert_eq!(sum, 4_999_950_000u64);
        });
    }

     #[test]
    fn recv_timeout_returns_timeout_when_empty() {
        let (_tx, rx) = bounded::<u64, 4>();
        match rx.recv_timeout(Duration::from_millis(20)) {
            Err(RecvError::TimeOut(_)) => {}
            other => panic!("expected Timeout, got {:?}", other),
        }
    }

    #[test]
    fn recv_timeout_returns_value_before_deadline() {
        let (tx, rx) = bounded::<u64, 4>();
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            tx.send(99u64).unwrap();
        });
        assert_eq!(rx.recv_timeout(Duration::from_millis(500)), Ok(99));
        h.join().unwrap();
    }

    #[test]
    fn send_timeout_returns_timeout_when_full() {
        let (tx, _rx) = bounded::<u64, 2>();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        match tx.send_timeout(3, Duration::from_millis(20)) {
            Err(SendError::TimeOut((3, _))) => {}
            other => panic!("expected Timeout, got {:?}", other),
        }
    }

    #[test]
    fn send_timeout_succeeds_when_slot_freed() {
        let (tx, rx) = bounded::<u64, 2>();
        let rx2  = rx.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            rx2.recv().unwrap();
        });
        assert!(tx.send_timeout(3, Duration::from_millis(500)).is_ok());
        h.join().unwrap();
    }

    #[test]
    fn spsc_recv_timeout() {
        let (_tx, rx) = scsp_bounded::<u64, 4>();
        match rx.recv_timeout(Duration::from_millis(20)) {
            Err(RecvError::TimeOut(_)) => {}
            other => panic!("expected Timeout, got {:?}", other),
        }
    }

    #[test]
    fn spsc_send_timeout() {
        let (tx, _rx) = scsp_bounded::<u64, 2>();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        match tx.send_timeout(3, Duration::from_millis(20)) {
            Err(SendError::TimeOut((3, _))) => {}
            other => panic!("expected Timeout, got {:?}", other),
        }
    }

    #[test]
    fn send_batch_all_sent() {
        let (tx, rx) = bounded::<u64, 256>();
        let mut buf: Vec<u64> = (0..100).collect();
        let sent = tx.send_batch(&mut buf);
        assert_eq!(sent, 100);
        assert!(buf.is_empty());
        drop(tx);
        let collected: Vec<u64> = rx.iter().collect();
        assert_eq!(collected, (0..100u64).collect::<Vec<_>>());
    }

    #[test]
    fn send_batch_partial_when_disconnected() {
        let (tx, rx) = bounded::<u64, 256>();
        drop(rx);
        let mut buf: Vec<u64> = (0..10).collect();
        let sent = tx.send_batch(&mut buf);
        assert_eq!(sent, 0);
        // Неотправленные элементы остаются в buf в исходном порядке
        assert_eq!(buf, (0..10u64).collect::<Vec<_>>());
    }

    #[test]
    fn send_batch_timeout_partial_when_full() {
        let (tx, _rx) = bounded::<u64, 4>();
        let mut buf: Vec<u64> = (0..10).collect();
        let sent = tx.send_batch_timeout(&mut buf, Duration::from_millis(20));
        // Отправлено не больше CAP=4, остаток в buf
        assert!(sent <= 4);
        assert_eq!(sent + buf.len(), 10);
        // Порядок сохранён
        assert_eq!(buf, (sent as u64..10).collect::<Vec<_>>());
    }

    #[test]
    fn send_batch_async_all_sent() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 256>();
            let mut buf: Vec<u64> = (0..100).collect();
            let sent = tx.send_async_batch(&mut buf).await;
            assert_eq!(sent, 100);
            assert!(buf.is_empty());
            drop(tx);
            let mut collected = Vec::new();
            loop {
                match rx.recv_async().await {
                    Ok(v) => collected.push(v),
                    Err(AsyncRecvError::Disconnected) => break,
                }
            }
            assert_eq!(collected, (0..100u64).collect::<Vec<_>>());
        });
    }

    #[test]
    fn spsc_send_batch() {
        let (tx, rx) = scsp_bounded::<u64, 256>();
        let mut buf: Vec<u64> = (0..100).collect();
        let sent = tx.send_batch(&mut buf);
        assert_eq!(sent, 100);
        assert!(buf.is_empty());
        drop(tx);
        let collected: Vec<u64> = rx.iter().collect();
        assert_eq!(collected, (0..100u64).collect::<Vec<_>>());
    }

    #[test]
    fn recv_batch_timeout_returns_empty_on_timeout() {
        let (_tx, rx) = bounded::<u64, 4>();
        let mut buf = Vec::new();
        let (n, disc) = rx.recv_batch_timeout(&mut buf, 8, Duration::from_millis(20));
        assert_eq!(n, 0);
        assert!(!disc);
    }

    #[test]
    fn recv_batch_timeout_collects_data() {
        let (tx, rx) = bounded::<u64, 256>();
        for i in 0..50u64 { tx.try_send(i).unwrap(); }
        let mut buf = Vec::new();
        let (n, _) = rx.recv_batch_timeout(&mut buf, 64, Duration::from_millis(100));
        assert!(n >= 1);
        assert_eq!(buf.iter().copied().sum::<u64>(), (0..n as u64).sum());
    }

    #[test]
    fn recv_batch_timeout_disconnected() {
        let (tx, rx) = bounded::<u64, 4>();
        drop(tx);
        let mut buf = Vec::new();
        let (n, disc) = rx.recv_batch_timeout(&mut buf, 8, Duration::from_millis(20));
        assert_eq!(n, 0);
        assert!(disc);
    }

    #[test]
    fn spsc_recv_batch_timeout() {
        let (_tx, rx) = scsp_bounded::<u64, 4>();
        let mut buf = Vec::new();
        let (n, disc) = rx.recv_batch_timeout(&mut buf, 8, Duration::from_millis(20));
        assert_eq!(n, 0);
        assert!(!disc);
    }

    #[test]
    fn drop_future_while_queued_does_not_corrupt() {
        // Создаём future, ставим в очередь, дропаем не дожидаясь
        // Затем проверяем что channel продолжает работать корректно
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded::<u64, 4>();
            for _ in 0..10 {
                {
                    let fut = rx.recv_async();
                    tokio::pin!(fut);
                    let waker = futures::task::noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    let _ = fut.as_mut().poll(&mut cx); // queued=true
                } // drop — нода уходит со стека
                // Channel должен продолжать работать
                tx.try_send(42u64).unwrap();
                assert_eq!(rx.recv_async().await.unwrap(), 42);
            }
        });
    }


    #[test]
    fn blocking_recv_spurious_wakeup_safety() {
        // Много итераций через park/unpark — spurious wakeups наиболее вероятны
        // при высокой нагрузке. Если dangling pointer — SIGSEGV или неверная сумма.
        let (tx, rx) = bounded::<u64, 4>();
        let h = thread::spawn(move || {
            for i in 0..100_000u64 { tx.send(i).unwrap(); }
        });
        let sum: u64 = rx.iter().sum();
        h.join().unwrap();
        assert_eq!(sum, 4_999_950_000u64);
    }

    #[test]
    fn concurrent_drop_senders() {
        let (tx, rx) = bounded::<u64, 64>();
        let handles: Vec<_> = (0..8).map(|_| {
            let tx = tx.clone();
            thread::spawn(move || drop(tx)) // все дропаются одновременно
        }).collect();
        drop(tx);
        for h in handles { h.join().unwrap(); }
        // rx должен получить Disconnected, не паниковать
        assert_eq!(rx.recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn mpmc_data_integrity_stress() {
        // Если memory safety нарушена — данные будут corrupted, сумма не сойдётся
        let (tx, rx) = bounded::<u64, 64>();
        let total = Arc::new(AtomicU64::new(0));
        let producers: Vec<_> = (0..4).map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..25_000u64 { tx.send(i).unwrap(); }
            })
        }).collect();
        drop(tx);
        let consumers: Vec<_> = (0..4).map(|_| {
            let rx = rx.clone();
            let total = total.clone();
            thread::spawn(move || {
                let sum: u64 = rx.iter().sum();
                total.fetch_add(sum, Rlx);
            })
        }).collect();
        for h in producers { h.join().unwrap(); }
        for h in consumers { h.join().unwrap(); }
        // 4 producers × sum(0..25000) = 4 × 312_487_500
        assert_eq!(total.load(Rlx), 4 * (0..25_000u64).sum::<u64>());
    }

}


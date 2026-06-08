pub mod core;
pub mod errors;
pub mod receiver;
pub mod sender;
pub mod sync;
pub mod traits;

use std::sync::Arc;

// Lock free CAS ring buffer.
pub fn mpmc_bounded<T: Send, const CAP: usize>()
-> (sender::Sender<T, CAP>, receiver::Receiver<T, CAP>) {
    let inner = Arc::new(core::MPMCInner::new());
    (
        sender::Sender::new(inner.clone()),
        receiver::Receiver::new(inner),
    )
}

#[allow(dead_code)]
pub fn scsp_bounded<T: Send, const CAP: usize>() -> (
    sender::SingleSender<T, CAP>,
    receiver::SingleReceiver<T, CAP>,
) {
    let inner = Arc::new(core::SingleInner::new());
    (
        sender::SingleSender::new(inner.clone()),
        receiver::SingleReceiver::new(inner),
    )
}

/// Helper function: Rounds n to the nearest power of two.
#[doc(hidden)]
#[inline]
pub const fn nearest_power_of_two(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    n.next_power_of_two()
}

// Tests (Miri compatible)
// Miri rules:
// small CAP (8..32) and N (4..16) Miri slow
// no tokio::test (Miri does not support tokio runtime)
// no spin/busy wait Miri detects infinite loops
// thread::spawn ok, but keep a minimum of threads

#[cfg(test)]
mod tests {
    use super::{
        errors::{AsyncRecvError, RecvError, TryRecvError},
        mpmc_bounded, scsp_bounded,
    };
    use std::{thread, time::Duration};

    // Try recv

    #[test]
    fn try_recv_empty() {
        let (_tx, rx) = mpmc_bounded::<u64, 8>(); // _tx is alive -the channel is open
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn try_recv_basic() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        tx.try_send(42).unwrap();
        assert_eq!(rx.try_recv(), Ok(42));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn try_recv_disconnected_empty_channel() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn try_recv_disconnected_drains_remaining() {
        // Disconnected only after the buffer is empty
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    // recv (blocking)

    #[test]
    fn recv_basic_1p1c() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        let p = thread::spawn(move || {
            for i in 0..4u64 {
                tx.send(i).unwrap();
            }
        });
        let mut vals = Vec::new();
        for _ in 0..4 {
            vals.push(rx.recv().unwrap());
        }
        p.join().unwrap();
        assert_eq!(vals, vec![0, 1, 2, 3]);
    }

    #[test]
    fn recv_disconnected_after_all_consumed() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        tx.send(99).unwrap();
        drop(tx);
        assert_eq!(rx.recv(), Ok(99));
        assert_eq!(rx.recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn recv_blocks_until_data() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        let p = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1));
            tx.send(7).unwrap();
        });
        assert_eq!(rx.recv(), Ok(7));
        p.join().unwrap();
    }

    // Recv timeout

    #[test]
    fn recv_timeout_gets_value() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        tx.send(5).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Ok(5));
    }

    #[test]
    fn recv_timeout_expires() {
        let (_tx, rx) = mpmc_bounded::<u64, 8>(); // _tx alive — otherwise Disconnected
        let result = rx.recv_timeout(Duration::from_millis(10));
        assert!(matches!(result, Err(RecvError::TimeOut(_))));
    }

    #[test]
    fn recv_timeout_disconnected() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        drop(tx);
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)),
            Err(RecvError::Disconnected)
        );
    }

    //  Recv batch

    #[test]
    fn recv_batch_basic() {
        let (tx, rx) = mpmc_bounded::<u64, 16>();
        for i in 0..8u64 {
            tx.try_send(i).unwrap();
        }
        drop(tx);
        let mut buf = Vec::new();
        let (count, _) = rx.recv_batch(&mut buf, 8);
        assert_eq!(count, 8);
        assert_eq!(buf, (0..8u64).collect::<Vec<_>>());
        // dc comes at the next recv_batch when the buffer is empty and tx is closed
        let (count2, dc) = rx.recv_batch(&mut buf, 8);
        assert_eq!(count2, 0);
        assert!(dc);
    }

    #[test]
    fn recv_batch_partial() {
        let (tx, rx) = mpmc_bounded::<u64, 16>();
        for i in 0..8u64 {
            tx.try_send(i).unwrap();
        }
        let mut buf = Vec::new();
        let (count, dc) = rx.recv_batch(&mut buf, 4);
        assert_eq!(count, 4);
        assert!(!dc);
        assert_eq!(buf, vec![0, 1, 2, 3]);
    }

    #[test]
    fn recv_batch_max_zero() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        tx.try_send(1).unwrap();
        let mut buf = Vec::new();
        let (count, dc) = rx.recv_batch(&mut buf, 0);
        assert_eq!(count, 0);
        assert!(!dc);
        assert!(buf.is_empty());
    }

    // Drop safety

    #[test]
    fn drop_receiver_notifies_sender() {
        let (tx, rx) = mpmc_bounded::<u64, 4>();
        // Filling the buffer
        for i in 0..4u64 {
            tx.try_send(i).unwrap();
        }
        drop(rx);
        // Sender should get Disconnected on next try
        assert!(tx.try_send(99).is_err());
    }

    #[test]
    fn drop_sender_unblocks_recv() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        let c = thread::spawn(move || rx.recv());
        thread::sleep(Duration::from_millis(1));
        drop(tx);
        assert_eq!(c.join().unwrap(), Err(RecvError::Disconnected));
    }

    #[test]
    fn multiple_receivers_clone() {
        let (tx, rx1) = mpmc_bounded::<u64, 8>();
        let rx2 = rx1.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        // Both clones see the channel
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    // Single receiver

    #[test]
    fn single_try_recv_empty() {
        let (_tx, rx) = scsp_bounded::<u64, 8>(); // _tx is alive -the channel is open
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn single_recv_basic_1p1c() {
        let (tx, rx) = scsp_bounded::<u64, 8>();
        let p = thread::spawn(move || {
            for i in 0..4u64 {
                tx.send(i).unwrap();
            }
        });
        let mut vals = Vec::new();
        for _ in 0..4 {
            vals.push(rx.recv().unwrap());
        }
        p.join().unwrap();
        assert_eq!(vals, vec![0, 1, 2, 3]);
    }

    #[test]
    fn single_recv_disconnected() {
        let (tx, rx) = scsp_bounded::<u64, 8>();
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn single_drop_receiver_signals_sender() {
        let (tx, rx) = scsp_bounded::<u64, 8>();
        drop(rx);
        assert!(tx.try_send(1).is_err());
    }

    #[test]
    fn single_ordering_strict() {
        const N: u64 = 8;
        let (tx, rx) = scsp_bounded::<u64, 16>();
        let p = thread::spawn(move || {
            for i in 0..N {
                tx.send(i).unwrap();
            }
        });
        let mut last = 0u64;
        for _ in 0..N {
            let v = rx.recv().unwrap();
            assert!(v >= last, "order violated: {v} < {last}");
            last = v;
        }
        p.join().unwrap();
    }

    // Async recv
    // Async tests do not run under Miri (no tokio),
    // but are compiled for type checking.

    #[cfg(not(miri))]
    #[tokio::test]
    async fn recv_async_basic() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        tx.send_async(42).await.unwrap();
        assert_eq!(rx.recv_async().await, Ok(42));
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn recv_async_disconnected() {
        let (tx, rx) = mpmc_bounded::<u64, 8>();
        drop(tx);
        assert_eq!(rx.recv_async().await, Err(AsyncRecvError::Disconnected));
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn recv_batch_async_basic() {
        let (tx, rx) = mpmc_bounded::<u64, 16>();
        for i in 0..8u64 {
            tx.try_send(i).unwrap();
        }
        drop(tx);
        let mut buf = Vec::new();
        let (count, _) = rx.recv_batch_async(&mut buf, 16).await;
        assert_eq!(count, 8);
        assert_eq!(buf, (0..8u64).collect::<Vec<_>>());
        // dc comes on the next call when the buffer is empty
        let (count2, dc) = rx.recv_batch_async(&mut buf, 16).await;
        assert_eq!(count2, 0);
        assert!(dc);
    }
}

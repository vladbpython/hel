// Loom tests for the Vyukov ring
#![cfg(all(test, loom))]

use super::core::SeqInner;
use super::traits::InnerChannel;
use loom::thread;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const CAP: usize = 2;

#[test]
fn algo_ring_1p1c_wraparound_exact() {
    loom::model(|| {
        let ch: Arc<SeqInner<usize, 2>> = SeqInner::new();
        let prod = {
            let ch = ch.clone();
            thread::spawn(move || {
                for v in 1..=3usize {
                    let mut item = v;
                    loop {
                        match ch.push(item) {
                            Ok(()) => break,
                            Err(back) => {
                                item = back;
                                thread::yield_now();
                            }
                        }
                    }
                }
            })
        };
        let mut got = Vec::new();
        while got.len() < 3 {
            match ch.pop() {
                Some(v) => got.push(v),
                None => thread::yield_now(),
            }
        }
        prod.join().unwrap();
        assert_eq!(got, vec![1, 2, 3], "SPSC order broken through wrap around");
        assert!(ch.pop().is_none());
    });
}

#[test]
fn algo_ring_2p1c_no_loss_no_dup() {
    loom::model(|| {
        let ch: Arc<SeqInner<usize, CAP>> = SeqInner::new();

        let mut handles = Vec::new();
        for p in 0..2usize {
            let ch = ch.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1usize {
                    let v = p * 10 + i;
                    let mut item = v;
                    // bounded retry: full ring -> yield so the consumer runs
                    loop {
                        match ch.push(item) {
                            Ok(()) => break,
                            Err(back) => {
                                item = back;
                                thread::yield_now();
                            }
                        }
                    }
                }
            }));
        }

        let mut got = Vec::new();
        while got.len() < 2 {
            match ch.pop() {
                Some(v) => got.push(v),
                None => thread::yield_now(),
            }
        }
        for h in handles {
            h.join().unwrap();
        }

        got.sort_unstable();
        assert_eq!(got, vec![0, 10], "loss or duplication in the ring");
        assert!(ch.pop().is_none(), "phantom element after drain");
    });
}

#[test]
fn algo_ring_push_batch_vs_pop() {
    loom::model(|| {
        let ch: Arc<SeqInner<usize, CAP>> = SeqInner::new();

        let prod = {
            let ch = ch.clone();
            thread::spawn(move || {
                let mut buf = vec![1usize, 2];
                let mut pushed = 0;
                while pushed < 2 {
                    pushed += ch.push_batch(&mut buf);
                    if pushed < 2 {
                        thread::yield_now();
                    }
                }
            })
        };

        let mut got = Vec::new();
        while got.len() < 2 {
            match ch.pop() {
                Some(v) => {
                    // published values only an unpublished slot would
                    // yield garbage/uninit, caught by the exact set check
                    got.push(v);
                }
                None => thread::yield_now(),
            }
        }
        prod.join().unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2], "batch publication visible out of order");
    });
}

#[test]
fn algo_ring_close_drain_no_lost_element() {
    loom::model(|| {
        let ch: Arc<SeqInner<usize, CAP>> = SeqInner::new();
        // register one sender and one receiver, as Sender/Receiver do
        ch.sender_add(Ordering::Relaxed);
        ch.receiver_add(Ordering::Relaxed);

        let prod = {
            let ch = ch.clone();
            thread::spawn(move || {
                ch.push(7usize).unwrap();
                // sender drops: close tx + notify, as Sender::drop does
                ch.sender_sub(Ordering::AcqRel);
                ch.tx_close();
                ch.notify_all_on_tx_close();
            })
        };

        let mut got = None;
        loop {
            match ch.pop() {
                Some(v) => {
                    got = Some(v);
                    break;
                }
                None => {
                    if ch.is_tx_closed() && ch.is_empty() {
                        break; // what try_recv_batch reports as dc=true
                    }
                    thread::yield_now();
                }
            }
        }
        prod.join().unwrap();
        assert_eq!(
            got,
            Some(7),
            "element lost on shutdown: closed + empty observed before publication"
        );
    });
}

struct Tracked(usize, Arc<AtomicUsize>);
impl Drop for Tracked {
    fn drop(&mut self) {
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn algo_ring_abort_seal_vs_pop() {
    loom::model(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let mk = |v: usize| Tracked(v, drops.clone());

        let ch: Arc<SeqInner<Tracked, CAP>> = SeqInner::new();
        assert!(ch.push(mk(1)).is_ok());
        assert!(ch.push(mk(2)).is_ok()); // full

        let prod = {
            let ch = ch.clone();
            let drops = drops.clone();
            thread::spawn(move || {
                match ch.push_fetch_add(Tracked(3, drops)) {
                    Ok(()) => true,   // slot freed in time, published
                    Err(_v) => false, // aborted: value returned, drops here
                }
            })
        };

        // consumer: one pop (item 1 or 2 is definitely there), then close
        let got = loop {
            if let Some(v) = ch.pop() {
                break v.0;
            }
            thread::yield_now();
        };
        assert!(got == 1 || got == 2, "popped a value never pushed");
        ch.rx_close();
        ch.notify_all_on_rx_close();

        let published = prod.join().unwrap();
        drop(ch);

        // total accounting: 3 constructed, all must be dropped exactly once
        assert_eq!(
            drops.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "value lost or double dropped (published = {published}, popped = {got})"
        );
    });
}

#[test]
fn algo_ring_two_aborts_over_reservation() {
    loom::model(|| {
        let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ch: Arc<SeqInner<Tracked, CAP>> = SeqInner::new();
        assert!(ch.push(Tracked(1, drops.clone())).is_ok());
        assert!(ch.push(Tracked(2, drops.clone())).is_ok()); // full

        let prod = {
            let ch = ch.clone();
            let drops = drops.clone();
            thread::spawn(move || ch.push_fetch_add(Tracked(3, drops)).is_err())
        };

        ch.rx_close();
        ch.notify_all_on_rx_close();
        assert!(
            prod.join().unwrap(),
            "blocked producer must abort with Err after rx_close"
        );

        // over reserved (tail = head + CAP + 1): batch must bail out, not spin
        let mut buf = vec![Tracked(9, drops.clone())];
        assert_eq!(
            ch.push_batch(&mut buf),
            0,
            "push_batch must not spin on over reservation"
        );
        drop(buf);
        drop(ch);
        // 1,2 reclaimed by Drop; 3 returned via Err; 9 dropped with buf
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 4);
    });
}

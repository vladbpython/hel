pub mod errors;
pub mod mpmc;
pub mod spsc;

#[cfg(all(test, not(miri), not(loom)))]
mod double_registration {
    use super::*;
    use mpmc::sender_round_robin::round_robin;
    use std::{sync::mpsc as std_mpsc, thread, time::Duration};

    /// Two receivers park on the same shard, two items arrive. Both must wake.
    #[test]
    fn two_parked_receivers_two_items_both_wake() {
        let (done_tx, done_rx) = std_mpsc::channel::<Vec<u64>>();
        thread::spawn(move || {
            let (tx, rx) = round_robin::<u64, 64>(1);
            let r = rx.into_receivers().pop().unwrap();
            let consumers: Vec<_> = (0..2)
                .map(|_| {
                    let r = r.clone();
                    thread::spawn(move || r.recv().unwrap())
                })
                .collect();
            // let both consumers pass the spin and yield phases and actually park.
            thread::sleep(Duration::from_millis(200));
            tx.try_send(1).unwrap();
            tx.try_send(2).unwrap();

            let mut got: Vec<u64> = consumers.into_iter().map(|c| c.join().unwrap()).collect();
            got.sort_unstable();
            let _ = done_tx.send(got);
        });

        let got = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a receiver was left parked: the notify landed on a duplicate node");
        assert_eq!(
            got,
            vec![1, 2],
            "both parked receivers must get one item each"
        );
    }
}

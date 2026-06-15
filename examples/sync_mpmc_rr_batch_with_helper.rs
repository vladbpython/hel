//sync round_robin + batch via drain_batch helper.
//Consumer uses drain_batch (drain→check rule is hardwired), producer
//sends in batches. Order: join producer → drop tx → join consumers.
use hel::{
    channel::{
        mpmc::round_robin,
        nearest_power_of_two,
    }, 
    helper::batch::drain_batch
};
use std::thread;

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(256);
const TOTAL: u64 = 100_000;

fn main() {
    let (tx, rx) = round_robin::<u64, CAPACITY>(4);

    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .map(|r| {
            thread::spawn(move || {
                //drain_batch: receiver type is inferred, closure recv calls recv_batch,
                //drain→check rule inside. handler summarizes the elements.
                drain_batch(
                    BATCH,
                    move |buf: &mut Vec<u64>, m| r.recv_batch(buf, m),
                    |v: u64, acc: &mut u64| *acc += v,
                    0u64,
                )
            })
        })
        .collect();

    let producer = {
        let tx = tx.clone();
        thread::spawn(move || {
            let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
            for i in 0..TOTAL {
                buf.push(i);
                if buf.len() == BATCH {
                    tx.send_batch(&mut buf).unwrap();
                }
            }
            if !buf.is_empty() {
                tx.send_batch(&mut buf).unwrap();
            }
        })
    };

    producer.join().unwrap();
    drop(tx);
    let total: u64 = consumers.into_iter().map(|h| h.join().unwrap()).sum();

    let expected = (0..TOTAL).sum::<u64>();
    println!("batch total = {total} (expected {expected})");
    assert_eq!(total, expected, "lost or duplicated data");
    println!("OK");
}

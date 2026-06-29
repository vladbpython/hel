// ShardGroup + keyed batch: sending the ENTIRE batch via drain_batch_async_sink.
// EXPLICIT grouping (by sector). Each element carries a character  keyed method
// orts into groups via key_fn. Consumer receives the entire batch and sends
// in one call (like socket.write_all). The sector producer sends packs of his
// symbol using the keyed method (all elements of one key → one shard).
// T = (String, u64).
use hel::{
    channel::{
        mpmc::{ShardGroupCase, shard_group},
        nearest_power_of_two,
    },
    helper::batch::drain_batch_async_sink,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};
use tokio::runtime::Builder;

const BATCH: usize = 64;
const CAPACITY: usize = nearest_power_of_two(1024);
const PER_PRODUCER: u64 = 100_000;

// channel element: (symbol, payload).
type Tick = (String, u64);

async fn send_over_network(batch: &[Tick], writes: &AtomicU64, bytes: &AtomicU64) {
    tokio::task::yield_now().await;
    bytes.fetch_add(batch.len() as u64 * 8, Relaxed);
    writes.fetch_add(1, Relaxed);
}

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();

    rt.block_on(async {
        let (tx, rx) = shard_group::<Tick, CAPACITY>(ShardGroupCase::Groups {
            groups: &[
                &["AAPL", "MSFT", "GOOG", "ORCL", "INTC", "AMD", "NVDA"],
                &["TSLA", "UBER", "LYFT"],
                &["BTC", "ETH"],
                &["META", "SNAP", "NFLX", "AMZN"],
            ],
        });

        let writes = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let items = Arc::new(AtomicU64::new(0));

        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                let writes = writes.clone();
                let bytes = bytes.clone();
                let items = items.clone();
                tokio::spawn(async move {
                    let total = drain_batch_async_sink(
                        r,
                        BATCH,
                        |rx, mut buf, max| async move {
                            let (n, dc) = rx.recv_batch_async(&mut buf, max).await;
                            (rx, buf, n, dc)
                        },
                        |batch: Vec<Tick>, mut acc: u64| {
                            let writes = writes.clone();
                            let bytes = bytes.clone();
                            async move {
                                send_over_network(&batch, &writes, &bytes).await;
                                acc += batch.len() as u64;
                                let mut b = batch;
                                b.clear();
                                (b, acc)
                            }
                        },
                        0u64,
                    )
                    .await;
                    println!("[network group shard {id}] sent {total} items");
                    items.fetch_add(total, Relaxed);
                })
            })
            .collect();

        let sectors = ["AAPL", "TSLA", "BTC", "META"];

        let producers: Vec<_> = sectors
            .iter()
            .map(|&sym| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    debug_assert!(tx.shard_for(sym).is_some(), "symbol must be registered");
                    let mut buf: Vec<Tick> = Vec::with_capacity(BATCH);
                    for i in 0..PER_PRODUCER {
                        buf.push((sym.to_string(), i));
                        if buf.len() == BATCH {
                            tx.send_batch_async(&mut buf, |(s, _)| s.as_str())
                                .await
                                .unwrap();
                        }
                    }
                    if !buf.is_empty() {
                        tx.send_batch_async(&mut buf, |(s, _)| s.as_str())
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in producers {
            h.await.unwrap();
        }
        drop(tx);
        for h in consumers {
            h.await.unwrap();
        }

        let total = items.load(Relaxed);
        let w = writes.load(Relaxed);
        let expected = sectors.len() as u64 * PER_PRODUCER;
        println!("items = {total} (expected {expected})");
        println!("network writes = {w}  (instead of {} per item)", total);
        println!("batching factor = {}x", total / w.max(1));
        println!("bytes sent = {}", bytes.load(Relaxed));
        assert_eq!(total, expected, "lost items");
    });
}

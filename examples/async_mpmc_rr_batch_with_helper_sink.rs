//round_robin + batch: sending the WHOLE batch via drain_batch_async_sink.
//Unlike element by element processing (drain_batch_async), here the consumer
//receives the entire array of batch ownership and sends it with one call
//like socket.write_all(&serialize(&batch)) on a real system.
use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
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

// Simulation of a network receiver (TcpStream /WebSocket).
async fn send_over_network(batch: &[u64], writes: &AtomicU64, bytes: &AtomicU64) {
    tokio::task::yield_now().await; // I/O point
    bytes.fetch_add(batch.len() as u64 * 8, Relaxed); // 8 bytes per u64 read batch
    writes.fetch_add(1, Relaxed);
}

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = round_robin::<u64, CAPACITY>(4);
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
                        |batch: Vec<u64>, mut acc: u64| {
                            let writes = writes.clone();
                            let bytes = bytes.clone();
                            async move {
                                send_over_network(&batch, &writes, &bytes).await; // ENTIRE batch
                                acc += batch.len() as u64;
                                let mut b = batch;
                                b.clear(); // return allocation
                                (b, acc) // (Vec, acc)
                            }
                        },
                        0u64, // init
                    )
                    .await;

                    println!("[batch shard {id}] sent {total} items");
                    items.fetch_add(total, Relaxed);
                })
            })
            .collect();

        let producers: Vec<_> = (0..4u64)
            .map(|_| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
                    for i in 0..PER_PRODUCER {
                        buf.push(i);
                        if buf.len() == BATCH {
                            tx.send_batch_async(&mut buf).await.unwrap();
                        }
                    }
                    if !buf.is_empty() {
                        tx.send_batch_async(&mut buf).await.unwrap();
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
        println!("items = {total} (expected {})", 4 * PER_PRODUCER);
        println!("network writes = {w}  (instead of {} per item)", total);
        println!("batching factor = {}x", total / w.max(1));
        println!("bytes sent = {}", bytes.load(Relaxed));
        assert_eq!(total, 4 * PER_PRODUCER, "lost items");
    });
}

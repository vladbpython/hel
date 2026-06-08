use hel::channel::spsc::shard_spsc;
use tokio::runtime::Builder;
const BATCH: usize = 64;
const CAPACITY: usize = 1024;

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    rt.block_on(async {
        let ch = shard_spsc::<u64, CAPACITY>(4);

        let handles: Vec<_> = ch
            .into_pairs()
            .map(|(shard_id, tx, rx)| {
                let consumer = tokio::spawn(async move {
                    let mut total = 0u64;
                    let mut buf = Vec::with_capacity(BATCH);
                    loop {
                        let (n, dc) = rx.recv_batch_async(&mut buf, BATCH).await;
                        for v in buf.drain(..n) {
                            total += v;
                        }
                        if dc {
                            break;
                        }
                    }
                    println!("[spsc shard {shard_id}] batch total = {total}");
                });
                let producer = tokio::spawn(async move {
                    let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
                    for i in 0..100_000u64 {
                        buf.push(i);
                        if buf.len() == BATCH {
                            tx.send_batch_async(&mut buf).await;
                        }
                    }
                    if !buf.is_empty() {
                        tx.send_batch_async(&mut buf).await;
                    }
                });
                (producer, consumer)
            })
            .collect();

        for (p, c) in handles {
            p.await.unwrap();
            c.await.unwrap();
        }
    });
}

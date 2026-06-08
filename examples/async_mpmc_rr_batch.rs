use hel::channel::mpmc::round_robin;
use tokio::runtime::Builder;
const BATCH: usize = 64;
const CAPACITY: usize = 1024;

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = round_robin::<u64, CAPACITY>(4);
        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                tokio::spawn(async move {
                    let mut total = 0u64;
                    let mut buf = Vec::with_capacity(BATCH);
                    loop {
                        let (n, dc) = r.recv_batch_async(&mut buf, BATCH).await;
                        for v in buf.drain(..n) {
                            total += v;
                        }
                        if dc {
                            break;
                        }
                    }
                    println!("[batch shard {id}] total = {total}");
                })
            })
            .collect();

        let producers: Vec<_> = (0..4)
            .map(|_| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
                    for i in 0..100_000u64 {
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

        drop(tx);
        for h in producers {
            h.await.unwrap();
        }
        for h in consumers {
            h.await.unwrap();
        }
    });
}

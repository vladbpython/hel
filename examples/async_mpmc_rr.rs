use hel::channel::{mpmc::round_robin, nearest_power_of_two};
use tokio::runtime::Builder;
const CAPACITY: usize = nearest_power_of_two(256);

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
                    while let Ok(v) = r.recv_async().await {
                        total += v;
                    }
                    println!("[rr shard {id}] total = {total}");
                })
            })
            .collect();

        let producers: Vec<_> = (0..8)
            .map(|_| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..1000u64 {
                        tx.send_async(i).await.unwrap();
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

use hel::channel::{
    errors::*, 
    nearest_power_of_two,
    spsc::shard_spsc
};
use tokio::runtime::Builder;
const CAPACITY: usize = nearest_power_of_two(256);

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
                    loop {
                        match rx.recv_async().await {
                            Ok(v) => total += v,
                            Err(AsyncRecvError::Disconnected) => break,
                        }
                    }
                    println!("[spsc shard {shard_id}] total = {total}");
                });
                let producer = tokio::spawn(async move {
                    for i in 0..10_000u64 {
                        tx.send_async(i).await.unwrap();
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

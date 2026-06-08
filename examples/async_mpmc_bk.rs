use hel::channel::{errors::*, mpmc::shard_key};
use tokio::runtime::Builder;
const CAPACITY: usize = 256;

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = shard_key::<u64, CAPACITY>(8);

        let symbols = [
            "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC",
            "ETH", "ORCL", "UBER", "LYFT", "SNAP",
        ];

        let consumers: Vec<_> = rx
            .into_receivers()
            .into_iter()
            .enumerate()
            .map(|(id, r)| {
                tokio::spawn(async move {
                    let mut count = 0u64;
                    loop {
                        match r.recv_async().await {
                            Ok(_) => count += 1,
                            Err(AsyncRecvError::Disconnected) => break,
                        }
                    }
                    println!("[key shard {id}] messages = {count}");
                })
            })
            .collect();

        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..1000u64 {
                        let sym = symbols[(p * 250 + i as usize) % symbols.len()];
                        tx.send_async(sym, i).await.unwrap();
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

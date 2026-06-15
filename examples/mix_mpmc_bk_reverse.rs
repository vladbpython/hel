use hel::channel::{
    errors::*, 
    nearest_power_of_two,
    mpmc::shard_key,
    
};
use std::thread;
use tokio::runtime::Builder;

const CAPACITY: usize = nearest_power_of_two(256);

// Async producers → by-key routing → sync consumers.
// Real world: async WebSocket feed → sync DB writers per symbol.

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let (tx, rx) = shard_key::<String, CAPACITY>(4);
    let symbols = [
        "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC",
        "ETH", "ORCL", "UBER", "LYFT", "SNAP",
    ];

    // Sync consumers — blocking, e.g. DB writer per symbol shard
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            thread::spawn(move || {
                let mut count = 0usize;
                loop {
                    match r.recv() {
                        Ok(msg) => {
                            count += 1;
                            // simulate blocking DB write
                            let _ = msg;
                        }
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                println!("[sync key consumer {id}] wrote {count} records");
            })
        })
        .collect();

    // Async producers WebSocket feed simulation
    rt.block_on(async {
        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..5_000u64 {
                        let sym = symbols[(p * 5000 + i as usize) % symbols.len()];
                        let payload = format!("{sym}:{i}");
                        tx.send_async(sym, payload).await.unwrap();
                    }
                })
            })
            .collect();

        drop(tx);
        for h in producers {
            h.await.unwrap();
        }
    });

    for h in consumers {
        h.join().unwrap();
    }
}

// shard_key + PerItem (async): symbol FIFO + async sink (I/O via await).
use hel::{
    channel::{mpmc::shard_key, nearest_power_of_two},
    pool::{
        async_pool_slot,
        handler::PerItem,
        instance::Config,
        traits::{AsyncJoinHandle, AsyncRuntime},
    },
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::runtime::Builder;

const CAP: usize = nearest_power_of_two(1024);
const SYMBOLS: [&str; 16] = [
    "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC", "ETH",
    "ORCL", "UBER", "LYFT", "SNAP",
];

// TokioRuntime adapter
#[derive(Clone)]
struct TokioRuntime;
impl AsyncRuntime for TokioRuntime {
    type JoinHandle = TokioJoinHandle;
    fn spawn<F>(&self, fut: F) -> TokioJoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        TokioJoinHandle(tokio::spawn(fut))
    }
    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(dur)
    }
}
struct TokioJoinHandle(tokio::task::JoinHandle<()>);
impl AsyncJoinHandle for TokioJoinHandle {
    async fn join(self) {
        let _ = self.0.await;
    }
}

fn main() {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = shard_key::<u64, CAP>(8);
        let sum = Arc::new(AtomicU64::new(0));
        let s = sum.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(1, 8),
            rx.into_receivers(),
            PerItem(move |v: &u64| {
                let s = s.clone();
                let v = *v;
                async move {
                    s.fetch_add(v, Relaxed);
                }
            }),
            |_poison, _panic_info| {},
        );

        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..1000u64 {
                        let sym = SYMBOLS[(p * 250 + i as usize) % SYMBOLS.len()];
                        tx.send_async(sym, i).await.unwrap();
                    }
                })
            })
            .collect();

        for h in producers {
            h.await.unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        println!("key_async_per_item: sum = {}", sum.load(Relaxed));
    });
}

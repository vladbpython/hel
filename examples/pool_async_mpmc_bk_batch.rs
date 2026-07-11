// shard_key + Batch (async): пачка символа + async bulk (возврат Vec для reuse).
use hel::{
    channel::{mpmc::shard_key, nearest_power_of_two},
    pool::{
        async_pool,
        handler::Batch,
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
        let pool = async_pool(
            TokioRuntime,
            Config::new(2, 8).batch_size(64),
            rx.into_receivers(),
            Batch(move |batch: &[u64]| {
                let s = s.clone();
                let part: u64 = batch.iter().sum();
                async move {
                    s.fetch_add(part, Relaxed);
                }
            }),
        );

        let producers: Vec<_> = (0..4)
            .map(|p| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..1000u64 {
                        let sym = SYMBOLS[(p * 250 + i as usize) % SYMBOLS.len()];
                        tx.send_async(sym, i).await.unwrap(); // async send
                    }
                })
            })
            .collect();

        // join задач через .await (не блокирует worker-поток)
        for h in producers {
            h.await.unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        println!("key_async_batch: sum = {}", sum.load(Relaxed));
    });
}

// round_robin + PerItem (async): uniform distribution, one element at a time.
use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
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
        let (tx, rx) = round_robin::<u64, CAP>(4);
        let sum = Arc::new(AtomicU64::new(0));

        let s = sum.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(1, 4),
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

        for i in 0..10_000u64 {
            tx.send_async(i).await.unwrap();
        }
        drop(tx);
        pool.wait_stopping().await;
        println!("rr_async_per_item: sum = {}", sum.load(Relaxed));
    });
}

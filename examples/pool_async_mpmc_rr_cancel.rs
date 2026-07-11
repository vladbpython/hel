// round_robin + PerItem (async): uniform distribution, one element at a time.
use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    pool::{
        async_pool,
        handler::PerItem,
        instance::Config,
        traits::{AsyncJoinHandle, AsyncRuntime},
    },
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

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
        let processed = Arc::new(AtomicU64::new(0));

        let p = processed.clone();
        let pool = async_pool(
            TokioRuntime,
            Config::new(1, 4),
            rx.into_receivers(),
            PerItem(move |v: u64| {
                let p = p.clone();
                async move {
                    tokio::time::sleep(Duration::from_micros(5)).await;
                    let _ = v;
                    p.fetch_add(1, Relaxed);
                }
            }),
        );

        let token = CancellationToken::new();

        // BRIDGE: token.cancelled() -> pool.get_signal_stop().stop()
        // stops pool WORKERS; producers will stop their own select!
        let stop = pool.get_singal_stop();
        let token_bridge = token.clone();
        tokio::spawn(async move {
            token_bridge.cancelled().await;
            stop.stop();
        });

        // producers: select!(token vs send) respond to cancellation immediately
        const TOTAL: u64 = 100_000;
        let producers: Vec<_> = (0..8)
            .map(|pi| {
                let tx = tx.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    for i in 0..(TOTAL / 8) {
                        tokio::select! {
                            biased; // cancellation takes precedence over send (deterministic)
                            _ = token.cancelled() => break, // cancellation -> producer exits
                            r = tx.send_async(pi * (TOTAL / 8) + i) => {
                                if r.is_err() { break; } // Disconnected
                            }
                        }
                    }
                })
            })
            .collect();

        // stop after 20ms (simulate external signal)
        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel(); // -> producers (select) + pool workers (bridge -> stop) stop

        for h in producers {
            let _ = h.await;
        }
        drop(tx);
        pool.wait_stopping().await;
        println!(
            "STOP pool (forced): {} of {TOTAL} processed (remainder skipped)",
            processed.load(Relaxed)
        );
    });
}

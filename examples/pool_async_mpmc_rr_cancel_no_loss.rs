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
const PER_PRODUCER: u64 = 100_000;

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
        let token = CancellationToken::new();

        let sent = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));

        // token only stops producers, the pool processes everything sent.
        let p = processed.clone();
        let pool = async_pool(
            TokioRuntime,
            Config::new(1, 4),
            rx.into_receivers(),
            PerItem(move |v: u64| {
                let p = p.clone();
                async move {
                    let _ = v;
                    p.fetch_add(1, Relaxed);
                }
            }),
        );

        //  PRODUCERS: biased select! — cancel stops sending immediately.
        //  What has already been sent (i increments) is NOT lost. tx clone is dropped at the end of each task.
        let producers: Vec<_> = (0..8)
            .map(|_pi| {
                let tx = tx.clone();
                let token_p = token.clone();
                let sent = sent.clone();
                tokio::spawn(async move {
                    let mut i = 0u64;
                    while i < PER_PRODUCER {
                        tokio::select! {
                            biased; // cancellation takes priority over send
                            _ = token_p.cancelled() => break, // stop sending
                            r = tx.send_async(i) => {
                                if r.is_err() { break; } // Disconnected
                                i += 1; // We count ONLY what is actually sent
                            }
                        }
                    }
                    sent.fetch_add(i, Relaxed); // how much did THIS producer send?
                })
            })
            .collect();

        // cancel after 50ms imitation of an external stop signal
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel(); // signal to producers: stop sending NEW

        // We are waiting for the producers to come out (their tx clones are dropping)
        for h in producers {
            h.await.unwrap();
        }
        // main tx drops -> all senders are gone -> pool auto drains the remainder
        drop(tx);

        // wait_stopping waits for complete drainage (does not throw the remainder).
        pool.wait_stopping().await;

        let s = sent.load(Relaxed);
        let recvd = processed.load(Relaxed);
        println!(
            "sent={s} processed={recvd} lost={}",
            s as i64 - recvd as i64
        );
        assert_eq!(s, recvd, "graceful shutdown: zero-loss broken");
        println!(
            "OK: zero-loss (cancellation stopped SENDING, the pool processed EVERYTHING sent)"
        );
    });
}

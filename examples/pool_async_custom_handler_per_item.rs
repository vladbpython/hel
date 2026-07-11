//! DbSink PerItem (struct handler) — processing one tick at a time.
//! Trait ONE: AsyncHandler<Vec<T> -> Vec<T>>. Per item logic is done INSIDE
//! via batch.drain(..) like a per item wrapper, but struct gives &self.shared
//! Some ticks go into the database, some don’t -> unsuccessful ones go into the failed buffer for analysis.

use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    pool::{
        async_pool,
        instance::Config,
        traits::{AsyncHandler, AsyncJoinHandle, AsyncRuntime},
    },
};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Builder;

const CAP: usize = nearest_power_of_two(1024);

#[derive(Clone, Debug)]
struct Tick {
    id: u64,
    price: f64,
}

struct Shared {
    inserted: AtomicU64,
    failed: Mutex<Vec<Tick>>,
}

// insert ONE tick: true = entered, false = rejected (double/constained).
async fn insert_one(tick: &Tick, inserted: &AtomicU64) -> bool {
    tokio::task::yield_now().await; // imitation await DB
    if tick.id % 2 == 0 {
        false // even ones "didn't come in"
    } else {
        inserted.fetch_add(1, Relaxed);
        true
    }
}

// Struct handler PerItem: trait Vec<T> -> Vec<T>, per item through drain
struct DbSink {
    shared: Arc<Shared>,
}

impl AsyncHandler<Tick> for DbSink {
    // The trait gives Vec (pool owns), return cleared for reuse.
    // We process INSIDE ONE AT A TIME via drain (like a per item wrapper).
    // &self.shared - WITHOUT Arc clone on batch (future holds &self via await).
    async fn handle(&self, mut batch: Vec<Tick>) -> Vec<Tick> {
        for tick in batch.drain(..) {
            //drain: processes and empties; owned tick via await
            if !insert_one(&tick, &self.shared.inserted).await {
                // did not log in -> failed (owned tick is moved; short lock without await)
                self.shared.failed.lock().unwrap().push(tick);
            }
        }
        // batch is empty (drain has emptied), capacity is saved -> reuse
        batch
    }
}

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
        let (tx, rx) = round_robin::<Tick, CAP>(4);

        // Arc<Shared> - ONE clone when creating a pool (struct handler holds &self,
        // there is NO clone on batch, unlike PerItem closure)
        let shared = Arc::new(Shared {
            inserted: AtomicU64::new(0),
            failed: Mutex::new(Vec::new()),
        });

        let pool = async_pool(
            TokioRuntime,
            Config::new(1, 4).batch_size(64),
            rx.into_receivers(),
            DbSink {
                shared: shared.clone(),
            }, // struct handler
        );

        const TOTAL: u64 = 10_000;
        let producers: Vec<_> = (0..4)
            .map(|pi| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..(TOTAL / 4) {
                        let id = pi * (TOTAL / 4) + i;
                        tx.send_async(Tick {
                            id,
                            price: id as f64 * 0.1,
                        })
                        .await
                        .unwrap();
                    }
                })
            })
            .collect();

        for h in producers {
            h.await.unwrap();
        }

        drop(tx);
        pool.wait_stopping().await;

        // after shutdown: unsuccessful ones collected for analysis
        let inserted = shared.inserted.load(Relaxed);
        let failed = shared.failed.lock().unwrap();
        println!("entered the database: {inserted}");
        println!("failed (failed): {}", failed.len());
        println!("total: {}", inserted + failed.len() as u64);

        if !failed.is_empty() {
            println!("\nexamples of unsuccessful ones (for analysis):");
            for tick in failed.iter().take(5) {
                println!("id={} price={:.1}", tick.id, tick.price);
            }
        }

        // zero loss: accepted + failed == sent
        assert_eq!(inserted + failed.len() as u64, TOTAL, "data loss");
        println!("OK: zero loss (passed + failed == {TOTAL})");
    });
}

// DbSink - struct handler for AsyncHandler:
// handle(Vec<T>) -> Future<Vec<T>> (owned + RETURN cleared Vec for reuse)
// Contract: return batch.clear() empty (len=0, capacity saved) -> zero alloc.
// Partial failure: some entered the database, some did not -> unsuccessful ones into the failed buffer.

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

// insert into the database: returns FAILED elements (empty = everything is in).
async fn insert_db(batch: &[Tick], inserted: &AtomicU64) -> Vec<Tick> {
    tokio::task::yield_now().await;
    let mut failed = Vec::new();
    for tick in batch {
        if tick.id % 2 == 0 {
            failed.push(tick.clone()); // even ones "didn't come in"
        } else {
            inserted.fetch_add(1, Relaxed);
        }
    }
    failed
}

// Struct handler: handle(Vec<T>) -> Vec<T> (RETURN cleared for reuse)
struct DbSink {
    shared: Arc<Shared>,
}

impl AsyncHandler<Tick> for DbSink {
    // Returns Future<Vec<Tick>> - cleared Vec (same allocation) for reuse.
    fn handle(&self, mut batch: Vec<Tick>) -> impl Future<Output = Vec<Tick>> + Send {
        let shared = self.shared.clone();
        async move {
            // insert takes &batch (we own batch, the slice is alive via await)
            let not_inserted = insert_db(&batch, &shared.inserted).await;
            if !not_inserted.is_empty() {
                shared.failed.lock().unwrap().extend(not_inserted);
            }
            // CONTRACT: return batch CLEARED (len=0, capacity saved -> reuse).
            // clear() does NOT deallocate, capacity remains, pool reuses.
            batch.clear();
            batch
        }
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
            },
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

        let inserted = shared.inserted.load(Relaxed);
        let failed = shared.failed.lock().unwrap();
        println!("entered the database: {inserted}");
        println!("failed: {}", failed.len());
        println!("total: {}", inserted + failed.len() as u64);

        if !failed.is_empty() {
            println!("\nexamples of unsuccessful ones (for analysis):");
            for tick in failed.iter().take(5) {
                println!("id={} price={:.1}", tick.id, tick.price);
            }
        }

        assert_eq!(inserted + failed.len() as u64, TOTAL, "data loss");
        println!("OK: zero loss (passed + failed == {TOTAL})");
    });
}

// - insert attempt runs by reference (no take) -> a panic anywhere in the DB call leaves the tick in the slot -> dead-letter, zero loss;
// - on rejection the handler take()s the tick to move it into the failed buffer — an explicit ownership transfer, visible in code.

use hel::{
    channel::{mpmc::round_robin, nearest_power_of_two},
    helper::panic::PanicReason,
    pool::{
        async_pool_slot,
        instance::Config,
        traits::{AsyncJoinHandle, AsyncRuntime, AsyncSlotHandler},
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

// insert ONE tick: true = inserted, false = rejected (dup/constraint).
async fn insert_one(tick: &Tick, inserted: &AtomicU64) -> bool {
    tokio::task::yield_now().await; // imitation of an awaited DB call
    if tick.id % 2 == 0 {
        false // even ids are "rejected"
    } else {
        inserted.fetch_add(1, Relaxed);
        true
    }
}

struct DbSink {
    shared: Arc<Shared>,
}

impl AsyncSlotHandler<Tick> for DbSink {
    async fn handle(&self, slot: &mut Option<Tick>) {
        // By reference: the db call happens while the worker still owns the tick.
        // A panic here (driver bug, serialization error) is recoverable: the tick goes to the dead-letter sink.
        let accepted = match slot.as_ref() {
            Some(tick) => insert_one(tick, &self.shared.inserted).await,
            None => return,
        };
        // explicit take() only where we must keep the tick:
        if !accepted {
            if let Some(tick) = slot.take() {
                // short lock, no await while holding it
                self.shared.failed.lock().unwrap().push(tick);
            }
        }
        // accepted: no take needed the worker clears the slot.
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

        // Dead letter sink completes the zero loss accounting: ticks whose
        // handler panicked (as opposed to cleanly rejected) land here with the cause attached.
        let dl_shared = shared.clone();
        let pool = async_pool_slot(
            TokioRuntime,
            Config::new(1, 4).batch_size(64),
            rx.into_receivers(),
            DbSink {
                shared: shared.clone(),
            },
            move |poison: Tick, panic_info: PanicReason| {
                eprintln!("dead letter: id={} panic_info={panic_info:?}", poison.id);
                dl_shared.failed.lock().unwrap().push(poison);
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
        println!("OK: zero loss (inserted + failed == {TOTAL})");
    });
}

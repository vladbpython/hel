use super::{
    instance::State,
    signal::Stop,
    traits::{AsyncJoinHandle, AsyncRuntime},
};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    thread::JoinHandle,
};

pub type WorkerHandles<H> = Arc<Mutex<Vec<H>>>;

pub(crate) fn lock_workers<H>(m: &Mutex<Vec<H>>) -> MutexGuard<'_, Vec<H>> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct SyncPool {
    state: Arc<State>,
    workers: WorkerHandles<JoinHandle<()>>,
}

impl SyncPool {
    pub fn new(state: Arc<State>, workers: WorkerHandles<JoinHandle<()>>) -> Self {
        Self { state, workers }
    }

    pub fn processed(&self) -> u64 {
        self.state.processed()
    }

    pub fn handler_panics(&self) -> u64 {
        self.state.handler_panics()
    }

    pub fn active(&self) -> usize {
        self.state.active()
    }

    pub fn max_active(&self) -> usize {
        self.state.max_active()
    }

    pub fn worker_handles(&self) -> usize {
        lock_workers(&self.workers).len()
    }

    pub fn get_signal_stop(&self) -> Stop {
        Stop::new(self.state.clone())
    }

    fn join_all(&self) {
        loop {
            let batch: Vec<JoinHandle<()>> = lock_workers(&self.workers).drain(..).collect();
            if batch.is_empty() {
                break;
            }
            for w in batch {
                let _ = w.join();
            }
        }
    }

    pub fn wait_stopping(self) {
        self.join_all();
    }

    /// Stop and wait for ALL workers to complete. Consumes the pool.
    /// join() parks the thread on the OS (zero CPU) and gives happens before: after
    /// return all worker records (processed, user) are visible.
    pub fn stop_and_wait(self) {
        self.state.stop();
        self.join_all();
    }
}

/// RAII: dropping the handle must not leak the spawned threads.
/// Before this Drop, a plain `drop(pool)` detached the workers forever:
/// they kept spinning, kept the receivers alive (so senders never saw a close),
/// and nothing could reach them any more — no handle, no signal.
///  Measured: an item sent after the drop was still processed by a leaked worker.
impl Drop for SyncPool {
    fn drop(&mut self) {
        self.state.stop();
        loop {
            let batch: Vec<JoinHandle<()>> = lock_workers(&self.workers).drain(..).collect();
            if batch.is_empty() {
                break;
            }
            for w in batch {
                let _ = w.join();
            }
        }
    }
}

pub struct AsyncPool<AR: AsyncRuntime> {
    state: Arc<State>,
    workers: WorkerHandles<AR::JoinHandle>,
}

impl<AR: AsyncRuntime> AsyncPool<AR> {
    pub fn new(state: Arc<State>, workers: WorkerHandles<AR::JoinHandle>) -> Self {
        Self { state, workers }
    }

    pub fn processed(&self) -> u64 {
        self.state.processed()
    }

    pub fn handler_panics(&self) -> u64 {
        self.state.handler_panics()
    }

    pub fn active(&self) -> usize {
        self.state.active()
    }

    pub fn max_active(&self) -> usize {
        self.state.max_active()
    }

    pub fn worker_handles(&self) -> usize {
        lock_workers(&self.workers).len()
    }

    pub fn get_signal_stop(&self) -> Stop {
        Stop::new(self.state.clone())
    }

    /// Monitor may lazily spawn between two drains,
    /// but it is in the store itself,
    /// so after it has been awaited nothing pushes any more and the loop terminates.
    async fn join_all(&self) {
        loop {
            let batch: Vec<AR::JoinHandle> = lock_workers(&self.workers).drain(..).collect();
            if batch.is_empty() {
                break;
            }
            for h in batch {
                let _ = h.join().await;
            }
        }
    }

    pub async fn wait_stopping(self) {
        self.join_all().await;
    }

    /// Stop and wait for all workers through a real join (await handles).
    pub async fn stop_and_wait(self) {
        self.state.stop();
        self.join_all().await;
    }
}

/// RAII: dropping the handle must stop the workers.
/// Drop cannot `.await`, so no join here, stop flag alone is enough:
/// every worker task re-checks it after each pass and each idle sleep, sees it and returns, releasing the receivers.
/// Without this plain `drop(pool)` left the tasks running forever with no way to reach them.
impl<AR: AsyncRuntime> Drop for AsyncPool<AR> {
    fn drop(&mut self) {
        self.state.stop();
    }
}

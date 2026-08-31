use super::{
    instance::State,
    signal::Stop,
    stats,
    traits::{AsyncJoinHandle, AsyncRuntime},
};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex, MutexGuard},
    thread::JoinHandle,
};

pub type WorkerHandles<H> = Arc<Mutex<Vec<H>>>;

pub(crate) fn lock_workers<H>(m: &Mutex<Vec<H>>) -> MutexGuard<'_, Vec<H>> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Handle to a running sync pool.
/// Dropping the handle stops the pool and join worker threads,
/// so `drop(pool)` blocks until every worker exits its loop - wedged handler  wedges the drop with it.
/// Batch already in flight is always finished (zero loss), batch: up to `batch_size` handler calls per worker.
/// so the drop waits at least the remainder of the current.
/// Use
/// [`Self::wait_stopping`] or [`Self::stop_and_wait`] when you want that wait to be explicit,
/// and beware of handlers that block on something the dropping thread itself must release - that is a deadlock.
pub struct SyncPool {
    state: Arc<State>,
    workers: WorkerHandles<JoinHandle<()>>,
}

impl SyncPool {
    pub(crate) fn new(state: Arc<State>, workers: WorkerHandles<JoinHandle<()>>) -> Self {
        Self { state, workers }
    }

    pub fn processed(&self) -> u64 {
        self.state.processed()
    }

    pub fn shards(&self) -> usize {
        self.state.shards()
    }

    pub fn shard_queued(&self, shard: usize) -> Option<usize> {
        if shard < self.state.shards() && self.depths_live() {
            Some(self.state.depth(shard))
        } else {
            None
        }
    }

    pub fn depths(&self) -> Option<Vec<usize>> {
        if !self.depths_live() {
            return None;
        }
        Some(
            (0..self.state.shards())
                .map(|s| self.state.depth(s))
                .collect(),
        )
    }

    fn depths_live(&self) -> bool {
        !self.state.channels_closed()
    }

    pub fn takeovers(&self) -> u64 {
        self.state.takeovers()
    }

    pub fn stats(&self) -> stats::Pool {
        stats::snapshot(&self.state)
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

    pub fn is_stopped(&self) -> bool {
        self.state.is_stopped()
    }

    /// Stop without joining: sets the stop flag and abandons the handles.
    /// Live workers exit within one idle cycle on their own; a worker stuck forever inside a handler stays stuck,
    /// its thread abandoned to the OS - exactly what tokio's `shutdown_timeout(0)` does with blocking tasks.
    /// This is the shutdown path that can never hang;
    /// prefer [`Self::stop_and_wait`] when the handlers are known to terminate.
    pub fn stop_and_detach(self) {
        self.state.stop();
        lock_workers(&self.workers).clear();
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

/// Handle to a running async pool.
/// Dropping the handle only sets the stop flag and gives no synchronous guarantee (`Drop` cannot `.await`):
/// task notices the flag between items and idle cycles, but a handler already inside `handler.handle(..).await` runs to completion,
/// holding the receivers alive until its task exits — an item sent right after `drop(pool)` may still be processed.
/// join use [`Self::wait_stopping`] or [`Self::stop_and_wait`].
pub struct AsyncPool<AR: AsyncRuntime> {
    state: Arc<State>,
    workers: WorkerHandles<AR::JoinHandle>,
}

impl<AR: AsyncRuntime> AsyncPool<AR> {
    pub(crate) fn new(state: Arc<State>, workers: WorkerHandles<AR::JoinHandle>) -> Self {
        Self { state, workers }
    }

    pub fn processed(&self) -> u64 {
        self.state.processed()
    }

    pub fn shards(&self) -> usize {
        self.state.shards()
    }

    pub fn shard_queued(&self, shard: usize) -> Option<usize> {
        if shard < self.state.shards() && self.depths_live() {
            Some(self.state.depth(shard))
        } else {
            None
        }
    }

    pub fn depths(&self) -> Option<Vec<usize>> {
        if !self.depths_live() {
            return None;
        }
        Some(
            (0..self.state.shards())
                .map(|s| self.state.depth(s))
                .collect(),
        )
    }

    fn depths_live(&self) -> bool {
        !self.state.channels_closed()
    }

    pub fn takeovers(&self) -> u64 {
        self.state.takeovers()
    }

    pub fn stats(&self) -> stats::Pool {
        stats::snapshot(&self.state)
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

    pub fn is_stopped(&self) -> bool {
        self.state.is_stopped()
    }

    /// Stop without awaiting: sets the stop flag and abandons the handles (dropping a runtime's JoinHandle detaches the task).
    /// Path that  can never hang; prefer [`Self::stop_and_wait`] when the handlers are known to terminate.
    pub fn stop_and_detach(self) {
        self.state.stop();
        lock_workers(&self.workers).clear();
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

/// RAII: dropping the handle must stop the workers. Drop cannot `.await`,
/// so no join here and only the stop flag. A worker rechecks it between items and idle sleeps,
/// but a handler already in flight finishes first, so this is a signal, not a synchronous guarantee.
/// Without this a plain `drop(pool)` left the tasks running forever with no way to reach them.
impl<AR: AsyncRuntime> Drop for AsyncPool<AR> {
    fn drop(&mut self) {
        self.state.stop();
    }
}

impl Debug for SyncPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncPool").finish_non_exhaustive()
    }
}
impl<AR: AsyncRuntime> Debug for AsyncPool<AR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncPool").finish_non_exhaustive()
    }
}

use super::{
    instance::State,
    signal::Stop,
    traits::{AsyncJoinHandle, AsyncRuntime},
};
use std::{sync::Arc, thread::JoinHandle};

pub struct SyncPool {
    state: Arc<State>,
    workers: Vec<JoinHandle<()>>,
}

impl SyncPool {
    pub fn new(state: Arc<State>, workers: Vec<JoinHandle<()>>) -> Self {
        Self { state, workers }
    }

    pub fn processed(&self) -> u64 {
        self.state.processed()
    }
    pub fn active(&self) -> usize {
        self.state.active()
    }

    pub fn get_singal_stop(&self) -> Stop {
        Stop::new(self.state.clone())
    }

    pub fn wait_stopping(self) {
        for w in self.workers {
            let _ = w.join();
        }
    }

    /// Stop and wait for ALL workers to complete. Consumes the pool.
    /// join() parks the thread on the OS (zero CPU) and gives happens before: after
    /// return all worker records (processed, user) are visible.
    pub fn stop_and_wait(self) {
        self.state.stop();
        for w in self.workers {
            let _ = w.join();
        }
    }
}

pub struct AsyncPool<AR: AsyncRuntime> {
    state: Arc<State>,
    workers: Vec<AR::JoinHandle>,
}

impl<AR: AsyncRuntime> AsyncPool<AR> {
    pub fn new(state: Arc<State>, workers: Vec<AR::JoinHandle>) -> Self {
        Self { state, workers }
    }

    pub fn processed(&self) -> u64 {
        self.state.processed()
    }
    pub fn active(&self) -> usize {
        self.state.active()
    }

    pub fn get_singal_stop(&self) -> Stop {
        Stop::new(self.state.clone())
    }

    pub async fn wait_stopping(self) {
        for w in self.workers {
            let _ = w.join().await;
        }
    }

    /// Stop and wait for all workers through a real join (await handles).
    pub async fn stop_and_wait(self) {
        self.state.stop();
        for h in self.workers {
            let _ = h.join().await;
        }
    }
}

use std::time::Duration;

pub trait AsyncHandler<T>: Send + Sync + 'static {
    /// Processes batch, RETURNS cleared Vec (same allocation) for reuse.
    /// Contract: return batch.clear() empty (len=0, capacity saved).
    fn handle(&self, batch: Vec<T>) -> impl Future<Output = Vec<T>> + Send;
}

pub trait SyncHandler<T>: Send + Sync + 'static {
    fn handle(&self, batch: &mut Vec<T>, n: usize);
}

/// Handle of the spawned task, which can be waited for.
pub trait AsyncJoinHandle: Send + 'static {
    /// Wait for the task to complete. Completion error (panic/cancel) is ignored
    /// at the pool level, the worker should not panic normally.
    fn join(self) -> impl Future<Output = ()> + Send;
}

/// Minimal runtime hook so the async pool stays runtime-agnostic. Implement for tokio / async-std / your executor.
pub trait AsyncRuntime: Clone + Send + Sync + 'static {
    /// The type of handle returned by spawn. One for each runtime.
    type JoinHandle: AsyncJoinHandle;

    fn spawn<F>(&self, fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;

    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send;
}

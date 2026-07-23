use std::time::Duration;

/// Contract:
///  - panic before `take()`: the item is still in the worker's slot and
///    is delivered to the dead letter sink, zero loss;
///  - panic after `take()`: the handler explicitly accepted ownership,
///    the item's fate is the handler's responsibility (counted, not lost silently by the pool).
pub trait AsyncSlotHandler<T>: Send + Sync + 'static {
    fn handle(&self, slot: &mut Option<T>) -> impl Future<Output = ()> + Send;
}

/// The worker owns the item; the handler `take()`s only at its commit point.
/// Panic before take -> item is dead-lettered (zero loss);
/// panic after take -> consumed by contract, counted.
pub trait SyncSlotHandler<T>: Send + Sync + 'static {
    fn handle(&self, slot: &mut Option<T>);
}
/// Handle of the spawned task, which can be waited for.
pub trait AsyncJoinHandle: Send + 'static {
    /// Wait for the task to complete. Completion error (panic/cancel) is ignored
    /// at the pool level, the worker should not panic normally.
    fn join(self) -> impl Future<Output = ()> + Send;
}

/// Minimal runtime hook so the async pool stays runtime-agnostic. Implement for tokio / smol / async-std / your executor.
pub trait AsyncRuntime: Clone + Send + Sync + 'static {
    /// The type of handle returned by spawn. One for each runtime.
    type JoinHandle: AsyncJoinHandle;

    fn spawn<F>(&self, fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;

    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send;
}

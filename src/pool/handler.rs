use super::traits::{AsyncHandler, SyncHandler};
pub struct Batch<S>(pub S);
pub struct PerItem<S>(pub S);

/// Async handler: process the whole batch in one call —> `Fn(Vec<T>) -> Fut`.
impl<T, F, Fut> AsyncHandler<T> for Batch<F>
where
    T: Send + 'static,
    F: Fn(&[T]) -> Fut + Send + Sync + 'static, // &[T] handler does NOT own, only reads
    Fut: Future<Output = ()> + Send,
{
    async fn handle(&self, mut batch: Vec<T>) -> Vec<T> {
        (self.0)(&batch).await; // process the cut
        batch.clear(); // clear (len=0, capacity remains)
        batch // reuse
    }
}

/// Sync handler: process the whole slice in one call —> `Fn(&[T])`
impl<T, S> SyncHandler<T> for Batch<S>
where
    T: Send + 'static,
    S: Fn(&[T]) + Send + Sync + 'static,
{
    fn handle(&self, batch: &mut Vec<T>, n: usize) {
        (self.0)(&batch[..n]);
        batch.drain(..n);
    }
}

/// Async handler: process one element at a time —> `Fn(T) -> Fut`.
impl<T, F, Fut> AsyncHandler<T> for PerItem<F>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    async fn handle(&self, mut batch: Vec<T>) -> Vec<T> {
        for item in batch.drain(..) {
            // drain: processes and empties (len=0)
            (self.0)(item).await;
        }
        batch // capacity is saved (drain does not affect allocation) -> reuse
    }
}

/// Sync handler: process one element at a time -> `Fn(&T)`.
impl<T, S> SyncHandler<T> for PerItem<S>
where
    T: Send + 'static,
    S: Fn(&T) + Send + Sync + 'static,
{
    fn handle(&self, batch: &mut Vec<T>, n: usize) {
        for item in &batch[..n] {
            (self.0)(item);
        }
        batch.drain(..n);
    }
}

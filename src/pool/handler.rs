use super::traits::{AsyncSlotHandler, SyncSlotHandler};
pub struct PerItem<S>(pub S);

impl<T, F, Fut> AsyncSlotHandler<T> for PerItem<F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    async fn handle(&self, slot: &mut Option<T>) {
        if let Some(v) = slot.as_ref() {
            (self.0)(v).await;
        }
    }
}

impl<T, F> SyncSlotHandler<T> for PerItem<F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    fn handle(&self, slot: &mut Option<T>) {
        if let Some(v) = slot.as_ref() {
            (self.0)(v);
        }
    }
}

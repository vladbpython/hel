use super::marker::{WORKER_CANCELLED, WorkerCancelled};
use std::{any::Any, fmt::Debug};
pub struct PanicReason(pub Box<dyn Any + Send + 'static>);

impl PanicReason {
    pub fn cancelled() -> Self {
        Self(Box::new(WorkerCancelled))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is::<WorkerCancelled>()
    }

    /// Readable panic message when the payload is a string which covers `panic!("...")` and `panic!("{x}")`,
    /// i.e. the vast majority of panics.
    /// `None` for non string payloads.
    pub fn message(&self) -> Option<&str> {
        if self.is_cancelled() {
            return Some(WORKER_CANCELLED);
        }
        self.0
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
    }

    /// The raw payload for custom downcasting.
    pub fn into_inner(self) -> Box<dyn Any + Send + 'static> {
        self.0
    }
}

impl Debug for PanicReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.message() {
            Some(m) => write!(f, "PanicReason({m:?})"),
            None => write!(f, "PanicReason(<non-string payload>)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_panic_with_same_text_is_not_a_cancellation() {
        let test = PanicReason::cancelled();
        assert!(test.is_cancelled());
        assert_eq!(test.message(), Some(WORKER_CANCELLED));
    }
}

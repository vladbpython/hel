use std::{any::Any, fmt::Debug};
pub struct PanicReason(pub Box<dyn Any + Send + 'static>);

impl PanicReason {
    /// Readable panic message when the payload is a string which covers `panic!("...")` and `panic!("{x}")`,
    /// i.e. the vast majority of panics.
    /// `None` for non string payloads.
    pub fn message(&self) -> Option<&str> {
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

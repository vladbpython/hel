use super::instance::State;
use std::sync::Arc;
// StopSignal
#[derive(Clone)]
pub struct Stop {
    state: Arc<State>,
}

impl Stop {
    pub fn new(state: Arc<State>) -> Self {
        Self { state }
    }

    pub fn stop(&self) {
        self.state.stop();
    }
}

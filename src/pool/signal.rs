use super::instance::State;
use std::{fmt::Debug, sync::Arc};
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

impl Debug for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stop").finish_non_exhaustive()
    }
}

use std::{error::Error as StdError, fmt::Display};

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    StallTakeoverNeedsSpareWorker {
        /// The effective ceiling: `max_consumers` after the shard count cap.
        effective_max: usize,
    },
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StallTakeoverNeedsSpareWorker { effective_max } => write!(
                f,
                "stall_takeover needs at least two effective workers, but \
                 max_consumers is {effective_max} after the shard-count cap: \
                 with one worker nobody can take over a stalled owner's shards"
            ),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    Config(ConfigError),
    ReceiverEmpty,
}

impl Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::ReceiverEmpty => write!(
                f,
                "pool needs at least one shard receiver: with zero shards autodrain never fires and wait_stopping() hangs forever"
            ),
        }
    }
}

impl StdError for ConfigError {}
impl StdError for PoolError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Config(err) => Some(err),
            Self::ReceiverEmpty => None,
        }
    }
}

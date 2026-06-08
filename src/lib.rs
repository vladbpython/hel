pub(crate) mod cache;
pub(crate) mod internal_channel;
pub mod macros;
pub(crate) mod shard;

pub mod channel {
    pub use super::internal_channel::{errors, nearest_power_of_two};
    pub use super::shard::{mpmc, spsc};

    pub mod error {
        pub use super::{super::shard::errors::*, errors::*};
    }
}

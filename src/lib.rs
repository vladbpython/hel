pub(crate) mod cache;
pub mod helper;
pub(crate) mod internal_channel;
pub mod macros;
pub(crate) mod shard;

pub mod channel {
    pub use super::internal_channel::{errors, nearest_power_of_two};
    pub mod mpmc {
        pub use super::super::shard::mpmc::{
            receiver::ShardReceiver, sender_group::*, sender_key::*, sender_round_robin::*,
        };
    }

    pub use super::shard::spsc;

    pub mod error {
        pub use super::{super::shard::errors::*, errors::*};
    }
}

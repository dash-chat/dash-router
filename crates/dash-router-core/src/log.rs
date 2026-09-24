//! A log identity with a *channel*: the half shared across authors, which
//! is what a subscription names. Subscribing to a channel means every
//! author's log under it, now and in the future. For the integer ids used
//! by tests, models and the sim the channel is the id itself, so a
//! subscription to `7` is exactly a subscription to log `7`.
//!
//! Vocabulary follows p2panda: a log is single-author, and the thing one
//! log id names across authors is a channel. At the Dash Chat boundary
//! `Channel` is p2panda's `LogId`.

use polestar::prelude::Id;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Log: Id {
    type Channel: Id;
    fn channel(&self) -> Self::Channel;
}

macro_rules! self_channeled {
    ($($t:ty),* $(,)?) => {
        $(impl Log for $t {
            type Channel = Self;
            fn channel(&self) -> Self { *self }
        })*
    };
}
self_channeled!(u8, u16, u32, u64, usize);

/// The bounded ids polestar models and the core's own model tests use for
/// `L`: self-channeled like the integers, so model-checked scenarios keep
/// their single-author meaning.
impl<const N: usize, const WRAP: bool> Log for polestar::id::UpTo<N, WRAP>
where
    Self: Id,
{
    type Channel = Self;
    fn channel(&self) -> Self {
        *self
    }
}

/// `Log` plus the serde bounds the wire needs on both halves. Blanket:
/// nothing implements this by hand.
pub trait WireLog:
    Log<Channel: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}
impl<T> WireLog for T where
    T: Log<Channel: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}

/// A two-level log id for tests and examples: `(channel, author)`, the
/// shape Dash Chat's `(LogId, author)` has. Orders channel-first so a
/// channel's logs are contiguous, exactly as the relay store keys them.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Pair {
    pub channel: u8,
    pub author: u8,
}

impl Pair {
    pub const fn new(channel: u8, author: u8) -> Self {
        Self { channel, author }
    }
}

impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.channel, self.author)
    }
}

impl Log for Pair {
    type Channel = u8;
    fn channel(&self) -> u8 {
        self.channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_ids_are_their_own_channel() {
        assert_eq!(7u8.channel(), 7u8);
        assert_eq!(9u32.channel(), 9u32);
    }

    #[test]
    fn pair_channel_is_the_first_half() {
        let l = Pair {
            channel: 3,
            author: 9,
        };
        assert_eq!(l.channel(), 3);
        assert_eq!(l.to_string(), "3/9");
    }

    #[test]
    fn pair_orders_by_channel_then_author() {
        let a = Pair {
            channel: 1,
            author: 9,
        };
        let b = Pair {
            channel: 2,
            author: 0,
        };
        assert!(a < b);
    }

    #[test]
    fn wire_log_is_satisfied_by_ints_and_pair() {
        fn wire<L: WireLog>() {}
        wire::<u8>();
        wire::<u32>();
        wire::<Pair>();
    }
}

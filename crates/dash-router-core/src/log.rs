//! A log identity with a *prefix*: the part a subscription names (spec
//! 2026-09-22 §3.5). Subscribing to a prefix means every log under it, now
//! and in the future. For the integer ids used by tests, models and the
//! sim the prefix is the id itself, so a prefix subscription to `7` is
//! exactly a subscription to log `7`.

use polestar::prelude::Id;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Log: Id {
    type Prefix: Id;
    fn prefix(&self) -> Self::Prefix;
}

macro_rules! self_prefixed {
    ($($t:ty),* $(,)?) => {
        $(impl Log for $t {
            type Prefix = Self;
            fn prefix(&self) -> Self { *self }
        })*
    };
}
self_prefixed!(u8, u16, u32, u64, usize);

/// The bounded ids polestar models and the core's own model tests use for
/// `L`: self-prefixed like the integers, so model-checked scenarios keep
/// their pre-prefix meaning.
impl<const N: usize, const WRAP: bool> Log for polestar::id::UpTo<N, WRAP>
where
    Self: Id,
{
    type Prefix = Self;
    fn prefix(&self) -> Self {
        *self
    }
}

/// `Log` plus the serde bounds the wire needs on both halves. Blanket:
/// nothing implements this by hand.
pub trait WireLog:
    Log<Prefix: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}
impl<T> WireLog for T where
    T: Log<Prefix: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}

/// A two-level log id for tests and examples: `(prefix, author)`, the
/// shape Dash Chat's `(LogId, author)` has. Orders prefix-first so a
/// prefix's logs are contiguous, exactly as the relay store keys them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Pair {
    pub prefix: u8,
    pub author: u8,
}

impl Pair {
    pub const fn new(prefix: u8, author: u8) -> Self {
        Self { prefix, author }
    }
}

impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.prefix, self.author)
    }
}

impl Log for Pair {
    type Prefix = u8;
    fn prefix(&self) -> u8 {
        self.prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_ids_are_their_own_prefix() {
        assert_eq!(7u8.prefix(), 7u8);
        assert_eq!(9u32.prefix(), 9u32);
    }

    #[test]
    fn pair_prefix_is_the_first_half() {
        let l = Pair { prefix: 3, author: 9 };
        assert_eq!(l.prefix(), 3);
        assert_eq!(l.to_string(), "3/9");
    }

    #[test]
    fn pair_orders_by_prefix_then_author() {
        let a = Pair { prefix: 1, author: 9 };
        let b = Pair { prefix: 2, author: 0 };
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

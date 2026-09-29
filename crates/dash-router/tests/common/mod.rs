//! Shared fixtures for the p2panda integration tests (`panda_swarm`,
//! `panda_delay_tolerance`): both run real shells over the real transport,
//! so both want the same node config and the same op shape.

use std::time::Duration;

use dash_router::{CoreConfig, PolicyIntervals};
use dash_router_core::{Op, RouterConfig};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use rand::SeedableRng;

/// The node config both swarm tests run: seconds-scale TTLs (real sockets,
/// real mDNS) and a relay cap far above anything these tests author.
pub fn config() -> CoreConfig {
    CoreConfig {
        router: RouterConfig {
            want_ttl: Duration::from_secs(2).into(),
            have_ttl: Duration::from_secs(2).into(),
            heard_ttl: Duration::from_secs(2).into(),
        },
        relay_cap: 1 << 20,
        evict_at: 0.75,
        debounce: PushDebouncePolicy {
            window_ms: 50,
            max_latency_ms: 200,
        },
        max_wire_bytes: None,
    }
}

/// Want/Have intervals for a node in a swarm of `n`. `n` is the caller's
/// estimate of the overlay size, which the interval policy scales by, so
/// each test passes its own.
pub fn intervals(seed: u64, n: usize) -> PolicyIntervals {
    PolicyIntervals {
        want: IntervalPolicy::Fixed {
            min_ms: 500.0,
            max_ms: 1500.0,
        },
        have: IntervalPolicy::Fixed {
            min_ms: 50.0,
            max_ms: 250.0,
        },
        n,
        rng: rand::rngs::StdRng::seed_from_u64(seed),
    }
}

/// The op node `i` authors: a header naming the author and a small payload,
/// so a Have bundling many logs stays well under p2panda's 4 KiB max
/// gossip message size.
pub fn authored_op(i: usize) -> Op {
    Op {
        header: vec![i as u8],
        payload: Some(vec![i as u8; 8]),
    }
}

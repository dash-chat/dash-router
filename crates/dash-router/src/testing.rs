//! Test-only helpers, shared between this crate's unit tests and the
//! integration tests that drive `NodeCore` from outside.
//!
//! Gated behind the `testing` feature (always on for this crate's own
//! `cfg(test)` build), so nothing here ships in a normal dependency.

use std::time::Duration;

use crate::IntervalSource;

/// Deterministic intervals for tests: pops from the front, repeats the last
/// entry forever.
#[derive(Clone, Debug)]
pub struct Scripted(Vec<Duration>, usize);

impl Scripted {
    pub fn ms(script: &[u64]) -> Self {
        Scripted(
            script.iter().map(|&m| Duration::from_millis(m)).collect(),
            0,
        )
    }

    fn next(&mut self) -> Duration {
        let i = self.1.min(self.0.len() - 1);
        self.1 += 1;
        self.0[i]
    }
}

impl IntervalSource for Scripted {
    fn next_want(&mut self) -> Duration {
        self.next()
    }
    fn next_have(&mut self) -> Duration {
        self.next()
    }
}

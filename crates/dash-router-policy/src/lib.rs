//! Pure timing policies: the tuning subject, shared by the simulator and the tokio shell.
//!
//! A policy is a pure sampling function from (rng, network size estimate)
//! to a duration. The simulation feeds it the *true* network size as an
//! oracle; once the protocol grows a size estimator, the same policies
//! run on estimates and the simulator measures the degradation.

use std::time::Duration;

use rand::Rng;
use serde::{Deserialize, Serialize};

/// How a node picks the interval until its next Want or Have fire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum IntervalPolicy {
    /// Uniform in `[min_ms, max_ms]`, independent of network size.
    Fixed { min_ms: f64, max_ms: f64 },
    /// Uniform in `[min_ms, max_ms]`, scaled by `(n / ref_n)^alpha`:
    /// intervals stretch as the network grows, so the aggregate rate of
    /// fires stays roughly constant (alpha = 1) or in between (alpha < 1).
    DensityScaled {
        min_ms: f64,
        max_ms: f64,
        ref_n: usize,
        #[serde(default = "default_alpha")]
        alpha: f64,
    },
}

fn default_alpha() -> f64 {
    1.0
}

impl IntervalPolicy {
    /// Sample an interval. `n` is the (estimated) network size.
    pub fn sample(&self, rng: &mut impl Rng, n: usize) -> Duration {
        let ms = match self {
            IntervalPolicy::Fixed { min_ms, max_ms } => rng.random_range(*min_ms..=*max_ms),
            IntervalPolicy::DensityScaled {
                min_ms,
                max_ms,
                ref_n,
                alpha,
            } => {
                let base = rng.random_range(*min_ms..=*max_ms);
                base * (n as f64 / *ref_n as f64).powf(*alpha)
            }
        };
        Duration::from_secs_f64(ms.max(0.001) / 1000.0)
    }
}

/// When to flush pending pushed appends (spec §4). Pure: given the times,
/// returns the flush deadline; the shell owns the clock.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PushDebouncePolicy {
    /// Quiet window after the latest append before flushing.
    pub window_ms: u64,
    /// Hard cap after the OLDEST pending append, so a steady append
    /// stream still pushes instead of re-arming forever.
    pub max_latency_ms: u64,
}

impl PushDebouncePolicy {
    /// The moment to flush, given when the oldest still-pending append
    /// happened and when the latest one did.
    pub fn deadline(&self, oldest_pending: Duration, latest_append: Duration) -> Duration {
        let window = latest_append + Duration::from_millis(self.window_ms);
        let cap = oldest_pending + Duration::from_millis(self.max_latency_ms);
        window.min(cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn fixed_stays_in_range_and_ignores_n() {
        let p = IntervalPolicy::Fixed {
            min_ms: 100.0,
            max_ms: 200.0,
        };
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        for n in [1, 10, 1000] {
            for _ in 0..100 {
                let d = p.sample(&mut rng, n);
                assert!((0.1..=0.2).contains(&d.as_secs_f64()));
            }
        }
    }

    #[test]
    fn density_scaling_stretches_with_n() {
        let p = IntervalPolicy::DensityScaled {
            min_ms: 100.0,
            max_ms: 100.0,
            ref_n: 10,
            alpha: 1.0,
        };
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let at_10 = p.sample(&mut rng, 10);
        let at_40 = p.sample(&mut rng, 40);
        assert_eq!(at_10, Duration::from_millis(100));
        assert_eq!(at_40, Duration::from_millis(400));
    }

    #[test]
    fn debounce_extends_on_appends_but_the_max_latency_cap_wins() {
        let p = PushDebouncePolicy {
            window_ms: 100,
            max_latency_ms: 250,
        };
        let ms = Duration::from_millis;
        // One lone append: flush a window after it.
        assert_eq!(p.deadline(ms(1000), ms(1000)), ms(1100));
        // A later append extends the quiet window...
        assert_eq!(p.deadline(ms(1000), ms(1120)), ms(1220));
        // ...until the cap from the OLDEST pending append wins.
        assert_eq!(p.deadline(ms(1000), ms(1200)), ms(1250));
        assert_eq!(p.deadline(ms(1000), ms(1400)), ms(1250));
    }
}

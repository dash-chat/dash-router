//! One seeded run: a [`BehaviorModel`] stepped to completion.

use std::{sync::Arc, time::Duration};

use polestar::{StateMachine, prelude::*};

use crate::{RunRecord, SimBehavior, SimNet, SimNetState};

type Model = BehaviorModel<SimBehavior>;

/// A single seeded simulation run.
///
/// The whole run is one deterministic machine — behavior state (queue,
/// RNG, collectors) rides inside the model state — so [`Self::jump_to`]
/// works by replay from the initial state, exactly.
pub struct Simulation {
    machine: Arc<Model>,
    sm: StateMachine<Model>,
    initial: (SimBehavior, SimNetState),
    steps: usize,
    duration: Duration,
}

impl Simulation {
    pub fn new(net: SimNet, state: SimNetState, behavior: SimBehavior, duration: Duration) -> Self {
        let machine = Arc::new(BehaviorModel::new(net));
        let initial = (behavior, state);
        let sm = machine.state_machine(initial.clone());
        Self {
            machine,
            sm,
            initial,
            steps: 0,
            duration,
        }
    }

    pub fn behavior(&self) -> &SimBehavior {
        &self.sm.state().0
    }

    pub fn net_state(&self) -> &SimNetState {
        &self.sm.state().1
    }

    pub fn steps(&self) -> usize {
        self.steps
    }

    /// Process one pending event. Returns false when the run is over:
    /// nothing pending, or past the drain window with no traffic left.
    pub fn step(&mut self) -> anyhow::Result<bool> {
        let b = self.behavior();
        let Some(at) = b
            .peek_at()
            .or_else(|| (!b.is_initialized()).then_some(Duration::ZERO))
        else {
            return Ok(false);
        };
        // Appends stop at `duration`; the drain window lets in-flight
        // traffic settle and repair traffic (fires) keep running, up to a
        // hard cap for pathologies that never converge. A momentary lull
        // is not the end: as long as ops are uncovered, the periodic
        // Wants will produce more traffic, so keep going.
        if at > self.duration * 2 {
            return Ok(false);
        }
        if at > self.duration
            && !b.has_pending_traffic()
            && self.net_state().inflight.is_empty()
            && b.metrics.all_covered()
        {
            return Ok(false);
        }
        self.sm.step(())?;
        self.steps += 1;
        Ok(true)
    }

    /// Run to completion and freeze the metrics.
    pub fn run(&mut self, seed: u64) -> anyhow::Result<RunRecord> {
        while self.step()? {}
        Ok(self.behavior().metrics.finish(seed))
    }

    /// Rewind to just after step `step` by replay from the initial state.
    pub fn jump_to(&mut self, step: usize) -> anyhow::Result<()> {
        self.sm = self.machine.state_machine(self.initial.clone());
        self.steps = 0;
        for _ in 0..step {
            if !self.step()? {
                break;
            }
        }
        Ok(())
    }
}

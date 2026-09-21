//! Seeded discrete-event simulation harness for the Dash Router network
//! model: the statistical counterpart to exhaustive model checking.
//!
//! The model ([`dash_router_net_model::NetMachine`]) says what the network
//! *can* do; this crate's [`SimBehavior`] decides what it *does*, by
//! sampling every choice the checker would enumerate — delivery order,
//! latency, loss, timer intervals — from seeded distributions. The
//! behavior rides inside a [`polestar::BehaviorModel`], so a whole run is
//! one deterministic machine: replay from the initial state is exact,
//! which is what makes [`Simulation::jump_to`] free.
//!
//! The tuning question lives in [`policy`]: interval policies are pure
//! functions, separately testable, and are what the production shell will
//! eventually sample from too.

pub mod behavior;
pub mod metrics;
pub mod policy;
pub mod report;
pub mod scenario;
pub mod sim;
pub mod sweep;

use dash_router_net_model::{NetAction, NetMachine, NetState};
use polestar::time::RealTime;

/// Node identifier in simulations: real-typed, unbounded.
pub type NodeId = u32;
/// Log identifier. One log per writer, so writers are capped at 256.
pub type LogId = u8;
/// Network-wide in-flight message cap. Large: overflow pressure is
/// handled by the behavior as backpressure, not explored as a boundary.
pub const K: usize = 4096;

/// The simulated network machine.
pub type SimNet = NetMachine<NodeId, LogId, RealTime, K>;
/// Its state.
pub type SimNetState = NetState<NodeId, LogId, RealTime>;
/// Its actions.
pub type SimNetAction = NetAction<NodeId, LogId, RealTime, K>;

pub use behavior::SimBehavior;
pub use metrics::{Metrics, RunRecord};
pub use scenario::{Config, ScenarioSpec};
pub use sim::Simulation;

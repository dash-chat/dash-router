//! A bounded network of Dash Router nodes as one pure state machine.
//!
//! This is the only place broadcast, loss, duplication and reordering
//! exist. A node's `Send` effect is consumed here and expanded into one
//! in-flight [`Flight`] per neighbour of the sender in the [`Topology`];
//! every other effect passes through tagged with the node it came from.
//! Delivery order is a choice: `Deliver(i)` can pick any in-flight entry,
//! so traversal explores every reordering. `Drop(i)` is packet loss and
//! `Duplicate(i)` is packet duplication, each an explicit action.
//!
//! The in-flight set is a sorted `Vec` used as a canonical multiset (a
//! set could not hold the duplicates that `Duplicate` exists to create),
//! capped at `K`: an expansion that would overflow makes the action not
//! enabled, which is the state-space boundary on outstanding messages.
//!
//! Fairness and boundary pruning, per the report, are encoded here:
//! - at most [`NetConfig::max_consecutive_drops`] `Drop`s in a row, so a
//!   schedule cannot starve the network forever and `G F` properties are
//!   checked over fair schedules only (core's `fetch_timed` idiom already
//!   forbids `Tick` past a due timer);
//! - at most [`NetConfig::max_appends`] `Append`s in total, bounding
//!   sequence numbers and hence the state space.
//!
//! Messages must travel through the network: `Node(n, Recv(..))` is not
//! enabled, only `Deliver` feeds a `Recv` to a node.
//!
//! Time is per-node: `Node(n, Tick(..))` advances one clock, so clocks
//! drift freely and traversal explores every relative schedule. Nothing
//! in the protocol assumes synchronised clocks, and this is where that
//! assumption would be caught if it crept in.

use std::collections::BTreeMap;

use anyhow::{anyhow, ensure};
use dash_router_core::{
    Effect, MessageEnvelope, RouterAction, RouterConfig, RouterMachine, RouterState,
};
use polestar::{prelude::*, time::TimeInterval};

use crate::topology::Topology;

/// Network-level bounds. Protocol parameters live in [`RouterConfig`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetConfig {
    /// How many `Drop`s may occur without an intervening `Deliver`.
    pub max_consecutive_drops: usize,
    /// How many `Append`s may occur in total, across all nodes.
    pub max_appends: usize,
}

/// The network model: a [`RouterMachine`] per node (all sharing one
/// config), a fixed topology, and network-level bounds. `K` caps the
/// number of in-flight messages.
#[derive(Clone, Debug)]
pub struct NetMachine<N: Ord, L, T, const K: usize> {
    pub router: RouterMachine<N, L, T>,
    pub topology: Topology<N>,
    pub config: NetConfig,
}

impl<N: Ord + Copy, L, T, const K: usize> NetMachine<N, L, T, K> {
    pub fn new(router: RouterConfig<T>, topology: Topology<N>, config: NetConfig) -> Self {
        Self {
            router: RouterMachine::new(router),
            topology,
            config,
        }
    }
}

/// A message on its way to one node. The sender is in the envelope.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Flight<N, L: Ord> {
    pub to: N,
    pub envelope: MessageEnvelope<N, L>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetState<N: Ord, L: Ord, T> {
    pub nodes: BTreeMap<N, RouterState<N, L, T>>,
    /// In-flight messages, kept sorted: a canonical multiset, so state
    /// equality and hashing see past insertion order. Actions address
    /// entries by position in this order.
    pub inflight: Vec<Flight<N, L>>,
    /// Fairness counter, reset by every `Deliver`.
    pub drops_in_a_row: usize,
    /// Boundary counter, incremented by every `Append`.
    pub appends: usize,
}

impl<N: Id, L: Id, T: TimeInterval> NetState<N, L, T> {
    pub fn new(nodes: impl IntoIterator<Item = RouterState<N, L, T>>) -> Self {
        Self {
            nodes: nodes.into_iter().map(|s| (s.id, s)).collect(),
            inflight: Vec::new(),
            drops_in_a_row: 0,
            appends: 0,
        }
    }

    pub fn node(&self, id: &N) -> &RouterState<N, L, T> {
        &self.nodes[id]
    }

    fn take_flight(&mut self, i: usize) -> anyhow::Result<Flight<N, L>> {
        ensure!(i < self.inflight.len(), "no in-flight message at {i}");
        Ok(self.inflight.remove(i))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NetAction<N, L: Ord, T, const K: usize> {
    /// A node acts on its own: anything but `Recv`, which only
    /// [`NetAction::Deliver`] may cause.
    Node(N, RouterAction<N, L, T>),
    /// The in-flight message at this position arrives at its target.
    Deliver(UpTo<K>),
    /// The in-flight message at this position is lost.
    Drop(UpTo<K>),
    /// The in-flight message at this position is duplicated.
    Duplicate(UpTo<K>),
}

impl<N: Id, L: Id, T: TimeInterval, const K: usize> NetMachine<N, L, T, K> {
    /// Run one router action on one node, expanding its `Send`s into
    /// flights and passing every other effect through, tagged.
    fn apply(
        &self,
        s: &mut NetState<N, L, T>,
        fx: &mut Vec<(N, Effect<N, L>)>,
        id: N,
        action: RouterAction<N, L, T>,
    ) -> anyhow::Result<()> {
        let node = s
            .nodes
            .get(&id)
            .ok_or_else(|| anyhow!("no node {id:?}"))?
            .clone();
        let (node, node_fx) = self.router.transition(node, action)?;
        s.nodes.insert(id, node);
        for effect in node_fx {
            match effect {
                Effect::Send(envelope) => {
                    for to in self.topology.neighbors(&id) {
                        ensure!(s.inflight.len() < K, "in-flight cap {K} reached");
                        insert_sorted(
                            &mut s.inflight,
                            Flight {
                                to,
                                envelope: envelope.clone(),
                            },
                        );
                    }
                }
                other => fx.push((id, other)),
            }
        }
        Ok(())
    }
}

fn insert_sorted<T: Ord>(vec: &mut Vec<T>, value: T) {
    let at = vec.binary_search(&value).unwrap_or_else(|e| e);
    vec.insert(at, value);
}

impl<N: Id, L: Id, T: TimeInterval, const K: usize> Machine for NetMachine<N, L, T, K> {
    type State = NetState<N, L, T>;
    type Action = NetAction<N, L, T, K>;
    type Fx = Vec<(N, Effect<N, L>)>;
    type Error = anyhow::Error;

    fn transition(&self, mut s: Self::State, action: Self::Action) -> TransitionResult<Self> {
        let mut fx = vec![];
        match action {
            NetAction::Node(id, action) => {
                ensure!(
                    !matches!(action, RouterAction::Recv(_)),
                    "messages arrive via Deliver, not Node"
                );
                if matches!(action, RouterAction::Append(..)) {
                    ensure!(s.appends < self.config.max_appends, "append boundary");
                    s.appends += 1;
                }
                self.apply(&mut s, &mut fx, id, action)?;
            }

            NetAction::Deliver(i) => {
                let flight = s.take_flight(*i)?;
                s.drops_in_a_row = 0;
                self.apply(
                    &mut s,
                    &mut fx,
                    flight.to,
                    RouterAction::Recv(flight.envelope),
                )?;
            }

            NetAction::Drop(i) => {
                ensure!(
                    s.drops_in_a_row < self.config.max_consecutive_drops,
                    "unfair: too many consecutive drops"
                );
                s.take_flight(*i)?;
                s.drops_in_a_row += 1;
            }

            NetAction::Duplicate(i) => {
                ensure!(s.inflight.len() < K, "in-flight cap {K} reached");
                let i = *i;
                ensure!(i < s.inflight.len(), "no in-flight message at {i}");
                let copy = s.inflight[i].clone();
                insert_sorted(&mut s.inflight, copy);
            }
        }
        Ok((s, fx))
    }
}

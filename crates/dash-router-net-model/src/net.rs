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
//! Schedule constraints deliberately do not live here: this machine says
//! what the network *can* do, and each harness decides what it *does*.
//! - Sequence numbers are bounded per scenario, not here. Appends leave
//!   their trace in log heads, so a traversal prunes with a state
//!   predicate (every `held()` range ends below M) instead of a counter.
//! - Fairness — a schedule must not starve the network with `Drop`s
//!   forever — is imposed by wrapping in [`crate::Fair`], or expressed as
//!   an LTL fairness assumption at checking time. (Core's `fetch_timed`
//!   idiom already forbids `Tick` past a due timer.)
//!
//! Messages must travel through the network: `Node(n, Recv(..))` is not
//! enabled, only `Deliver` feeds a `Recv` to a node.
//!
//! Time is per-node: `Node(n, Tick(..))` advances one clock, so clocks
//! drift freely and traversal explores every relative schedule. Nothing
//! in the protocol assumes synchronised clocks, and this is where that
//! assumption would be caught if it crept in.

use std::{collections::BTreeMap, marker::PhantomData};

use anyhow::ensure;
use dash_router_core::{
    Effect, MessageEnvelope, RouterAction, RouterState, router::RouterStateMachine,
};
use polestar::{prelude::*, time::TimeInterval};

use crate::topology::Topology;

/// The network model: a fixed topology over nodes that each carry
/// their own router (a [`RouterStateMachine`], typically all sharing
/// one machine). `K` caps the number of in-flight messages.
#[derive(Clone, Debug)]
pub struct NetMachine<N: Ord, L, T, const K: usize> {
    pub topology: Topology<N>,
    _marker: PhantomData<(L, T)>,
}

impl<N: Ord + Copy, L, T, const K: usize> NetMachine<N, L, T, K> {
    pub fn new(topology: Topology<N>) -> Self {
        Self {
            topology,
            _marker: PhantomData,
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
pub struct NetState<N: Id, L: Id, T: TimeInterval> {
    pub nodes: BTreeMap<N, RouterStateMachine<N, L, T>>,

    /// In-flight messages, kept sorted: a canonical multiset, so state
    /// equality and hashing see past insertion order. Actions address
    /// entries by position in this order.
    pub inflight: Vec<Flight<N, L>>,
}

impl<N: Id, L: Id, T: TimeInterval> NetState<N, L, T> {
    pub fn new(nodes: impl IntoIterator<Item = RouterStateMachine<N, L, T>>) -> Self {
        Self {
            nodes: nodes.into_iter().map(|s| (s.id, s)).collect(),
            inflight: Vec::new(),
        }
    }

    pub fn node(&self, id: &N) -> &RouterState<N, L, T> {
        &self.nodes[id]
    }

    pub fn node_mut(&mut self, id: &N) -> anyhow::Result<&mut RouterStateMachine<N, L, T>> {
        self.nodes
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("no node {id:?}"))
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
    /// Run one router action on one node. `Send` effects are absorbed
    /// into in-flight state, one [`Flight`] per neighbour of the sender;
    /// everything else passes through, tagged with the node.
    fn apply(
        &self,
        s: &mut NetState<N, L, T>,
        id: N,
        action: RouterAction<N, L, T>,
    ) -> anyhow::Result<Vec<(N, Effect<N, L>)>> {
        let node_fx = s
            .nodes
            .owned_update(id, |_, node| node.transition(action))?;
        let inflight = &mut s.inflight;

        absorb_fx(node_fx, |effect| match effect {
            Effect::Send(envelope) => {
                for to in self.topology.neighbors(&id) {
                    ensure!(inflight.len() < K, "in-flight cap {K} reached");
                    insert_sorted(
                        inflight,
                        Flight {
                            to,
                            envelope: envelope.clone(),
                        },
                    );
                }
                Ok(None)
            }
            other => Ok(Some((id, other))),
        })
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
                fx.extend(self.apply(&mut s, id, action)?);
            }

            NetAction::Deliver(i) => {
                let flight = s.take_flight(*i)?;
                fx.extend(self.apply(&mut s, flight.to, RouterAction::Recv(flight.envelope))?);
            }

            NetAction::Drop(i) => {
                s.take_flight(*i)?;
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

//! The observer between the driver and the network: a [`Machine`] that
//! wraps another, records [`Metrics`] about every transition it forwards,
//! and reports the flights each one put in the air.
//!
//! Observation needs both sides of a transition. A receipt is redundant
//! only if the receiver's held ranges did not grow across it; a delivery's
//! latency and push-vs-repair bucket belong to the flight the net consumes
//! in that same transition. Only something that wraps `transition` sees
//! the state before and after without snapshotting the world, so the
//! collectors live here: the behavior makes the choices, the net defines
//! their meaning, and this layer watches the two meet.
//!
//! The net has no global clock — each node ticks its own, and a node's
//! clock lags whenever the driver defers a timer fire — so every action
//! arrives stamped with the driver's `now`, and latencies are measured in
//! that.
//!
//! What the driver itself did (appends it shed, fires it deferred, samples
//! it took) is not a transition and stays in its own
//! [`DriverMetrics`](crate::metrics::DriverMetrics).

use std::{collections::BTreeMap, time::Duration};

use anyhow::anyhow;
use dash_router_core::{LogRanges, NodeAction, NodeEffect, RouterAction, WireBody};
use dash_router_net_model::Flight;
use polestar::prelude::*;

use crate::{HaveOrigin, LogId, Metrics, NodeId, SimNetAction, SimNetFx, SimNetState};

type SimFlight = Flight<NodeId, LogId>;

/// A machine that forwards every action to `M` and meters the transition.
/// See the [module docs](self).
#[derive(Clone, Debug)]
pub struct Metered<M> {
    inner: M,
}

impl<M> Metered<M> {
    pub fn new(inner: M) -> Self {
        Self { inner }
    }

    /// The wrapped machine.
    pub fn inner(&self) -> &M {
        &self.inner
    }
}

/// The observer's state: the collectors, plus what they need to carry
/// from one transition to the next.
#[derive(Clone, Debug)]
pub struct Meter {
    pub metrics: Metrics,
    /// Mirror of the wrapped state's `inflight`, maintained incrementally
    /// so the flights a transition added can be picked out without
    /// cloning the whole multiset beforehand.
    known_inflight: Vec<SimFlight>,
    /// Push-vs-repair attribution for in-flight Have messages, keyed by
    /// flight identity: recorded when the flight is sent, taken when it is
    /// delivered or dropped, so the map stays bounded by what is in flight.
    flight_origins: BTreeMap<SimFlight, HaveOrigin>,
}

impl Meter {
    pub fn new(metrics: Metrics) -> Self {
        Self {
            metrics,
            known_inflight: Vec::new(),
            flight_origins: BTreeMap::new(),
        }
    }
}

/// What a metered transition produced: the flights it put in the air (for
/// the driver to schedule) and the node effects the net passed up.
#[derive(Clone, Debug, Default)]
pub struct MeteredFx {
    pub sent: Vec<SimFlight>,
    pub node: SimNetFx,
}

/// A `Deliver` about to happen, read off the pre-state.
struct Receipt {
    to: NodeId,
    /// A Have receipt as opposed to a Want receipt (the wire no longer
    /// marks a Have as fresh-vs-reply, so that finer split is gone).
    is_have: bool,
    /// The receiver's router-held snapshot, for the growth check after.
    held_before: LogRanges<LogId>,
}

impl<M> Machine for Metered<M>
where
    M: Machine<State = SimNetState, Action = SimNetAction, Fx = SimNetFx, Error = anyhow::Error>,
{
    type State = (SimNetState, Meter);
    type Action = (Duration, SimNetAction);
    type Fx = MeteredFx;
    type Error = anyhow::Error;

    fn transition(
        &self,
        (net, mut meter): Self::State,
        (now, action): Self::Action,
    ) -> TransitionResult<Self> {
        let Meter {
            metrics,
            known_inflight,
            flight_origins,
        } = &mut meter;

        // Before: what the action is about to consume, while it is still
        // there. `origin` is the attribution any Have this transition sends
        // will inherit: the push flood of an append, the repair of a Have
        // fire, or — for a relay — that of the Have being received.
        let mut origin = None;
        let mut receipt = None;
        let mut taken = None;
        match &action {
            SimNetAction::Deliver(i) | SimNetAction::Drop(i) => {
                let i = **i;
                let flight = net
                    .inflight
                    .get(i)
                    .ok_or_else(|| anyhow!("no in-flight message at {i}"))?;
                origin = flight_origins.remove(flight);
                taken = Some(i);
                if matches!(action, SimNetAction::Deliver(_)) {
                    metrics.receives += 1;
                    receipt = Some(Receipt {
                        to: flight.to,
                        is_have: matches!(flight.message.body, WireBody::Have(_)),
                        held_before: net.node(&flight.to).router.held.clone(),
                    });
                } else {
                    metrics.drops += 1;
                }
            }
            SimNetAction::Node(n, node_action) => match node_action {
                NodeAction::Authored(log, seq, _) => {
                    metrics.authored(*log, *seq, now);
                    origin = Some(HaveOrigin::Push);
                }
                NodeAction::Router(RouterAction::FireHave) => origin = Some(HaveOrigin::Repair),
                NodeAction::NativeSync(log, seq, _) => {
                    metrics.native_syncs += 1;
                    // Out-of-band arrival still counts as this node having
                    // the op: no `NodeEffect::Deliver` fires for a NativeSync.
                    metrics.delivered(*n, *log, *seq, now, None);
                }
                NodeAction::RelayEvictPayloads(candidates) => {
                    // Payloads-first (DESIGN.md GC): units freed = one per payload.
                    let freed: usize = candidates.iter().filter_map(|(_, r)| r.len()).sum();
                    metrics.payload_evictions += freed as u64;
                }
                NodeAction::RelayEvict(_) => metrics.full_evictions += 1,
                NodeAction::AppGc(_) => metrics.app_gc_runs += 1,
                _ => {}
            },
            SimNetAction::Duplicate(_) => {}
        }

        let (net, node_fx) = self.inner.transition(net, action)?;

        // After: what the receipt taught.
        if let Some(receipt) = receipt {
            let mut taught = false;
            for (node, effect) in &node_fx {
                if let NodeEffect::Deliver(log, seq) = effect {
                    taught = true;
                    metrics.delivered(*node, *log, *seq, now, origin);
                }
            }
            // "Taught nothing" also covers state growth with no subscriber
            // delivery: e.g. relay-only ingest of an unsubscribed log's
            // bytes. `Deliver` only fires for novel subscribed data, so the
            // held-ranges comparison is the only way to see that.
            if !taught {
                let held_after = &net.node(&receipt.to).router.held;
                taught = !held_after.difference(&receipt.held_before).is_empty();
            }
            if !taught {
                metrics.redundant_receives += 1;
                if receipt.is_have {
                    metrics.duplicate_replies += 1;
                }
            } else if receipt.is_have {
                metrics.backfill_receives += 1;
            }
        }

        // And what went out.
        let sent = sync_inflight(known_inflight, taken, &net.inflight);
        for flight in &sent {
            match &flight.message.body {
                WireBody::Want { .. } => metrics.want_msgs += 1,
                WireBody::Have(_) => {
                    metrics.have_msgs += 1;
                    if let Some(o) = origin {
                        flight_origins.entry(flight.clone()).or_insert(o);
                    }
                }
            }
        }

        Ok((
            (net, meter),
            MeteredFx {
                sent,
                node: node_fx,
            },
        ))
    }
}

/// Bring `known` (the in-flight multiset before the transition) up to
/// `after`, given the position the transition removed, and return the
/// flights it added.
fn sync_inflight(
    known: &mut Vec<SimFlight>,
    removed: Option<usize>,
    after: &[SimFlight],
) -> Vec<SimFlight> {
    if let Some(i) = removed {
        known.remove(i);
    }
    // Sorted-multiset difference: after \ known.
    let mut new = Vec::new();
    let mut old = known.iter().peekable();
    for f in after {
        loop {
            match old.peek() {
                // Known entry no longer present: cannot happen, the one
                // removal is already applied — but stay a proper multiset
                // difference regardless.
                Some(o) if *o < f => {
                    old.next();
                }
                // Matched: consumes one known copy per present copy.
                Some(o) if *o == f => {
                    old.next();
                    break;
                }
                // Nothing known at or below f: it is new.
                _ => {
                    new.push(f.clone());
                    break;
                }
            }
        }
    }
    for f in &new {
        let at = known.binary_search(f).unwrap_or_else(|e| e);
        known.insert(at, f.clone());
    }
    debug_assert_eq!(known.as_slice(), after, "in-flight mirror drifted");
    new
}

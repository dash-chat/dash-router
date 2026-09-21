//! The glue routing table: composes the storage-less [`RouterMachine`] with
//! the [`RelayStoreMachine`]/[`ExtStoreMachine`] pair into a single node.
//!
//! Deviation from the spec's sketch: [`NodeState`] holds plain sub-states
//! (`RouterState`, `RelayStoreState`, `ExtStoreState`), not nested
//! `StateMachine`s. `NodeMachine` holds the three sub-machine *configs*
//! itself and calls their `transition`s directly from its own `transition`.
//! This keeps `NodeState` a plain, triple-`Arc`-free value while preserving
//! the same semantics as composing three independently steppable machines.
//!
//! `NodeAction::Recv` is the only path by which wire bytes enter a node:
//! `RouterAction::RecvWant`/`RecvHave` are disabled via `NodeAction::Router`
//! since routing a raw wire message (parking bytes, then reconciling) is the
//! node's business, not the router's.
//!
//! Second deviation: the `*_step` helpers advance each sub-state by cloning
//! it rather than `std::mem::take`. `RouterState` has no `Default` (it needs
//! an `id`), and deriving `Default` for the storage states in an `L`-generic
//! context here would force an `L: Default` bound onto every `Machine`
//! method (the derive macro adds that bound unconditionally, even though
//! `OpsMap`'s `BTreeMap` doesn't need it). Cloning keeps the bound off the
//! hot path; correctness is identical since the clone is immediately
//! discarded on write-back.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::ensure;
use polestar::prelude::*;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Effect, EvictableStorage, LogRanges, Op, Ranges, RelayStoreAction, RelayStoreMachine,
    RelayStoreState, RouterAction, RouterConfig, RouterMachine, RouterState, Seq, Storage, Units,
    WireBody, WireMessage,
    storage::{ExtStoreAction, ExtStoreMachine, ExtStoreState},
};

/// Composes the router with the relay/ext stores.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeMachine<N, L, T> {
    pub router: RouterMachine<N, L, T>,
    pub relay: RelayStoreMachine<L>,
    pub ext: ExtStoreMachine<L>,
}

impl<N, L: Default, T> NodeMachine<N, L, T> {
    pub fn new(config: RouterConfig<T>, relay_cap: Units) -> Self {
        Self {
            router: RouterMachine::new(config),
            relay: RelayStoreMachine::new(relay_cap),
            ext: ExtStoreMachine::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeState<N: Ord, L: Ord, T> {
    pub router: RouterState<N, L, T>,
    pub relay: RelayStoreState<L>,
    pub ext: ExtStoreState<L>,
    pub subscriptions: BTreeSet<L>,
}

impl<N: Id, L: Id, T: polestar::time::TimeInterval> NodeState<N, L, T> {
    /// held snapshot = relay ∪ ext ∪ empty keys for subscriptions (§5: a
    /// subscription with nothing stored yet is still "known" and wants
    /// everything).
    pub fn held_union(&self) -> LogRanges<L> {
        let mut out = self.relay.0.held_all().union(&self.ext.0.held_all());
        for log in &self.subscriptions {
            if out.get(log).is_none() {
                out.insert(*log, Ranges::empty());
            }
        }
        out
    }

    /// Whether either storage side currently holds this exact `(log, seq)`.
    pub fn holds(&self, log: &L, seq: Seq) -> bool {
        self.ext.0.held_all().contains(log, seq) || self.relay.0.held_all().contains(log, seq)
    }

    /// See [`eviction_candidates`].
    pub fn eviction_candidates(&self) -> LogRanges<L> {
        eviction_candidates(&self.relay.0.held_payloads(), &self.router.others_wants())
    }
}

impl<N: Id, L: Id + Default, T: polestar::time::TimeInterval> NodeState<N, L, T> {
    /// A fresh node with empty stores, subscribed to `subscriptions`; the
    /// router starts with the resulting held snapshot.
    pub fn new(id: N, subscriptions: impl IntoIterator<Item = L>) -> Self {
        let mut s = Self {
            router: RouterState::new(id, LogRanges::empty()),
            relay: RelayStoreState::default(),
            ext: ExtStoreState::default(),
            subscriptions: subscriptions.into_iter().collect(),
        };
        s.router.held = s.held_union();
        s
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NodeAction<N, L: Ord, T> {
    /// Any router action except receiving: those go through `Recv`, since
    /// parking wire bytes is the node's business, not the router's.
    Router(RouterAction<N, L, T>),
    /// The only receive path: a wire message from a peer. Bytes carried by a
    /// `Have` are parked here before the router ever sees them.
    Recv(WireMessage<N, L>),
    /// Locally authored data: ingest to `ext`, then `Push` to the router.
    Authored(L, Seq, Op),
    /// Start caring about a log: migrate any relay-side bytes for it to
    /// `ext`.
    Subscribe(L),
    Unsubscribe(L),
    /// The application synced natively (out of band) into `ext`.
    NativeSync(L, Seq, Op),
    /// The application GC'd its own store.
    AppGc(LogRanges<L>),
    /// The relay evicted to reclaim space; nondeterministic in the model,
    /// policy lives in the simulator.
    RelayEvict(LogRanges<L>),
    /// The relay dropped payloads (keeping headers) to reclaim space:
    /// DESIGN.md's payloads-first GC stage. Policy lives above the machine
    /// ([`eviction_candidates`]); the model takes the ranges as an action.
    RelayEvictPayloads(LogRanges<L>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NodeEffect<N, L: Ord> {
    Broadcast(WireMessage<N, L>),
    Deliver(L, Seq),
}

/// Fold a sorted, deduplicated flat op list into the wire's grouped shape.
fn group_ops<L: PartialEq>(ops: Vec<(L, Seq, Op)>) -> Vec<(L, Vec<(Seq, Op)>)> {
    let mut out: Vec<(L, Vec<(Seq, Op)>)> = Vec::new();
    for (log, seq, op) in ops {
        match out.last_mut() {
            Some((last_log, seqs)) if *last_log == log => seqs.push((seq, op)),
            _ => out.push((log, vec![(seq, op)])),
        }
    }
    out
}

/// The per-log ranges spanned by a flat set of parked `(log, seq)` keys.
fn ranges_of<L: Ord + Clone>(parked: &BTreeMap<(L, Seq), Op>) -> LogRanges<L> {
    let mut by_log: BTreeMap<L, Vec<Seq>> = BTreeMap::new();
    for (log, seq) in parked.keys() {
        by_log.entry(log.clone()).or_default().push(*seq);
    }
    LogRanges::from_pairs(
        by_log
            .into_iter()
            .map(|(log, seqs)| (log, Ranges::from_seqs(seqs))),
    )
}

/// Payload-eviction candidates: relay payloads nobody currently wants
/// (DESIGN.md's payloads-first GC). Evicting a wanted payload would force
/// the network to re-send it, so recent Wants are spared. Pure policy,
/// shared verbatim by the model composition and the tokio shell.
pub fn eviction_candidates<L: Id>(
    relay_held_payloads: &LogRanges<L>,
    others_wants: &LogRanges<L>,
) -> LogRanges<L> {
    relay_held_payloads.difference(others_wants)
}

impl<N, L, T> Machine for NodeMachine<N, L, T>
where
    N: Id + Serialize + DeserializeOwned,
    L: Id + Serialize + DeserializeOwned,
    T: polestar::time::TimeInterval,
{
    type State = NodeState<N, L, T>;
    type Action = NodeAction<N, L, T>;
    type Fx = Vec<NodeEffect<N, L>>;
    type Error = anyhow::Error;

    fn transition(&self, mut s: Self::State, action: Self::Action) -> TransitionResult<Self> {
        let mut out = vec![];
        match action {
            NodeAction::Router(a) => {
                ensure!(
                    !matches!(
                        a,
                        RouterAction::RecvWant { .. } | RouterAction::RecvHave { .. }
                    ),
                    "receiving is the node's business: use NodeAction::Recv"
                );
                ensure!(
                    !matches!(a, RouterAction::Held(..) | RouterAction::Push(..)),
                    "held/push are the node glue's business: reconcile_held/Authored are the \
                     only legitimate writers, not a bare NodeAction::Router"
                );
                let fx = self.router_step(&mut s, a)?;
                self.route_router_fx(&mut s, fx, &BTreeMap::new(), &mut out)?;
            }
            NodeAction::Recv(wire) => match wire.body {
                WireBody::Want(ranges) => {
                    let fx = self.router_step(
                        &mut s,
                        RouterAction::RecvWant {
                            from: wire.sender,
                            ranges,
                        },
                    )?;
                    self.route_router_fx(&mut s, fx, &BTreeMap::new(), &mut out)?;
                }
                WireBody::Have(ops) => {
                    let parked: BTreeMap<(L, Seq), Op> = ops
                        .into_iter()
                        .flat_map(|(log, seqs)| seqs.into_iter().map(move |(q, o)| ((log, q), o)))
                        .collect();
                    let ranges = ranges_of(&parked);
                    let fx = self.router_step(
                        &mut s,
                        RouterAction::RecvHave {
                            from: wire.sender,
                            ranges,
                        },
                    )?;
                    // Ingest ALL parked bytes first (idempotent; payload
                    // upgrades are invisible to the router's novelty check
                    // — spec §5).
                    self.ingest_parked(&mut s, &parked)?;
                    self.reconcile_held(&mut s)?;
                    self.route_router_fx(&mut s, fx, &parked, &mut out)?;
                }
            },
            NodeAction::Authored(log, seq, op) => {
                self.ext_step(&mut s, ExtStoreAction::Ingest(log, seq, op))?;
                self.reconcile_held(&mut s)?;
                let ranges = LogRanges::from_pairs([(log, Ranges::from_seqs([seq]))]);
                let fx = self.router_step(&mut s, RouterAction::Push(ranges))?;
                self.route_router_fx(&mut s, fx, &BTreeMap::new(), &mut out)?;
            }
            NodeAction::Subscribe(log) => {
                s.subscriptions.insert(log);
                let all = LogRanges::from_pairs([(log, Ranges::full())]);
                for (log, seq, op) in s.relay.0.fetch(&all) {
                    self.ext_step(&mut s, ExtStoreAction::Ingest(log, seq, op))?;
                }
                self.relay_step(&mut s, RelayStoreAction::Evict(all))?;
                self.reconcile_held(&mut s)?;
            }
            NodeAction::Unsubscribe(log) => {
                s.subscriptions.remove(&log);
                self.reconcile_held(&mut s)?;
            }
            NodeAction::NativeSync(log, seq, op) => {
                self.ext_step(&mut s, ExtStoreAction::NativeSync(log, seq, op))?;
                self.reconcile_held(&mut s)?;
            }
            NodeAction::AppGc(ranges) => {
                self.ext_step(&mut s, ExtStoreAction::AppGc(ranges))?;
                self.reconcile_held(&mut s)?;
            }
            NodeAction::RelayEvict(ranges) => {
                self.relay_step(&mut s, RelayStoreAction::Evict(ranges))?;
                self.reconcile_held(&mut s)?;
            }
            NodeAction::RelayEvictPayloads(ranges) => {
                self.relay_step(&mut s, RelayStoreAction::EvictPayloads(ranges))?;
                self.reconcile_held(&mut s)?;
            }
        }
        Ok((s, out))
    }
}

impl<N, L, T> NodeMachine<N, L, T>
where
    N: Id + Serialize + DeserializeOwned,
    L: Id + Serialize + DeserializeOwned,
    T: polestar::time::TimeInterval,
{
    fn router_step(
        &self,
        s: &mut NodeState<N, L, T>,
        action: RouterAction<N, L, T>,
    ) -> anyhow::Result<Vec<Effect<L>>> {
        let router = s.router.clone();
        let (router, fx) = self.router.transition(router, action)?;
        s.router = router;
        Ok(fx)
    }

    fn relay_step(
        &self,
        s: &mut NodeState<N, L, T>,
        action: RelayStoreAction<L>,
    ) -> anyhow::Result<()> {
        let relay = s.relay.clone();
        let (relay, _fx) = self.relay.transition(relay, action)?;
        s.relay = relay;
        Ok(())
    }

    fn ext_step(
        &self,
        s: &mut NodeState<N, L, T>,
        action: ExtStoreAction<L>,
    ) -> anyhow::Result<()> {
        let ext = s.ext.clone();
        let (ext, _fx) = self.ext.transition(ext, action)?;
        s.ext = ext;
        Ok(())
    }

    /// Cheap and unconditional after any storage change: `HeldChanged` fx
    /// from the storage machines are consumed by this call (dropped — the
    /// snapshot below supersedes any delta).
    fn reconcile_held(&self, s: &mut NodeState<N, L, T>) -> anyhow::Result<()> {
        let snap = s.held_union();
        self.router_step(s, RouterAction::Held(snap))?;
        Ok(())
    }

    /// For each parked op: subscribed logs go to `ext`; unsubscribed logs go
    /// to `relay`, shed silently (not an error) if ingesting would exceed
    /// the relay's cap — the shell sheds until an eviction frees room.
    fn ingest_parked(
        &self,
        s: &mut NodeState<N, L, T>,
        parked: &BTreeMap<(L, Seq), Op>,
    ) -> anyhow::Result<()> {
        for ((log, seq), op) in parked {
            if s.subscriptions.contains(log) {
                self.ext_step(s, ExtStoreAction::Ingest(*log, *seq, op.clone()))?;
            } else {
                let units: Units = s.relay.0.ingest_delta(log, *seq, op);
                if s.relay.0.usage() + units > self.relay.cap {
                    continue; // shed: no room, and no eviction happened yet
                }
                self.relay_step(s, RelayStoreAction::Ingest(*log, *seq, op.clone()))?;
            }
        }
        Ok(())
    }

    /// Turn the router's decisions into node-level effects, in fx order.
    fn route_router_fx(
        &self,
        s: &mut NodeState<N, L, T>,
        fx: Vec<Effect<L>>,
        parked: &BTreeMap<(L, Seq), Op>,
        out: &mut Vec<NodeEffect<N, L>>,
    ) -> anyhow::Result<()> {
        for e in fx {
            match e {
                Effect::Accept(novel) => {
                    for (log, r) in novel.iter() {
                        if !s.subscriptions.contains(log) {
                            continue;
                        }
                        // Never iterate `Ranges` directly — it may be open;
                        // walk the parked keys instead.
                        for (pl, seq) in parked.keys() {
                            if pl == log && r.contains(*seq) {
                                out.push(NodeEffect::Deliver(*log, *seq));
                            }
                        }
                    }
                }
                Effect::SendWant(r) => {
                    out.push(NodeEffect::Broadcast(WireMessage::want(s.router.id, r)));
                }
                Effect::SendHave(r) => {
                    let mut ops = s.relay.0.fetch(&r);
                    ops.extend(s.ext.0.fetch(&r));
                    // Order by (log, seq), with a payload-bearing copy first
                    // within a run: relay and ext can each hold their own
                    // copy of the same (log, seq), and the dedup below keeps
                    // only the first — it must not be the degraded one.
                    ops.sort_by(|a, b| {
                        (&a.0, a.1)
                            .cmp(&(&b.0, b.1))
                            .then_with(|| b.2.payload.is_some().cmp(&a.2.payload.is_some()))
                    });
                    ops.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                    if !ops.is_empty() {
                        out.push(NodeEffect::Broadcast(WireMessage::have(
                            s.router.id,
                            group_ops(ops),
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

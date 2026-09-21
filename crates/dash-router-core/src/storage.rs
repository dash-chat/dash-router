//! The storage boundary: sync traits any concrete store implements, an
//! in-memory reference implementation (`OpsMap`), and two pure machines
//! (`RelayStoreMachine`, `ExtStoreMachine`) that wrap it and report snapshots
//! of what they hold via [`StoreEffect::HeldChanged`].
//!
//! These machines hold data (unlike [`crate::router::RouterMachine`], which
//! only decides ranges); Task 4 composes them with the router into a single
//! node machine.

use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
};

use anyhow::ensure;
use polestar::prelude::*;

use crate::{LogRanges, Op, Ranges, Seq};

/// Abstract unit of storage usage: 2 per op with a payload, 1 per header-only op.
pub type Units = u64;

/// A synchronous store of ops, keyed by log and sequence number.
pub trait Storage<L: Ord> {
    /// Ranges held for exactly the requested logs; a requested-but-unknown
    /// log appears with an empty range, mirroring the request.
    fn held_of(&self, logs: &BTreeSet<L>) -> LogRanges<L>;
    /// Ranges held for every log with any content at all.
    fn held_all(&self) -> LogRanges<L>;
    /// The ops within the given ranges, present in the store.
    fn fetch(&self, ranges: &LogRanges<L>) -> Vec<(L, Seq, Op)>;
    /// Store an op, never downgrading a payload already held (see
    /// [`OpsMap::ingest`]).
    fn ingest(&mut self, log: L, seq: Seq, op: Op);
}

/// A [`Storage`] that can also report and shed its resource usage.
pub trait EvictableStorage<L: Ord>: Storage<L> {
    fn usage(&self) -> Units;
    /// Ranges of ops still held that also have a payload.
    fn held_payloads(&self) -> LogRanges<L>;
    /// Drop payloads (keeping headers) within the given ranges.
    fn evict_payloads(&mut self, ranges: &LogRanges<L>);
    /// Drop ops (header and payload) within the given ranges.
    fn evict(&mut self, ranges: &LogRanges<L>);
    /// The unit delta `ingest(log, seq, op)` would add: 0 duplicate, 1
    /// payload-upgrade or new header-only, 2 new payload-bearing. Cap checks
    /// must use the same arithmetic as the store (see [`OpsMap::ingest_delta`]).
    fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units;
}

/// A simple in-memory reference [`Storage`]/[`EvictableStorage`]: a
/// `BTreeMap` of logs, each a `BTreeMap` of sequence number to op.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct OpsMap<L: Ord>(BTreeMap<L, BTreeMap<Seq, Op>>);

impl<L: Ord + Clone> Storage<L> for OpsMap<L> {
    fn held_of(&self, logs: &BTreeSet<L>) -> LogRanges<L> {
        LogRanges::from_pairs(logs.iter().map(|log| {
            let ranges = self
                .0
                .get(log)
                .map(|ops| Ranges::from_seqs(ops.keys().copied()))
                .unwrap_or_else(Ranges::empty);
            (log.clone(), ranges)
        }))
    }

    fn held_all(&self) -> LogRanges<L> {
        LogRanges::from_pairs(self.0.iter().filter_map(|(l, ops)| {
            if ops.is_empty() {
                None
            } else {
                Some((l.clone(), Ranges::from_seqs(ops.keys().copied())))
            }
        }))
    }

    fn fetch(&self, ranges: &LogRanges<L>) -> Vec<(L, Seq, Op)> {
        let mut out = vec![];
        for (log, r) in ranges.iter() {
            let Some(ops) = self.0.get(log) else {
                continue;
            };
            for (&seq, op) in ops {
                if r.contains(seq) {
                    out.push((log.clone(), seq, op.clone()));
                }
            }
        }
        out
    }

    fn ingest(&mut self, log: L, seq: Seq, op: Op) {
        match self.0.entry(log).or_default().entry(seq) {
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(op);
            }
            std::collections::btree_map::Entry::Occupied(mut o) => {
                if op.payload.is_some() && o.get().payload.is_none() {
                    o.insert(op);
                }
            }
        }
    }
}

impl<L: Ord> OpsMap<L> {
    /// The unit delta that `ingest(log, seq, op)` would add: 0 for a
    /// duplicate (op already held at least as good), 1 for a payload
    /// upgrade over a held header-only op, 2/1 for a genuinely new
    /// payload-bearing/header-only op. Shared by [`RelayStoreMachine`]'s cap
    /// check and the node glue's shed check so the two cannot drift.
    pub fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units {
        let existing = self.0.get(log).and_then(|ops| ops.get(&seq));
        match existing {
            None => {
                if op.payload.is_some() {
                    2
                } else {
                    1
                }
            }
            Some(existing) if existing.payload.is_none() && op.payload.is_some() => 1,
            Some(_) => 0,
        }
    }
}

impl<L: Ord + Clone> EvictableStorage<L> for OpsMap<L> {
    fn usage(&self) -> Units {
        self.0
            .values()
            .flat_map(|ops| ops.values())
            .map(|op| if op.payload.is_some() { 2 } else { 1 })
            .sum()
    }

    fn held_payloads(&self) -> LogRanges<L> {
        LogRanges::from_pairs(self.0.iter().filter_map(|(l, ops)| {
            let seqs = ops
                .iter()
                .filter(|(_, op)| op.payload.is_some())
                .map(|(&seq, _)| seq);
            let ranges = Ranges::from_seqs(seqs);
            if ranges.is_empty() {
                None
            } else {
                Some((l.clone(), ranges))
            }
        }))
    }

    fn evict_payloads(&mut self, ranges: &LogRanges<L>) {
        for (log, r) in ranges.iter() {
            if let Some(ops) = self.0.get_mut(log) {
                for (&seq, op) in ops.iter_mut() {
                    if r.contains(seq) {
                        op.payload = None;
                    }
                }
            }
        }
    }

    fn evict(&mut self, ranges: &LogRanges<L>) {
        for (log, r) in ranges.iter() {
            if let Some(ops) = self.0.get_mut(log) {
                ops.retain(|&seq, _| !r.contains(seq));
                if ops.is_empty() {
                    self.0.remove(log);
                }
            }
        }
    }

    fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units {
        OpsMap::ingest_delta(self, log, seq, op)
    }
}

/// Reports snapshots of what a store machine holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StoreEffect<L: Ord> {
    HeldChanged(LogRanges<L>),
}

/// A capped relay-side store: refuses ingests that would exceed `cap` units.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelayStoreMachine<L> {
    pub cap: Units,
    phantom: PhantomData<L>,
}

impl<L> RelayStoreMachine<L> {
    pub fn new(cap: Units) -> Self {
        Self {
            cap,
            phantom: PhantomData,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RelayStoreState<L: Ord>(pub OpsMap<L>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RelayStoreAction<L: Ord> {
    Ingest(L, Seq, Op),
    EvictPayloads(LogRanges<L>),
    Evict(LogRanges<L>),
}

impl<L: Id> Machine for RelayStoreMachine<L> {
    type State = RelayStoreState<L>;
    type Action = RelayStoreAction<L>;
    type Fx = Vec<StoreEffect<L>>;
    type Error = anyhow::Error;

    fn transition(&self, mut s: Self::State, action: Self::Action) -> TransitionResult<Self> {
        let mut fx = vec![];
        match action {
            RelayStoreAction::Ingest(log, seq, op) => {
                let delta: Units = s.0.ingest_delta(&log, seq, &op);
                ensure!(
                    self.cap >= s.0.usage() + delta,
                    "would exceed cap: not enabled"
                );
                let before = s.0.held_all();
                s.0.ingest(log, seq, op);
                let after = s.0.held_all();
                if after != before {
                    fx.push(StoreEffect::HeldChanged(after));
                }
            }
            RelayStoreAction::EvictPayloads(ranges) => {
                s.0.evict_payloads(&ranges);
                // Headers still held: held_all is unchanged, so no effect.
            }
            RelayStoreAction::Evict(ranges) => {
                let before = s.0.held_all();
                s.0.evict(&ranges);
                let after = s.0.held_all();
                if after != before {
                    fx.push(StoreEffect::HeldChanged(after));
                }
            }
        }
        Ok((s, fx))
    }
}

/// An uncapped, application-side store: the app can sync natively and GC on
/// its own terms.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ExtStoreMachine<L> {
    phantom: PhantomData<L>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ExtStoreState<L: Ord>(pub OpsMap<L>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ExtStoreAction<L: Ord> {
    Ingest(L, Seq, Op),
    NativeSync(L, Seq, Op),
    AppGc(LogRanges<L>),
}

impl<L: Id> Machine for ExtStoreMachine<L> {
    type State = ExtStoreState<L>;
    type Action = ExtStoreAction<L>;
    type Fx = Vec<StoreEffect<L>>;
    type Error = anyhow::Error;

    fn transition(&self, mut s: Self::State, action: Self::Action) -> TransitionResult<Self> {
        let mut fx = vec![];
        match action {
            ExtStoreAction::Ingest(log, seq, op) | ExtStoreAction::NativeSync(log, seq, op) => {
                let before = s.0.held_all();
                s.0.ingest(log, seq, op);
                let after = s.0.held_all();
                if after != before {
                    fx.push(StoreEffect::HeldChanged(after));
                }
            }
            ExtStoreAction::AppGc(ranges) => {
                let before = s.0.held_all();
                s.0.evict(&ranges);
                let after = s.0.held_all();
                if after != before {
                    fx.push(StoreEffect::HeldChanged(after));
                }
            }
        }
        Ok((s, fx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_is_idempotent_and_upgrades_headers() {
        let mut m = OpsMap::<u8>::default();
        let header_only = Op {
            header: vec![1],
            payload: None,
        };
        let full = Op {
            header: vec![1],
            payload: Some(vec![2]),
        };
        m.ingest(0, 0, header_only.clone());
        m.ingest(0, 0, header_only.clone()); // no-op
        assert_eq!(m.fetch(&m.held_all()).len(), 1);
        m.ingest(0, 0, full.clone()); // payload upgrade
        assert_eq!(m.fetch(&m.held_all())[0].2, full);
        m.ingest(0, 0, header_only); // never downgrades
        assert_eq!(m.fetch(&m.held_all())[0].2, full);
    }

    #[test]
    fn held_of_mirrors_the_request_with_empty_ranges() {
        let mut m = OpsMap::<u8>::default();
        m.ingest(0, 0, Op::default());
        let held = m.held_of(&BTreeSet::from([0, 5]));
        assert_eq!(held.get(&0).unwrap(), &Ranges::from_seqs([0]));
        assert!(
            held.get(&5).unwrap().is_empty(),
            "requested-but-absent log appears empty"
        );
    }

    #[test]
    fn usage_counts_units_and_eviction_shrinks_them() {
        let mut m = OpsMap::<u8>::default();
        m.ingest(
            0,
            0,
            Op {
                header: vec![1],
                payload: Some(vec![2]),
            },
        ); // 2 units
        m.ingest(
            0,
            1,
            Op {
                header: vec![1],
                payload: None,
            },
        ); // 1 unit
        assert_eq!(m.usage(), 3);
        m.evict_payloads(&m.held_all());
        assert_eq!(m.usage(), 2);
        assert!(m.held_payloads().is_empty());
        assert_eq!(
            m.held_all().get(&0).unwrap(),
            &Ranges::from_seqs([0, 1]),
            "headers survive"
        );
        m.evict(&m.held_all());
        assert_eq!(m.usage(), 0);
        assert!(m.held_all().is_empty());
    }

    #[test]
    fn relay_machine_refuses_ingest_over_cap_and_emits_held_changed() {
        let m = RelayStoreMachine::<u8>::new(2);
        let payload_op = Op {
            header: vec![1],
            payload: Some(vec![2]),
        };
        let (s, fx) = m
            .transition(
                RelayStoreState::default(),
                RelayStoreAction::Ingest(0, 0, payload_op.clone()),
            )
            .unwrap();
        assert_eq!(fx, vec![StoreEffect::HeldChanged(s.0.held_all())]);
        assert!(
            m.transition(s.clone(), RelayStoreAction::Ingest(0, 1, payload_op))
                .is_err(),
            "would exceed cap: not enabled"
        );
        // Re-ingesting a held op is a no-op and emits nothing.
        let held = s.0.held_all();
        let (s2, fx2) = m
            .transition(
                s,
                RelayStoreAction::Ingest(
                    0,
                    0,
                    Op {
                        header: vec![1],
                        payload: Some(vec![2]),
                    },
                ),
            )
            .unwrap();
        assert_eq!(s2.0.held_all(), held);
        assert!(fx2.is_empty());
    }

    #[test]
    fn ext_machine_native_sync_and_app_gc_report_held_changes() {
        let m = ExtStoreMachine::<u8>::default();
        let (s, fx) = m
            .transition(
                ExtStoreState::default(),
                ExtStoreAction::NativeSync(0, 0, Op::default()),
            )
            .unwrap();
        assert!(matches!(fx[0], StoreEffect::HeldChanged(_)));
        let (s, fx) = m
            .transition(
                s,
                ExtStoreAction::AppGc(LogRanges::from_pairs([(0, Ranges::full())])),
            )
            .unwrap();
        assert!(s.0.held_all().is_empty());
        assert_eq!(fx, vec![StoreEffect::HeldChanged(LogRanges::empty())]);
    }
}

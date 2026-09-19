//! A single Dash Router node as a pure state machine.
//!
//! Everything nondeterministic is an action: how much time passes (`Tick`),
//! which interval a timer is armed with (`ArmWantTimer`/`ArmHaveTimer`), and
//! which message arrives (`Recv`). Nothing in here draws from an RNG or reads
//! a clock, so the same machine can be exhaustively traversed with bounded
//! `N`, `L`, `T` and driven by a simulator or a real network with large ones.
//!
//! Timers follow polestar's `fetch_timed` idiom with zero grace: a timer counts
//! down, `Fire*` is enabled exactly when it reaches zero, and `Tick` is not
//! enabled if it would carry any armed timer below zero. That makes randomised
//! intervals explorable without letting the schedule skip a due timer.
//!
//! Deliberate simplifications at this stage, to revisit:
//! - `Append` emits its fresh Have immediately. DESIGN.md wants a brief
//!   debounce so that several appends share one Have.
//! - The relay cap is measured in units (2 per op with payload, 1 per
//!   header-only op) rather than bytes.
//! - GC "oldest" means lowest `(log, seq)`; recency-of-mention is not tracked.
//! - The relay seen-set (`RouterState::relayed`) carries one TTL for the whole
//!   accumulated range set rather than one per range, so a later flood extends
//!   the life of earlier entries. That only ever suppresses more relaying.

use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
};

use anyhow::{bail, ensure};
use polestar::{prelude::*, time::TimeInterval};

use crate::{
    message::{HaveOps, Message, MessageEnvelope, Op, have_ops_ranges},
    ranges::{LogRanges, Ranges, Seq},
};

/// Parameters shared by every node running the protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterConfig<T> {
    /// How long a witnessed Want influences this node's own Wants and Haves.
    pub want_ttl: T,
    /// How long a witnessed Have suppresses re-sending the same ranges.
    pub have_ttl: T,
    /// Cap on relay-store usage, in units: 2 per op with payload, 1 per
    /// header-only op. Subscribed logs are never counted or collected.
    pub relay_cap: usize,
}

/// The protocol. Holds only configuration; all per-node state is in [`RouterState`].
#[derive(Clone, Debug)]
pub struct RouterMachine<N, L, T> {
    pub config: RouterConfig<T>,
    phantom: PhantomData<(N, L)>,
}

impl<N, L, T> RouterMachine<N, L, T> {
    pub fn new(config: RouterConfig<T>) -> Self {
        Self {
            config,
            phantom: PhantomData,
        }
    }
}

/// A countdown. Due when it reaches zero.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Timer<T> {
    pub remaining: T,
}

/// A Want or Have witnessed from a peer, forgotten when `ttl_left` runs out.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Record<L: Ord, T> {
    pub ranges: LogRanges<L>,
    pub ttl_left: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterState<N: Ord, L: Ord, T> {
    pub id: N,
    /// Logs this node cares about for its own sake. Ops in these logs are
    /// delivered to the application and never garbage collected.
    pub subscriptions: BTreeSet<L>,
    /// Every op held, whether subscribed or relayed.
    pub store: BTreeMap<L, BTreeMap<Seq, Op>>,
    /// Recent Wants from other nodes.
    pub wants: BTreeMap<N, Record<L, T>>,
    /// Recent Haves from other nodes, and this node's own last non-fresh Have
    /// (keyed by its own id) so it does not repeat itself.
    pub haves: BTreeMap<N, Record<L, T>>,
    /// Ranges this node has already flooded onward as part of a fresh Have.
    /// This is the seen-set that terminates the flood: a node relays any given
    /// range at most once per `have_ttl`. It is deliberately separate from
    /// `haves`, which governs responses to Wants — a node that has just
    /// flooded an op must still answer a Want for it, since the Want is
    /// evidence the flood did not reach everyone.
    pub relayed: Option<Record<L, T>>,
    pub want_timer: Option<Timer<T>>,
    pub have_timer: Option<Timer<T>>,
}

impl<N: Id, L: Id, T: TimeInterval> RouterState<N, L, T> {
    pub fn new(id: N, subscriptions: impl IntoIterator<Item = L>) -> Self {
        Self {
            id,
            subscriptions: subscriptions.into_iter().collect(),
            store: BTreeMap::new(),
            wants: BTreeMap::new(),
            haves: BTreeMap::new(),
            relayed: None,
            want_timer: None,
            have_timer: None,
        }
    }

    /// Logs this node knows about: subscribed or holding ops for.
    fn known_logs(&self) -> impl Iterator<Item = L> + '_ {
        let mut logs: BTreeSet<L> = self.subscriptions.clone();
        logs.extend(self.store.keys().copied());
        logs.into_iter()
    }

    /// Ranges held (header at least) for every known log.
    pub fn held(&self) -> LogRanges<L> {
        LogRanges::from_pairs(self.known_logs().map(|log| {
            let seqs = self
                .store
                .get(&log)
                .into_iter()
                .flat_map(|m| m.keys().copied());
            (log, Ranges::from_seqs(seqs))
        }))
    }

    /// Everything not held for every known log: the gaps plus the open tail
    /// after the highest seq.
    pub fn wanted(&self) -> LogRanges<L> {
        let held = self.held();
        LogRanges::from_pairs(self.known_logs().map(|log| {
            let h = held.get(&log).cloned().unwrap_or_default();
            (log, h.complement())
        }))
    }

    /// Union of recent Wants from other nodes.
    pub fn others_wants(&self) -> LogRanges<L> {
        self.wants
            .values()
            .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
    }

    /// Union of recent Haves, including this node's own last one.
    pub fn recent_haves(&self) -> LogRanges<L> {
        self.haves
            .values()
            .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
    }

    /// DESIGN.md §1: what this node wants, minus what others recently asked for.
    pub fn next_want(&self) -> LogRanges<L> {
        self.wanted().difference(&self.others_wants())
    }

    /// DESIGN.md §3: what this node holds that others recently asked for,
    /// minus what has recently been circulating.
    pub fn next_have(&self) -> HaveOps<L> {
        let ranges = self
            .held()
            .intersection(&self.others_wants())
            .difference(&self.recent_haves());
        self.ops_in(&ranges)
    }

    fn ops_in(&self, ranges: &LogRanges<L>) -> HaveOps<L> {
        ranges
            .iter()
            .filter_map(|(log, r)| {
                let ops: BTreeMap<Seq, Op> = self
                    .store
                    .get(log)?
                    .iter()
                    .filter(|(seq, _)| r.contains(**seq))
                    .map(|(seq, op)| (*seq, op.clone()))
                    .collect();
                (!ops.is_empty()).then_some((*log, ops))
            })
            .collect()
    }

    /// Ranges already flooded onward, if the record has not expired.
    pub fn relayed_ranges(&self) -> LogRanges<L> {
        self.relayed
            .as_ref()
            .map(|r| r.ranges.clone())
            .unwrap_or_default()
    }

    /// Note that `ranges` have been flooded onward. Ranges accumulate and the
    /// TTL is reset, which is coarse — refreshing extends the life of earlier
    /// ranges — but errs towards relaying less, never towards a cycle.
    fn note_relayed(&mut self, ranges: LogRanges<L>, ttl: T) {
        let record = self.relayed.get_or_insert_with(|| Record {
            ranges: LogRanges::empty(),
            ttl_left: ttl,
        });
        record.ranges = record.ranges.union(&ranges);
        record.ttl_left = ttl;
    }

    /// The subset of `ops` not yet flooded onward by this node.
    fn unrelayed(&self, ops: &HaveOps<L>) -> HaveOps<L> {
        let relayed = self.relayed_ranges();
        ops.iter()
            .filter_map(|(log, seqs)| {
                let picked: BTreeMap<Seq, Op> = seqs
                    .iter()
                    .filter(|(seq, _)| !relayed.contains(log, **seq))
                    .map(|(seq, op)| (*seq, op.clone()))
                    .collect();
                (!picked.is_empty()).then_some((*log, picked))
            })
            .collect()
    }

    pub fn holds(&self, log: &L, seq: Seq) -> Option<&Op> {
        self.store.get(log)?.get(&seq)
    }

    fn is_relayed(&self, log: &L) -> bool {
        !self.subscriptions.contains(log)
    }

    /// Relay-store usage in cap units (see [`RouterConfig::relay_cap`]).
    pub fn relay_usage(&self) -> usize {
        self.store
            .iter()
            .filter(|(log, _)| self.is_relayed(log))
            .flat_map(|(_, ops)| ops.values())
            .map(|op| if op.payload.is_some() { 2 } else { 1 })
            .sum()
    }

    /// Drop payloads oldest-first until under the cap, then headers.
    fn gc(&mut self, cap: usize) {
        while self.relay_usage() > cap {
            let oldest_with_payload = self
                .store
                .iter()
                .filter(|(log, _)| self.is_relayed(log))
                .flat_map(|(log, ops)| ops.iter().map(move |(seq, op)| (*log, *seq, op)))
                .find(|(_, _, op)| op.payload.is_some())
                .map(|(log, seq, _)| (log, seq));
            if let Some((log, seq)) = oldest_with_payload {
                self.store
                    .get_mut(&log)
                    .unwrap()
                    .get_mut(&seq)
                    .unwrap()
                    .payload = None;
                continue;
            }
            let oldest = self
                .store
                .iter()
                .filter(|(log, ops)| self.is_relayed(log) && !ops.is_empty())
                .map(|(log, ops)| (*log, *ops.keys().next().unwrap()))
                .next()
                .expect("usage > 0 implies a relayed op exists");
            let ops = self.store.get_mut(&oldest.0).unwrap();
            ops.remove(&oldest.1);
            if ops.is_empty() {
                self.store.remove(&oldest.0);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouterAction<N, L: Ord, T> {
    /// Time passes. Not enabled for zero, nor if it would carry an armed timer past due.
    Tick(T),
    /// Choose the interval until the next Want. Only when no Want timer is armed.
    ArmWantTimer(T),
    /// Choose the interval until the next Have. Only when no Have timer is
    /// armed and a Want from another node has been witnessed.
    ArmHaveTimer(T),
    /// Only when the Want timer is due.
    FireWant,
    /// Only when the Have timer is due.
    FireHave,
    /// Author the next op on a subscribed log.
    Append(L, Op),
    /// A message arrives from another node.
    Recv(MessageEnvelope<N, L>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect<N, L: Ord> {
    /// Broadcast to the LAN.
    Send(MessageEnvelope<N, L>),
    /// Persist an op (into the selfish or relay store, per subscription).
    Store(L, Seq, Op),
    /// Hand an op in a subscribed log to the application.
    Deliver(L, Seq),
}

impl<N: Id, L: Id, T: TimeInterval> Machine for RouterMachine<N, L, T> {
    type State = RouterState<N, L, T>;
    type Action = RouterAction<N, L, T>;
    type Fx = Vec<Effect<N, L>>;
    type Error = anyhow::Error;

    fn transition(&self, mut s: Self::State, action: Self::Action) -> TransitionResult<Self> {
        let mut fx = vec![];
        match action {
            RouterAction::Tick(dur) => {
                ensure!(!dur.is_zero(), "zero tick");
                for timer in [&mut s.want_timer, &mut s.have_timer].into_iter().flatten() {
                    ensure!(dur <= timer.remaining, "tick would carry a timer past due");
                    timer.remaining = timer.remaining - dur;
                }
                for records in [&mut s.wants, &mut s.haves] {
                    records.retain(|_, r| dur < r.ttl_left);
                    for r in records.values_mut() {
                        r.ttl_left = r.ttl_left - dur;
                    }
                }
                match &mut s.relayed {
                    Some(r) if dur < r.ttl_left => r.ttl_left = r.ttl_left - dur,
                    _ => s.relayed = None,
                }
            }

            RouterAction::ArmWantTimer(interval) => {
                ensure!(s.want_timer.is_none(), "want timer already armed");
                s.want_timer = Some(Timer {
                    remaining: interval,
                });
            }

            RouterAction::ArmHaveTimer(interval) => {
                ensure!(s.have_timer.is_none(), "have timer already armed");
                ensure!(
                    !s.wants.is_empty(),
                    "no wants witnessed, nothing to respond to"
                );
                s.have_timer = Some(Timer {
                    remaining: interval,
                });
            }

            RouterAction::FireWant => {
                let Some(timer) = &s.want_timer else {
                    bail!("want timer not armed")
                };
                ensure!(timer.remaining.is_zero(), "want timer not due");
                s.want_timer = None;
                let ranges = s.next_want();
                if !ranges.is_empty() {
                    fx.push(Effect::Send(MessageEnvelope::want(s.id, ranges)));
                }
            }

            RouterAction::FireHave => {
                let Some(timer) = &s.have_timer else {
                    bail!("have timer not armed")
                };
                ensure!(timer.remaining.is_zero(), "have timer not due");
                s.have_timer = None;
                let ops = s.next_have();
                if !ops.is_empty() {
                    s.haves.insert(
                        s.id,
                        Record {
                            ranges: have_ops_ranges(&ops),
                            ttl_left: self.config.have_ttl,
                        },
                    );
                    fx.push(Effect::Send(MessageEnvelope::have(s.id, ops, false)));
                }
            }

            RouterAction::Append(log, op) => {
                ensure!(
                    s.subscriptions.contains(&log),
                    "can only append to a subscribed log"
                );
                let seq = s
                    .store
                    .get(&log)
                    .and_then(|ops| ops.keys().next_back())
                    .map_or(0, |last| last + 1);
                s.store.entry(log).or_default().insert(seq, op.clone());
                fx.push(Effect::Store(log, seq, op.clone()));
                let ops = HaveOps::from([(log, BTreeMap::from([(seq, op)]))]);
                s.note_relayed(have_ops_ranges(&ops), self.config.have_ttl);
                fx.push(Effect::Send(MessageEnvelope::have(s.id, ops, true)));
            }

            RouterAction::Recv(MessageEnvelope { from, message }) => {
                ensure!(from != s.id, "received own message");
                match message {
                    Message::Want { ranges } => {
                        s.wants.insert(
                            from,
                            Record {
                                ranges,
                                ttl_left: self.config.want_ttl,
                            },
                        );
                    }
                    Message::Have { ops, fresh } => {
                        // A fresh Have is relayed by everyone who receives it,
                        // whether or not its ops are new to this node: a peer
                        // out of the sender's earshot may still need them. The
                        // flood terminates on the seen-set, not on novelty.
                        if fresh {
                            let relay = s.unrelayed(&ops);
                            if !relay.is_empty() {
                                s.note_relayed(have_ops_ranges(&relay), self.config.have_ttl);
                                fx.push(Effect::Send(MessageEnvelope::have(s.id, relay, true)));
                            }
                        }
                        s.haves.insert(
                            from,
                            Record {
                                ranges: have_ops_ranges(&ops),
                                ttl_left: self.config.have_ttl,
                            },
                        );
                        let mut new_ops: HaveOps<L> = BTreeMap::new();
                        for (log, seqs) in ops {
                            for (seq, op) in seqs {
                                let improves = match s.holds(&log, seq) {
                                    None => true,
                                    Some(held) => held.payload.is_none() && op.payload.is_some(),
                                };
                                if improves {
                                    new_ops.entry(log).or_default().insert(seq, op);
                                }
                            }
                        }
                        for (log, seqs) in &new_ops {
                            for (seq, op) in seqs {
                                s.store.entry(*log).or_default().insert(*seq, op.clone());
                                fx.push(Effect::Store(*log, *seq, op.clone()));
                                if s.subscriptions.contains(log) {
                                    fx.push(Effect::Deliver(*log, *seq));
                                }
                            }
                        }
                        s.gc(self.config.relay_cap);
                    }
                }
            }
        }
        Ok((s, fx))
    }
}

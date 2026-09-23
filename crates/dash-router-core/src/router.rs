//! A single Dash Router node as a pure state machine.
//!
//! Everything nondeterministic is an action: how much time passes (`Tick`),
//! which interval a timer is armed with (`ArmWantTimer`/`ArmHaveTimer`), and
//! which message arrives (`RecvWant`/`RecvHave`). Nothing in here draws from
//! an RNG or reads a clock, so the same machine can be exhaustively traversed
//! with bounded `N`, `L`, `T` and driven by a simulator or a real network
//! with large ones.
//!
//! Timers follow polestar's `fetch_timed` idiom with zero grace: a timer counts
//! down, `Fire*` is enabled exactly when it reaches zero, and `Tick` is not
//! enabled if it would carry any armed timer below zero. That makes randomised
//! intervals explorable without letting the schedule skip a due timer.
//!
//! This machine holds no data: it decides ranges, not ops. Storage,
//! hydration (turning a `SendHave`'s ranges into wire ops with payloads) and
//! turning a received `Accept` into stored data all live in the shell above
//! it (spec §5).
//!
//! Deliberate simplifications at this stage, to revisit:
//! - The seen-set (`RouterState::relayed_haves`) carries one TTL for the
//!   whole accumulated range set rather than one per range, so a later flood
//!   extends the life of earlier entries. That only ever suppresses more
//!   relaying.

use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
};

use anyhow::{bail, ensure};
use polestar::{StateMachine, prelude::*, time::TimeInterval};

use crate::{log::Log, ranges::LogRanges};

/// Parameters shared by every node running the protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterConfig<T> {
    /// How long a witnessed Want influences this node's own Wants and Haves.
    pub want_ttl: T,
    /// How long a witnessed Have suppresses re-sending the same ranges.
    pub have_ttl: T,
}

pub type RouterStateMachine<N, L, T> = StateMachine<RouterMachine<N, L, T>>;

/// The protocol. Holds only configuration; all per-node state is in [`RouterState`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
pub struct Record<L: Log, T> {
    pub ranges: LogRanges<L>,
    /// Wholesale interests carried by a Want; always empty on Have records.
    pub prefixes: BTreeSet<L::Prefix>,
    pub ttl_left: T,
}

impl<L: Log, T> Record<L, T> {
    fn ranges_only(ranges: LogRanges<L>, ttl_left: T) -> Self {
        Self {
            ranges,
            prefixes: BTreeSet::new(),
            ttl_left,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterState<N: Ord, L: Log, T> {
    pub id: N,
    /// Ranges held for every known log, as reported by the storage layer
    /// above. An empty range for a log means the log is known but empty
    /// ("known-but-empty"): that log wants everything.
    pub held: LogRanges<L>,
    /// Recent Wants from other nodes: one record per received Want, each
    /// with its own TTL (the `relayed_wants` mechanism), keyed by the
    /// Want's *origin* — the node that wanted, not the relayer that
    /// delivered it — so every piece of one node's Want lands under one
    /// key whichever path it took, and an echo of this node's own Want
    /// (relayed back to it) is never recorded at all: it would otherwise
    /// name this node's logs on a relayer's behalf and stop them going
    /// wholesale to a prefix wanter behind that relayer. A Want may arrive
    /// split across several wire messages (the shell's `pack_want`), and a
    /// node's successive Wants overlap; each record dies `want_ttl` after
    /// its own arrival, so pieces union and stale ranges expire on their
    /// own schedule. A stale record can still
    /// trigger one bounded re-send if this node's own `haves` entry was
    /// replaced by a Push in the meantime; it dies within `want_ttl`. A
    /// peer is present iff it has at least one live record.
    pub wants: BTreeMap<N, Vec<Record<L, T>>>,
    /// Recent Haves from other nodes, plus this node's own last Have
    /// emission (keyed by its own id) for §3 suppression.
    pub haves: BTreeMap<N, Record<L, T>>,
    /// Have ranges this node has already flooded onward. This is the
    /// seen-set that terminates the flood: a node relays any given range at
    /// most once per `have_ttl`. Each flood adds its own record with its own
    /// TTL, so older ranges fall out independently — a single accumulating
    /// record whose TTL refreshes on every update would let steady traffic
    /// keep the whole set alive forever, suppressing relays that should
    /// happen. Deliberately separate from `haves`, which governs responses
    /// to Wants — a node that has just flooded a range must still answer a
    /// Want for it, since the Want is evidence the flood did not reach
    /// everyone.
    pub relayed_haves: Vec<Record<L, T>>,
    /// Want ranges this node has already flooded onward: the same seen-set
    /// mechanism, for the Want flood, on `want_ttl`.
    pub relayed_wants: Vec<Record<L, T>>,
    /// This node's wholesale interests: every log under these prefixes,
    /// known or not. Set by `Open` from the node glue's subscriptions.
    pub open: BTreeSet<L::Prefix>,
    pub want_timer: Option<Timer<T>>,
    pub have_timer: Option<Timer<T>>,
}

impl<N: Id, L: Log, T: TimeInterval> RouterState<N, L, T> {
    pub fn new(id: N, held: LogRanges<L>) -> Self {
        Self {
            id,
            held,
            wants: BTreeMap::new(),
            haves: BTreeMap::new(),
            relayed_haves: Vec::new(),
            relayed_wants: Vec::new(),
            open: BTreeSet::new(),
            want_timer: None,
            have_timer: None,
        }
    }

    /// Everything not held, for every known log: the gaps plus the open
    /// tail. A known-but-empty log (empty range in `held`) wants everything.
    pub fn wanted(&self) -> LogRanges<L> {
        LogRanges::from_pairs(self.held.iter().map(|(log, r)| (*log, r.complement())))
    }

    /// Held logs with data whose prefix is in `prefixes` and that `named`
    /// does not mention: the wholesale half of a Want's answer.
    fn held_under(&self, prefixes: &BTreeSet<L::Prefix>, named: &LogRanges<L>) -> LogRanges<L> {
        LogRanges::from_pairs(
            self.held
                .iter()
                .filter(|(log, r)| {
                    !r.is_empty() && prefixes.contains(&log.prefix()) && named.get(log).is_none()
                })
                .map(|(log, r)| (*log, r.clone())),
        )
    }

    /// Union of recent Wants from other nodes, with each wanter's prefix
    /// interests expanded over what this node holds (spec §3.5). A wanter's
    /// live records (keyed by origin) are unioned *before* the prefix
    /// expansion, so a log the wanter named in one piece of a split Want is
    /// not sent wholesale because of a prefix carried in another piece.
    pub fn others_wants(&self) -> LogRanges<L> {
        self.wants
            .values()
            .fold(LogRanges::empty(), |acc, records| {
                let (named, prefixes) = records.iter().fold(
                    (LogRanges::empty(), BTreeSet::new()),
                    |(named, mut prefixes), r| {
                        prefixes.extend(r.prefixes.iter().copied());
                        (named.union(&r.ranges), prefixes)
                    },
                );
                let wholesale = self.held_under(&prefixes, &named);
                acc.union(&named).union(&wholesale)
            })
    }

    /// Union of recent Want prefixes from other nodes.
    pub fn others_prefixes(&self) -> BTreeSet<L::Prefix> {
        self.wants
            .values()
            .flatten()
            .flat_map(|r| r.prefixes.iter().copied())
            .collect()
    }

    /// Union of recent Haves, including this node's own last one.
    pub fn recent_haves(&self) -> LogRanges<L> {
        self.haves
            .values()
            .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
    }

    /// DESIGN.md §1, plus this node's open prefixes. Prefixes are never
    /// suppressed by others' prefixes: two nodes' explicit knowledge under
    /// the same prefix differs, so one node's answer is not the other's.
    /// Logs under this node's own open prefixes stay named even when another
    /// peer already wants them, so an answerer never sends them wholesale
    /// (spec §3.5: a wanter that already knows a log names it explicitly).
    pub fn next_want(&self) -> (LogRanges<L>, BTreeSet<L::Prefix>) {
        let wanted = self.wanted();
        let suppressed = wanted.difference(&self.others_wants());
        let named_under_open = LogRanges::from_pairs(
            wanted
                .iter()
                .filter(|(log, _)| self.open.contains(&log.prefix()))
                .map(|(log, r)| (*log, r.clone())),
        );
        (suppressed.union(&named_under_open), self.open.clone())
    }

    /// DESIGN.md §3 — now ranges, not ops; hydration happens above.
    pub fn next_have(&self) -> LogRanges<L> {
        self.held
            .intersection(&self.others_wants())
            .difference(&self.recent_haves())
    }

    pub fn relayed_have_ranges(&self) -> LogRanges<L> {
        combined_ranges(&self.relayed_haves)
    }

    pub fn relayed_want_ranges(&self) -> LogRanges<L> {
        combined_ranges(&self.relayed_wants)
    }

    pub fn relayed_want_prefixes(&self) -> BTreeSet<L::Prefix> {
        self.relayed_wants
            .iter()
            .flat_map(|r| r.prefixes.iter().copied())
            .collect()
    }

    fn note_relayed_haves(&mut self, ranges: LogRanges<L>, ttl: T) {
        self.relayed_haves.push(Record::ranges_only(ranges, ttl));
    }

    fn note_relayed_wants(&mut self, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix>, ttl: T) {
        self.relayed_wants.push(Record {
            ranges,
            prefixes,
            ttl_left: ttl,
        });
    }

    /// Record an own or received Have emission for §3 suppression.
    fn note_have(&mut self, key: N, ranges: LogRanges<L>, ttl: T) {
        self.haves.insert(key, Record::ranges_only(ranges, ttl));
    }
}

/// Union of the ranges across a set of records.
fn combined_ranges<L: Log, T>(records: &[Record<L, T>]) -> LogRanges<L> {
    records
        .iter()
        .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouterAction<N, L: Log, T> {
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
    /// Replace this node's wholesale interests (the node glue's
    /// subscriptions, by prefix).
    Open(BTreeSet<L::Prefix>),
    /// A Want message arrives from another node. `from` is the wire
    /// sender (the last relayer, or the wanter itself); `origin` is the
    /// node that wanted, carried unchanged through every relay.
    RecvWant {
        from: N,
        origin: N,
        ranges: LogRanges<L>,
        prefixes: BTreeSet<L::Prefix>,
    },
    /// A Have message arrives from another node.
    RecvHave { from: N, ranges: LogRanges<L> },
    /// An absolute snapshot of what the storage layer holds, replacing
    /// `held` wholesale.
    Held(LogRanges<L>),
    /// Storage has grown by these ranges (e.g. a local append); the router
    /// grows `held` incrementally and floods the news.
    Push(LogRanges<L>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect<N, L: Log> {
    /// Broadcast a Want to the LAN on behalf of `origin`: this node for its
    /// own Wants, the incoming Want's origin for a relay.
    SendWant {
        origin: N,
        ranges: LogRanges<L>,
        prefixes: BTreeSet<L::Prefix>,
    },
    /// Broadcast a Have for these ranges to the LAN; the shell hydrates them
    /// into wire ops.
    SendHave(LogRanges<L>),
    /// Ranges newly learned about via a Have, to be fetched/stored above.
    Accept(LogRanges<L>),
}

impl<N: Id, L: Log, T: TimeInterval> Machine for RouterMachine<N, L, T> {
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
                s.haves.retain(|_, r| dur < r.ttl_left);
                for r in s.haves.values_mut() {
                    r.ttl_left = r.ttl_left - dur;
                }
                let decay = |records: &mut Vec<Record<L, T>>| {
                    records.retain_mut(|r| {
                        let alive = dur < r.ttl_left;
                        if alive {
                            r.ttl_left = r.ttl_left - dur;
                        }
                        alive
                    });
                };
                for records in s.wants.values_mut() {
                    decay(records);
                }
                s.wants.retain(|_, records| !records.is_empty());
                for records in [&mut s.relayed_haves, &mut s.relayed_wants] {
                    decay(records);
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
                let (ranges, prefixes) = s.next_want();
                if !ranges.is_empty() || !prefixes.is_empty() {
                    // Own emissions count as relayed, or the flood's echo
                    // would be re-flooded by its own author.
                    s.note_relayed_wants(ranges.clone(), prefixes.clone(), self.config.want_ttl);
                    fx.push(Effect::SendWant {
                        origin: s.id,
                        ranges,
                        prefixes,
                    });
                }
            }

            RouterAction::FireHave => {
                let Some(timer) = &s.have_timer else {
                    bail!("have timer not armed")
                };
                ensure!(timer.remaining.is_zero(), "have timer not due");
                s.have_timer = None;
                let ranges = s.next_have();
                if !ranges.is_empty() {
                    s.note_have(s.id, ranges.clone(), self.config.have_ttl);
                    s.note_relayed_haves(ranges.clone(), self.config.have_ttl);
                    fx.push(Effect::SendHave(ranges));
                }
            }

            RouterAction::Push(ranges) => {
                ensure!(!ranges.is_empty(), "empty push");
                // The author certainly holds what it pushes.
                s.held = s.held.union(&ranges);
                let relay = ranges.difference(&s.relayed_have_ranges());
                if !relay.is_empty() {
                    s.note_have(s.id, relay.clone(), self.config.have_ttl);
                    s.note_relayed_haves(relay.clone(), self.config.have_ttl);
                    fx.push(Effect::SendHave(relay));
                }
            }

            RouterAction::Held(ranges) => {
                // Absolute snapshot from the storage layer; replaces wholesale.
                s.held = ranges;
            }

            RouterAction::Open(prefixes) => {
                s.open = prefixes;
            }

            RouterAction::RecvWant {
                from,
                origin,
                ranges,
                prefixes,
            } => {
                ensure!(from != s.id, "received own message");
                // DESIGN.md: every received message is re-transmitted:
                // simple flooding, terminated by the seen-set. Relay only
                // the not-yet-relayed portion, re-signed.
                let relay_prefixes: BTreeSet<L::Prefix> = prefixes
                    .difference(&s.relayed_want_prefixes())
                    .copied()
                    .collect();
                // Prefixes are relayed once per want_ttl, so carrying the
                // full named ranges with them is bounded, and it keeps the
                // naming that stops answerers from sending named logs
                // wholesale.
                let relay_ranges = if relay_prefixes.is_empty() {
                    ranges.difference(&s.relayed_want_ranges())
                } else {
                    ranges.clone()
                };
                if !relay_ranges.is_empty() || !relay_prefixes.is_empty() {
                    s.note_relayed_wants(
                        relay_ranges.clone(),
                        relay_prefixes.clone(),
                        self.config.want_ttl,
                    );
                    fx.push(Effect::SendWant {
                        origin,
                        ranges: relay_ranges,
                        prefixes: relay_prefixes,
                    });
                }
                // Keyed by origin, never by `from`; an echo of this node's
                // own Want is not recorded (see `RouterState::wants`).
                // Accumulate, never replace.
                if origin != s.id {
                    s.wants.entry(origin).or_default().push(Record {
                        ranges,
                        prefixes,
                        ttl_left: self.config.want_ttl,
                    });
                }
            }

            RouterAction::RecvHave { from, ranges } => {
                ensure!(from != s.id, "received own message");
                let novel = ranges.difference(&s.held);
                if !novel.is_empty() {
                    s.held = s.held.union(&novel);
                    fx.push(Effect::Accept(novel));
                }
                // Every Have is relayed by everyone who receives it, whether
                // or not its ranges are new to this node: a peer out of the
                // sender's earshot may still need them. The flood
                // terminates on the seen-set, not on novelty.
                let relay = ranges.difference(&s.relayed_have_ranges());
                if !relay.is_empty() {
                    s.note_relayed_haves(relay.clone(), self.config.have_ttl);
                    fx.push(Effect::SendHave(relay));
                }
                s.note_have(from, ranges, self.config.have_ttl);
            }
        }
        Ok((s, fx))
    }
}

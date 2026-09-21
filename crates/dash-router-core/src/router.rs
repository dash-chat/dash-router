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

use std::{collections::BTreeMap, marker::PhantomData};

use anyhow::{bail, ensure};
use polestar::{StateMachine, prelude::*, time::TimeInterval};

use crate::ranges::LogRanges;

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
pub struct Record<L: Ord, T> {
    pub ranges: LogRanges<L>,
    pub ttl_left: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterState<N: Ord, L: Ord, T> {
    pub id: N,
    /// Ranges held for every known log, as reported by the storage layer
    /// above. An empty range for a log means the log is known but empty
    /// ("known-but-empty"): that log wants everything.
    pub held: LogRanges<L>,
    /// Recent Wants from other nodes.
    pub wants: BTreeMap<N, Record<L, T>>,
    /// Recent Haves from other nodes, and this node's own last non-fresh Have
    /// (keyed by its own id) so it does not repeat itself.
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
    pub want_timer: Option<Timer<T>>,
    pub have_timer: Option<Timer<T>>,
}

impl<N: Id, L: Id, T: TimeInterval> RouterState<N, L, T> {
    pub fn new(id: N, held: LogRanges<L>) -> Self {
        Self {
            id,
            held,
            wants: BTreeMap::new(),
            haves: BTreeMap::new(),
            relayed_haves: Vec::new(),
            relayed_wants: Vec::new(),
            want_timer: None,
            have_timer: None,
        }
    }

    /// Everything not held, for every known log: the gaps plus the open
    /// tail. A known-but-empty log (empty range in `held`) wants everything.
    pub fn wanted(&self) -> LogRanges<L> {
        LogRanges::from_pairs(self.held.iter().map(|(log, r)| (*log, r.complement())))
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

    /// DESIGN.md §1.
    pub fn next_want(&self) -> LogRanges<L> {
        self.wanted().difference(&self.others_wants())
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

    fn note_relayed_haves(&mut self, ranges: LogRanges<L>, ttl: T) {
        self.relayed_haves.push(Record {
            ranges,
            ttl_left: ttl,
        });
    }

    fn note_relayed_wants(&mut self, ranges: LogRanges<L>, ttl: T) {
        self.relayed_wants.push(Record {
            ranges,
            ttl_left: ttl,
        });
    }

    /// Record an own or received Have emission for §3 suppression.
    fn note_have(&mut self, key: N, ranges: LogRanges<L>, ttl: T) {
        self.haves.insert(
            key,
            Record {
                ranges,
                ttl_left: ttl,
            },
        );
    }
}

/// Union of the ranges across a set of records.
fn combined_ranges<L: Id, T>(records: &[Record<L, T>]) -> LogRanges<L> {
    records
        .iter()
        .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
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
    /// A Want message arrives from another node.
    RecvWant { from: N, ranges: LogRanges<L> },
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
pub enum Effect<L: Ord> {
    /// Broadcast a Want for these ranges to the LAN.
    SendWant(LogRanges<L>),
    /// Broadcast a Have for these ranges to the LAN; the shell hydrates them
    /// into wire ops.
    SendHave(LogRanges<L>),
    /// Ranges newly learned about via a Have, to be fetched/stored above.
    Accept(LogRanges<L>),
}

impl<N: Id, L: Id, T: TimeInterval> Machine for RouterMachine<N, L, T> {
    type State = RouterState<N, L, T>;
    type Action = RouterAction<N, L, T>;
    type Fx = Vec<Effect<L>>;
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
                for records in [&mut s.relayed_haves, &mut s.relayed_wants] {
                    records.retain_mut(|r| {
                        let alive = dur < r.ttl_left;
                        if alive {
                            r.ttl_left = r.ttl_left - dur;
                        }
                        alive
                    });
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
                    // Own emissions count as relayed, or the flood's echo
                    // would be re-flooded by its own author.
                    s.note_relayed_wants(ranges.clone(), self.config.want_ttl);
                    fx.push(Effect::SendWant(ranges));
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

            RouterAction::RecvWant { from, ranges } => {
                ensure!(from != s.id, "received own message");
                // DESIGN.md: every received message is re-transmitted:
                // simple flooding, terminated by the seen-set. Relay only
                // the not-yet-relayed portion, re-signed.
                let relay = ranges.difference(&s.relayed_want_ranges());
                if !relay.is_empty() {
                    s.note_relayed_wants(relay.clone(), self.config.want_ttl);
                    fx.push(Effect::SendWant(relay));
                }
                s.wants.insert(
                    from,
                    Record {
                        ranges,
                        ttl_left: self.config.want_ttl,
                    },
                );
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

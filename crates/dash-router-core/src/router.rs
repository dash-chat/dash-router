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
//! Wants (spec 2026-09-29): a Want carries an [`Interest`], what the emitter
//! *has* under each channel it cares about, and asks for everything else.
//! It has no origin. Emission *cancels*: a node stays silent while the
//! Wants it has recently seen (its own included) already ask for everything
//! it would ask for, so in a stable LAN one representative speaks for each
//! distinct interest. Interest in a channel comes from subscriptions, from
//! held data (which is how interest survives across LANs), and, briefly,
//! from hearing a channel wanted by someone who holds data under it.
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

use crate::{interest::Interest, log::Log, ranges::LogRanges};

/// Parameters shared by every node running the protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterConfig<T> {
    /// How long a seen Want (own or received) suppresses this node's own
    /// Wants and remains answerable.
    pub want_ttl: T,
    /// How long a witnessed Have suppresses re-sending the same ranges.
    pub have_ttl: T,
    /// How long a heard channel (one this node holds nothing under) stays
    /// a channel of interest without being heard again.
    pub heard_ttl: T,
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

/// A Have witnessed from a peer (or emitted), forgotten when `ttl_left`
/// runs out.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Record<L: Log, T> {
    pub ranges: LogRanges<L>,
    pub ttl_left: T,
}

/// A Want seen on the wire or emitted by this node, forgotten when
/// `ttl_left` runs out. Nothing marks which records are this node's own:
/// an echo asks for nothing this node holds, so it needs no telling apart.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SeenWant<L: Log, T> {
    pub interest: Interest<L>,
    pub ttl_left: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouterState<N: Ord, L: Log, T> {
    pub id: N,
    /// Ranges held for every known log, as reported by the storage layer
    /// above. An empty range for a log means the log is known but empty
    /// ("known-but-empty"); it contributes nothing to a Want's have.
    pub held: LogRanges<L>,
    /// Wants seen within `want_ttl`, own emissions included, each on its
    /// own clock. Both the answer side (what to Have) and the emission side
    /// (what not to re-ask) read this; so does relay-on-first-sight.
    pub seen: Vec<SeenWant<L, T>>,
    /// Channels heard in received Wants that this node holds nothing under,
    /// each on its own clock. Never persisted: it exists only to break the
    /// deadlock at an encounter (a node holding nothing wants nothing, and
    /// a holder answers only Wants).
    pub heard: BTreeMap<L::Channel, T>,
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
    /// This node's subscriptions, by channel. Set by `Open` from the node
    /// glue. One of three sources of interest (see `channels_of_interest`).
    pub open: BTreeSet<L::Channel>,
    pub want_timer: Option<Timer<T>>,
    pub have_timer: Option<Timer<T>>,
}

impl<N: Id, L: Log, T: TimeInterval> RouterState<N, L, T> {
    pub fn new(id: N, held: LogRanges<L>) -> Self {
        Self {
            id,
            held,
            seen: Vec::new(),
            heard: BTreeMap::new(),
            haves: BTreeMap::new(),
            relayed_haves: Vec::new(),
            open: BTreeSet::new(),
            want_timer: None,
            have_timer: None,
        }
    }

    /// The furthest a single `Tick` may go: the smallest remaining time
    /// across armed timers, or `None` when nothing is armed. A harness
    /// advancing its clock toward a target clamps the step to this, so
    /// that every due point is visited and a `Fire*` can run there.
    pub fn next_due(&self) -> Option<T> {
        [&self.want_timer, &self.have_timer]
            .into_iter()
            .flatten()
            .map(|t| t.remaining)
            .min()
    }

    /// Whether the Want timer is armed and due (`FireWant` is enabled).
    pub fn want_due(&self) -> bool {
        self.want_timer
            .as_ref()
            .is_some_and(|t| t.remaining.is_zero())
    }

    /// Whether the Have timer is armed and due (`FireHave` is enabled).
    pub fn have_due(&self) -> bool {
        self.have_timer
            .as_ref()
            .is_some_and(|t| t.remaining.is_zero())
    }

    /// Whether this node holds at least one op under `c`, in either store.
    pub fn holds_under(&self, c: &L::Channel) -> bool {
        self.held.channel(c).any(|(_, r)| !r.is_empty())
    }

    /// The channels this node has a reason to ask about: subscribed, held
    /// under, or recently heard wanted by a holder.
    pub fn channels_of_interest(&self) -> BTreeSet<L::Channel> {
        let mut out = self.open.clone();
        out.extend(self.held.channels().filter(|c| self.holds_under(c)));
        out.extend(self.heard.keys().copied());
        out
    }

    /// What this node would ask for if nobody else were asking: its have
    /// under every channel of interest, whole-scope. Derived, never stored.
    pub fn my_interest(&self) -> Interest<L> {
        Interest::from_held(&self.held, self.channels_of_interest())
    }

    /// Emission cancellation: `my_interest` minus every entry the seen
    /// Wants already cover.
    pub fn next_want(&self) -> Interest<L> {
        let seen: Vec<&Interest<L>> = self.seen.iter().map(|r| &r.interest).collect();
        self.my_interest().uncovered_by(&seen)
    }

    /// The union over seen Wants of what each asks for out of what this
    /// node holds: everything some asker lacks. Also the "currently
    /// wanted" input to relay eviction.
    pub fn network_ask(&self) -> LogRanges<L> {
        self.seen.iter().fold(LogRanges::empty(), |acc, r| {
            acc.union(&r.interest.ask_within(&self.held))
        })
    }

    /// Union of recent Haves, including this node's own last one.
    pub fn recent_haves(&self) -> LogRanges<L> {
        self.haves
            .values()
            .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
    }

    /// DESIGN.md §3 — what to Have next: everything some asker lacks that
    /// was not recently sent.
    pub fn next_have(&self) -> LogRanges<L> {
        self.network_ask().difference(&self.recent_haves())
    }

    /// `network_ask` as it would be right after `RecvWant { interest }`:
    /// what a harness checks to know whether that receipt will enable
    /// `ArmHaveTimer`. Receiving a Want changes nothing else this reads.
    pub fn network_ask_with(&self, interest: &Interest<L>) -> LogRanges<L> {
        self.network_ask().union(&interest.ask_within(&self.held))
    }

    pub fn relayed_have_ranges(&self) -> LogRanges<L> {
        self.relayed_haves
            .iter()
            .fold(LogRanges::empty(), |acc, r| acc.union(&r.ranges))
    }

    fn note_relayed_haves(&mut self, ranges: LogRanges<L>, ttl: T) {
        self.relayed_haves.push(Record {
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

    fn note_seen(&mut self, interest: Interest<L>, ttl: T) {
        self.seen.push(SeenWant {
            interest,
            ttl_left: ttl,
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouterAction<N, L: Log, T> {
    /// Time passes. Not enabled for zero, nor if it would carry an armed timer past due.
    Tick(T),
    /// Choose the interval until the next Want. Only when no Want timer is armed.
    ArmWantTimer(T),
    /// Choose the interval until the next Have. Only when no Have timer is
    /// armed and some seen Want asks for something this node holds
    /// (`network_ask` is non-empty): an echo of this node's own Want, or a
    /// Want for data it lacks, arms nothing. The shell, sim and conformance
    /// driver arm right after such a receipt and re-arm after each fire
    /// while that still holds, so a fire suppressed by a recent Have is
    /// retried until the Want record expires.
    ArmHaveTimer(T),
    /// Only when the Want timer is due.
    FireWant,
    /// Only when the Have timer is due.
    FireHave,
    /// Replace this node's subscriptions (the node glue's, by channel).
    Open(BTreeSet<L::Channel>),
    /// A Want arrives from another node. `from` is the wire sender (the
    /// last relayer, or the wanter itself); nothing says who wanted.
    RecvWant { from: N, interest: Interest<L> },
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
pub enum Effect<L: Log> {
    /// Broadcast a Want to the LAN: this node's own next interest, or a
    /// received one relayed whole.
    SendWant(Interest<L>),
    /// Broadcast a Have for these ranges to the LAN; the shell hydrates them
    /// into wire ops.
    SendHave(LogRanges<L>),
    /// Ranges newly learned about via a Have, to be fetched/stored above.
    Accept(LogRanges<L>),
}

impl<N: Id, L: Log, T: TimeInterval> Machine for RouterMachine<N, L, T> {
    type State = RouterState<N, L, T>;
    type Action = RouterAction<N, L, T>;
    type Fx = Vec<Effect<L>>;
    type Error = anyhow::Error;

    fn transition(&self, mut s: Self::State, action: Self::Action) -> TransitionResult<Self> {
        let mut fx = vec![];
        match action {
            RouterAction::Tick(dur) => {
                ensure!(!dur.is_zero(), "zero tick");
                ensure!(
                    s.next_due().is_none_or(|due| dur <= due),
                    "tick would carry a timer past due"
                );
                for timer in [&mut s.want_timer, &mut s.have_timer].into_iter().flatten() {
                    timer.remaining = timer.remaining - dur;
                }
                s.haves.retain(|_, r| dur < r.ttl_left);
                for r in s.haves.values_mut() {
                    r.ttl_left = r.ttl_left - dur;
                }
                s.relayed_haves.retain_mut(|r| {
                    let alive = dur < r.ttl_left;
                    if alive {
                        r.ttl_left = r.ttl_left - dur;
                    }
                    alive
                });
                s.seen.retain_mut(|r| {
                    let alive = dur < r.ttl_left;
                    if alive {
                        r.ttl_left = r.ttl_left - dur;
                    }
                    alive
                });
                s.heard.retain(|_, t| dur < *t);
                for t in s.heard.values_mut() {
                    *t = *t - dur;
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
                    !s.network_ask().is_empty(),
                    "nothing this node holds is asked for, nothing to respond with"
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
                let out = s.next_want();
                if !out.is_empty() {
                    // Own emissions are seen: that is what rate-limits an
                    // unchanged interest to once per want_ttl and stops the
                    // echo from being relayed.
                    s.note_seen(out.clone(), self.config.want_ttl);
                    fx.push(Effect::SendWant(out));
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

            RouterAction::Open(channels) => {
                s.open = channels;
            }

            RouterAction::RecvWant { from, interest } => {
                ensure!(from != s.id, "received own message");
                // Relay on first sight, whole: a Want asking for anything
                // the seen set does not already ask for goes on to peers
                // out of the sender's earshot. Checked against the seen set
                // as it stands before this receipt, so a repeat, or an echo
                // of this node's own Want, is dropped.
                let seen: Vec<&Interest<L>> = s.seen.iter().map(|r| &r.interest).collect();
                if interest.any_uncovered_by(&seen) {
                    fx.push(Effect::SendWant(interest.clone()));
                }
                // Adopt: a channel wanted by someone who holds data under it
                // (the entry lists an author) becomes a channel of interest
                // here if this node holds nothing under it yet. A pure want
                // is never adopted, so a want with no data behind it cannot
                // chain from node to node and live forever.
                for (c, e) in interest.entries() {
                    if !e.is_pure() && !s.holds_under(c) {
                        s.heard.insert(*c, self.config.heard_ttl);
                    }
                }
                s.note_seen(interest, self.config.want_ttl);
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

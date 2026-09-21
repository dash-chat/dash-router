//! The seeded driver: a [`Behavior`] over the network model.
//!
//! Every choice the exhaustive checker would enumerate is sampled here
//! from one seeded RNG: which in-flight message is delivered when
//! (latency), which is lost (loss), what interval each timer is armed
//! with (the policies under tuning). All of it lives in behavior state,
//! so a run is a deterministic function of (scenario, seed).
//!
//! One event queue entry is processed per tick. Events reference flights
//! *by value*: the model addresses its canonical sorted in-flight multiset
//! positionally, so the behavior binary-searches for the current index at
//! fire time — duplicates are identical, so any match is correct.
//!
//! Backpressure discipline: the behavior only ever proposes enabled
//! actions. Actions that would overflow the in-flight cap are deferred
//! (timer fires: the node's clock freezes at the due time, which the
//! protocol permits) or shed (appends: the writer backs off), and both
//! are counted — that's the saturation signal, not a crash.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    time::Duration,
};

use anyhow::ensure;
use dash_router_core::{
    EvictableStorage, LogRanges, NodeAction, NodeEffect, Op, RouterAction, Seq, WireBody,
};
use dash_router_net_model::Flight;
use polestar::prelude::*;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    K, LogId, Metrics, NodeId, SimNet, SimNetAction, SimNetState, policy::IntervalPolicy,
    scenario::LatencySpec,
};

/// How long a deferred event waits before retrying.
const BACKOFF: Duration = Duration::from_millis(50);
/// A delivery deferred this many times is dropped instead (counted).
const MAX_DEFERRALS: u8 = 8;

/// Driver parameters, resolved from a scenario.
#[derive(Clone, Debug)]
pub struct SimParams {
    pub n: usize,
    pub loss: f64,
    pub latency: LatencySpec,
    pub want_policy: IntervalPolicy,
    pub have_policy: IntervalPolicy,
    pub writers: u8,
    pub appends_per_sec: f64,
    pub payload_bytes: usize,
    pub duration: Duration,
    pub sample_interval: Duration,
    pub expected: BTreeMap<LogId, BTreeSet<NodeId>>,
}

#[derive(Clone, Debug)]
enum Ev {
    Deliver {
        flight: Flight<NodeId, LogId>,
        deferrals: u8,
    },
    Drop {
        flight: Flight<NodeId, LogId>,
    },
    FireWant(NodeId),
    FireHave(NodeId),
    Append,
    Sample,
}

#[derive(Clone, Debug)]
struct Entry {
    at: Duration,
    /// Tie-break for a total, deterministic order at equal times.
    seq: u64,
    ev: Ev,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed: BinaryHeap is a max-heap, we want earliest first.
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

/// What each proposed action was, so `handle_fx` can attribute effects.
#[derive(Clone, Debug)]
enum Tag {
    Plumbing,
    /// `is_have` distinguishes a Have receipt from a Want receipt (the wire
    /// no longer marks a Have as fresh-vs-reply, so that finer split is
    /// gone). `held_before` is the receiving node's router-held snapshot
    /// just before this transition, for the growth check in `handle_fx`.
    Recv {
        to: NodeId,
        is_have: bool,
        held_before: LogRanges<LogId>,
    },
    Append,
}

/// The seeded discrete-event driver. See the [module docs](self).
#[derive(Clone, Debug)]
pub struct SimBehavior {
    topology: dash_router_net_model::Topology<NodeId>,
    params: SimParams,
    rng: ChaCha8Rng,
    now: Duration,
    queue: BinaryHeap<Entry>,
    next_seq: u64,
    /// Per-(node, log) next sequence number to author: `Authored` takes an
    /// explicit seq, so this is the behavior's own bookkeeping of what
    /// each writer has authored so far.
    authored_seq: BTreeMap<(NodeId, LogId), Seq>,
    clocks: BTreeMap<NodeId, Duration>,
    known_inflight: Vec<Flight<NodeId, LogId>>,
    tags: VecDeque<Tag>,
    initialized: bool,
    append_count: u64,
    pub metrics: Metrics,
}

impl SimBehavior {
    pub fn new(
        topology: dash_router_net_model::Topology<NodeId>,
        params: SimParams,
        seed: u64,
    ) -> Self {
        let metrics = Metrics::new(params.expected.clone());
        Self {
            topology,
            params,
            rng: ChaCha8Rng::seed_from_u64(seed),
            now: Duration::ZERO,
            queue: BinaryHeap::new(),
            next_seq: 0,
            authored_seq: BTreeMap::new(),
            clocks: BTreeMap::new(),
            known_inflight: Vec::new(),
            tags: VecDeque::new(),
            initialized: false,
            append_count: 0,
            metrics,
        }
    }

    /// Simulated time of the next pending event.
    pub fn peek_at(&self) -> Option<Duration> {
        self.queue.peek().map(|e| e.at)
    }

    /// Current simulated time.
    pub fn now(&self) -> Duration {
        self.now
    }

    /// Whether the first tick (arming every node's Want timer) has run.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Whether anything other than housekeeping (fires, samples) is pending.
    pub fn has_pending_traffic(&self) -> bool {
        self.queue
            .iter()
            .any(|e| matches!(e.ev, Ev::Deliver { .. } | Ev::Drop { .. } | Ev::Append))
    }

    fn schedule(&mut self, at: Duration, ev: Ev) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push(Entry { at, seq, ev });
    }

    fn headroom(&self, state: &SimNetState) -> usize {
        K - state.inflight.len()
    }

    fn degree(&self, n: NodeId) -> usize {
        self.topology.neighbors(&n).count()
    }

    /// Advance node `n`'s clock toward `self.now`, stopping at any armed
    /// timer's due point (the model forbids ticking past one). Returns
    /// the tick applied and the want/have timer remainders after it.
    fn advance(
        &mut self,
        state: &SimNetState,
        n: NodeId,
        actions: &mut Vec<SimNetAction>,
    ) -> (Duration, Option<Duration>, Option<Duration>) {
        let node = state.node(&n);
        let want_rem = node.router.want_timer.as_ref().map(|t| *t.remaining);
        let have_rem = node.router.have_timer.as_ref().map(|t| *t.remaining);
        let clock = self.clocks.entry(n).or_default();
        let mut dt = self.now.saturating_sub(*clock);
        for rem in [want_rem, have_rem].into_iter().flatten() {
            dt = dt.min(rem);
        }
        if !dt.is_zero() {
            *clock += dt;
            actions.push(SimNetAction::Node(
                n,
                NodeAction::Router(RouterAction::Tick(dt.into())),
            ));
            self.tags.push_back(Tag::Plumbing);
        }
        (dt, want_rem.map(|r| r - dt), have_rem.map(|r| r - dt))
    }

    fn arm_want(&mut self, n: NodeId, actions: &mut Vec<SimNetAction>) {
        let interval = self.params.want_policy.sample(&mut self.rng, self.params.n);
        actions.push(SimNetAction::Node(
            n,
            NodeAction::Router(RouterAction::ArmWantTimer(interval.into())),
        ));
        self.tags.push_back(Tag::Plumbing);
        let due = self.clocks.get(&n).copied().unwrap_or_default() + interval;
        self.schedule(due.max(self.now), Ev::FireWant(n));
    }

    fn arm_have(&mut self, n: NodeId, actions: &mut Vec<SimNetAction>) {
        let interval = self.params.have_policy.sample(&mut self.rng, self.params.n);
        actions.push(SimNetAction::Node(
            n,
            NodeAction::Router(RouterAction::ArmHaveTimer(interval.into())),
        ));
        self.tags.push_back(Tag::Plumbing);
        let due = self.clocks.get(&n).copied().unwrap_or_default() + interval;
        self.schedule(due.max(self.now), Ev::FireHave(n));
    }

    /// The next sequence number to author for `(node, log)`, tracked here
    /// since `NodeAction::Authored` takes an explicit seq.
    fn next_authored_seq(&mut self, node: NodeId, log: LogId) -> Seq {
        let counter = self.authored_seq.entry((node, log)).or_insert(0);
        let seq = *counter;
        *counter += 1;
        seq
    }

    fn schedule_next_append(&mut self) {
        let mean_gap = 1.0 / self.params.appends_per_sec;
        let exp = rand_distr::Exp::new(1.0 / mean_gap).expect("rate > 0");
        let gap = Duration::from_secs_f64(self.rng.sample(exp));
        self.schedule(self.now + gap, Ev::Append);
    }

    /// Register flights the model created since we last looked, sampling
    /// each one's fate (latency, loss) and scheduling it.
    fn sync_inflight(&mut self, state: &SimNetState) {
        // Sorted-multiset difference: state.inflight \ known_inflight.
        let mut new = Vec::new();
        let mut old = self.known_inflight.iter().peekable();
        for f in &state.inflight {
            loop {
                match old.peek() {
                    // Known entry no longer present (delivered/dropped).
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
        for flight in new {
            match &flight.message.body {
                WireBody::Want(_) => self.metrics.want_msgs += 1,
                WireBody::Have(_) => self.metrics.have_msgs += 1,
            }
            let latency = self.params.latency.sample(&mut self.rng);
            let at = self.now + latency;
            if self.rng.random_bool(self.params.loss) {
                self.schedule(at, Ev::Drop { flight });
            } else {
                self.schedule(
                    at,
                    Ev::Deliver {
                        flight,
                        deferrals: 0,
                    },
                );
            }
        }
        self.known_inflight = state.inflight.clone();
    }

    fn op(&mut self) -> Op {
        self.append_count += 1;
        Op {
            header: self.append_count.to_be_bytes().to_vec(),
            payload: Some(vec![0u8; self.params.payload_bytes]),
        }
    }
}

impl Behavior for SimBehavior {
    type Model = SimNet;

    fn next_tick(&mut self, state: &SimNetState) -> anyhow::Result<Vec<SimNetAction>> {
        let mut actions = Vec::new();

        if !self.initialized {
            self.initialized = true;
            for n in state.nodes.keys().copied().collect::<Vec<_>>() {
                self.clocks.insert(n, Duration::ZERO);
                self.arm_want(n, &mut actions);
            }
            self.schedule_next_append();
            self.schedule(self.params.sample_interval, Ev::Sample);
            return Ok(actions);
        }

        let Some(entry) = self.queue.pop() else {
            return Ok(actions);
        };
        self.now = entry.at;

        match entry.ev {
            Ev::Deliver { flight, deferrals } => {
                // Every message floods: any receipt may be relayed to every
                // neighbour, so require headroom for the worst case, or the
                // tick would fail mid-flight.
                if self.headroom(state) < self.degree(flight.to) {
                    if deferrals >= MAX_DEFERRALS {
                        self.metrics.forced_drops += 1;
                        self.schedule(self.now, Ev::Drop { flight });
                    } else {
                        self.schedule(
                            self.now + BACKOFF,
                            Ev::Deliver {
                                flight,
                                deferrals: deferrals + 1,
                            },
                        );
                    }
                    return Ok(actions);
                }
                let idx = state
                    .inflight
                    .binary_search(&flight)
                    .map_err(|_| anyhow::anyhow!("scheduled flight not in flight: {flight:?}"))?;
                let to = flight.to;
                let is_want = matches!(flight.message.body, WireBody::Want(_));
                let held_before = state.node(&to).router.held.clone();
                self.advance(state, to, &mut actions);
                actions.push(SimNetAction::Deliver(UpTo::new(idx)));
                self.tags.push_back(Tag::Recv {
                    to,
                    is_have: !is_want,
                    held_before,
                });
                // A witnessed Want is what makes arming the Have timer
                // legal; do it in the same tick, right after the Recv.
                if is_want && state.node(&to).router.have_timer.is_none() {
                    self.arm_have(to, &mut actions);
                }
            }

            Ev::Drop { flight } => {
                let idx = state
                    .inflight
                    .binary_search(&flight)
                    .map_err(|_| anyhow::anyhow!("scheduled flight not in flight: {flight:?}"))?;
                self.metrics.drops += 1;
                actions.push(SimNetAction::Drop(UpTo::new(idx)));
                self.tags.push_back(Tag::Plumbing);
            }

            Ev::FireWant(n) => {
                let (_, want_rem, _) = self.advance(state, n, &mut actions);
                match want_rem {
                    Some(rem) if rem.is_zero() => {
                        if self.headroom(state) >= self.degree(n) {
                            actions.push(SimNetAction::Node(
                                n,
                                NodeAction::Router(RouterAction::FireWant),
                            ));
                            self.tags.push_back(Tag::Plumbing);
                            self.arm_want(n, &mut actions);
                        } else {
                            self.metrics.fire_backpressure += 1;
                            self.schedule(self.now + BACKOFF, Ev::FireWant(n));
                        }
                    }
                    // Not yet due (the node's clock lagged, e.g. behind a
                    // deferred fire): try again when it will be.
                    Some(rem) => self.schedule(self.now + rem, Ev::FireWant(n)),
                    // Disarmed: stale event, drop it.
                    None => {}
                }
            }

            Ev::FireHave(n) => {
                let (dt, _, have_rem) = self.advance(state, n, &mut actions);
                match have_rem {
                    Some(rem) if rem.is_zero() => {
                        if self.headroom(state) >= self.degree(n) {
                            actions.push(SimNetAction::Node(
                                n,
                                NodeAction::Router(RouterAction::FireHave),
                            ));
                            self.tags.push_back(Tag::Plumbing);
                            // Wants may still be outstanding — but only the
                            // ones that survive the tick we just proposed:
                            // arming with none witnessed is not enabled.
                            let wants_survive = state
                                .node(&n)
                                .router
                                .wants
                                .values()
                                .any(|r| dt < *r.ttl_left);
                            if wants_survive {
                                self.arm_have(n, &mut actions);
                            }
                        } else {
                            self.metrics.fire_backpressure += 1;
                            self.schedule(self.now + BACKOFF, Ev::FireHave(n));
                        }
                    }
                    Some(rem) => self.schedule(self.now + rem, Ev::FireHave(n)),
                    None => {}
                }
            }

            Ev::Append => {
                if self.now <= self.params.duration {
                    let writer: NodeId = self.rng.random_range(0..self.params.writers as u32);
                    let log: LogId = writer as LogId;
                    if self.headroom(state) >= self.degree(writer) {
                        self.advance(state, writer, &mut actions);
                        let op = self.op();
                        let seq = self.next_authored_seq(writer, log);
                        self.metrics.authored(log, seq, self.now);
                        actions.push(SimNetAction::Node(
                            writer,
                            NodeAction::Authored(log, seq, op),
                        ));
                        self.tags.push_back(Tag::Append);
                    } else {
                        self.metrics.shed_appends += 1;
                    }
                    self.schedule_next_append();
                }
            }

            Ev::Sample => {
                let usages: Vec<usize> = state
                    .nodes
                    .values()
                    .map(|n| n.relay.0.usage() as usize)
                    .collect();
                let mean = usages.iter().sum::<usize>() as f64 / usages.len().max(1) as f64;
                let max = usages.iter().copied().max().unwrap_or(0);
                self.metrics
                    .sample_occupancy(mean, max, state.inflight.len());
                if self.now < self.params.duration * 2 {
                    self.schedule(self.now + self.params.sample_interval, Ev::Sample);
                }
            }
        }

        Ok(actions)
    }

    fn handle_fx(
        &mut self,
        state: &SimNetState,
        fx: Vec<(NodeId, NodeEffect<NodeId, LogId>)>,
    ) -> anyhow::Result<Option<Vec<(NodeId, NodeEffect<NodeId, LogId>)>>> {
        let tag = self.tags.pop_front();
        ensure!(
            tag.is_some(),
            "effects arrived with no proposed action to attribute them to"
        );
        match tag.unwrap() {
            Tag::Recv {
                to,
                is_have,
                held_before,
            } => {
                self.metrics.receives += 1;
                let mut taught = false;
                for (node, effect) in &fx {
                    if let NodeEffect::Deliver(log, seq) = effect {
                        taught = true;
                        self.metrics.delivered(*node, *log, *seq, self.now);
                    }
                }
                // "Taught nothing" also covers state growth with no
                // subscriber delivery: e.g. relay-only ingest of an
                // unsubscribed log's bytes. `Deliver` only fires for novel
                // subscribed data (Task 6 brief), so the held-ranges
                // comparison is the only way to see that.
                if !taught {
                    let held_after = &state.node(&to).router.held;
                    taught = !held_after.difference(&held_before).is_empty();
                }
                if !taught {
                    self.metrics.redundant_receives += 1;
                    if is_have {
                        self.metrics.duplicate_replies += 1;
                    }
                } else if is_have {
                    self.metrics.backfill_receives += 1;
                }
            }
            Tag::Append => {
                // `metrics.authored` is recorded eagerly in `next_tick`,
                // since `Authored` takes an explicit seq the behavior
                // already knows — there is no `Effect::Store` to wait for
                // anymore.
            }
            Tag::Plumbing => {}
        }
        self.sync_inflight(state);
        Ok(None)
    }
}

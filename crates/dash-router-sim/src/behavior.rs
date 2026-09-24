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
//!
//! The behavior drives the net through [`Metered`], which does the
//! protocol accounting and hands back the flights each transition sent;
//! all this one records is what it did itself ([`DriverMetrics`]). Each
//! action goes out stamped with `now`, the meter's clock.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    time::Duration,
};

use dash_router_core::{
    EvictableStorage, LogRanges, NodeAction, Op, Ranges, RouterAction, Seq, Storage, Units,
    WireBody,
};
use dash_router_net_model::Flight;
use polestar::prelude::*;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    K, LogId, NodeId, SimNet, SimNetAction, SimNetState,
    metered::{Meter, Metered, MeteredFx},
    metrics::DriverMetrics,
    policy::IntervalPolicy,
    scenario::{AppGcSpec, LatencySpec},
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
    pub relay_cap: Units,
    pub evict_at: f64,
    pub maintain_interval: Option<Duration>,
    pub native_sync_per_sec: f64,
    pub app_gc: Option<AppGcSpec>,
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
    Maintain(NodeId),
    NativeSync,
    AppGc,
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
    /// Every op ever authored, for `NativeSync` to draw an already-real one
    /// from.
    authored_ops: BTreeMap<(LogId, Seq), Op>,
    clocks: BTreeMap<NodeId, Duration>,
    initialized: bool,
    append_count: u64,
    /// What the driver itself did: backpressure applied, samples taken.
    pub driver: DriverMetrics,
}

impl SimBehavior {
    pub fn new(
        topology: dash_router_net_model::Topology<NodeId>,
        params: SimParams,
        seed: u64,
    ) -> Self {
        Self {
            topology,
            params,
            rng: ChaCha8Rng::seed_from_u64(seed),
            now: Duration::ZERO,
            queue: BinaryHeap::new(),
            next_seq: 0,
            authored_seq: BTreeMap::new(),
            authored_ops: BTreeMap::new(),
            clocks: BTreeMap::new(),
            initialized: false,
            append_count: 0,
            driver: DriverMetrics::default(),
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
        let router = &state.node(&n).router;
        let want_rem = router.want_timer.as_ref().map(|t| *t.remaining);
        let have_rem = router.have_timer.as_ref().map(|t| *t.remaining);
        let clock = self.clocks.entry(n).or_default();
        let mut dt = self.now.saturating_sub(*clock);
        if let Some(due) = router.next_due() {
            dt = dt.min(*due);
        }
        if !dt.is_zero() {
            *clock += dt;
            actions.push(SimNetAction::Node(
                n,
                NodeAction::Router(RouterAction::Tick(dt.into())),
            ));
        }
        (dt, want_rem.map(|r| r - dt), have_rem.map(|r| r - dt))
    }

    fn arm_want(&mut self, n: NodeId, actions: &mut Vec<SimNetAction>) {
        let interval = self.params.want_policy.sample(&mut self.rng, self.params.n);
        actions.push(SimNetAction::Node(
            n,
            NodeAction::Router(RouterAction::ArmWantTimer(interval.into())),
        ));
        let due = self.clocks.get(&n).copied().unwrap_or_default() + interval;
        self.schedule(due.max(self.now), Ev::FireWant(n));
    }

    fn arm_have(&mut self, n: NodeId, actions: &mut Vec<SimNetAction>) {
        let interval = self.params.have_policy.sample(&mut self.rng, self.params.n);
        actions.push(SimNetAction::Node(
            n,
            NodeAction::Router(RouterAction::ArmHaveTimer(interval.into())),
        ));
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

    fn schedule_next_native_sync(&mut self) {
        let exp = rand_distr::Exp::new(self.params.native_sync_per_sec).expect("rate > 0");
        let gap = Duration::from_secs_f64(self.rng.sample(exp));
        self.schedule(self.now + gap, Ev::NativeSync);
    }

    /// A flight the model just sent: sample its fate (latency, loss) and
    /// schedule it.
    fn schedule_flight(&mut self, flight: Flight<NodeId, LogId>) {
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

    fn op(&mut self) -> Op {
        self.append_count += 1;
        Op {
            header: self.append_count.to_be_bytes().to_vec(),
            payload: Some(vec![0u8; self.params.payload_bytes]),
        }
    }
}

impl Behavior for SimBehavior {
    type Model = Metered<SimNet>;

    fn next_tick(
        &mut self,
        (state, _): &(SimNetState, Meter),
    ) -> anyhow::Result<Vec<(Duration, SimNetAction)>> {
        let mut actions = Vec::new();
        self.tick(state, &mut actions)?;
        // One event per tick, so everything proposed happened at `now`.
        Ok(actions.into_iter().map(|a| (self.now, a)).collect())
    }

    fn handle_fx(
        &mut self,
        _state: &(SimNetState, Meter),
        fx: MeteredFx,
    ) -> anyhow::Result<Option<MeteredFx>> {
        for flight in fx.sent {
            self.schedule_flight(flight);
        }
        // Node effects are the meter's business; nothing above wants them.
        Ok(None)
    }
}

impl SimBehavior {
    /// Process one pending event, proposing the actions it amounts to.
    fn tick(&mut self, state: &SimNetState, actions: &mut Vec<SimNetAction>) -> anyhow::Result<()> {
        if !self.initialized {
            self.initialized = true;
            for n in state.nodes.keys().copied().collect::<Vec<_>>() {
                self.clocks.insert(n, Duration::ZERO);
                self.arm_want(n, actions);
            }
            self.schedule_next_append();
            self.schedule(self.params.sample_interval, Ev::Sample);
            if let Some(interval) = self.params.maintain_interval {
                for n in state.nodes.keys().copied().collect::<Vec<_>>() {
                    self.schedule(interval, Ev::Maintain(n));
                }
            }
            if self.params.native_sync_per_sec > 0.0 {
                self.schedule_next_native_sync();
            }
            if let Some(gc) = &self.params.app_gc {
                self.schedule(Duration::from_millis(gc.interval_ms), Ev::AppGc);
            }
            return Ok(());
        }

        let Some(entry) = self.queue.pop() else {
            return Ok(());
        };
        self.now = entry.at;

        match entry.ev {
            Ev::Deliver { flight, deferrals } => {
                // Every message floods: any receipt may be relayed to every
                // neighbour, so require headroom for the worst case, or the
                // tick would fail mid-flight.
                if self.headroom(state) < self.degree(flight.to) {
                    if deferrals >= MAX_DEFERRALS {
                        self.driver.forced_drops += 1;
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
                    return Ok(());
                }
                let idx = state
                    .inflight
                    .binary_search(&flight)
                    .map_err(|_| anyhow::anyhow!("scheduled flight not in flight: {flight:?}"))?;
                let to = flight.to;
                // An echo of `to`'s own Want is not recorded, so it
                // witnesses nothing and must not arm the Have timer.
                let foreign_want =
                    matches!(flight.message.body, WireBody::Want { origin, .. } if origin != to);
                self.advance(state, to, actions);
                actions.push(SimNetAction::Deliver(UpTo::new(idx)));
                // A witnessed Want is what makes arming the Have timer
                // legal; do it in the same tick, right after the Recv.
                if foreign_want && state.node(&to).router.have_timer.is_none() {
                    self.arm_have(to, actions);
                }
            }

            Ev::Drop { flight } => {
                let idx = state
                    .inflight
                    .binary_search(&flight)
                    .map_err(|_| anyhow::anyhow!("scheduled flight not in flight: {flight:?}"))?;
                actions.push(SimNetAction::Drop(UpTo::new(idx)));
            }

            Ev::FireWant(n) => {
                let (_, want_rem, _) = self.advance(state, n, actions);
                match want_rem {
                    Some(rem) if rem.is_zero() => {
                        if self.headroom(state) >= self.degree(n) {
                            actions.push(SimNetAction::Node(
                                n,
                                NodeAction::Router(RouterAction::FireWant),
                            ));
                            self.arm_want(n, actions);
                        } else {
                            self.driver.fire_backpressure += 1;
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
                let (dt, _, have_rem) = self.advance(state, n, actions);
                match have_rem {
                    Some(rem) if rem.is_zero() => {
                        if self.headroom(state) >= self.degree(n) {
                            actions.push(SimNetAction::Node(
                                n,
                                NodeAction::Router(RouterAction::FireHave),
                            ));
                            // Wants may still be outstanding — but only the
                            // ones that survive the tick we just proposed:
                            // arming with none witnessed is not enabled.
                            let wants_survive = state
                                .node(&n)
                                .router
                                .wants
                                .values()
                                .flatten()
                                .any(|r| dt < *r.ttl_left);
                            if wants_survive {
                                self.arm_have(n, actions);
                            }
                        } else {
                            self.driver.fire_backpressure += 1;
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
                        self.advance(state, writer, actions);
                        let op = self.op();
                        let seq = self.next_authored_seq(writer, log);
                        self.authored_ops.insert((log, seq), op.clone());
                        actions.push(SimNetAction::Node(
                            writer,
                            NodeAction::Authored(log, seq, op),
                        ));
                    } else {
                        self.driver.shed_appends += 1;
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
                self.driver
                    .sample_occupancy(mean, max, state.inflight.len());
                if self.now < self.params.duration * 2 {
                    self.schedule(self.now + self.params.sample_interval, Ev::Sample);
                }
            }

            Ev::Maintain(n) => {
                let node = state.node(&n);
                let threshold =
                    ((self.params.evict_at * self.params.relay_cap as f64) as Units).max(1);
                if node.relay.0.usage() >= threshold {
                    let candidates = node.eviction_candidates();
                    if !candidates.is_empty() {
                        self.advance(state, n, actions);
                        actions.push(SimNetAction::Node(
                            n,
                            NodeAction::RelayEvictPayloads(candidates),
                        ));
                    } else {
                        // Headers-later stage: shed whole non-wanted ranges.
                        let full = node
                            .relay
                            .0
                            .held_all()
                            .difference(&node.router.others_wants());
                        if !full.is_empty() {
                            self.advance(state, n, actions);
                            actions.push(SimNetAction::Node(n, NodeAction::RelayEvict(full)));
                        }
                    }
                }
                if self.now < self.params.duration * 2 {
                    let interval = self
                        .params
                        .maintain_interval
                        .expect("scheduled only when set");
                    self.schedule(self.now + interval, Ev::Maintain(n));
                }
            }

            Ev::NativeSync => {
                if self.now <= self.params.duration {
                    if !self.authored_ops.is_empty() {
                        let idx = self.rng.random_range(0..self.authored_ops.len());
                        let ((log, seq), op) = self
                            .authored_ops
                            .iter()
                            .nth(idx)
                            .map(|(k, v)| (*k, v.clone()))
                            .expect("idx < len");
                        let subs: Vec<NodeId> = self
                            .params
                            .expected
                            .get(&log)
                            .map(|s| s.iter().copied().collect())
                            .unwrap_or_default();
                        if !subs.is_empty() {
                            let node = subs[self.rng.random_range(0..subs.len())];
                            self.advance(state, node, actions);
                            actions.push(SimNetAction::Node(
                                node,
                                NodeAction::NativeSync(log, seq, op),
                            ));
                        }
                    }
                    self.schedule_next_native_sync();
                }
            }

            Ev::AppGc => {
                let gc_spec = self.params.app_gc.clone().expect("scheduled only when set");
                for n in state.nodes.keys().copied().collect::<Vec<_>>() {
                    let node = state.node(&n);
                    let mut gc = LogRanges::empty();
                    // Hoisted: `held_all` rebuilds the whole store summary,
                    // and nothing in the loop mutates the node.
                    let ext_held = node.ext.0.held_all();
                    for log in &node.subscriptions {
                        if let Some(r) = ext_held.get(log)
                            && let Some(last) = r.last()
                            && last + 1 > gc_spec.keep_last
                        {
                            gc.insert(*log, Ranges::range(0, last + 1 - gc_spec.keep_last));
                        }
                    }
                    if !gc.is_empty() {
                        self.advance(state, n, actions);
                        actions.push(SimNetAction::Node(n, NodeAction::AppGc(gc)));
                    }
                }
                if self.now < self.params.duration {
                    self.schedule(
                        self.now + Duration::from_millis(gc_spec.interval_ms),
                        Ev::AppGc,
                    );
                }
            }
        }

        Ok(())
    }
}

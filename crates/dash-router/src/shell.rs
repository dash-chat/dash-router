//! The tokio shell's routing table (spec §2): one owner of the pure
//! [`RouterState`], the async relay/ext stores, and the debounce/maintenance
//! policy above them. Every `on_*` method advances time first (`advance_to`),
//! then runs the same glue `NodeMachine` runs in `dash-router-core`, `.await`s
//! in hand of the storage calls.
//!
//! Deliberate duplication (spec §3): this is a second, async transcription of
//! `NodeMachine`'s transition logic, not a wrapper around it — the pure core
//! machine has no way to `.await` a fallible store. A later lockstep test
//! (Task 11) drives both machines from the same action sequence and checks
//! they agree; keep this file's control flow a faithful mirror of
//! `crates/dash-router-core/src/node.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::Result;
use dash_router_core::router::RouterStateMachine;
use dash_router_core::{
    Effect, Log, LogRanges, Op, Ranges, RouterAction, RouterConfig, RouterMachine, RouterState,
    Seq, Units, WireBody, WireLog, WireMessage, eviction_candidates, ranges_of,
};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use polestar::prelude::*;
use polestar::time::RealTime;
use rand::rngs::StdRng;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::handle::{Command, RouterEvent, RouterHandle, StatsSnapshot, StorageErrorReport};
use crate::lan::is_lan;
use crate::pack::{pack_have, pack_want};
use crate::storage::{AsyncEvictableStorage, AsyncStorage, WatchableStorage};
use crate::transport::{Incoming, PeerIdentity, Transport};

/// Where a `NodeCore` gets its randomized Want/Have re-arm intervals from.
/// Production uses [`PolicyIntervals`]; tests use a scripted sequence so the
/// deterministic outcomes are checkable.
pub trait IntervalSource {
    fn next_want(&mut self) -> Duration;
    fn next_have(&mut self) -> Duration;
}

/// Production intervals: the policy crate sampled with a seeded RNG.
/// `n` is the network-size estimate (a constant until the protocol grows
/// an estimator — the sim uses the true size as an oracle).
pub struct PolicyIntervals {
    pub want: IntervalPolicy,
    pub have: IntervalPolicy,
    pub n: usize,
    pub rng: StdRng,
}

impl IntervalSource for PolicyIntervals {
    fn next_want(&mut self) -> Duration {
        self.want.sample(&mut self.rng, self.n)
    }
    fn next_have(&mut self) -> Duration {
        self.have.sample(&mut self.rng, self.n)
    }
}

#[derive(Clone, Debug)]
pub struct CoreConfig {
    pub router: RouterConfig<RealTime>,
    pub relay_cap: Units,
    /// Maintenance evicts once relay usage reaches `evict_at * relay_cap`.
    pub evict_at: f64,
    pub debounce: PushDebouncePolicy,
    /// Every broadcast is packed to encode at or under this many bytes; see
    /// [`pack::DEFAULT_MAX_WIRE_BYTES`](crate::pack::DEFAULT_MAX_WIRE_BYTES).
    pub max_wire_bytes: usize,
}

/// A single unit of shell output: either a wire message to broadcast, or an
/// event for the embedder (spec §5).
#[derive(Debug)]
pub enum Out<N, L: Log> {
    Broadcast(WireMessage<N, L>),
    Event(RouterEvent<L>),
}

/// The shell's routing table: one owner of the pure [`RouterState`] plus
/// both async stores. See the module doc for the deliberate-duplication
/// rationale relative to `NodeMachine`.
pub struct NodeCore<N: Id, L: Log, E, R, I> {
    pub router: RouterStateMachine<N, L, RealTime>,
    pub ext: E,
    pub relay: R,
    /// Subscriptions by prefix (spec 2026-09-22 §3.5): a log is subscribed
    /// iff its prefix is. Mirrors `NodeState::subscriptions`.
    pub subscriptions: BTreeSet<L::Prefix>,
    /// Derived: last known ext ∪ relay. No empty markers: a subscribed
    /// prefix with nothing stored is advertised by the Want's `prefixes`
    /// (the router's `open` set), not by a per-log marker.
    held_cache: LogRanges<L>,
    intervals: I,
    debounce: PushDebouncePolicy,
    relay_cap: Units,
    evict_at: f64,
    max_wire_bytes: usize,
    pending_push: LogRanges<L>,
    pending_since: Option<Duration>,
    latest_append: Duration,
    /// Shell time the router has been ticked up to.
    now: Duration,
    pub dropped_msgs: u64,
    pub relay_errors: u64,
    /// Items (ops, prefixes, per-log ranges) that could not fit
    /// `max_wire_bytes` even alone and were left out of a broadcast.
    pub oversize_drops: u64,
}

impl<N, L, E, R, I> NodeCore<N, L, E, R, I>
where
    N: Id + serde::Serialize + serde::de::DeserializeOwned + PeerIdentity,
    L: WireLog,
    E: AsyncStorage<L>,
    R: AsyncEvictableStorage<L>,
    I: IntervalSource,
{
    pub fn new(
        id: N,
        config: CoreConfig,
        subscriptions: BTreeSet<L::Prefix>,
        ext: E,
        relay: R,
        intervals: I,
    ) -> Self {
        // Mirrors `NodeState::new`: the router starts with the subscribed
        // prefixes open.
        let mut state = RouterState::new(id, LogRanges::empty());
        state.open = subscriptions.clone();
        let machine = RouterMachine::new(config.router);
        let router = RouterStateMachine::new(machine, state);
        Self {
            router,
            ext,
            relay,
            subscriptions,
            held_cache: LogRanges::empty(),
            intervals,
            debounce: config.debounce,
            relay_cap: config.relay_cap,
            evict_at: config.evict_at,
            max_wire_bytes: config.max_wire_bytes,
            pending_push: LogRanges::empty(),
            pending_since: None,
            latest_append: Duration::ZERO,
            now: Duration::ZERO,
            dropped_msgs: 0,
            relay_errors: 0,
            oversize_drops: 0,
        }
    }

    /// A log is subscribed iff its prefix is (`NodeState::is_subscribed`).
    pub fn is_subscribed(&self, log: &L) -> bool {
        self.subscriptions.contains(&log.prefix())
    }

    /// Read the initial held snapshot from storage and arm the first Want
    /// timer. Mirrors the sim behavior's one-time init (`arm_want` for every
    /// node before the first tick).
    pub async fn init(&mut self) -> Result<Vec<Out<N, L>>> {
        let mut out = Vec::new();
        self.reconcile_held(None, &mut out).await?;
        let next = self.intervals.next_want();
        self.router.step(RouterAction::ArmWantTimer(next.into()))?;
        Ok(out)
    }

    /// The next instant at which this node has something to do on its own
    /// (a timer fire or a debounced push flush), absolute in shell time.
    pub fn next_deadline(&self) -> Option<Duration> {
        let mut d: Option<Duration> = None;
        for t in [&self.router.want_timer, &self.router.have_timer]
            .into_iter()
            .flatten()
        {
            let due = self.now + *t.remaining;
            d = Some(d.map_or(due, |x| x.min(due)));
        }
        if let Some(oldest) = self.pending_since {
            let due = self.debounce.deadline(oldest, self.latest_append);
            d = Some(d.map_or(due, |x| x.min(due)));
        }
        d
    }

    /// The tick/fire engine (binding semantics #1). The conformance driver
    /// (Task 11) replicates this loop verbatim against `NodeMachine`, so its
    /// shape must not change.
    pub async fn advance_to(&mut self, now: Duration) -> Result<Vec<Out<N, L>>> {
        let mut out = Vec::new();
        loop {
            // 1. Fire everything due at the current instant.
            if let Some(oldest) = self.pending_since
                && self.debounce.deadline(oldest, self.latest_append) <= self.now
            {
                let fx = self.flush_push()?;
                self.route_fx(fx, &BTreeMap::new(), &BTreeSet::new(), &mut out)
                    .await?;
                continue;
            }
            if self
                .router
                .want_timer
                .as_ref()
                .is_some_and(|t| t.remaining.is_zero())
            {
                let fx = self.router.step(RouterAction::FireWant)?;
                self.route_fx(fx, &BTreeMap::new(), &BTreeSet::new(), &mut out)
                    .await?;
                let next = self.intervals.next_want();
                self.router.step(RouterAction::ArmWantTimer(next.into()))?;
                continue;
            }
            if self
                .router
                .have_timer
                .as_ref()
                .is_some_and(|t| t.remaining.is_zero())
            {
                let fx = self.router.step(RouterAction::FireHave)?;
                self.route_fx(fx, &BTreeMap::new(), &BTreeSet::new(), &mut out)
                    .await?;
                if !self.router.wants.is_empty() {
                    let next = self.intervals.next_have();
                    self.router.step(RouterAction::ArmHaveTimer(next.into()))?;
                }
                continue;
            }
            // 2. Caught up?
            if self.now >= now {
                break;
            }
            // 3. Tick to the nearest of: target, armed timers, push deadline.
            //    (All are strictly ahead of self.now after step 1.)
            let mut step = now - self.now;
            for t in [&self.router.want_timer, &self.router.have_timer]
                .into_iter()
                .flatten()
            {
                step = step.min(*t.remaining);
            }
            if let Some(oldest) = self.pending_since {
                step = step.min(self.debounce.deadline(oldest, self.latest_append) - self.now);
            }
            self.router.step(RouterAction::Tick(step.into()))?;
            self.now += step;
        }
        Ok(out)
    }

    /// A wire message arrives (spec §5 receive path). Drops per binding
    /// semantics #8 without touching state.
    pub async fn on_wire(&mut self, now: Duration, inc: Incoming) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        if let Some(remote) = inc.remote
            && !is_lan(remote)
        {
            self.dropped_msgs += 1;
            return Ok(out);
        }
        let msg: WireMessage<N, L> = match WireMessage::decode(&inc.bytes) {
            Ok(m) => m,
            Err(_) => {
                self.dropped_msgs += 1;
                return Ok(out);
            }
        };
        if msg.sender == self.router.id {
            self.dropped_msgs += 1;
            return Ok(out);
        }
        if let Some(author) = inc.author
            && msg.sender.peer_key() != Some(author)
        {
            self.dropped_msgs += 1;
            return Ok(out);
        }
        match msg.body {
            WireBody::Want { ranges, prefixes } => {
                // Finding 8(a): an EMPTY Want must not arm the have timer —
                // otherwise it arms a forever no-op fire/re-arm loop (fire
                // finds nothing to reply to, re-arms, repeats). Gate on the
                // received Want naming something — ranges or prefixes (a
                // prefix-only Want is a real request: it asks for every log
                // under the prefix) — mirrored exactly by the conformance
                // driver (see tests/conformance.rs).
                let want_nonempty = !ranges.is_empty() || !prefixes.is_empty();
                let fx = self.router.step(RouterAction::RecvWant {
                    from: msg.sender,
                    ranges,
                    prefixes,
                })?;
                self.route_fx(fx, &BTreeMap::new(), &BTreeSet::new(), &mut out)
                    .await?;
                // A witnessed Want is what makes arming the Have timer
                // legal (mirrors the sim behavior's arm-on-recv): do it
                // right after the Recv, in the same call.
                if want_nonempty && self.router.have_timer.is_none() {
                    let next = self.intervals.next_have();
                    self.router.step(RouterAction::ArmHaveTimer(next.into()))?;
                }
            }
            WireBody::Have(ops) => {
                let parked: BTreeMap<(L, Seq), Op> = ops
                    .into_iter()
                    .flat_map(|(log, seqs)| seqs.into_iter().map(move |(q, o)| ((log, q), o)))
                    .collect();
                let ranges = ranges_of(&parked);
                let fx = self.router.step(RouterAction::RecvHave {
                    from: msg.sender,
                    ranges,
                })?;
                // Ingest ALL parked bytes first (idempotent; payload
                // upgrades are invisible to the router's novelty check).
                // Keys whose ext.ingest failed must not be reported as
                // Delivered below (finding 1: Delivered must not lie).
                let failed = self.ingest_parked(&parked, &mut out).await?;
                let touched: BTreeSet<L> = parked.keys().map(|(l, _)| *l).collect();
                self.reconcile_held(Some(&touched), &mut out).await?;
                self.route_fx(fx, &parked, &failed, &mut out).await?;
            }
        }
        Ok(out)
    }

    /// Locally authored data (spec §4): ingest+reconcile immediately, but
    /// only accumulate the debounced push. The ext-ingest failure is the
    /// one storage error that fails the whole command (binding semantics
    /// #4's exception).
    pub async fn on_append(
        &mut self,
        now: Duration,
        log: L,
        seq: Seq,
        op: Op,
    ) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        self.ext
            .ingest(log, seq, op)
            .await
            .map_err(|e| anyhow::anyhow!("on_append: ext ingest failed: {e}"))?;
        let touched: BTreeSet<L> = BTreeSet::from([log]);
        self.reconcile_held(Some(&touched), &mut out).await?;
        self.pending_push = self
            .pending_push
            .union(&LogRanges::from_pairs([(log, Ranges::from_seqs([seq]))]));
        if self.pending_since.is_none() {
            self.pending_since = Some(now);
        }
        self.latest_append = now;
        Ok(out)
    }

    /// Start caring about every log under a prefix (`NodeAction::Subscribe`'s
    /// glue): migrate every relay-held log under it to `ext`, evict those
    /// logs from the relay, then open the prefix on the router.
    pub async fn on_subscribe(
        &mut self,
        now: Duration,
        prefix: L::Prefix,
    ) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        self.subscriptions.insert(prefix);
        // `NodeState::relay_logs_under`: every relay-held log under the
        // prefix, as full ranges.
        let under: LogRanges<L> = match self.relay.held_all().await {
            Ok(all) => LogRanges::from_pairs(
                all.iter()
                    .filter(|(log, _)| log.prefix() == prefix)
                    .map(|(log, _)| (*log, Ranges::full())),
            ),
            Err(_) => {
                self.relay_errors += 1;
                LogRanges::empty()
            }
        };
        // Shell-only error handling (the core's stores cannot fail, so
        // `NodeMachine` evicts `under` wholesale and conformance is
        // unaffected): evict from the relay only what provably landed in
        // ext. A failed relay fetch evicts nothing; an op whose ext ingest
        // failed stays parked in the relay. Either way the bytes survive in
        // one store instead of vanishing from both — which a prefix-wide
        // migration would otherwise do to every log under the prefix.
        if !under.is_empty() {
            match self.relay.fetch(&under).await {
                Ok(ops) => {
                    let mut ingested: BTreeMap<L, Vec<Seq>> = BTreeMap::new();
                    for (l, seq, op) in ops {
                        match self.ext.ingest(l, seq, op).await {
                            Ok(()) => ingested.entry(l).or_default().push(seq),
                            Err(e) => {
                                out.push(Out::Event(RouterEvent::StorageError(
                                    StorageErrorReport {
                                        context: "on_subscribe: ext.ingest",
                                        message: e.to_string(),
                                    },
                                )));
                            }
                        }
                    }
                    let ingested = LogRanges::from_pairs(
                        ingested
                            .into_iter()
                            .map(|(l, seqs)| (l, Ranges::from_seqs(seqs))),
                    );
                    if !ingested.is_empty() && self.relay.evict(&ingested).await.is_err() {
                        self.relay_errors += 1;
                    }
                }
                Err(_) => self.relay_errors += 1, // nothing evicted
            }
        }
        self.router
            .step(RouterAction::Open(self.subscriptions.clone()))?;
        let touched: BTreeSet<L> = under.iter().map(|(l, _)| *l).collect();
        self.reconcile_held(Some(&touched), &mut out).await?;
        Ok(out)
    }

    /// Kept-as-is ruling (binding semantics #7): remove the subscription,
    /// close the prefix on the router, and reconcile. Nothing is migrated
    /// or forgotten: still-held data keeps advertising until it is
    /// otherwise evicted, and later Haves under the prefix park in the
    /// relay. The `Held` re-step is a full rebuild (there is no per-log
    /// marker to drop any more) so the router sees the same snapshot the
    /// reference's `reconcile_held` gives it.
    pub async fn on_unsubscribe(
        &mut self,
        now: Duration,
        prefix: L::Prefix,
    ) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        self.subscriptions.remove(&prefix);
        self.router
            .step(RouterAction::Open(self.subscriptions.clone()))?;
        self.reconcile_held(None, &mut out).await?;
        Ok(out)
    }

    /// A lossy change hint from a watchable store (spec §2): empty means
    /// "anything changed," so re-read everything.
    pub async fn on_hint(&mut self, now: Duration, logs: BTreeSet<L>) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        if logs.is_empty() {
            self.reconcile_held(None, &mut out).await?;
        } else {
            self.reconcile_held(Some(&logs), &mut out).await?;
        }
        Ok(out)
    }

    /// Task 4's policy, mirrored exactly: evict payloads nobody wants once
    /// usage crosses `evict_at * relay_cap`; if nothing qualifies, shed
    /// whole unwanted ranges instead (headers included).
    pub async fn on_maintain(&mut self, now: Duration) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        let usage = match self.relay.usage().await {
            Ok(u) => u,
            Err(_) => {
                self.relay_errors += 1;
                return Ok(out);
            }
        };
        let threshold = ((self.evict_at * self.relay_cap as f64) as Units).max(1);
        if usage >= threshold {
            let held_payloads = match self.relay.held_payloads().await {
                Ok(h) => h,
                Err(_) => {
                    self.relay_errors += 1;
                    return Ok(out);
                }
            };
            let others_wants = self.router.others_wants();
            let candidates = eviction_candidates(&held_payloads, &others_wants);
            if !candidates.is_empty() {
                // Payloads-first: headers survive, so no `Held` snapshot is
                // needed.
                if self.relay.evict_payloads(&candidates).await.is_err() {
                    self.relay_errors += 1;
                }
            } else {
                let held_all = match self.relay.held_all().await {
                    Ok(h) => h,
                    Err(_) => {
                        self.relay_errors += 1;
                        return Ok(out);
                    }
                };
                let full = held_all.difference(&others_wants);
                if !full.is_empty() {
                    if self.relay.evict(&full).await.is_err() {
                        self.relay_errors += 1;
                    } else {
                        let touched: BTreeSet<L> = full.iter().map(|(l, _)| *l).collect();
                        self.reconcile_held(Some(&touched), &mut out).await?;
                    }
                }
            }
        }
        Ok(out)
    }

    /// Take the accumulated pending push and run it through the router, if
    /// non-empty.
    fn flush_push(&mut self) -> Result<Vec<Effect<L>>> {
        let ranges = std::mem::replace(&mut self.pending_push, LogRanges::empty());
        self.pending_since = None;
        if ranges.is_empty() {
            Ok(Vec::new())
        } else {
            self.router.step(RouterAction::Push(ranges))
        }
    }

    /// Turn the router's decisions into shell-level output, in fx order —
    /// the async transcription of `NodeMachine::route_router_fx`.
    async fn route_fx(
        &mut self,
        fx: Vec<Effect<L>>,
        parked: &BTreeMap<(L, Seq), Op>,
        failed: &BTreeSet<(L, Seq)>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<()> {
        for e in fx {
            match e {
                Effect::Accept(novel) => {
                    for (log, r) in novel.iter() {
                        if !self.is_subscribed(log) {
                            continue;
                        }
                        // Never iterate `Ranges` directly — it may be open;
                        // walk the parked keys instead.
                        for (pl, seq) in parked.keys() {
                            // Finding 1: a key whose ext.ingest failed did
                            // NOT land in ext, so it must not be reported as
                            // Delivered — Delivered must not lie.
                            if pl == log && r.contains(*seq) && !failed.contains(&(*pl, *seq)) {
                                out.push(Out::Event(RouterEvent::Delivered(*log, *seq)));
                            }
                        }
                    }
                }
                Effect::SendWant { ranges, prefixes } => {
                    let (msgs, dropped) =
                        pack_want(self.router.id, ranges, prefixes, self.max_wire_bytes);
                    self.oversize_drops += dropped;
                    out.extend(msgs.into_iter().map(Out::Broadcast));
                }
                Effect::SendHave(r) => {
                    for msg in self.hydrate(&r, out).await? {
                        out.push(Out::Broadcast(msg));
                    }
                }
            }
        }
        Ok(())
    }

    /// Merge relay + ext fetches into hydrated Haves, mirroring the
    /// `SendHave` arm of `route_router_fx` (binding semantics #3). Yields
    /// one *or more* Haves (none if nothing is held): the ops are packed
    /// under `max_wire_bytes` (spec 2026-09-22 §3.3), so the model's single
    /// Have and the shell's split Haves carry the same ops in the same
    /// `(log, seq)` order — save that an op too big to fit alone goes
    /// header-only, and a header too big to fit alone is dropped (counted in
    /// `oversize_drops`). Relay errors are silent shrinkage (counted in
    /// `relay_errors`); ext errors are reported but don't stop hydration.
    async fn hydrate(
        &mut self,
        r: &LogRanges<L>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<Vec<WireMessage<N, L>>> {
        let mut ops = match self.relay.fetch(r).await {
            Ok(v) => v,
            Err(_) => {
                self.relay_errors += 1;
                Vec::new()
            }
        };
        match self.ext.fetch(r).await {
            Ok(more) => ops.extend(more),
            Err(e) => {
                out.push(Out::Event(RouterEvent::StorageError(StorageErrorReport {
                    context: "hydrate: ext.fetch",
                    message: e.to_string(),
                })));
            }
        }
        // Order by (log, seq), with a payload-bearing copy first within a
        // run: relay and ext can each hold their own copy of the same
        // (log, seq), and the dedup below keeps only the first — it must
        // not be the degraded one.
        ops.sort_by(|a, b| {
            (&a.0, a.1)
                .cmp(&(&b.0, b.1))
                .then_with(|| b.2.payload.is_some().cmp(&a.2.payload.is_some()))
        });
        ops.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        let (msgs, dropped) = pack_have(self.router.id, ops, self.max_wire_bytes);
        self.oversize_drops += dropped;
        Ok(msgs)
    }

    /// Cheap and unconditional after any storage change (binding semantics
    /// #2, #6, #7): `touched` reconciles just those logs; `None` is a full
    /// rebuild (init, or a lossy hint with no specific logs named).
    async fn reconcile_held(
        &mut self,
        touched: Option<&BTreeSet<L>>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<()> {
        match touched {
            Some(logs) => {
                let ext_held = match self.ext.held_of(logs).await {
                    Ok(h) => h,
                    Err(e) => {
                        out.push(Out::Event(RouterEvent::StorageError(StorageErrorReport {
                            context: "reconcile_held: ext.held_of",
                            message: e.to_string(),
                        })));
                        return Ok(()); // stale held_cache stands
                    }
                };
                let relay_held = match self.relay.held_of(logs).await {
                    Ok(h) => h,
                    Err(_) => {
                        self.relay_errors += 1;
                        return Ok(()); // stale held_cache stands
                    }
                };
                let merged = ext_held.union(&relay_held);
                for log in logs {
                    let m = merged.get(log).cloned().unwrap_or_else(Ranges::empty);
                    if m.is_empty() {
                        self.held_cache.remove(log);
                    } else {
                        self.held_cache.insert(*log, m);
                    }
                }
            }
            None => {
                let ext_all = match self.ext.held_all().await {
                    Ok(h) => h,
                    Err(e) => {
                        out.push(Out::Event(RouterEvent::StorageError(StorageErrorReport {
                            context: "reconcile_held: ext.held_all",
                            message: e.to_string(),
                        })));
                        return Ok(());
                    }
                };
                let relay_all = match self.relay.held_all().await {
                    Ok(h) => h,
                    Err(_) => {
                        self.relay_errors += 1;
                        return Ok(());
                    }
                };
                self.held_cache = ext_all.union(&relay_all);
            }
        }
        self.router
            .step(RouterAction::Held(self.held_cache.clone()))?;
        Ok(())
    }

    /// For each parked op: subscribed logs go to `ext`; unsubscribed logs go
    /// to `relay`, shed silently (not an error) if ingesting would exceed
    /// the relay's cap — the shell sheds until an eviction frees room.
    async fn ingest_parked(
        &mut self,
        parked: &BTreeMap<(L, Seq), Op>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<BTreeSet<(L, Seq)>> {
        let mut failed = BTreeSet::new();
        // Mirrors `NodeMachine::ingest_parked`: `usage()` is O(store) (for
        // `DiskRelayStore` a derived sum over every log's ranges — see its
        // `usage` doc), so read it once and track it across the batch rather
        // than re-reading per op. `None` means "unknown, re-read before the
        // next check": initially, and after any relay error, since the
        // trait doesn't promise a failed ingest left the store untouched.
        // A usage read that fails still sheds just that one op, as before.
        //
        // Tracking can only over-count, never under-count: `ingest_delta`
        // is served from the store's cache, which may under-report what is
        // really on disk (after a degraded rebuild), in which case the real
        // ingest is a duplicate and adds less than `units`. Over-counting
        // only sheds a little early within this batch, and the next batch's
        // fresh read corrects it — the same under-report-not-over-report
        // direction the disk store itself lives by.
        let mut usage: Option<Units> = None;
        for ((log, seq), op) in parked {
            if self.is_subscribed(log) {
                if let Err(e) = self.ext.ingest(*log, *seq, op.clone()).await {
                    failed.insert((*log, *seq));
                    out.push(Out::Event(RouterEvent::StorageError(StorageErrorReport {
                        context: "ingest_parked: ext.ingest",
                        message: e.to_string(),
                    })));
                }
            } else {
                let units = match self.relay.ingest_delta(log, *seq, op).await {
                    Ok(u) => u,
                    Err(_) => {
                        self.relay_errors += 1;
                        usage = None;
                        continue;
                    }
                };
                let current = match usage {
                    Some(u) => u,
                    None => match self.relay.usage().await {
                        Ok(u) => u,
                        Err(_) => {
                            self.relay_errors += 1;
                            continue;
                        }
                    },
                };
                if current + units > self.relay_cap {
                    usage = Some(current);
                    continue; // shed: no room, and no eviction happened yet
                }
                if self.relay.ingest(*log, *seq, op.clone()).await.is_err() {
                    self.relay_errors += 1;
                    usage = None;
                } else {
                    usage = Some(current + units);
                }
            }
        }
        Ok(failed)
    }
}

/// The thin rind (spec §2, §5): one tokio task running [`NodeCore`] behind
/// a [`RouterHandle`] and an event stream, wired to a real [`Transport`].
///
/// `hints = ext.changed()` is taken before `ext` moves into the `NodeCore`
/// (the core's stored sender keeps the broadcast channel alive, so `hints`
/// never sees `Closed` while the task runs).
#[allow(clippy::too_many_arguments)] // spec §5's literal API; not a config struct by design
pub fn spawn<N, L, E, R, T, I>(
    id: N,
    config: CoreConfig,
    maintain_interval: Duration,
    subscriptions: BTreeSet<L::Prefix>,
    ext: E,
    relay: R,
    transport: T,
    intervals: I,
) -> (
    RouterHandle<L>,
    mpsc::Receiver<RouterEvent<L>>,
    JoinHandle<Result<()>>,
)
where
    N: Id + serde::Serialize + serde::de::DeserializeOwned + PeerIdentity + Send + 'static,
    L: WireLog + Send + 'static,
    E: AsyncStorage<L> + WatchableStorage<L> + Send + 'static,
    R: AsyncEvictableStorage<L> + Send + 'static,
    T: Transport + Send + 'static,
    I: IntervalSource + Send + 'static,
{
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command<L>>(64);
    let (event_tx, event_rx) = mpsc::channel::<RouterEvent<L>>(64);

    let mut hints = ext.changed();
    // Finding 8(b): once the hints channel reports `Closed`, `hints.recv()`
    // returns `Closed` immediately forever, which would busy-loop `select!`
    // if the arm stayed enabled. Guard the arm with this flag instead of
    // panicking (the core holds the sending store alive, so `Closed` isn't
    // expected in practice, but a trait method here must never panic).
    let mut hints_open = true;
    let mut core = NodeCore::new(id, config, subscriptions, ext, relay, intervals);
    let mut transport = transport;

    let task = tokio::spawn(async move {
        let epoch = tokio::time::Instant::now();
        let mut maintain = tokio::time::interval(maintain_interval);
        maintain.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let init_out = core.init().await?;
        if !route_outs(init_out, &mut transport, &event_tx).await? {
            return Ok(()); // transport already gone; clean stop, drain nothing
        }

        loop {
            let deadline = core
                .next_deadline()
                .unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 365));
            let sleep = tokio::time::sleep_until(epoch + deadline);

            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        None | Some(Command::Shutdown) => break,
                        Some(Command::Append { log, seq, op, reply }) => {
                            let now = epoch.elapsed();
                            match core.on_append(now, log, seq, op).await {
                                Ok(out) => {
                                    let _ = reply.send(Ok(()));
                                    if !route_outs(out, &mut transport, &event_tx).await? {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    continue;
                                }
                            }
                        }
                        Some(Command::Subscribe { prefix, reply }) => {
                            let now = epoch.elapsed();
                            let out = core.on_subscribe(now, prefix).await?;
                            let _ = reply.send(Ok(()));
                            if !route_outs(out, &mut transport, &event_tx).await? {
                                break;
                            }
                        }
                        Some(Command::Unsubscribe { prefix, reply }) => {
                            let now = epoch.elapsed();
                            let out = core.on_unsubscribe(now, prefix).await?;
                            let _ = reply.send(Ok(()));
                            if !route_outs(out, &mut transport, &event_tx).await? {
                                break;
                            }
                        }
                        Some(Command::Stats { reply }) => {
                            let _ = reply.send(StatsSnapshot {
                                dropped_msgs: core.dropped_msgs,
                                relay_errors: core.relay_errors,
                                relay_store_errors: core.relay.error_count(),
                                oversize_drops: core.oversize_drops,
                            });
                        }
                    }
                }
                inc = transport.recv() => {
                    match inc {
                        None => break,
                        Some(inc) => {
                            let now = epoch.elapsed();
                            let out = core.on_wire(now, inc).await?;
                            if !route_outs(out, &mut transport, &event_tx).await? {
                                break;
                            }
                        }
                    }
                }
                hint = hints.recv(), if hints_open => {
                    let now = epoch.elapsed();
                    let out = match hint {
                        Ok(logs) => core.on_hint(now, logs).await?,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            core.on_hint(now, BTreeSet::new()).await?
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Not expected (the core holds the sending store
                            // alive) but never panic on it: disable further
                            // hint polling so a `Closed` receiver (which
                            // would otherwise be immediately ready forever)
                            // can't busy-loop the select.
                            hints_open = false;
                            Vec::new()
                        }
                    };
                    if !route_outs(out, &mut transport, &event_tx).await? {
                        break;
                    }
                }
                _ = sleep => {
                    let now = epoch.elapsed();
                    let out = core.advance_to(now).await?;
                    if !route_outs(out, &mut transport, &event_tx).await? {
                        break;
                    }
                }
                _ = maintain.tick() => {
                    let now = epoch.elapsed();
                    let out = core.on_maintain(now).await?;
                    if !route_outs(out, &mut transport, &event_tx).await? {
                        break;
                    }
                }
            }
        }
        Ok(())
    });

    (RouterHandle::new(cmd_tx), event_rx, task)
}

/// Route a batch of [`Out`] values: broadcasts go to the transport, events
/// go to the embedder's stream (ignoring a closed receiver — an embedder
/// that dropped the stream still gets gossip). Returns `false` if the
/// transport is gone and the loop should end.
async fn route_outs<N, L, T>(
    out: Vec<Out<N, L>>,
    transport: &mut T,
    event_tx: &mpsc::Sender<RouterEvent<L>>,
) -> Result<bool>
where
    N: serde::Serialize + serde::de::DeserializeOwned,
    L: WireLog,
    T: Transport,
{
    for o in out {
        match o {
            Out::Broadcast(msg) => {
                if transport.broadcast(msg.encode()).await.is_err() {
                    return Ok(false);
                }
            }
            Out::Event(e) => {
                let _ = event_tx.send(e).await;
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use dash_router_core::{
        EvictableStorage, LogRanges, Op, OpsMap, Ranges, RouterConfig, Storage, WireBody,
        WireMessage,
    };
    // NOTE: OpsMap has both the sync traits and (via the blanket bridge) the
    // async ones; assertions below use UFCS on the sync traits to
    // disambiguate.

    use super::*;
    use crate::handle::RouterEvent;
    use crate::transport::Incoming;

    /// Deterministic intervals for tests: pops from the front, repeats the
    /// last entry forever.
    struct Scripted(Vec<Duration>, usize);
    impl Scripted {
        fn ms(script: &[u64]) -> Self {
            Scripted(
                script.iter().map(|&m| Duration::from_millis(m)).collect(),
                0,
            )
        }
        fn next(&mut self) -> Duration {
            let i = self.1.min(self.0.len() - 1);
            self.1 += 1;
            self.0[i]
        }
    }
    impl IntervalSource for Scripted {
        fn next_want(&mut self) -> Duration {
            self.next()
        }
        fn next_have(&mut self) -> Duration {
            self.next()
        }
    }

    type Core = NodeCore<u32, u8, OpsMap<u8>, OpsMap<u8>, Scripted>;

    fn config(cap: Units) -> CoreConfig {
        CoreConfig {
            router: RouterConfig {
                want_ttl: Duration::from_millis(500).into(),
                have_ttl: Duration::from_millis(500).into(),
            },
            relay_cap: cap,
            evict_at: 0.75,
            debounce: PushDebouncePolicy {
                window_ms: 100,
                max_latency_ms: 250,
            },
            max_wire_bytes: 3800,
        }
    }

    async fn core(cap: Units, subs: &[u8]) -> Core {
        let mut c = NodeCore::new(
            0u32,
            config(cap),
            subs.iter().copied().collect::<BTreeSet<u8>>(),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(&[100]),
        );
        c.init().await.unwrap();
        c
    }

    fn op(h: u8, payload: bool) -> Op {
        Op {
            header: vec![h],
            payload: payload.then(|| vec![h; 4]),
        }
    }

    fn wire(msg: WireMessage<u32, u8>) -> Incoming {
        Incoming {
            remote: Some("192.168.0.9".parse().unwrap()),
            author: None,
            bytes: msg.encode(),
        }
    }

    fn broadcasts(out: &[Out<u32, u8>]) -> Vec<&WireMessage<u32, u8>> {
        out.iter()
            .filter_map(|o| match o {
                Out::Broadcast(m) => Some(m),
                _ => None,
            })
            .collect()
    }

    fn delivered(out: &[Out<u32, u8>]) -> Vec<(u8, u32)> {
        out.iter()
            .filter_map(|o| match o {
                Out::Event(RouterEvent::Delivered(l, s)) => Some((*l, *s)),
                _ => None,
            })
            .collect()
    }

    /// Mirror of `recv_have_routes_bytes_delivers_and_rebroadcasts_hydrated`
    /// in dash-router-core/tests/node.rs, over the async routing table.
    #[tokio::test]
    async fn recv_have_delivers_subscribed_and_rebroadcasts_hydrated() {
        let mut c = core(100, &[0]).await;
        let have = WireMessage::have(
            7,
            vec![(0u8, vec![(0, op(1, true))]), (1u8, vec![(0, op(2, true))])],
        );
        let out = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
        assert_eq!(
            delivered(&out),
            vec![(0, 0)],
            "only the subscribed log delivers"
        );
        let bs = broadcasts(&out);
        assert_eq!(bs.len(), 1, "the flood relays once, hydrated");
        let WireBody::Have(groups) = &bs[0].body else {
            panic!("expected Have")
        };
        assert_eq!(groups.len(), 2, "both logs rebroadcast");
        // Subscribed bytes in ext, the rest in the relay.
        assert!(Storage::held_all(&c.ext).contains(&0, 0));
        assert!(Storage::held_all(&c.relay).contains(&1, 0));
    }

    /// Shed-at-cap: parked ops that don't fit are dropped silently and the
    /// rebroadcast carries only what was stored (the documented truncation).
    #[tokio::test]
    async fn relay_sheds_at_cap_and_broadcasts_only_stored_ops() {
        let mut c = core(2, &[]).await; // room for exactly one payload op
        let have = WireMessage::have(7, vec![(1u8, vec![(0, op(1, true)), (1, op(2, true))])]);
        let out = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
        assert_eq!(
            EvictableStorage::usage(&c.relay),
            2,
            "one op stored, one shed"
        );
        let bs = broadcasts(&out);
        let WireBody::Have(groups) = &bs[0].body else {
            panic!()
        };
        assert_eq!(groups[0].1.len(), 1, "hydration only finds the stored op");
    }

    /// Mirror of core `prefix::subscribe_prefix_migrates_all_relay_logs_under_it`
    /// and review focus 1, over the async core with `Pair` logs.
    #[tokio::test]
    async fn subscribe_prefix_migrates_relay_logs_and_delivers_new_authors() {
        use dash_router_core::Pair;
        // The module's `wire` helper is typed for `u8` logs; this test's
        // logs are `Pair`, so encode the same LAN-sourced `Incoming` here.
        let wire = |msg: WireMessage<u32, Pair>| Incoming {
            remote: Some("192.168.0.9".parse().unwrap()),
            author: None,
            bytes: msg.encode(),
        };
        let mut c: NodeCore<u32, Pair, OpsMap<Pair>, OpsMap<Pair>, Scripted> = NodeCore::new(
            0,
            config(1 << 20),
            BTreeSet::new(),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(&[1000]),
        );
        c.init().await.unwrap();
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let have = |from: u32, log: Pair, seqs: &[u32]| {
            WireMessage::have(
                from,
                vec![(log, seqs.iter().map(|&q| (q, op(q as u8, true))).collect())],
            )
        };
        c.on_wire(Duration::from_millis(10), wire(have(7, a1, &[0])))
            .await
            .unwrap();
        c.on_wire(Duration::from_millis(11), wire(have(7, a2, &[0, 1])))
            .await
            .unwrap();
        assert!(
            Storage::held_all(&c.ext).is_empty(),
            "unsubscribed: relay only"
        );

        c.on_subscribe(Duration::from_millis(20), 1u8)
            .await
            .unwrap();
        assert_eq!(
            Storage::held_all(&c.ext).get(&a1),
            Some(&Ranges::range(0, 1))
        );
        assert_eq!(
            Storage::held_all(&c.ext).get(&a2),
            Some(&Ranges::range(0, 2))
        );
        assert!(Storage::held_all(&c.relay).is_empty());
        assert_eq!(c.router.open, BTreeSet::from([1u8]));

        let a3 = Pair::new(1, 3);
        let out = c
            .on_wire(Duration::from_millis(30), wire(have(7, a3, &[0])))
            .await
            .unwrap();
        assert!(
            out.iter()
                .any(|o| matches!(o, Out::Event(RouterEvent::Delivered(l, 0)) if *l == a3))
        );
    }

    /// Review focus 6: a Want naming no ranges and no prefixes must not arm
    /// the have timer; a prefix-only Want must.
    #[tokio::test]
    async fn prefix_only_want_arms_have_timer_but_empty_want_does_not() {
        let mut c = core(100, &[]).await;
        let empty = WireMessage::want(7, LogRanges::<u8>::empty(), BTreeSet::new());
        c.on_wire(Duration::from_millis(1), wire(empty))
            .await
            .unwrap();
        assert!(c.router.have_timer.is_none(), "empty Want must not arm");
        let prefix_only = WireMessage::want(7, LogRanges::<u8>::empty(), BTreeSet::from([0u8]));
        c.on_wire(Duration::from_millis(2), wire(prefix_only))
            .await
            .unwrap();
        assert!(c.router.have_timer.is_some(), "prefix-only Want arms");
    }

    /// Push debounce: appends accumulate; the flush pushes one hydrated Have
    /// carrying every pending op; a steady stream is capped by max_latency.
    #[tokio::test]
    async fn append_debounce_flushes_one_have_for_the_batch() {
        let mut c = core(100, &[0]).await;
        let ms = Duration::from_millis;
        let out = c.on_append(ms(0), 0, 0, op(1, true)).await.unwrap();
        assert!(broadcasts(&out).is_empty(), "no immediate push");
        let out = c.on_append(ms(50), 0, 1, op(2, true)).await.unwrap();
        assert!(broadcasts(&out).is_empty());
        assert_eq!(
            c.next_deadline(),
            Some(ms(100)),
            "want timer at 100 ties the flush window 50+100; flush due at 150"
        );
        let out = c.advance_to(ms(150)).await.unwrap();
        let haves: Vec<_> = broadcasts(&out)
            .into_iter()
            .filter(|m| matches!(m.body, WireBody::Have(_)))
            .collect();
        assert_eq!(haves.len(), 1, "one flush for the whole batch");
        let WireBody::Have(groups) = &haves[0].body else {
            panic!()
        };
        assert_eq!(groups[0].1.len(), 2, "both appends carried");
    }

    /// The want timer fires on schedule and a peer's Want arms the have timer;
    /// the eventual Have reply is hydrated from storage.
    ///
    /// Finding 6 (test honesty): the original version of this test appended
    /// at t=100 and only advanced to t=400 afterward, so its final
    /// assertion ("the reply is hydrated") was actually satisfied by the
    /// *debounce flush* at t=200 (the append's own push), not by the
    /// have-timer fire it claimed to exercise — the have timer's fire found
    /// `recent_haves` already noting that range (from the flush) and
    /// emitted nothing. Restructured (small option from the brief): push
    /// the debounce window far out of reach for this test (so the append's
    /// own flush can never race the have timer), then show the have-timer's
    /// fire is what genuinely hydrates and replies to the peer's Want.
    #[tokio::test]
    async fn want_fire_and_have_reply_flow() {
        let mut config = config(100);
        config.debounce = PushDebouncePolicy {
            window_ms: 100_000,
            max_latency_ms: 100_000,
        }; // never flushes within this test's timeline
        // Scripted intervals consumed in order: [0] the initial Want-timer
        // arm (fires at t=100); [1] that fire's re-arm, made huge so a
        // second Want fire can't interfere; [2] the Have-timer arm on
        // witnessing the peer's Want (fires 100ms later, at t=250).
        let mut c: Core = NodeCore::new(
            0u32,
            config,
            BTreeSet::from([0u8]),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(&[100, 100_000, 100]),
        );
        c.init().await.unwrap();

        let out = c.advance_to(Duration::from_millis(100)).await.unwrap();
        let bs = broadcasts(&out);
        // No empty per-log marker any more: a subscribed prefix with nothing
        // stored is wanted through the Want's `prefixes`, not a `(0, full)`
        // range.
        assert!(
            matches!(
                &bs[0].body,
                WireBody::Want { ranges, prefixes }
                    if prefixes.contains(&0) && ranges.get(&0).is_none()
            ),
            "fresh subscription wants the whole prefix"
        );

        // Seed storage: `held` updates immediately (before any debounced
        // push flushes — which, per the config above, won't happen within
        // this test at all), so the have timer below sees genuinely fresh,
        // never-yet-sent data.
        let _ = c
            .on_append(Duration::from_millis(110), 0, 0, op(1, true))
            .await
            .unwrap();

        let want = WireMessage::want(
            7,
            LogRanges::from_pairs([(0u8, Ranges::full())]),
            BTreeSet::new(),
        );
        let _out = c
            .on_wire(Duration::from_millis(150), wire(want))
            .await
            .unwrap();
        // NOTE (deviation from the brief's literal assertion, see task-9
        // report): RouterMachine's RecvWant relays only the range NOT
        // already in `relayed_want_ranges()` (the flood's seen-set,
        // DESIGN.md-mandated termination). Our own FireWant at t=100
        // already flooded the full range for log 0 with a 500ms want_ttl,
        // so a peer's identical want at t=150 is witnessed (recorded in
        // `wants`, which is what legalizes arming the have timer) but is
        // correctly NOT re-flooded — re-broadcasting it would be redundant
        // per the seen-set's own accounting. This is core, unmodified
        // behavior (verified directly against `RouterMachine::transition`),
        // not a shell bug.
        assert!(
            c.router.wants.contains_key(&7),
            "the peer's want is witnessed"
        );
        assert!(
            c.router.have_timer.is_some(),
            "witnessed Want arms the have timer"
        );
        // The push from the append is still pending (not yet flushed) and,
        // by construction (window/max_latency both 100s), cannot flush
        // within this test's timeline — so the broadcast captured below can
        // only be the have-timer's own fire.
        assert!(c.pending_since.is_some(), "the push has not flushed");
        let out = c.advance_to(Duration::from_millis(400)).await.unwrap();
        assert!(
            broadcasts(&out)
                .iter()
                .any(|m| matches!(&m.body, WireBody::Have(g) if !g.is_empty())),
            "the have-timer's own fire hydrates and replies — the debounced push, by \
             construction, could not have produced this broadcast"
        );
    }

    /// Spec 2026-09-22 §3.3: a Have reply bigger than `max_wire_bytes` is
    /// split into several Haves, each under budget, together carrying every
    /// op in `(log, seq)` order.
    #[tokio::test]
    async fn have_reply_is_packed_under_max_wire_bytes() {
        let mut config = config(100);
        config.max_wire_bytes = 600;
        config.debounce = PushDebouncePolicy {
            window_ms: 100_000,
            max_latency_ms: 100_000,
        }; // the appends' own push never flushes; only the have timer replies
        // Scripted: [0] initial Want arm, far out of reach; [1..] the Have
        // arm on witnessing the peer's Want (fires 100ms later).
        let mut c: Core = NodeCore::new(
            0u32,
            config,
            BTreeSet::from([0u8]),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(&[100_000, 100]),
        );
        c.init().await.unwrap();
        let big = |h: u8| Op {
            header: vec![h],
            payload: Some(vec![h; 300]),
        };
        for q in 0..3u32 {
            let _ = c
                .on_append(Duration::from_millis(1), 0, q, big(q as u8))
                .await
                .unwrap();
        }
        let want = WireMessage::want(
            7,
            LogRanges::from_pairs([(0u8, Ranges::full())]),
            BTreeSet::new(),
        );
        let _ = c
            .on_wire(Duration::from_millis(10), wire(want))
            .await
            .unwrap();
        let out = c.advance_to(Duration::from_millis(200)).await.unwrap();
        let haves: Vec<_> = broadcasts(&out)
            .into_iter()
            .filter(|m| matches!(m.body, WireBody::Have(_)))
            .collect();
        assert!(
            haves.len() >= 2,
            "3 × ~305 bytes cannot fit one 600-byte Have"
        );
        let mut seen = Vec::new();
        for m in &haves {
            assert!(m.encode().len() <= 600, "Have over max_wire_bytes");
            let WireBody::Have(groups) = &m.body else {
                unreachable!()
            };
            for (log, seqs) in groups {
                for (seq, op) in seqs {
                    assert!(op.payload.is_some(), "fits alone, so sent whole");
                    seen.push((*log, *seq));
                }
            }
        }
        assert_eq!(seen, vec![(0, 0), (0, 1), (0, 2)], "all ops, in order");
        assert_eq!(c.oversize_drops, 0);
    }

    /// Spec 2026-09-22 §3.3 end to end: a Want that `pack_want` splits into
    /// several wire messages is recorded whole by the receiver — prefixes
    /// (first piece) and every piece's ranges — not just the last piece.
    #[tokio::test]
    async fn split_want_is_fully_recorded_by_the_receiver() {
        let ranges = LogRanges::from_pairs((0..200u8).map(|l| (l, Ranges::from(3))));
        let prefixes: BTreeSet<u8> = (100..110).collect();
        let (msgs, dropped) = crate::pack::pack_want(7u32, ranges.clone(), prefixes.clone(), 200);
        assert_eq!(dropped, 0);
        assert!(msgs.len() >= 3, "the Want must actually split");
        let mut c = core(100, &[]).await;
        for (i, msg) in msgs.into_iter().enumerate() {
            let _ = c
                .on_wire(Duration::from_millis(i as u64), wire(msg))
                .await
                .unwrap();
        }
        assert_eq!(c.router.others_wants(), ranges, "every piece's ranges");
        assert_eq!(c.router.others_prefixes(), prefixes, "the prefix piece too");
    }

    /// Maintenance evicts payloads nobody wants once usage crosses the line.
    #[tokio::test]
    async fn maintain_evicts_payloads_first() {
        let mut c = core(4, &[]).await;
        let have = WireMessage::have(7, vec![(1u8, vec![(0, op(1, true)), (1, op(2, true))])]);
        let _ = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
        assert_eq!(EvictableStorage::usage(&c.relay), 4);
        let _ = c.on_maintain(Duration::from_millis(1)).await.unwrap();
        assert_eq!(
            EvictableStorage::usage(&c.relay),
            2,
            "payloads evicted, headers kept"
        );
        assert!(EvictableStorage::held_payloads(&c.relay).is_empty());
        assert!(
            !Storage::held_all(&c.relay).is_empty(),
            "still advertising headers"
        );
    }

    /// Non-LAN senders, garbage bytes, and own echoes are dropped statelessly.
    #[tokio::test]
    async fn on_wire_drops_foreign_garbage_and_echoes() {
        let mut c = core(100, &[0]).await;
        let held = c.router.held.clone();
        let foreign = Incoming {
            remote: Some("8.8.8.8".parse().unwrap()),
            author: None,
            bytes: WireMessage::have(7, vec![(0u8, vec![(0, op(1, true))])]).encode(),
        };
        let garbage = Incoming {
            remote: Some("192.168.0.9".parse().unwrap()),
            author: None,
            bytes: vec![0xff, 0x00],
        };
        let echo = wire(WireMessage::want(
            0,
            LogRanges::from_pairs([(0u8, Ranges::full())]),
            BTreeSet::new(),
        ));
        for inc in [foreign, garbage, echo] {
            assert!(c.on_wire(Duration::ZERO, inc).await.unwrap().is_empty());
        }
        assert_eq!(c.dropped_msgs, 3);
        assert_eq!(c.router.held, held, "no state change from dropped input");
    }

    /// A wire identity with a transport key, for the sender check.
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        serde::Serialize,
        serde::Deserialize,
    )]
    struct Keyed(u8);
    impl std::fmt::Display for Keyed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "k{}", self.0)
        }
    }
    impl crate::transport::PeerIdentity for Keyed {
        fn peer_key(&self) -> Option<crate::transport::PeerKey> {
            Some(crate::transport::PeerKey([self.0; 32]))
        }
    }

    /// Review focus 5 (spec §3.2).
    #[tokio::test]
    async fn sender_must_match_envelope_author() {
        use crate::transport::PeerKey;
        let mut c: NodeCore<Keyed, u8, OpsMap<u8>, OpsMap<u8>, Scripted> = NodeCore::new(
            Keyed(0),
            config(1 << 20),
            BTreeSet::from([0u8]),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(&[1000]),
        );
        c.init().await.unwrap();
        let msg = WireMessage::want(
            Keyed(2),
            LogRanges::from_pairs([(0u8, Ranges::full())]),
            BTreeSet::new(),
        );
        let forged = Incoming {
            remote: None,
            author: Some(PeerKey([3; 32])),
            bytes: msg.encode(),
        };
        c.on_wire(Duration::from_millis(1), forged).await.unwrap();
        assert_eq!(c.dropped_msgs, 1, "claimed k2, envelope says k3: dropped");
        assert!(c.router.wants.is_empty());

        let genuine = Incoming {
            remote: None,
            author: Some(PeerKey([2; 32])),
            bytes: msg.encode(),
        };
        c.on_wire(Duration::from_millis(2), genuine).await.unwrap();
        assert_eq!(c.dropped_msgs, 1);
        assert!(c.router.wants.contains_key(&Keyed(2)));

        let unverified = Incoming {
            remote: None,
            author: None,
            bytes: msg.encode(),
        };
        c.on_wire(Duration::from_millis(3), unverified)
            .await
            .unwrap();
        assert_eq!(
            c.dropped_msgs, 1,
            "no envelope: no check (loopback / plain gossip)"
        );
    }

    /// A test-local `ext` store that fails `ingest` for chosen `(log, seq)`
    /// keys. Implements the ASYNC `AsyncStorage` trait directly (not the
    /// sync `Storage` trait): a type that had both a hand-written async impl
    /// and the sync `Storage` impl would fight the blanket sync→async bridge
    /// in `storage.rs` for coherence (E0119), so this must NOT implement
    /// `Storage`.
    struct FailingExt<L: Ord> {
        inner: OpsMap<L>,
        fail: BTreeSet<(L, Seq)>,
    }

    // Concrete impls per log type: a generic `impl<L> AsyncStorage<L> for
    // FailingExt<L>` overlaps the blanket sync→async bridge (E0119).
    macro_rules! failing_ext_impl {
        ($($l:ty),*) => {$(
    impl AsyncStorage<$l> for FailingExt<$l> {
        async fn held_of(&self, logs: &BTreeSet<$l>) -> Result<LogRanges<$l>> {
            Ok(Storage::held_of(&self.inner, logs))
        }
        async fn held_all(&self) -> Result<LogRanges<$l>> {
            Ok(Storage::held_all(&self.inner))
        }
        async fn fetch(&self, ranges: &LogRanges<$l>) -> Result<Vec<($l, Seq, Op)>> {
            Ok(Storage::fetch(&self.inner, ranges))
        }
        async fn ingest(&mut self, log: $l, seq: Seq, op: Op) -> Result<()> {
            if self.fail.contains(&(log, seq)) {
                anyhow::bail!("FailingExt: ingest({log:?}, {seq}) deliberately fails");
            }
            Storage::ingest(&mut self.inner, log, seq, op);
            Ok(())
        }
    }
        )*};
    }
    failing_ext_impl!(u8, dash_router_core::Pair);

    /// A test-local relay whose `fetch` fails while `fail_fetch` is set;
    /// everything else delegates to an `OpsMap`. Async-only for the same
    /// coherence reason as [`FailingExt`].
    struct FailingRelay<L: Ord> {
        inner: OpsMap<L>,
        fail_fetch: bool,
    }

    macro_rules! failing_relay_impl {
        ($($l:ty),*) => {$(
    impl AsyncStorage<$l> for FailingRelay<$l> {
        async fn held_of(&self, logs: &BTreeSet<$l>) -> Result<LogRanges<$l>> {
            Ok(Storage::held_of(&self.inner, logs))
        }
        async fn held_all(&self) -> Result<LogRanges<$l>> {
            Ok(Storage::held_all(&self.inner))
        }
        async fn fetch(&self, ranges: &LogRanges<$l>) -> Result<Vec<($l, Seq, Op)>> {
            if self.fail_fetch {
                anyhow::bail!("FailingRelay: fetch deliberately fails");
            }
            Ok(Storage::fetch(&self.inner, ranges))
        }
        async fn ingest(&mut self, log: $l, seq: Seq, op: Op) -> Result<()> {
            Storage::ingest(&mut self.inner, log, seq, op);
            Ok(())
        }
    }

    impl AsyncEvictableStorage<$l> for FailingRelay<$l> {
        async fn usage(&self) -> Result<Units> {
            Ok(EvictableStorage::usage(&self.inner))
        }
        async fn ingest_delta(&self, log: &$l, seq: Seq, op: &Op) -> Result<Units> {
            Ok(EvictableStorage::ingest_delta(&self.inner, log, seq, op))
        }
        async fn held_payloads(&self) -> Result<LogRanges<$l>> {
            Ok(EvictableStorage::held_payloads(&self.inner))
        }
        async fn evict_payloads(&mut self, ranges: &LogRanges<$l>) -> Result<()> {
            EvictableStorage::evict_payloads(&mut self.inner, ranges);
            Ok(())
        }
        async fn evict(&mut self, ranges: &LogRanges<$l>) -> Result<()> {
            EvictableStorage::evict(&mut self.inner, ranges);
            Ok(())
        }
    }
        )*};
    }
    failing_relay_impl!(dash_router_core::Pair);

    fn pair_wire(msg: WireMessage<u32, dash_router_core::Pair>) -> Incoming {
        Incoming {
            remote: Some("192.168.0.9".parse().unwrap()),
            author: None,
            bytes: msg.encode(),
        }
    }

    fn pair_have(
        log: dash_router_core::Pair,
        seqs: &[u32],
    ) -> WireMessage<u32, dash_router_core::Pair> {
        WireMessage::have(
            7,
            vec![(log, seqs.iter().map(|&q| (q, op(q as u8, true))).collect())],
        )
    }

    /// Finding (controller ruling): a failed relay fetch during Subscribe
    /// must evict nothing — the ops stay parked in the relay rather than
    /// vanishing from both stores — while the prefix still opens.
    #[tokio::test]
    async fn subscribe_keeps_relay_ops_when_fetch_fails() {
        use dash_router_core::Pair;
        let relay = FailingRelay {
            inner: OpsMap::default(),
            fail_fetch: false,
        };
        let mut c: NodeCore<u32, Pair, OpsMap<Pair>, FailingRelay<Pair>, Scripted> = NodeCore::new(
            0,
            config(1 << 20),
            BTreeSet::new(),
            OpsMap::default(),
            relay,
            Scripted::ms(&[1000]),
        );
        c.init().await.unwrap();
        let a1 = Pair::new(1, 1);
        c.on_wire(Duration::from_millis(10), pair_wire(pair_have(a1, &[0, 1])))
            .await
            .unwrap();
        let before = Storage::held_all(&c.relay.inner);
        assert_eq!(before.get(&a1), Some(&Ranges::range(0, 2)));
        let errors_before = c.relay_errors;

        c.relay.fail_fetch = true;
        c.on_subscribe(Duration::from_millis(20), 1u8)
            .await
            .unwrap();

        assert_eq!(
            Storage::held_all(&c.relay.inner),
            before,
            "nothing evicted after a failed fetch"
        );
        assert!(
            Storage::held_all(&c.ext).get(&a1).is_none(),
            "nothing reached ext"
        );
        assert_eq!(
            c.relay_errors,
            errors_before + 1,
            "the fetch error is counted"
        );
        assert!(c.router.open.contains(&1u8), "the prefix still opens");
        assert_eq!(
            c.router.held.get(&a1),
            Some(&Ranges::range(0, 2)),
            "still held (in the relay), still advertised"
        );
    }

    /// Finding (controller ruling): an op whose ext ingest fails during
    /// Subscribe stays in the relay; its siblings under the same prefix
    /// migrate.
    #[tokio::test]
    async fn subscribe_keeps_relay_ops_whose_ingest_fails() {
        use dash_router_core::Pair;
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let ext = FailingExt {
            inner: OpsMap::default(),
            fail: BTreeSet::from([(a1, 0u32), (a1, 1u32)]),
        };
        let mut c: NodeCore<u32, Pair, FailingExt<Pair>, OpsMap<Pair>, Scripted> = NodeCore::new(
            0,
            config(1 << 20),
            BTreeSet::new(),
            ext,
            OpsMap::default(),
            Scripted::ms(&[1000]),
        );
        c.init().await.unwrap();
        c.on_wire(Duration::from_millis(10), pair_wire(pair_have(a1, &[0, 1])))
            .await
            .unwrap();
        c.on_wire(Duration::from_millis(11), pair_wire(pair_have(a2, &[0])))
            .await
            .unwrap();

        let out = c
            .on_subscribe(Duration::from_millis(20), 1u8)
            .await
            .unwrap();

        let failures = out
            .iter()
            .filter(|o| matches!(o, Out::Event(RouterEvent::StorageError(_))))
            .count();
        assert_eq!(failures, 2, "one StorageError per failed ingest");
        let relay = Storage::held_all(&c.relay);
        assert_eq!(
            relay.get(&a1),
            Some(&Ranges::range(0, 2)),
            "a1's ops failed to ingest, so they stay in the relay"
        );
        assert!(relay.get(&a2).is_none(), "a2 migrated out of the relay");
        let ext = Storage::held_all(&c.ext.inner);
        assert_eq!(ext.get(&a2), Some(&Ranges::range(0, 1)), "a2 is in ext");
        assert!(ext.get(&a1).is_none(), "a1 never reached ext");
    }

    /// Finding 1: a `Have` containing a subscribed op whose `ext.ingest`
    /// fails must produce a `StorageError` and NOT a `Delivered` for that
    /// key — `Delivered` must not lie about bytes actually landing in the
    /// embedder's store. A later, successful re-receive of the same key
    /// does deliver (the failed ingest left `held` unchanged, so the op is
    /// still novel to the router).
    #[tokio::test]
    async fn failed_ext_ingest_is_not_reported_delivered() {
        let ext = FailingExt {
            inner: OpsMap::default(),
            fail: BTreeSet::from([(0u8, 0u32)]),
        };
        let mut c: NodeCore<u32, u8, FailingExt<u8>, OpsMap<u8>, Scripted> = NodeCore::new(
            0u32,
            config(100),
            BTreeSet::from([0u8]),
            ext,
            OpsMap::default(),
            Scripted::ms(&[100]),
        );
        c.init().await.unwrap();

        let have = WireMessage::have(7, vec![(0u8, vec![(0, op(1, true))])]);
        let out = c.on_wire(Duration::ZERO, wire(have.clone())).await.unwrap();
        assert!(
            out.iter()
                .any(|o| matches!(o, Out::Event(RouterEvent::StorageError(_)))),
            "the failed ingest is reported"
        );
        assert!(
            delivered(&out).is_empty(),
            "Delivered must not lie: ext.ingest failed, so no Delivered for (0, 0)"
        );
        assert!(
            !AsyncStorage::held_all(&c.ext)
                .await
                .unwrap()
                .contains(&0, 0),
            "the bytes really aren't in ext"
        );

        // Stop failing, then re-receive the same key: still novel to the
        // router (held never advanced), so it delivers this time.
        c.ext.fail.clear();
        let out2 = c
            .on_wire(Duration::from_millis(1), wire(have))
            .await
            .unwrap();
        assert_eq!(
            delivered(&out2),
            vec![(0, 0)],
            "a later successful re-receive delivers"
        );
    }
}

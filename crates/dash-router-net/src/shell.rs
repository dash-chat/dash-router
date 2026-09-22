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
use dash_router_core::{
    eviction_candidates, group_ops, ranges_of, Effect, LogRanges, Op, Ranges, RouterAction,
    RouterConfig, RouterMachine, RouterState, Seq, Units, WireBody, WireMessage,
};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use polestar::prelude::*;
use polestar::time::RealTime;
use rand::rngs::StdRng;

use crate::handle::{RouterEvent, StorageErrorReport};
use crate::lan::is_lan;
use crate::storage::{AsyncEvictableStorage, AsyncStorage};
use crate::transport::Incoming;

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
}

/// A single unit of shell output: either a wire message to broadcast, or an
/// event for the embedder (spec §5).
pub enum Out<N, L: Ord> {
    Broadcast(WireMessage<N, L>),
    Event(RouterEvent<L>),
}

/// The shell's routing table: one owner of the pure [`RouterState`] plus
/// both async stores. See the module doc for the deliberate-duplication
/// rationale relative to `NodeMachine`.
pub struct NodeCore<N: Ord, L: Ord, E, R, I> {
    machine: RouterMachine<N, L, RealTime>,
    pub router: RouterState<N, L, RealTime>,
    pub ext: E,
    pub relay: R,
    pub subscriptions: BTreeSet<L>,
    /// Derived: last known ext ∪ relay ∪ empty markers for subscriptions.
    held_cache: LogRanges<L>,
    intervals: I,
    debounce: PushDebouncePolicy,
    relay_cap: Units,
    evict_at: f64,
    pending_push: LogRanges<L>,
    pending_since: Option<Duration>,
    latest_append: Duration,
    /// Shell time the router has been ticked up to.
    now: Duration,
    pub dropped_msgs: u64,
    pub relay_errors: u64,
}

impl<N, L, E, R, I> NodeCore<N, L, E, R, I>
where
    N: Id + serde::Serialize + serde::de::DeserializeOwned,
    L: Id + serde::Serialize + serde::de::DeserializeOwned,
    E: AsyncStorage<L>,
    R: AsyncEvictableStorage<L>,
    I: IntervalSource,
{
    pub fn new(
        id: N,
        config: CoreConfig,
        subscriptions: BTreeSet<L>,
        ext: E,
        relay: R,
        intervals: I,
    ) -> Self {
        Self {
            machine: RouterMachine::new(config.router),
            router: RouterState::new(id, LogRanges::empty()),
            ext,
            relay,
            subscriptions,
            held_cache: LogRanges::empty(),
            intervals,
            debounce: config.debounce,
            relay_cap: config.relay_cap,
            evict_at: config.evict_at,
            pending_push: LogRanges::empty(),
            pending_since: None,
            latest_append: Duration::ZERO,
            now: Duration::ZERO,
            dropped_msgs: 0,
            relay_errors: 0,
        }
    }

    /// Read the initial held snapshot from storage and arm the first Want
    /// timer. Mirrors the sim behavior's one-time init (`arm_want` for every
    /// node before the first tick).
    pub async fn init(&mut self) -> Result<Vec<Out<N, L>>> {
        let mut out = Vec::new();
        self.reconcile_held(None, &mut out).await?;
        let next = self.intervals.next_want();
        self.router_step(RouterAction::ArmWantTimer(next.into()))?;
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
                self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
                continue;
            }
            if self
                .router
                .want_timer
                .as_ref()
                .is_some_and(|t| t.remaining.is_zero())
            {
                let fx = self.router_step(RouterAction::FireWant)?;
                self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
                let next = self.intervals.next_want();
                self.router_step(RouterAction::ArmWantTimer(next.into()))?;
                continue;
            }
            if self
                .router
                .have_timer
                .as_ref()
                .is_some_and(|t| t.remaining.is_zero())
            {
                let fx = self.router_step(RouterAction::FireHave)?;
                self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
                if !self.router.wants.is_empty() {
                    let next = self.intervals.next_have();
                    self.router_step(RouterAction::ArmHaveTimer(next.into()))?;
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
            self.router_step(RouterAction::Tick(step.into()))?;
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
        match msg.body {
            WireBody::Want(ranges) => {
                let fx = self.router_step(RouterAction::RecvWant {
                    from: msg.sender,
                    ranges,
                })?;
                self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
                // A witnessed Want is what makes arming the Have timer
                // legal (mirrors the sim behavior's arm-on-recv): do it
                // right after the Recv, in the same call.
                if self.router.have_timer.is_none() {
                    let next = self.intervals.next_have();
                    self.router_step(RouterAction::ArmHaveTimer(next.into()))?;
                }
            }
            WireBody::Have(ops) => {
                let parked: BTreeMap<(L, Seq), Op> = ops
                    .into_iter()
                    .flat_map(|(log, seqs)| seqs.into_iter().map(move |(q, o)| ((log, q), o)))
                    .collect();
                let ranges = ranges_of(&parked);
                let fx = self.router_step(RouterAction::RecvHave {
                    from: msg.sender,
                    ranges,
                })?;
                // Ingest ALL parked bytes first (idempotent; payload
                // upgrades are invisible to the router's novelty check).
                self.ingest_parked(&parked, &mut out).await?;
                let touched: BTreeSet<L> = parked.keys().map(|(l, _)| *l).collect();
                self.reconcile_held(Some(&touched), &mut out).await?;
                self.route_fx(fx, &parked, &mut out).await?;
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

    /// Start caring about a log: migrate any relay-side bytes for it to
    /// `ext`, then evict it from the relay (`NodeAction::Subscribe`'s glue).
    pub async fn on_subscribe(&mut self, now: Duration, log: L) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        self.subscriptions.insert(log);
        let all = LogRanges::from_pairs([(log, Ranges::full())]);
        match self.relay.fetch(&all).await {
            Ok(ops) => {
                for (l, seq, op) in ops {
                    if let Err(e) = self.ext.ingest(l, seq, op).await {
                        out.push(Out::Event(RouterEvent::StorageError(StorageErrorReport {
                            context: "on_subscribe: ext.ingest",
                            message: e.to_string(),
                        })));
                    }
                }
            }
            Err(_) => self.relay_errors += 1,
        }
        if self.relay.evict(&all).await.is_err() {
            self.relay_errors += 1;
        }
        let touched = BTreeSet::from([log]);
        self.reconcile_held(Some(&touched), &mut out).await?;
        Ok(out)
    }

    /// Kept-as-is ruling (binding semantics #7): remove the subscription and
    /// reconcile; still-held data keeps advertising and wanting until it is
    /// otherwise evicted.
    pub async fn on_unsubscribe(&mut self, now: Duration, log: L) -> Result<Vec<Out<N, L>>> {
        let mut out = self.advance_to(now).await?;
        self.subscriptions.remove(&log);
        let touched = BTreeSet::from([log]);
        self.reconcile_held(Some(&touched), &mut out).await?;
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

    /// Clone-based stepping (same rationale as `NodeMachine`'s `*_step`
    /// helpers): `RouterState` has no `Default`, so the sub-state is cloned
    /// rather than taken.
    fn router_step(&mut self, action: RouterAction<N, L, RealTime>) -> Result<Vec<Effect<L>>> {
        let router = self.router.clone();
        let (router, fx) = self.machine.transition(router, action)?;
        self.router = router;
        Ok(fx)
    }

    /// Take the accumulated pending push and run it through the router, if
    /// non-empty.
    fn flush_push(&mut self) -> Result<Vec<Effect<L>>> {
        let ranges = std::mem::replace(&mut self.pending_push, LogRanges::empty());
        self.pending_since = None;
        if ranges.is_empty() {
            Ok(Vec::new())
        } else {
            self.router_step(RouterAction::Push(ranges))
        }
    }

    /// Turn the router's decisions into shell-level output, in fx order —
    /// the async transcription of `NodeMachine::route_router_fx`.
    async fn route_fx(
        &mut self,
        fx: Vec<Effect<L>>,
        parked: &BTreeMap<(L, Seq), Op>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<()> {
        for e in fx {
            match e {
                Effect::Accept(novel) => {
                    for (log, r) in novel.iter() {
                        if !self.subscriptions.contains(log) {
                            continue;
                        }
                        // Never iterate `Ranges` directly — it may be open;
                        // walk the parked keys instead.
                        for (pl, seq) in parked.keys() {
                            if pl == log && r.contains(*seq) {
                                out.push(Out::Event(RouterEvent::Delivered(*log, *seq)));
                            }
                        }
                    }
                }
                Effect::SendWant(r) => {
                    out.push(Out::Broadcast(WireMessage::want(self.router.id, r)));
                }
                Effect::SendHave(r) => {
                    if let Some(msg) = self.hydrate(&r, out).await? {
                        out.push(Out::Broadcast(msg));
                    }
                }
            }
        }
        Ok(())
    }

    /// Merge relay + ext fetches into one hydrated Have, byte-for-byte the
    /// `SendHave` arm of `route_router_fx` (binding semantics #3). Relay
    /// errors are silent shrinkage (counted in `relay_errors`); ext errors
    /// are reported but don't stop hydration.
    async fn hydrate(
        &mut self,
        r: &LogRanges<L>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<Option<WireMessage<N, L>>> {
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
        if ops.is_empty() {
            Ok(None)
        } else {
            Ok(Some(WireMessage::have(self.router.id, group_ops(ops))))
        }
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
                    if m.is_empty() && !self.subscriptions.contains(log) {
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
                let mut merged = ext_all.union(&relay_all);
                for log in &self.subscriptions {
                    if merged.get(log).is_none() {
                        merged.insert(*log, Ranges::empty());
                    }
                }
                self.held_cache = merged;
            }
        }
        self.router_step(RouterAction::Held(self.held_cache.clone()))?;
        Ok(())
    }

    /// For each parked op: subscribed logs go to `ext`; unsubscribed logs go
    /// to `relay`, shed silently (not an error) if ingesting would exceed
    /// the relay's cap — the shell sheds until an eviction frees room.
    async fn ingest_parked(
        &mut self,
        parked: &BTreeMap<(L, Seq), Op>,
        out: &mut Vec<Out<N, L>>,
    ) -> Result<()> {
        for ((log, seq), op) in parked {
            if self.subscriptions.contains(log) {
                if let Err(e) = self.ext.ingest(*log, *seq, op.clone()).await {
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
                        continue;
                    }
                };
                let usage = match self.relay.usage().await {
                    Ok(u) => u,
                    Err(_) => {
                        self.relay_errors += 1;
                        continue;
                    }
                };
                if usage + units > self.relay_cap {
                    continue; // shed: no room, and no eviction happened yet
                }
                if self.relay.ingest(*log, *seq, op.clone()).await.is_err() {
                    self.relay_errors += 1;
                }
            }
        }
        Ok(())
    }
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
            Scripted(script.iter().map(|&m| Duration::from_millis(m)).collect(), 0)
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
            debounce: PushDebouncePolicy { window_ms: 100, max_latency_ms: 250 },
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
        Op { header: vec![h], payload: payload.then(|| vec![h; 4]) }
    }

    fn wire(msg: WireMessage<u32, u8>) -> Incoming {
        Incoming { remote: Some("192.168.0.9".parse().unwrap()), bytes: msg.encode() }
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
        assert_eq!(delivered(&out), vec![(0, 0)], "only the subscribed log delivers");
        let bs = broadcasts(&out);
        assert_eq!(bs.len(), 1, "the flood relays once, hydrated");
        let WireBody::Have(groups) = &bs[0].body else { panic!("expected Have") };
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
        assert_eq!(EvictableStorage::usage(&c.relay), 2, "one op stored, one shed");
        let bs = broadcasts(&out);
        let WireBody::Have(groups) = &bs[0].body else { panic!() };
        assert_eq!(groups[0].1.len(), 1, "hydration only finds the stored op");
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
        assert_eq!(c.next_deadline(), Some(ms(100)), "want timer at 100 ties the flush window 50+100; flush due at 150");
        let out = c.advance_to(ms(150)).await.unwrap();
        let haves: Vec<_> = broadcasts(&out)
            .into_iter()
            .filter(|m| matches!(m.body, WireBody::Have(_)))
            .collect();
        assert_eq!(haves.len(), 1, "one flush for the whole batch");
        let WireBody::Have(groups) = &haves[0].body else { panic!() };
        assert_eq!(groups[0].1.len(), 2, "both appends carried");
    }

    /// The want timer fires on schedule and a peer's Want arms the have timer;
    /// the eventual Have reply is hydrated from storage.
    #[tokio::test]
    async fn want_fire_and_have_reply_flow() {
        let mut c = core(100, &[0]).await; // empty subscribed log: wants everything
        let out = c.advance_to(Duration::from_millis(100)).await.unwrap();
        let bs = broadcasts(&out);
        assert!(
            matches!(&bs[0].body, WireBody::Want(r) if r.get(&0) == Some(&Ranges::full())),
            "fresh subscription wants the whole log"
        );
        // Seed storage, then a peer wants it.
        let _ = c.on_append(Duration::from_millis(100), 0, 0, op(1, true)).await.unwrap();
        let want = WireMessage::want(7, LogRanges::from_pairs([(0u8, Ranges::full())]));
        let _out = c.on_wire(Duration::from_millis(110), wire(want)).await.unwrap();
        // NOTE (deviation from the brief's literal assertion, see task-9
        // report): RouterMachine's RecvWant relays only the range NOT
        // already in `relayed_want_ranges()` (the flood's seen-set,
        // DESIGN.md-mandated termination). Our own FireWant at t=100
        // already flooded the full range for log 0 with a 500ms want_ttl,
        // so a peer's identical want at t=110 is witnessed (recorded in
        // `wants`, which is what legalizes arming the have timer) but is
        // correctly NOT re-flooded — re-broadcasting it would be redundant
        // per the seen-set's own accounting. This is core, unmodified
        // behavior (verified directly against `RouterMachine::transition`),
        // not a shell bug.
        assert!(c.router.wants.contains_key(&7), "the peer's want is witnessed");
        assert!(c.router.have_timer.is_some(), "witnessed Want arms the have timer");
        let out = c.advance_to(Duration::from_millis(400)).await.unwrap();
        assert!(
            broadcasts(&out).iter().any(|m| matches!(&m.body, WireBody::Have(g) if !g.is_empty())),
            "the reply is hydrated"
        );
    }

    /// Maintenance evicts payloads nobody wants once usage crosses the line.
    #[tokio::test]
    async fn maintain_evicts_payloads_first() {
        let mut c = core(4, &[]).await;
        let have = WireMessage::have(7, vec![(1u8, vec![(0, op(1, true)), (1, op(2, true))])]);
        let _ = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
        assert_eq!(EvictableStorage::usage(&c.relay), 4);
        let _ = c.on_maintain(Duration::from_millis(1)).await.unwrap();
        assert_eq!(EvictableStorage::usage(&c.relay), 2, "payloads evicted, headers kept");
        assert!(EvictableStorage::held_payloads(&c.relay).is_empty());
        assert!(!Storage::held_all(&c.relay).is_empty(), "still advertising headers");
    }

    /// Non-LAN senders, garbage bytes, and own echoes are dropped statelessly.
    #[tokio::test]
    async fn on_wire_drops_foreign_garbage_and_echoes() {
        let mut c = core(100, &[0]).await;
        let held = c.router.held.clone();
        let foreign = Incoming {
            remote: Some("8.8.8.8".parse().unwrap()),
            bytes: WireMessage::have(7, vec![(0u8, vec![(0, op(1, true))])]).encode(),
        };
        let garbage = Incoming { remote: Some("192.168.0.9".parse().unwrap()), bytes: vec![0xff, 0x00] };
        let echo = wire(WireMessage::want(0, LogRanges::from_pairs([(0u8, Ranges::full())])));
        for inc in [foreign, garbage, echo] {
            assert!(c.on_wire(Duration::ZERO, inc).await.unwrap().is_empty());
        }
        assert_eq!(c.dropped_msgs, 3);
        assert_eq!(c.router.held, held, "no state change from dropped input");
    }
}

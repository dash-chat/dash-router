//! Task 11: lockstep conformance of the tokio shell's `NodeCore` (spec §2)
//! against the pure `NodeMachine` reference (`dash-router-core`).
//!
//! `NodeCore` is a deliberate, async transcription of `NodeMachine`'s
//! transition logic (see the module doc on `dash_router::shell`), kept
//! separate because the pure core machine has no way to `.await` a fallible
//! store. This test drives an identical action sequence into both, over the
//! same synchronous `OpsMap` stores (via the blanket sync-to-async bridge),
//! and asserts their state projections and effect streams agree after every
//! step.
//!
//! Deviation from spec §8, on the record: plain `proptest` with a generated
//! `Vec<Step>`, not the `proptest-state-machine` crate — the reference model
//! IS our state machine, so the crate's Reference/SUT scaffolding would
//! duplicate what `NodeMachine` already is.
//!
//! Two step universes (final review F2): `u8` logs, where every channel
//! holds at most one log, and [`Pair`] logs, where each channel holds two
//! — so channel subscriptions migrate several logs at once and a Want
//! mixing named ranges with channels (and split into pieces) actually
//! discriminates named from wholesale. The harness is generic over the
//! log type ([`TestLog`]) and instantiated once per universe.
//!
//! Zero-debounce ruling: conformance runs with
//! `PushDebouncePolicy { window_ms: 0, max_latency_ms: 0 }`, so an `Append`
//! step's SUT call (`on_append` + `advance_to`) flushes immediately and maps
//! to the reference's atomic `NodeAction::Authored`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::time::Duration;

use dash_router::{CoreConfig, IntervalSource, NodeCore, Out, RouterEvent};
use dash_router_core::{
    EvictableStorage, Log, LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, OpsMap,
    Pair, Ranges, RouterAction, RouterConfig, RouterState, Seq, Storage, Units, WireBody, WireLog,
    WireMessage, eviction_candidates,
};
use dash_router_policy::PushDebouncePolicy;
use polestar::prelude::*;
use polestar::time::RealTime;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

// --- Scripted intervals (own copy: a #[cfg(test)] item in another crate is
// not reachable from an integration test) -----------------------------------

/// Deterministic intervals: pops from the front, repeats the last entry
/// forever. Mirrors `dash_router::shell`'s private test-only `Scripted`.
#[derive(Clone)]
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

// --- Log universes ----------------------------------------------------

/// A log type the harness can be instantiated over. Both universes use
/// `u8` channels.
trait TestLog: WireLog + Log<Channel = u8> + Default + Debug + Send + Sync + 'static {
    /// A byte identifying the log in op headers.
    fn tag(&self) -> u8;
}

impl TestLog for u8 {
    fn tag(&self) -> u8 {
        *self
    }
}

impl TestLog for Pair {
    fn tag(&self) -> u8 {
        self.channel * 16 + self.author
    }
}

// --- Step vocabulary ---------------------------------------------------

#[derive(Clone, Debug)]
enum Step<L> {
    /// `origin` 0 is this node: its own Want, echoed back by `from`.
    RecvWant {
        from: u32,
        origin: u32,
        log: L,
        start: u32,
        end: u32,
    },
    /// A Want naming no ranges, only a channel: "every log under it".
    RecvChannelWant {
        from: u32,
        origin: u32,
        channel: u8,
    },
    /// A Want mixing named ranges with channels, packed by `pack_want`
    /// under `budget` bytes and delivered piece by piece (a small budget
    /// splits it), so both machines record a split Want.
    RecvMixedWant {
        from: u32,
        origin: u32,
        ranges: Vec<(L, u32, u32)>,
        channels: Vec<u8>,
        budget: usize,
    },
    RecvHave {
        from: u32,
        log: L,
        seqs: Vec<(u32, bool)>,
    },
    Append {
        log: L,
    },
    /// Subscribe to a channel.
    Subscribe(u8),
    Unsubscribe(u8),
    Advance(u64),
    /// Finding 4(b): a relay-maintenance tick. The SUT calls
    /// `NodeCore::on_maintain`; the reference mirrors that same policy by
    /// hand (see `Driver::ref_maintain`) since the pure `NodeMachine` takes
    /// `RelayEvict(Payloads)` as an action rather than deciding when to
    /// fire one — the shell/sim layer owns that policy in both worlds.
    Maintain,
}

/// The step universe over logs drawn from `log` and channels from
/// `channel`.
fn step_strategy<L: TestLog>(
    log: impl Strategy<Value = L> + Clone + 'static,
    channel: impl Strategy<Value = u8> + Clone + 'static,
) -> impl Strategy<Value = Step<L>> {
    let mixed = (
        1u32..4,
        0u32..5,
        proptest::collection::vec((log.clone(), 0u32..8, 0u32..8), 0..4),
        proptest::collection::vec(channel.clone(), 0..3),
        prop_oneof![Just(12usize), Just(3800usize)],
    )
        .prop_map(
            |(from, origin, ranges, channels, budget)| Step::RecvMixedWant {
                from,
                origin,
                ranges,
                channels,
                budget,
            },
        );
    prop_oneof![
        3 => (1u32..4, 0u32..5, log.clone(), 0u32..8, 0u32..8).prop_map(
            |(from, origin, log, start, end)| Step::RecvWant { from, origin, log, start, end }
        ),
        1 => (1u32..4, 0u32..5, channel.clone())
            .prop_map(|(from, origin, channel)| Step::RecvChannelWant { from, origin, channel }),
        2 => mixed,
        3 => (1u32..4, log.clone(), proptest::collection::vec((0u32..8, any::<bool>()), 0..4))
            .prop_map(|(from, log, seqs)| Step::RecvHave { from, log, seqs }),
        2 => log.prop_map(|log| Step::Append { log }),
        1 => channel.clone().prop_map(Step::Subscribe),
        1 => channel.prop_map(Step::Unsubscribe),
        2 => (10u64..600).prop_map(Step::Advance),
        1 => Just(Step::Maintain),
    ]
}

// --- Deterministic op construction --------------------------------------

/// Locally authored op (brief's fixture): `header = [log, seq as u8]`,
/// always carries a payload.
fn authored_op<L: TestLog>(log: L, seq: Seq) -> Op {
    Op {
        header: vec![log.tag(), seq as u8],
        payload: Some(vec![seq as u8]),
    }
}

/// A wire-carried op for `RecvHave`, with payload presence controlled by the
/// generated bool.
fn wire_op<L: TestLog>(log: L, seq: Seq, has_payload: bool) -> Op {
    Op {
        header: vec![log.tag(), seq as u8],
        payload: has_payload.then(|| vec![seq as u8]),
    }
}

fn incoming<L: TestLog>(msg: &WireMessage<u32, L>) -> dash_router::Incoming {
    dash_router::Incoming {
        remote: Some("192.168.0.9".parse().unwrap()),
        author: None,
        bytes: msg.encode(),
    }
}

// --- Comparison helpers --------------------------------------------------

macro_rules! check {
    ($idx:expr, $name:expr, $sut:expr, $refr:expr) => {
        if $sut != $refr {
            return Err(TestCaseError::fail(format!(
                "step {}: {} mismatch\n  sut = {:?}\n  ref = {:?}",
                $idx, $name, $sut, $refr
            )));
        }
    };
}

fn assert_router_matches<L: TestLog>(
    idx: usize,
    sut: &RouterState<u32, L, RealTime>,
    refr: &RouterState<u32, L, RealTime>,
) -> Result<(), TestCaseError> {
    check!(idx, "held", sut.held, refr.held);
    check!(idx, "open", sut.open, refr.open);
    check!(idx, "wants", sut.wants, refr.wants);
    check!(idx, "haves", sut.haves, refr.haves);
    // Final review F2: the whole seen-sets, record by record — ranges,
    // channels and TTLs — not just their range unions.
    check!(idx, "relayed_wants", sut.relayed_wants, refr.relayed_wants);
    check!(idx, "relayed_haves", sut.relayed_haves, refr.relayed_haves);
    check!(idx, "want_timer", sut.want_timer, refr.want_timer);
    check!(idx, "have_timer", sut.have_timer, refr.have_timer);
    Ok(())
}

type Core<L> = NodeCore<u32, L, OpsMap<L>, OpsMap<L>, Scripted>;

/// Drives a `NodeCore` (the SUT) and a `NodeMachine`/`NodeState` (the
/// reference) through an identical action sequence, asserting agreement
/// after every step.
struct Driver<L: TestLog> {
    core: Core<L>,
    ref_machine: NodeMachine<u32, L, RealTime>,
    ref_state: NodeState<u32, L, RealTime>,
    ref_script: Scripted,
    now: Duration,
    next_seq: BTreeMap<L, Seq>,
    /// Finding 4(b): `NodeCore::on_maintain`'s policy constants, mirrored by
    /// `ref_maintain` since the pure `NodeMachine` has no maintenance
    /// action of its own to fire — only the resulting `RelayEvict(Payloads)`.
    relay_cap: Units,
    evict_at: f64,
}

impl<L: TestLog> Driver<L> {
    async fn new(
        subs: BTreeSet<u8>,
        intervals: &[u64],
        relay_cap: Units,
    ) -> Result<Self, TestCaseError> {
        let router_config: RouterConfig<RealTime> = RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
        };
        let core_config = CoreConfig {
            router: router_config.clone(),
            relay_cap,
            evict_at: 0.75,
            debounce: PushDebouncePolicy {
                window_ms: 0,
                max_latency_ms: 0,
            },
            // Far above anything the generated steps produce (a few logs of
            // few-byte ops), so packing never splits a broadcast here and the
            // exact `broadcasts` comparison below stays one-to-one with the
            // model's single Want/Have.
            max_wire_bytes: None,
        };
        let mut core = NodeCore::new(
            0u32,
            core_config,
            subs.clone(),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(intervals),
        );
        core.init()
            .await
            .map_err(|e| TestCaseError::fail(format!("SUT init failed: {e}")))?;

        let ref_machine = NodeMachine::new(router_config, relay_cap);
        let ref_state = NodeState::new(0u32, ref_machine.clone(), subs);
        let mut d = Self {
            core,
            ref_machine,
            ref_state,
            ref_script: Scripted::ms(intervals),
            now: Duration::ZERO,
            next_seq: BTreeMap::new(),
            relay_cap,
            evict_at: 0.75, // must match core_config.evict_at above
        };
        // init() arms the SUT's first Want timer with the script's first
        // interval; mirror it on the reference with the SAME script, so both
        // consume scripted intervals in identical order from here on.
        let next = d.ref_script.next_want();
        d.ref_step(
            0,
            NodeAction::Router(RouterAction::ArmWantTimer(next.into())),
        )?;
        Ok(d)
    }

    fn ref_step(
        &mut self,
        idx: usize,
        action: NodeAction<u32, L, RealTime>,
    ) -> Result<Vec<NodeEffect<u32, L>>, TestCaseError> {
        let state = self.ref_state.clone();
        match self.ref_machine.transition(state, action) {
            Ok((s, fx)) => {
                self.ref_state = s;
                Ok(fx)
            }
            Err(e) => Err(TestCaseError::fail(format!(
                "step {idx}: reference transition returned Err (mapping produced a disabled action?): {e}"
            ))),
        }
    }

    /// Mirrors `NodeCore::advance_to`'s loop verbatim (see that fn's doc: the
    /// conformance driver replicates its shape) against the reference.
    fn ref_advance_to(
        &mut self,
        idx: usize,
        target: Duration,
    ) -> Result<Vec<NodeEffect<u32, L>>, TestCaseError> {
        let mut out = Vec::new();
        let mut now = self.now;
        loop {
            // 1. Flush: vacuous at zero debounce — the driver always forces
            // the flush within the same Append step (see `apply`), so no
            // pending push ever survives to a later step.
            if self.ref_state.router.want_due() {
                let fx = self.ref_step(idx, NodeAction::Router(RouterAction::FireWant))?;
                out.extend(fx);
                let next = self.ref_script.next_want();
                let fx2 = self.ref_step(
                    idx,
                    NodeAction::Router(RouterAction::ArmWantTimer(next.into())),
                )?;
                out.extend(fx2);
                continue;
            }
            if self.ref_state.router.have_due() {
                let fx = self.ref_step(idx, NodeAction::Router(RouterAction::FireHave))?;
                out.extend(fx);
                if !self.ref_state.router.wants.is_empty() {
                    let next = self.ref_script.next_have();
                    let fx2 = self.ref_step(
                        idx,
                        NodeAction::Router(RouterAction::ArmHaveTimer(next.into())),
                    )?;
                    out.extend(fx2);
                }
                continue;
            }
            if now >= target {
                break;
            }
            let mut step = target - now;
            if let Some(due) = self.ref_state.router.next_due() {
                step = step.min(*due);
            }
            let fx = self.ref_step(idx, NodeAction::Router(RouterAction::Tick(step.into())))?;
            debug_assert!(fx.is_empty(), "Tick never produces effects");
            now += step;
        }
        self.now = now;
        Ok(out)
    }

    /// Mirrors `NodeCore::on_maintain`'s policy exactly (see that fn):
    /// once relay usage crosses `evict_at * relay_cap`, prefer evicting
    /// payloads nobody wants (`eviction_candidates`); if nothing qualifies,
    /// shed whole unwanted ranges instead. Applied only when the resulting
    /// proposal is non-empty — both `RelayEvictPayloads`/`RelayEvict` are
    /// disabled-on-empty actions in `NodeMachine` (see `node.rs`), matching
    /// the "enabled actions only" rule the rest of this driver already
    /// follows for `ArmHaveTimer` etc.
    fn ref_maintain(&mut self, idx: usize) -> Result<Vec<NodeEffect<u32, L>>, TestCaseError> {
        let usage = EvictableStorage::usage(&self.ref_state.relay.0);
        let threshold = ((self.evict_at * self.relay_cap as f64) as Units).max(1);
        if usage < threshold {
            return Ok(Vec::new());
        }
        let held_payloads = EvictableStorage::held_payloads(&self.ref_state.relay.0);
        let others_wants = self.ref_state.router.others_wants();
        let candidates = eviction_candidates(&held_payloads, &others_wants);
        if !candidates.is_empty() {
            return self.ref_step(idx, NodeAction::RelayEvictPayloads(candidates));
        }
        let held_all = Storage::held_all(&self.ref_state.relay.0);
        let full = held_all.difference(&others_wants);
        if !full.is_empty() {
            return self.ref_step(idx, NodeAction::RelayEvict(full));
        }
        Ok(Vec::new())
    }

    async fn apply(&mut self, idx: usize, step: &Step<L>) -> Result<(), TestCaseError> {
        let sut_out: Vec<Out<u32, L>>;
        let ref_fx: Vec<NodeEffect<u32, L>>;
        match step.clone() {
            Step::Subscribe(log) => {
                sut_out = self.core.on_subscribe(self.now, log).await.map_err(|e| {
                    TestCaseError::fail(format!("step {idx}: SUT on_subscribe: {e}"))
                })?;
                ref_fx = self.ref_step(idx, NodeAction::Subscribe(log))?;
            }
            Step::Unsubscribe(log) => {
                sut_out = self.core.on_unsubscribe(self.now, log).await.map_err(|e| {
                    TestCaseError::fail(format!("step {idx}: SUT on_unsubscribe: {e}"))
                })?;
                ref_fx = self.ref_step(idx, NodeAction::Unsubscribe(log))?;
            }
            Step::Append { log } => {
                let seq = *self.next_seq.entry(log).or_insert(0);
                self.next_seq.insert(log, seq + 1);
                let op = authored_op(log, seq);
                let mut out = self
                    .core
                    .on_append(self.now, log, seq, op.clone())
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT on_append: {e}")))?;
                // Zero-debounce ruling: force the flush within this step so
                // it maps to the reference's atomic `Authored`, not a
                // pending push that leaks into a later step.
                let flushed = self.core.advance_to(self.now).await.map_err(|e| {
                    TestCaseError::fail(format!("step {idx}: SUT post-append advance_to: {e}"))
                })?;
                out.extend(flushed);
                sut_out = out;
                ref_fx = self.ref_step(idx, NodeAction::Authored(log, seq, op))?;
            }
            Step::RecvWant {
                from,
                origin,
                log,
                start,
                end,
            } => {
                let ranges = LogRanges::from_pairs([(log, Ranges::range(start, end))]);
                let msg: WireMessage<u32, L> =
                    WireMessage::want(from, origin, ranges, BTreeSet::new());
                (sut_out, ref_fx) = self.recv_want(idx, msg).await?;
            }
            Step::RecvChannelWant {
                from,
                origin,
                channel,
            } => {
                let msg: WireMessage<u32, L> =
                    WireMessage::want(from, origin, LogRanges::empty(), BTreeSet::from([channel]));
                (sut_out, ref_fx) = self.recv_want(idx, msg).await?;
            }
            Step::RecvMixedWant {
                from,
                origin,
                ranges,
                channels,
                budget,
            } => {
                let ranges = LogRanges::from_pairs(
                    ranges
                        .into_iter()
                        .map(|(log, start, end)| (log, Ranges::range(start, end))),
                );
                let channels: BTreeSet<u8> = channels.into_iter().collect();
                let (pieces, dropped) =
                    dash_router::pack::pack_want(from, origin, ranges, channels, budget);
                // The small budget must split the Want, never hollow it: a
                // dropped range would silently turn "named" into "unnamed"
                // for both machines at once and hide a wholesale-vs-named
                // divergence.
                if dropped != 0 {
                    return Err(TestCaseError::fail(format!(
                        "budget {budget} dropped {dropped} named log(s)"
                    )));
                }
                let (mut sut, mut refr) = (Vec::new(), Vec::new());
                for msg in pieces {
                    let (s, r) = self.recv_want(idx, msg).await?;
                    sut.extend(s);
                    refr.extend(r);
                }
                (sut_out, ref_fx) = (sut, refr);
            }
            Step::RecvHave { from, log, seqs } => {
                let group: Vec<(Seq, Op)> = seqs
                    .iter()
                    .map(|&(s, has)| (s, wire_op(log, s, has)))
                    .collect();
                let msg: WireMessage<u32, L> = WireMessage::have(from, vec![(log, group)]);
                sut_out = self
                    .core
                    .on_wire(self.now, incoming(&msg))
                    .await
                    .map_err(|e| {
                        TestCaseError::fail(format!("step {idx}: SUT on_wire(Have): {e}"))
                    })?;
                ref_fx = self.ref_step(idx, NodeAction::Recv(msg))?;
            }
            Step::Advance(ms) => {
                let target = self.now + Duration::from_millis(ms);
                sut_out =
                    self.core.advance_to(target).await.map_err(|e| {
                        TestCaseError::fail(format!("step {idx}: SUT advance_to: {e}"))
                    })?;
                ref_fx = self.ref_advance_to(idx, target)?;
            }
            Step::Maintain => {
                sut_out = self.core.on_maintain(self.now).await.map_err(|e| {
                    TestCaseError::fail(format!("step {idx}: SUT on_maintain: {e}"))
                })?;
                ref_fx = self.ref_maintain(idx)?;
            }
        }
        self.compare(idx, &sut_out, &ref_fx)?;
        Ok(())
    }

    /// Deliver a Want to both machines, mirroring the SUT's arm-on-recv.
    ///
    /// Finding 8(a) / review focus 6: the SUT only arms the have timer when
    /// the RECEIVED Want names something — ranges or channels (an empty
    /// Want must not arm a forever no-op fire/re-arm loop; a channel-only
    /// Want is a real request and must arm) and only when it is someone
    /// else's (an echo of this node's own Want is not recorded, so arming
    /// on it would be illegal). Mirror that exact gate here, not
    /// `wants.is_empty()` (which is always false after a foreign Want,
    /// since `RouterAction::RecvWant` records its origin's entry in `wants`
    /// regardless of whether its ranges were empty).
    async fn recv_want(
        &mut self,
        idx: usize,
        msg: WireMessage<u32, L>,
    ) -> Result<(Vec<Out<u32, L>>, Vec<NodeEffect<u32, L>>), TestCaseError> {
        let WireBody::Want {
            origin,
            ranges,
            channels,
        } = &msg.body
        else {
            unreachable!("recv_want is only called with a Want");
        };
        let want_nonempty = !ranges.is_empty() || !channels.is_empty();
        let foreign = *origin != self.ref_state.router.id;
        let sut_out = self
            .core
            .on_wire(self.now, incoming(&msg))
            .await
            .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT on_wire(Want): {e}")))?;
        let mut fx = self.ref_step(idx, NodeAction::Recv(msg))?;
        // Binding semantics #1: a witnessed non-empty Want arms the Have
        // timer when none is armed. Mirror the SUT's arm-on-recv, in the
        // same call.
        if want_nonempty && foreign && self.ref_state.router.have_timer.is_none() {
            let next = self.ref_script.next_have();
            let more = self.ref_step(
                idx,
                NodeAction::Router(RouterAction::ArmHaveTimer(next.into())),
            )?;
            fx.extend(more);
        }
        Ok((sut_out, fx))
    }

    fn compare(
        &self,
        idx: usize,
        sut_out: &[Out<u32, L>],
        ref_fx: &[NodeEffect<u32, L>],
    ) -> Result<(), TestCaseError> {
        assert_router_matches(idx, &self.core.router, &self.ref_state.router)?;

        let sut_ext = Storage::held_all(&self.core.ext);
        let ref_ext = self.ref_state.ext.0.held_all();
        check!(idx, "ext.held_all", sut_ext, ref_ext);

        let sut_relay = Storage::held_all(&self.core.relay);
        let ref_relay = self.ref_state.relay.0.held_all();
        check!(idx, "relay.held_all", sut_relay, ref_relay);

        // Finding 4(c): the relay's eviction-relevant summaries, not just
        // its held ranges — these are exactly what `ref_maintain`/
        // `NodeCore::on_maintain` decide eviction on, so a drift here would
        // otherwise go unnoticed by `held_all` alone.
        check!(
            idx,
            "relay.usage",
            EvictableStorage::usage(&self.core.relay),
            EvictableStorage::usage(&self.ref_state.relay.0)
        );
        check!(
            idx,
            "relay.held_payloads",
            EvictableStorage::held_payloads(&self.core.relay),
            EvictableStorage::held_payloads(&self.ref_state.relay.0)
        );

        check!(
            idx,
            "subscriptions",
            self.core.subscriptions,
            self.ref_state.subscriptions
        );

        let mut sut_bc: Vec<WireMessage<u32, L>> = sut_out
            .iter()
            .filter_map(|o| match o {
                Out::Broadcast(m) => Some(m.clone()),
                _ => None,
            })
            .collect();
        let mut ref_bc: Vec<WireMessage<u32, L>> = ref_fx
            .iter()
            .filter_map(|e| match e {
                NodeEffect::Broadcast(m) => Some(m.clone()),
                _ => None,
            })
            .collect();
        sut_bc.sort();
        ref_bc.sort();
        check!(idx, "broadcasts", sut_bc, ref_bc);

        let sut_delivered: BTreeSet<(L, u32)> = sut_out
            .iter()
            .filter_map(|o| match o {
                Out::Event(RouterEvent::Delivered(l, s)) => Some((*l, *s)),
                _ => None,
            })
            .collect();
        let ref_delivered: BTreeSet<(L, u32)> = ref_fx
            .iter()
            .filter_map(|e| match e {
                NodeEffect::Deliver(l, s) => Some((*l, *s)),
                _ => None,
            })
            .collect();
        check!(idx, "delivered", sut_delivered, ref_delivered);

        Ok(())
    }
}

fn run_lockstep<L: TestLog>(
    subs: BTreeSet<u8>,
    intervals: Vec<u64>,
    steps: Vec<Step<L>>,
) -> Result<(), TestCaseError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async move {
            // Finding 4(a): 10, not 64 — the `u8` universe tops out at 48
            // units (3 logs × 8 seqs × up to 2 units each; the `Pair`
            // universe at 64), so a cap of 64 could never be reached and
            // the shed-at-cap branch in `ingest_parked` was dead in this
            // suite. 10 makes both the shed branch and `on_maintain`'s
            // eviction genuinely reachable.
            let mut driver = Driver::new(subs, &intervals, 10).await?;
            for (idx, step) in steps.iter().enumerate() {
                driver.apply(idx, step).await?;
            }
            Ok(())
        })
}

#[test]
fn fixed_regression_sequence() {
    // Subscribe, append, advance past the want-timer fire, receive a Want,
    // advance past the have-timer fire, receive a mixed Have (subscribed +
    // unsubscribed logs), unsubscribe, then advance past TTL expiry.
    let steps = vec![
        Step::Subscribe(0),
        Step::Append { log: 0 },
        Step::Advance(150), // past the initial 100ms want interval
        Step::RecvWant {
            from: 1,
            origin: 1,
            log: 1,
            start: 0,
            end: 5,
        },
        Step::Advance(150), // past the ~90-110ms have interval
        Step::RecvHave {
            from: 2,
            log: 1,
            seqs: vec![(0, true), (1, false)],
        }, // log 1: unsubscribed
        Step::RecvHave {
            from: 3,
            log: 0,
            seqs: vec![(5, true)],
        }, // log 0: subscribed, delivers
        // Finding 4(d): fill unsubscribed log 2 with payload-bearing ops
        // past the (relay_cap=10) cap, forcing at least one shed in
        // `ingest_parked` (usage sits at 3 units from log 1 above; seqs 0-2
        // fit — usage climbs to 9 — but seqs 3-4 each push usage+2 past 10
        // and are shed). This makes the shed-at-cap branch provably execute
        // every run, not just when the proptest strategy happens to hit it.
        Step::RecvHave {
            from: 4,
            log: 2,
            seqs: vec![(0, true), (1, true), (2, true), (3, true), (4, true)],
        },
        // Usage (9) now sits above the evict_at*relay_cap threshold (7), so
        // this Maintain genuinely evicts: log 2's payloads aren't wanted by
        // anyone (only log 1 is, via peer 1's still-live Want above), so
        // `eviction_candidates` picks them for payload-first GC.
        Step::Maintain,
        Step::Unsubscribe(0),
        Step::Advance(700), // past want_ttl/have_ttl (500ms) expiry
    ];
    let intervals = vec![100, 90, 110, 95, 105, 100];
    let subs = BTreeSet::from([0u8]);
    run_lockstep::<u8>(subs, intervals, steps).expect("SUT and reference must agree at every step");
}

/// Final review F2: the `Pair` universe's shape, pinned. Two logs under
/// channel 1 park in the relay, then a Subscribe migrates both; a peer's
/// mixed Want (a named tail plus the channel) arrives split into pieces, and
/// this node's own Want comes back echoed by a relay; the Have that answers
/// must agree between the machines.
#[test]
fn fixed_pair_regression_sequence() {
    let (a, b, other) = (Pair::new(1, 0), Pair::new(1, 1), Pair::new(0, 0));
    let mixed = |from, origin, log, budget| Step::RecvMixedWant {
        from,
        origin,
        ranges: vec![(log, 1, 8)],
        channels: vec![1],
        budget,
    };
    let split = dash_router::pack::pack_want(
        1u32,
        4u32,
        LogRanges::from_pairs([(a, Ranges::range(1, 8))]),
        BTreeSet::from([1u8]),
        12,
    );
    assert_eq!(
        (split.0.len(), split.1),
        (2, 0),
        "a 12-byte budget splits channels from ranges, dropping nothing"
    );
    let steps = vec![
        Step::RecvHave {
            from: 2,
            log: a,
            seqs: vec![(0, true), (1, true)],
        },
        Step::RecvHave {
            from: 3,
            log: b,
            seqs: vec![(0, true)],
        },
        Step::RecvHave {
            from: 3,
            log: other,
            seqs: vec![(0, false)],
        },
        Step::Subscribe(1),   // migrates a and b, not `other`
        Step::Advance(600),   // past have_ttl: the Haves above expire
        mixed(1, 4, a, 12),   // 4 names a's tail and wants channel 1, split
        mixed(2, 0, b, 3800), // this node's own Want, echoed by 2
        Step::Advance(300),   // the have timer fires and answers 4
        Step::Unsubscribe(1),
        Step::Append { log: other },
        Step::Advance(700),
    ];
    let intervals = vec![100, 90, 110, 95, 105, 100];
    run_lockstep(BTreeSet::new(), intervals, steps)
        .expect("SUT and reference must agree at every step");
}

fn pair_log() -> impl Strategy<Value = Pair> + Clone {
    (0u8..2, 0u8..2).prop_map(|(channel, author)| Pair::new(channel, author))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    #[test]
    fn shell_matches_the_node_machine(
        steps in proptest::collection::vec(step_strategy(0u8..3, 0u8..3), 1..40),
        intervals in proptest::collection::vec(20u64..400, 4..16),
        subs in proptest::collection::btree_set(0u8..3, 0..3),
    ) {
        run_lockstep(subs, intervals, steps)?;
    }

    /// Final review F2: two logs per channel (channel 2 holds none).
    #[test]
    fn shell_matches_the_node_machine_with_pair_logs(
        steps in proptest::collection::vec(step_strategy(pair_log(), 0u8..3), 1..40),
        intervals in proptest::collection::vec(20u64..400, 4..16),
        subs in proptest::collection::btree_set(0u8..3, 0..3),
    ) {
        run_lockstep(subs, intervals, steps)?;
    }
}

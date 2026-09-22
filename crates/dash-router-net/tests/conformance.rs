//! Task 11: lockstep conformance of the tokio shell's `NodeCore` (spec §2)
//! against the pure `NodeMachine` reference (`dash-router-core`).
//!
//! `NodeCore` is a deliberate, async transcription of `NodeMachine`'s
//! transition logic (see the module doc on `dash_router_net::shell`), kept
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
//! Zero-debounce ruling: conformance runs with
//! `PushDebouncePolicy { window_ms: 0, max_latency_ms: 0 }`, so an `Append`
//! step's SUT call (`on_append` + `advance_to`) flushes immediately and maps
//! to the reference's atomic `NodeAction::Authored`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use dash_router_core::{
    LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, OpsMap, Ranges, RouterAction,
    RouterConfig, RouterState, Seq, Storage, Units, WireMessage,
};
use dash_router_net::{CoreConfig, IntervalSource, NodeCore, Out, RouterEvent};
use dash_router_policy::PushDebouncePolicy;
use polestar::prelude::*;
use polestar::time::RealTime;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

// --- Scripted intervals (own copy: a #[cfg(test)] item in another crate is
// not reachable from an integration test) -----------------------------------

/// Deterministic intervals: pops from the front, repeats the last entry
/// forever. Mirrors `dash_router_net::shell`'s private test-only `Scripted`.
#[derive(Clone)]
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

// --- Step vocabulary ---------------------------------------------------

#[derive(Clone, Debug)]
enum Step {
    RecvWant { from: u32, log: u8, start: u32, end: u32 },
    RecvHave { from: u32, log: u8, seqs: Vec<(u32, bool)> },
    Append { log: u8 },
    Subscribe(u8),
    Unsubscribe(u8),
    Advance(u64),
}

fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (1u32..4, 0u8..3, 0u32..8, 0u32..8)
            .prop_map(|(from, log, start, end)| Step::RecvWant { from, log, start, end }),
        3 => (1u32..4, 0u8..3, proptest::collection::vec((0u32..8, any::<bool>()), 0..4))
            .prop_map(|(from, log, seqs)| Step::RecvHave { from, log, seqs }),
        2 => (0u8..3).prop_map(|log| Step::Append { log }),
        1 => (0u8..3).prop_map(Step::Subscribe),
        1 => (0u8..3).prop_map(Step::Unsubscribe),
        2 => (10u64..600).prop_map(Step::Advance),
    ]
}

// --- Deterministic op construction --------------------------------------

/// Locally authored op (brief's fixture): `header = [log, seq as u8]`,
/// always carries a payload.
fn authored_op(log: u8, seq: Seq) -> Op {
    Op { header: vec![log, seq as u8], payload: Some(vec![seq as u8]) }
}

/// A wire-carried op for `RecvHave`, with payload presence controlled by the
/// generated bool.
fn wire_op(log: u8, seq: Seq, has_payload: bool) -> Op {
    Op { header: vec![log, seq as u8], payload: has_payload.then(|| vec![seq as u8]) }
}

fn incoming(msg: &WireMessage<u32, u8>) -> dash_router_net::Incoming {
    dash_router_net::Incoming {
        remote: Some("192.168.0.9".parse().unwrap()),
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

fn assert_router_matches(
    idx: usize,
    sut: &RouterState<u32, u8, RealTime>,
    refr: &RouterState<u32, u8, RealTime>,
) -> Result<(), TestCaseError> {
    check!(idx, "held", sut.held, refr.held);
    check!(idx, "wants", sut.wants, refr.wants);
    check!(idx, "haves", sut.haves, refr.haves);
    check!(idx, "relayed_want_ranges", sut.relayed_want_ranges(), refr.relayed_want_ranges());
    check!(idx, "relayed_have_ranges", sut.relayed_have_ranges(), refr.relayed_have_ranges());
    check!(idx, "want_timer", sut.want_timer, refr.want_timer);
    check!(idx, "have_timer", sut.have_timer, refr.have_timer);
    Ok(())
}

type Core = NodeCore<u32, u8, OpsMap<u8>, OpsMap<u8>, Scripted>;

/// Drives a `NodeCore` (the SUT) and a `NodeMachine`/`NodeState` (the
/// reference) through an identical action sequence, asserting agreement
/// after every step.
struct Driver {
    core: Core,
    ref_machine: NodeMachine<u32, u8, RealTime>,
    ref_state: NodeState<u32, u8, RealTime>,
    ref_script: Scripted,
    now: Duration,
    next_seq: BTreeMap<u8, Seq>,
}

impl Driver {
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
            debounce: PushDebouncePolicy { window_ms: 0, max_latency_ms: 0 },
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
        let ref_state = NodeState::new(0u32, subs);
        let mut d = Self {
            core,
            ref_machine,
            ref_state,
            ref_script: Scripted::ms(intervals),
            now: Duration::ZERO,
            next_seq: BTreeMap::new(),
        };
        // init() arms the SUT's first Want timer with the script's first
        // interval; mirror it on the reference with the SAME script, so both
        // consume scripted intervals in identical order from here on.
        let next = d.ref_script.next_want();
        d.ref_step(0, NodeAction::Router(RouterAction::ArmWantTimer(next.into())))?;
        Ok(d)
    }

    fn ref_step(
        &mut self,
        idx: usize,
        action: NodeAction<u32, u8, RealTime>,
    ) -> Result<Vec<NodeEffect<u32, u8>>, TestCaseError> {
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
    ) -> Result<Vec<NodeEffect<u32, u8>>, TestCaseError> {
        let mut out = Vec::new();
        let mut now = self.now;
        loop {
            // 1. Flush: vacuous at zero debounce — the driver always forces
            // the flush within the same Append step (see `apply`), so no
            // pending push ever survives to a later step.
            if self
                .ref_state
                .router
                .want_timer
                .as_ref()
                .is_some_and(|t| t.remaining.is_zero())
            {
                let fx = self.ref_step(idx, NodeAction::Router(RouterAction::FireWant))?;
                out.extend(fx);
                let next = self.ref_script.next_want();
                let fx2 =
                    self.ref_step(idx, NodeAction::Router(RouterAction::ArmWantTimer(next.into())))?;
                out.extend(fx2);
                continue;
            }
            if self
                .ref_state
                .router
                .have_timer
                .as_ref()
                .is_some_and(|t| t.remaining.is_zero())
            {
                let fx = self.ref_step(idx, NodeAction::Router(RouterAction::FireHave))?;
                out.extend(fx);
                if !self.ref_state.router.wants.is_empty() {
                    let next = self.ref_script.next_have();
                    let fx2 = self
                        .ref_step(idx, NodeAction::Router(RouterAction::ArmHaveTimer(next.into())))?;
                    out.extend(fx2);
                }
                continue;
            }
            if now >= target {
                break;
            }
            let mut step = target - now;
            for t in [&self.ref_state.router.want_timer, &self.ref_state.router.have_timer]
                .into_iter()
                .flatten()
            {
                step = step.min(*t.remaining);
            }
            let fx = self.ref_step(idx, NodeAction::Router(RouterAction::Tick(step.into())))?;
            debug_assert!(fx.is_empty(), "Tick never produces effects");
            now += step;
        }
        self.now = now;
        Ok(out)
    }

    async fn apply(&mut self, idx: usize, step: &Step) -> Result<(), TestCaseError> {
        let sut_out: Vec<Out<u32, u8>>;
        let ref_fx: Vec<NodeEffect<u32, u8>>;
        match step.clone() {
            Step::Subscribe(log) => {
                sut_out = self
                    .core
                    .on_subscribe(self.now, log)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT on_subscribe: {e}")))?;
                ref_fx = self.ref_step(idx, NodeAction::Subscribe(log))?;
            }
            Step::Unsubscribe(log) => {
                sut_out = self
                    .core
                    .on_unsubscribe(self.now, log)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT on_unsubscribe: {e}")))?;
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
                let flushed = self
                    .core
                    .advance_to(self.now)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT post-append advance_to: {e}")))?;
                out.extend(flushed);
                sut_out = out;
                ref_fx = self.ref_step(idx, NodeAction::Authored(log, seq, op))?;
            }
            Step::RecvWant { from, log, start, end } => {
                let msg: WireMessage<u32, u8> =
                    WireMessage::want(from, LogRanges::from_pairs([(log, Ranges::range(start, end))]));
                sut_out = self
                    .core
                    .on_wire(self.now, incoming(&msg))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT on_wire(Want): {e}")))?;
                let mut fx = self.ref_step(idx, NodeAction::Recv(msg))?;
                // Binding semantics #1: a witnessed Want arms the Have timer
                // when none is armed. Mirror the SUT's arm-on-recv, in the
                // same call.
                if self.ref_state.router.have_timer.is_none() && !self.ref_state.router.wants.is_empty()
                {
                    let next = self.ref_script.next_have();
                    let more = self
                        .ref_step(idx, NodeAction::Router(RouterAction::ArmHaveTimer(next.into())))?;
                    fx.extend(more);
                }
                ref_fx = fx;
            }
            Step::RecvHave { from, log, seqs } => {
                let group: Vec<(Seq, Op)> =
                    seqs.iter().map(|&(s, has)| (s, wire_op(log, s, has))).collect();
                let msg: WireMessage<u32, u8> = WireMessage::have(from, vec![(log, group)]);
                sut_out = self
                    .core
                    .on_wire(self.now, incoming(&msg))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT on_wire(Have): {e}")))?;
                ref_fx = self.ref_step(idx, NodeAction::Recv(msg))?;
            }
            Step::Advance(ms) => {
                let target = self.now + Duration::from_millis(ms);
                sut_out = self
                    .core
                    .advance_to(target)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("step {idx}: SUT advance_to: {e}")))?;
                ref_fx = self.ref_advance_to(idx, target)?;
            }
        }
        self.compare(idx, &sut_out, &ref_fx)?;
        Ok(())
    }

    fn compare(
        &self,
        idx: usize,
        sut_out: &[Out<u32, u8>],
        ref_fx: &[NodeEffect<u32, u8>],
    ) -> Result<(), TestCaseError> {
        assert_router_matches(idx, &self.core.router, &self.ref_state.router)?;

        let sut_ext = Storage::held_all(&self.core.ext);
        let ref_ext = self.ref_state.ext.0.held_all();
        check!(idx, "ext.held_all", sut_ext, ref_ext);

        let sut_relay = Storage::held_all(&self.core.relay);
        let ref_relay = self.ref_state.relay.0.held_all();
        check!(idx, "relay.held_all", sut_relay, ref_relay);

        check!(idx, "subscriptions", self.core.subscriptions, self.ref_state.subscriptions);

        let mut sut_bc: Vec<WireMessage<u32, u8>> = sut_out
            .iter()
            .filter_map(|o| match o {
                Out::Broadcast(m) => Some(m.clone()),
                _ => None,
            })
            .collect();
        let mut ref_bc: Vec<WireMessage<u32, u8>> = ref_fx
            .iter()
            .filter_map(|e| match e {
                NodeEffect::Broadcast(m) => Some(m.clone()),
                _ => None,
            })
            .collect();
        sut_bc.sort();
        ref_bc.sort();
        check!(idx, "broadcasts", sut_bc, ref_bc);

        let sut_delivered: BTreeSet<(u8, u32)> = sut_out
            .iter()
            .filter_map(|o| match o {
                Out::Event(RouterEvent::Delivered(l, s)) => Some((*l, *s)),
                _ => None,
            })
            .collect();
        let ref_delivered: BTreeSet<(u8, u32)> = ref_fx
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

fn run_lockstep(
    subs: BTreeSet<u8>,
    intervals: Vec<u64>,
    steps: Vec<Step>,
) -> Result<(), TestCaseError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async move {
            let mut driver = Driver::new(subs, &intervals, 64).await?;
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
        Step::RecvWant { from: 1, log: 1, start: 0, end: 5 },
        Step::Advance(150), // past the ~90-110ms have interval
        Step::RecvHave { from: 2, log: 1, seqs: vec![(0, true), (1, false)] }, // log 1: unsubscribed
        Step::RecvHave { from: 3, log: 0, seqs: vec![(5, true)] },            // log 0: subscribed, delivers
        Step::Unsubscribe(0),
        Step::Advance(700), // past want_ttl/have_ttl (500ms) expiry
    ];
    let intervals = vec![100, 90, 110, 95, 105, 100];
    let subs = BTreeSet::from([0u8]);
    run_lockstep(subs, intervals, steps).expect("SUT and reference must agree at every step");
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    #[test]
    fn shell_matches_the_node_machine(
        steps in proptest::collection::vec(step_strategy(), 1..40),
        intervals in proptest::collection::vec(20u64..400, 4..16),
        subs in proptest::collection::btree_set(0u8..3, 0..3),
    ) {
        run_lockstep(subs, intervals, steps)?;
    }
}

//! Scenario tests driving `RouterMachine` by hand, with bounded model types.
//! The router decides ranges only; hydration, storage and delivery live in
//! the shell above it (spec §5) and are out of scope here.

use std::sync::Arc;

use dash_router_core::{
    Effect, LogRanges, Ranges, RouterAction as A, RouterConfig, RouterMachine, RouterState,
};
use polestar::{StateMachine, prelude::*, time::FiniteTime};

type N = UpTo<3>;
type L = UpTo<2>;
type T = FiniteTime<4, 1000>;
type Router = RouterMachine<N, L, T>;
type State = RouterState<N, L, T>;
type Node = StateMachine<Router>;

fn t(n: usize) -> T {
    UpTo::new(n).into()
}
fn n(i: usize) -> N {
    UpTo::new(i)
}

fn lr(pairs: impl IntoIterator<Item = (usize, Ranges)>) -> LogRanges<L> {
    LogRanges::from_pairs(pairs.into_iter().map(|(l, r)| (UpTo::new(l), r)))
}

fn machine() -> Arc<Router> {
    Arc::new(RouterMachine::new(RouterConfig {
        want_ttl: t(2),
        have_ttl: t(2),
    }))
}

fn node(m: &Arc<Router>, id: usize, held: LogRanges<L>) -> Node {
    m.state_machine(State::new(n(id), held))
}

fn disabled(m: &Router, s: &State, action: A<N, L, T>) {
    assert!(
        m.transition(s.clone(), action.clone()).is_err(),
        "{action:?} should not be enabled"
    );
}

fn sends_want(fx: &[Effect<L>]) -> Vec<&LogRanges<L>> {
    fx.iter()
        .filter_map(|e| match e {
            Effect::SendWant(r) => Some(r),
            _ => None,
        })
        .collect()
}

fn sends_have(fx: &[Effect<L>]) -> Vec<&LogRanges<L>> {
    fx.iter()
        .filter_map(|e| match e {
            Effect::SendHave(r) => Some(r),
            _ => None,
        })
        .collect()
}

/// DESIGN.md: Wants flood too — this is how a request crosses hops to reach
/// a node that actually holds the data. Once per hop, and never back to a
/// node that already emitted it.
#[test]
fn a_want_floods_once_per_hop_and_never_echoes() {
    let m = machine();
    let mut b = node(&m, 1, LogRanges::empty());

    let full = lr([(0, Ranges::full())]);
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            ranges: full.clone(),
        })
        .unwrap();
    assert_eq!(sends_want(&fx), vec![&full]);

    // A repeat of the same Want is not re-relayed: the seen-set suppresses it.
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            ranges: full.clone(),
        })
        .unwrap();
    assert!(sends_want(&fx).is_empty(), "repeated Want is not re-relayed");

    // Once the seen-set entry expires, the same Want floods again.
    b.step(A::Tick(t(2))).unwrap();
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            ranges: full.clone(),
        })
        .unwrap();
    assert_eq!(sends_want(&fx), vec![&full], "relays again after expiry");
}

/// DESIGN.md: every received Have floods onward, novel to this node or not
/// — its neighbours may still need it, and only this node can reach them.
/// The seen-set alone ends the flood; novelty alone gates `Accept`.
#[test]
fn a_have_floods_once_and_accepts_only_novelty() {
    let m = machine();
    let mut b = node(&m, 1, LogRanges::empty());

    let ranges = lr([(0, Ranges::range(0, 2))]);
    let fx = b
        .step(A::RecvHave {
            from: n(0),
            ranges: ranges.clone(),
        })
        .unwrap();
    assert_eq!(
        fx,
        vec![Effect::Accept(ranges.clone()), Effect::SendHave(ranges.clone())],
        "novel ranges are accepted before being relayed"
    );

    // A second, identical Have from a different peer: already held, so no
    // Accept; already relayed, so no SendHave either.
    let fx = b
        .step(A::RecvHave {
            from: n(2),
            ranges: ranges.clone(),
        })
        .unwrap();
    assert_eq!(fx, vec![]);
}

/// Each flood adds its own seen-set record with its own TTL. A single
/// accumulating record whose TTL refreshes on every update would let steady
/// traffic keep old ranges suppressed forever.
#[test]
fn seen_set_records_expire_independently() {
    let m = machine(); // have_ttl = 2
    let mut b = node(&m, 1, LogRanges::empty());
    let have = |seq: usize| lr([(0, Ranges::range(seq as u32, seq as u32 + 1))]);

    let fx = b
        .step(A::RecvHave {
            from: n(0),
            ranges: have(0),
        })
        .unwrap();
    assert_eq!(sends_have(&fx).len(), 1);
    b.step(A::Tick(t(1))).unwrap();
    // A second flood must not extend the first record's life.
    let fx = b
        .step(A::RecvHave {
            from: n(0),
            ranges: have(1),
        })
        .unwrap();
    assert_eq!(sends_have(&fx).len(), 1);
    b.step(A::Tick(t(1))).unwrap();

    // Range 0's record has expired: it floods again. Range 1's has not.
    let fx = b
        .step(A::RecvHave {
            from: n(0),
            ranges: have(0),
        })
        .unwrap();
    assert_eq!(sends_have(&fx).len(), 1, "expired range floods again");
    let fx = b
        .step(A::RecvHave {
            from: n(0),
            ranges: have(1),
        })
        .unwrap();
    assert!(sends_have(&fx).is_empty(), "younger record still suppresses");
}

/// `Push` is storage telling the router "I now hold this too": `held` grows
/// incrementally (unlike `Held`, which replaces it wholesale) and the news
/// floods exactly like a received Have would, with the same seen-set
/// suppressing echoes and repeats.
#[test]
fn push_grows_held_suppresses_echo_and_floods() {
    let m = machine();
    let mut a = node(&m, 0, LogRanges::empty());

    let pushed = lr([(0, Ranges::range(0, 1))]);
    let fx = a.step(A::Push(pushed.clone())).unwrap();
    assert_eq!(fx, vec![Effect::SendHave(pushed.clone())]);

    // A peer echoes the same range back: already held (no Accept), already
    // relayed by this node's own Push (no SendHave).
    let fx = a
        .step(A::RecvHave {
            from: n(1),
            ranges: pushed.clone(),
        })
        .unwrap();
    assert_eq!(fx, vec![]);

    // Pushing the identical range again has nothing new to report.
    let fx = a.step(A::Push(pushed.clone())).unwrap();
    assert_eq!(fx, vec![]);
}

/// `Held` is an absolute snapshot from the storage layer, not a merge: if it
/// reports less than before (e.g. eviction), the router's want reopens.
#[test]
fn a_held_snapshot_shrink_reopens_the_want() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 3))]));

    a.step(A::Held(lr([(0, Ranges::range(0, 1))]))).unwrap();
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&lr([(0, Ranges::from(1))])]);
}

/// BLOCKED: per the brief, an empty-range key in `held` should mark a log as
/// "known but empty," so it is wanted in full. `RouterState::wanted` already
/// implements that (see `router.rs`), but `LogRanges::from_pairs`/`insert`
/// (ranges.rs, from Task 1) drop empty-valued entries by construction, so no
/// public API can currently produce such a `held`. This test documents the
/// intended behaviour and is `#[ignore]`d until `LogRanges` gains a way to
/// record a known-but-empty log; see task-2-report.md for detail.
#[test]
#[ignore = "needs LogRanges support for a known-but-empty log entry; see task-2-report.md"]
fn an_empty_held_key_wants_the_whole_log() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::empty())]));
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&lr([(0, Ranges::full())])]);
}

/// A node that already holds data answers a Want with exactly its ranges
/// intersected with what was asked for, and remembers its own emission so
/// an immediate repeat has nothing left to say.
#[test]
fn want_then_have_backfills_a_late_subscriber() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 2))]));

    a.step(A::RecvWant {
        from: n(1),
        ranges: lr([(0, Ranges::full())]),
    })
    .unwrap();
    a.step(A::ArmHaveTimer(t(0))).unwrap();
    let fx = a.step(A::FireHave).unwrap();
    assert_eq!(sends_have(&fx), vec![&lr([(0, Ranges::range(0, 2))])]);

    // A remembers its own Have, so it has nothing left to say immediately.
    assert!(a.next_have().is_empty());
}

#[test]
fn want_timer_follows_the_fetch_timed_idiom() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 2))]));

    disabled(&m, &a, A::FireWant); // nothing armed
    disabled(&m, &a, A::Tick(t(0))); // zero tick is not a transition
    a.step(A::ArmWantTimer(t(2))).unwrap();
    disabled(&m, &a, A::ArmWantTimer(t(1))); // already armed
    disabled(&m, &a, A::FireWant); // not due
    disabled(&m, &a, A::Tick(t(3))); // would skip past due
    a.step(A::Tick(t(1))).unwrap();
    disabled(&m, &a, A::FireWant);
    a.step(A::Tick(t(1))).unwrap();
    disabled(&m, &a, A::Tick(t(1))); // due: must fire before time moves on

    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&lr([(0, Ranges::from(2))])]);
    assert!(a.want_timer.is_none(), "must re-arm explicitly");
}

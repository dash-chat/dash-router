//! Scenario tests driving `RouterMachine` by hand, with bounded model types.
//! The router decides ranges only; hydration, storage and delivery live in
//! the shell above it (spec §5) and are out of scope here.

use std::{collections::BTreeSet, sync::Arc};

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
            Effect::SendWant { ranges, .. } => Some(ranges),
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
            prefixes: BTreeSet::new(),
        })
        .unwrap();
    assert_eq!(sends_want(&fx), vec![&full]);

    // A repeat of the same Want is not re-relayed: the seen-set suppresses it.
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            ranges: full.clone(),
            prefixes: BTreeSet::new(),
        })
        .unwrap();
    assert!(
        sends_want(&fx).is_empty(),
        "repeated Want is not re-relayed"
    );

    // Once the seen-set entry expires, the same Want floods again.
    b.step(A::Tick(t(2))).unwrap();
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            ranges: full.clone(),
            prefixes: BTreeSet::new(),
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
        vec![
            Effect::Accept(ranges.clone()),
            Effect::SendHave(ranges.clone())
        ],
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
    assert!(
        sends_have(&fx).is_empty(),
        "younger record still suppresses"
    );
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

/// An empty-range key in `held` marks a log as "known but empty" (see
/// `LogRanges`'s type doc in ranges.rs), so it is wanted in full.
#[test]
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
        prefixes: BTreeSet::new(),
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

mod prefix {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use dash_router_core::{
        Effect, LogRanges, Pair, Ranges, RouterAction, RouterConfig, RouterMachine, RouterState,
    };
    use polestar::prelude::*;
    use polestar::time::RealTime;

    type M = RouterMachine<u32, Pair, RealTime>;
    type A = RouterAction<u32, Pair, RealTime>;

    fn machine() -> M {
        RouterMachine::new(RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
        })
    }

    fn held(pairs: &[(Pair, Ranges)]) -> LogRanges<Pair> {
        LogRanges::from_pairs(pairs.iter().cloned())
    }

    fn sent_have(fx: &[Effect<Pair>]) -> Option<LogRanges<Pair>> {
        fx.iter().find_map(|e| match e {
            Effect::SendHave(r) => Some(r.clone()),
            _ => None,
        })
    }

    fn sent_want(fx: &[Effect<Pair>]) -> Option<(LogRanges<Pair>, BTreeSet<u8>)> {
        fx.iter().find_map(|e| match e {
            Effect::SendWant { ranges, prefixes } => Some((ranges.clone(), prefixes.clone())),
            _ => None,
        })
    }

    /// Spec §3.5: a prefix Want is answered with every held log under the
    /// prefix that the wanter did not name explicitly; a log it named gets
    /// only its named ranges.
    #[test]
    fn prefix_want_excludes_logs_the_wanter_named() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let other = Pair::new(2, 1);
        let s = RouterState::new(
            0u32,
            held(&[
                (a1, Ranges::range(0, 10)),
                (a2, Ranges::range(0, 5)),
                (other, Ranges::range(0, 3)),
            ]),
        );
        // Peer 7 holds a1 up to 4 and wants its tail; knows nothing else under prefix 1.
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: held(&[(a1, Ranges::from(4))]),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmHaveTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireHave).unwrap();
        let have = sent_have(&fx).expect("a Have is sent");
        assert_eq!(
            have.get(&a1),
            Some(&Ranges::range(4, 10)),
            "named log: only the named tail"
        );
        assert_eq!(
            have.get(&a2),
            Some(&Ranges::range(0, 5)),
            "unnamed log under prefix: wholesale"
        );
        assert_eq!(have.get(&other), None, "other prefix: untouched");
    }

    /// `Open` prefixes ride on every own Want even when no ranges are wanted.
    #[test]
    fn open_prefixes_are_sent_with_fire_want() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let (s, _) = m
            .transition(s, A::Open(BTreeSet::from([4u8, 5u8])))
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmWantTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireWant).unwrap();
        let (ranges, prefixes) = sent_want(&fx).expect("a Want is sent");
        assert!(ranges.is_empty());
        assert_eq!(prefixes, BTreeSet::from([4u8, 5u8]));
    }

    /// A received Want's prefixes are relayed once (seen-set), like ranges.
    #[test]
    fn received_prefixes_are_relayed_once() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let want = |from| A::RecvWant {
            from,
            ranges: LogRanges::empty(),
            prefixes: BTreeSet::from([9u8]),
        };
        let (s, fx1) = m.transition(s, want(1)).unwrap();
        assert_eq!(sent_want(&fx1).map(|(_, p)| p), Some(BTreeSet::from([9u8])));
        let (_, fx2) = m.transition(s, want(2)).unwrap();
        assert!(sent_want(&fx2).is_none(), "already relayed within want_ttl");
    }

    /// Eviction policy input: logs under a peer's wanted prefix count as wanted.
    #[test]
    fn others_wants_includes_held_logs_under_wanted_prefixes() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let s = RouterState::new(0u32, held(&[(a1, Ranges::range(0, 10))]));
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: LogRanges::empty(),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        assert_eq!(s.others_wants().get(&a1), Some(&Ranges::range(0, 10)));
    }

    /// Spec §3.5: a log this node knows under its own open prefix stays
    /// named in its Want even when a peer already wants the same ranges,
    /// or answerers would treat it as unnamed and send it wholesale. Logs
    /// under other prefixes are still suppressed as before.
    #[test]
    fn logs_under_own_open_prefix_stay_named_when_suppressed() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let b1 = Pair::new(2, 1);
        let s = RouterState::new(
            0u32,
            held(&[(a1, Ranges::range(0, 4)), (b1, Ranges::range(0, 4))]),
        );
        let (s, _) = m.transition(s, A::Open(BTreeSet::from([1u8]))).unwrap();
        // Peer Y already wants both open tails, with no prefixes.
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: held(&[(a1, Ranges::from(4)), (b1, Ranges::from(4))]),
                    prefixes: BTreeSet::new(),
                },
            )
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmWantTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireWant).unwrap();
        let (ranges, prefixes) = sent_want(&fx).expect("a Want is sent");
        assert_eq!(prefixes, BTreeSet::from([1u8]));
        assert_eq!(
            ranges.get(&a1),
            Some(&Ranges::from(4)),
            "log under own open prefix stays named"
        );
        assert_eq!(
            ranges.get(&b1),
            None,
            "log under another prefix is suppressed"
        );
    }

    /// A relayed prefix Want keeps the wanter's named ranges, even ranges
    /// this relayer already relayed for someone else, so a two-hop
    /// answerer never sends a named log wholesale.
    #[test]
    fn relayed_prefixes_carry_the_wanters_named_ranges() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let s = RouterState::new(0u32, LogRanges::empty());
        let tail = || held(&[(a1, Ranges::from(4))]);
        // Y's Want for a1's tail is relayed first.
        let (s, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: 1,
                    ranges: tail(),
                    prefixes: BTreeSet::new(),
                },
            )
            .unwrap();
        assert!(sent_want(&fx).is_some(), "Y's Want is relayed");
        // X names the same tail and adds prefix 1.
        let (s, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: 2,
                    ranges: tail(),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        let (ranges, prefixes) = sent_want(&fx).expect("X's Want is relayed");
        assert_eq!(prefixes, BTreeSet::from([1u8]));
        assert_eq!(
            ranges.get(&a1),
            Some(&Ranges::from(4)),
            "named ranges travel with the prefixes"
        );
        // Z repeats X's Want: both halves already relayed.
        let (_, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: 3,
                    ranges: tail(),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        assert!(sent_want(&fx).is_none(), "already relayed within want_ttl");
    }

    fn ms(n: u64) -> RealTime {
        Duration::from_millis(n).into()
    }

    /// A Want split across wire messages (the shell's `pack_want` puts
    /// prefixes and ranges in different pieces) is recorded whole: the
    /// pieces union, and a log named in one piece is still answered by name,
    /// not wholesale, even though another piece carries its prefix.
    #[test]
    fn split_wants_from_one_peer_accumulate() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let b1 = Pair::new(1, 2);
        let s = RouterState::new(
            0u32,
            held(&[(a1, Ranges::range(0, 10)), (b1, Ranges::range(0, 10))]),
        );
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: held(&[(a1, Ranges::from(5))]),
                    prefixes: BTreeSet::new(),
                },
            )
            .unwrap();
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: LogRanges::empty(),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        let have = s.next_have();
        assert_eq!(
            have.get(&a1),
            Some(&Ranges::range(5, 10)),
            "named in one piece: only the named tail, not wholesale"
        );
        assert_eq!(
            have.get(&b1),
            Some(&Ranges::range(0, 10)),
            "unnamed under the other piece's prefix: wholesale"
        );
        let (s, _) = m.transition(s, A::Tick(ms(501))).unwrap();
        assert!(s.wants.is_empty(), "every piece expires after want_ttl");
    }

    /// Each received Want dies `want_ttl` after its own arrival, so a stale
    /// piece falls out while a later one from the same peer lives on.
    #[test]
    fn stale_want_pieces_expire_independently() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let s = RouterState::new(0u32, LogRanges::empty());
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: held(&[(a1, Ranges::from(5))]),
                    prefixes: BTreeSet::new(),
                },
            )
            .unwrap();
        let (s, _) = m.transition(s, A::Tick(ms(250))).unwrap();
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: held(&[(a1, Ranges::from(8))]),
                    prefixes: BTreeSet::new(),
                },
            )
            .unwrap();
        assert_eq!(s.wants[&7].len(), 2, "both pieces live");
        assert_eq!(s.others_wants().get(&a1), Some(&Ranges::from(5)));
        let (s, _) = m.transition(s, A::Tick(ms(260))).unwrap();
        assert_eq!(s.wants[&7].len(), 1, "the first piece expired");
        assert_eq!(
            s.others_wants().get(&a1),
            Some(&Ranges::from(8)),
            "only the later piece remains"
        );
    }
}

//! Scenario tests driving `RouterMachine` by hand, with bounded model types.
//! The router decides ranges only; hydration, storage and delivery live in
//! the shell above it (spec §5) and are out of scope here.

use std::sync::Arc;

use dash_router_core::{
    Effect, Entry, Interest, LogRanges, Ranges, RouterAction as A, RouterConfig, RouterMachine,
    RouterState,
};
use polestar::{StateMachine, id::IdUnit, prelude::*, time::FiniteTime};

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

/// A whole-scope Interest: one entry per log (the log is its own channel,
/// with the unit author), claiming `have`. An empty have is a pure want.
fn want(pairs: impl IntoIterator<Item = (usize, Ranges)>) -> Interest<L> {
    let mut i = Interest::empty();
    for (l, have) in pairs {
        i.insert(
            UpTo::new(l),
            Entry::whole([(IdUnit, have)].into_iter().collect()),
        );
    }
    i
}

fn machine() -> Arc<Router> {
    Arc::new(RouterMachine::new(RouterConfig {
        want_ttl: t(2),
        have_ttl: t(2),
        heard_ttl: t(2),
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

fn sends_want(fx: &[Effect<L>]) -> Vec<&Interest<L>> {
    fx.iter()
        .filter_map(|e| match e {
            Effect::SendWant(i) => Some(i),
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

    let pure = want([(0, Ranges::empty())]);
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            interest: pure.clone(),
        })
        .unwrap();
    assert_eq!(sends_want(&fx), vec![&pure]);

    // A repeat of the same Want is not re-relayed: the seen set covers it.
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            interest: pure.clone(),
        })
        .unwrap();
    assert!(
        sends_want(&fx).is_empty(),
        "repeated Want is not re-relayed"
    );

    // Once the seen record expires, the same Want floods again.
    b.step(A::Tick(t(2))).unwrap();
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            interest: pure.clone(),
        })
        .unwrap();
    assert_eq!(sends_want(&fx), vec![&pure], "relays again after expiry");
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
/// reports less than before (e.g. eviction), the Want's have shrinks and the
/// network is asked for the rest again.
#[test]
fn a_held_snapshot_shrink_reopens_the_want() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 3))]));

    a.step(A::Held(lr([(0, Ranges::range(0, 1))]))).unwrap();
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(
        sends_want(&fx),
        vec![&want([(0, Ranges::range(0, 1))])],
        "claims only what is held now"
    );
}

/// Interest comes from subscriptions and from held data, not from
/// known-but-empty markers: an open channel with nothing held is a pure
/// want, while a bare marker asks for nothing at all.
#[test]
fn an_open_channel_with_nothing_held_is_a_pure_want() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::empty())]));
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert!(sends_want(&fx).is_empty(), "a marker alone is no interest");

    a.step(A::Open([UpTo::new(0)].into_iter().collect()))
        .unwrap();
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&want([(0, Ranges::empty())])]);
}

/// A node that already holds data answers a Want with exactly what the
/// asker lacks of it, and remembers its own emission so an immediate
/// repeat has nothing left to say.
#[test]
fn want_then_have_backfills_a_late_subscriber() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 2))]));

    disabled(&m, &a, A::ArmHaveTimer(t(0))); // nothing seen yet
    a.step(A::RecvWant {
        from: n(1),
        interest: want([(0, Ranges::empty())]),
    })
    .unwrap();
    a.step(A::ArmHaveTimer(t(0))).unwrap();
    let fx = a.step(A::FireHave).unwrap();
    assert_eq!(sends_have(&fx), vec![&lr([(0, Ranges::range(0, 2))])]);

    // A remembers its own Have, so it has nothing left to say immediately.
    assert!(a.next_have().is_empty());
}

/// The answer is the union over seen Wants of what each asker lacks: two
/// askers with different haves are served by one Have of their union.
#[test]
fn answer_is_what_any_asker_lacks() {
    let m = machine();
    let mut a = node(
        &m,
        0,
        lr([(0, Ranges::range(0, 4)), (1, Ranges::range(0, 2))]),
    );
    a.step(A::RecvWant {
        from: n(1),
        interest: want([(0, Ranges::range(0, 2)), (1, Ranges::range(0, 2))]),
    })
    .unwrap();
    a.step(A::RecvWant {
        from: n(2),
        interest: want([(0, Ranges::range(0, 3))]),
    })
    .unwrap();
    assert_eq!(
        a.next_have(),
        lr([(0, Ranges::range(2, 4))]),
        "log 0: peer 1 lacks 2..4, peer 2 lacks 3..4; log 1: nobody lacks anything"
    );
    assert_eq!(a.network_ask(), a.next_have(), "nothing recently sent");
}

/// Emission cancels: a node's own Want is seen, so an unchanged interest is
/// re-emitted only once the record expires.
#[test]
fn own_emission_suppresses_reemission_until_want_ttl() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 2))]));
    let mine = want([(0, Ranges::range(0, 2))]);

    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&mine]);
    assert_eq!(a.seen.len(), 1, "own emission is seen");

    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert!(sends_want(&fx).is_empty(), "already represented");

    a.step(A::Tick(t(2))).unwrap();
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&mine], "re-emitted after expiry");
}

/// A peer's Want that asks for at least everything this node would ask for
/// silences it; one that asks for less does not.
#[test]
fn a_covering_want_from_a_peer_silences_this_node() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 2))]));

    // Peer 1 claims more than a holds: it asks for 3.., a asks for 2...
    a.step(A::RecvWant {
        from: n(1),
        interest: want([(0, Ranges::range(0, 3))]),
    })
    .unwrap();
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx).len(), 1, "peer asks for less than a");

    // Peer 2 wants the whole log: everything a asks for is already asked.
    a.step(A::Tick(t(2))).unwrap();
    a.step(A::RecvWant {
        from: n(2),
        interest: want([(0, Ranges::empty())]),
    })
    .unwrap();
    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    assert!(sends_want(&fx).is_empty(), "covered by peer 2's pure want");
}

/// A channel heard wanted by a holder becomes a channel of interest for a
/// node holding nothing under it, until `heard_ttl`; a pure want (no data
/// behind it) is never adopted.
#[test]
fn heard_channels_are_adopted_from_holders_only_and_expire() {
    let m = machine();
    let mut b = node(&m, 1, LogRanges::empty());

    b.step(A::RecvWant {
        from: n(0),
        interest: want([(0, Ranges::range(0, 2)), (1, Ranges::empty())]),
    })
    .unwrap();
    assert_eq!(
        b.heard.keys().copied().collect::<Vec<L>>(),
        vec![UpTo::new(0)]
    );
    b.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = b.step(A::FireWant).unwrap();
    assert_eq!(
        sends_want(&fx),
        vec![&want([(0, Ranges::empty())])],
        "asks for all of the heard channel, not covered by the holder's own ask"
    );

    b.step(A::Tick(t(2))).unwrap();
    assert!(b.heard.is_empty(), "expired");
    b.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = b.step(A::FireWant).unwrap();
    assert!(sends_want(&fx).is_empty(), "no interest left");
}

/// A Want this node cannot help with is seen and relayed but there is
/// nothing to answer: `next_have` stays empty.
#[test]
fn a_want_for_data_this_node_lacks_answers_nothing() {
    let m = machine();
    let mut b = node(&m, 1, lr([(1, Ranges::range(0, 1))]));
    let fx = b
        .step(A::RecvWant {
            from: n(0),
            interest: want([(0, Ranges::empty())]),
        })
        .unwrap();
    assert_eq!(sends_want(&fx).len(), 1, "relayed");
    assert!(b.next_have().is_empty());
    disabled(&m, &b, A::ArmHaveTimer(t(0)));
}

#[test]
fn want_timer_follows_the_fetch_timed_idiom() {
    let m = machine();
    let mut a = node(&m, 0, lr([(0, Ranges::range(0, 2))]));

    disabled(&m, &a, A::FireWant); // nothing armed
    disabled(&m, &a, A::Tick(t(0))); // zero tick is not a transition
    assert_eq!(a.next_due(), None); // any tick is legal
    a.step(A::ArmWantTimer(t(2))).unwrap();
    disabled(&m, &a, A::ArmWantTimer(t(1))); // already armed
    disabled(&m, &a, A::FireWant); // not due
    assert!(!a.want_due());
    assert_eq!(a.next_due(), Some(t(2))); // the tick bound
    disabled(&m, &a, A::Tick(t(3))); // would skip past due
    a.step(A::Tick(t(1))).unwrap();
    disabled(&m, &a, A::FireWant);
    assert_eq!(a.next_due(), Some(t(1)));
    a.step(A::Tick(t(1))).unwrap();
    assert!(a.want_due());
    assert_eq!(a.next_due(), Some(t(0))); // no tick at all
    disabled(&m, &a, A::Tick(t(1))); // due: must fire before time moves on

    let fx = a.step(A::FireWant).unwrap();
    assert_eq!(sends_want(&fx), vec![&want([(0, Ranges::range(0, 2))])]);
    assert!(a.want_timer.is_none(), "must re-arm explicitly");
}

mod channel {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use dash_router_core::{
        Effect, Entry, Interest, Log, LogRanges, Pair, Ranges, RouterAction, RouterConfig,
        RouterMachine, RouterState,
    };
    use polestar::prelude::*;
    use polestar::time::RealTime;

    type M = RouterMachine<u32, Pair, RealTime>;
    type A = RouterAction<u32, Pair, RealTime>;

    fn machine() -> M {
        RouterMachine::new(RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
            heard_ttl: Duration::from_millis(500).into(),
        })
    }

    fn held(pairs: &[(Pair, Ranges)]) -> LogRanges<Pair> {
        LogRanges::from_pairs(pairs.iter().cloned())
    }

    /// One whole-scope entry for `channel`.
    fn entry(channel: u8, have: &[(u8, Ranges)]) -> Interest<Pair> {
        Interest::single(channel, Entry::whole(have.iter().cloned().collect()))
    }

    fn p(a: u8) -> u32 {
        Pair::author_prefix(&a)
    }

    fn sent_have(fx: &[Effect<Pair>]) -> Option<LogRanges<Pair>> {
        fx.iter().find_map(|e| match e {
            Effect::SendHave(r) => Some(r.clone()),
            _ => None,
        })
    }

    fn sent_want(fx: &[Effect<Pair>]) -> Option<Interest<Pair>> {
        fx.iter().find_map(|e| match e {
            Effect::SendWant(i) => Some(i.clone()),
            _ => None,
        })
    }

    fn ms(n: u64) -> RealTime {
        Duration::from_millis(n).into()
    }

    /// A channel Want is answered with every held log under the channel
    /// the wanter did not list, and only the missing ranges of those it did.
    #[test]
    fn channel_want_is_answered_wholesale_except_listed_haves() {
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
        // Peer 7 holds a1 up to 4; knows nothing else under channel 1.
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: entry(1, &[(1, Ranges::range(0, 4))]),
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
            "listed log: only what the wanter lacks"
        );
        assert_eq!(
            have.get(&a2),
            Some(&Ranges::range(0, 5)),
            "unlisted log under channel: wholesale"
        );
        assert_eq!(have.get(&other), None, "other channel: untouched");
    }

    /// Open channels with nothing held are pure wants on every own Want.
    #[test]
    fn open_channels_are_sent_with_fire_want() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let (s, _) = m
            .transition(s, A::Open(BTreeSet::from([4u8, 5u8])))
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmWantTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireWant).unwrap();
        let want = sent_want(&fx).expect("a Want is sent");
        let mut expected = Interest::empty();
        expected.insert(4u8, Entry::default());
        expected.insert(5u8, Entry::default());
        assert_eq!(want, expected);
    }

    /// A received Want is relayed whole on first sight; a repeat within
    /// `want_ttl` is covered and dropped.
    #[test]
    fn received_wants_are_relayed_once() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let want = |from| A::RecvWant {
            from,
            interest: entry(9, &[]),
        };
        let (s, fx1) = m.transition(s, want(1)).unwrap();
        assert_eq!(sent_want(&fx1), Some(entry(9, &[])));
        let (_, fx2) = m.transition(s, want(2)).unwrap();
        assert!(sent_want(&fx2).is_none(), "already relayed within want_ttl");
    }

    /// A relayed Want is forwarded whole, and a later Want asking for
    /// something new (here, all of a log the first one listed) is relayed
    /// too, while one asking for nothing new is not.
    #[test]
    fn a_relayed_want_is_forwarded_whole() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let named = entry(1, &[(1, Ranges::range(0, 4))]);
        let (s, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: 1,
                    interest: named.clone(),
                },
            )
            .unwrap();
        assert_eq!(sent_want(&fx), Some(named.clone()), "forwarded unchanged");
        let (s, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: 2,
                    interest: entry(1, &[]),
                },
            )
            .unwrap();
        assert!(
            sent_want(&fx).is_some(),
            "the pure want asks for all of author 1"
        );
        let (_, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: 3,
                    interest: named,
                },
            )
            .unwrap();
        assert!(
            sent_want(&fx).is_none(),
            "asks for nothing not already asked"
        );
    }

    /// Eviction policy input: what any asker lacks of what this node holds.
    #[test]
    fn network_ask_includes_held_logs_under_wanted_channels() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let s = RouterState::new(0u32, held(&[(a1, Ranges::range(0, 10))]));
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: entry(1, &[]),
                },
            )
            .unwrap();
        assert_eq!(s.network_ask().get(&a1), Some(&Ranges::range(0, 10)));
    }

    /// The answer is a channel lookup, not a scan: a wanter of channel 1
    /// gets both of channel 1's logs and nothing from channel 2.
    #[test]
    fn channel_want_answers_every_author_under_the_channel_only() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let a9 = Pair::new(1, 9);
        let b5 = Pair::new(2, 5);
        let s = RouterState::new(
            0u32,
            held(&[
                (a1, Ranges::from(0)),
                (a9, Ranges::range(0, 4)),
                (b5, Ranges::from(0)),
            ]),
        );
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: entry(1, &[]),
                },
            )
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmHaveTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireHave).unwrap();
        let have = sent_have(&fx).expect("a Have is sent");
        assert_eq!(have.get(&a1), Some(&Ranges::from(0)));
        assert_eq!(have.get(&a9), Some(&Ranges::range(0, 4)));
        assert_eq!(have.get(&b5), None, "other channel: untouched");
    }

    /// Cancellation is per channel: a peer's Want covering one of my
    /// channels but claiming more than I hold under another silences only
    /// the first.
    #[test]
    fn cancellation_is_per_channel() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let b1 = Pair::new(2, 1);
        let s = RouterState::new(
            0u32,
            held(&[(a1, Ranges::range(0, 4)), (b1, Ranges::range(0, 4))]),
        );
        let (s, _) = m.transition(s, A::Open(BTreeSet::from([1u8]))).unwrap();
        let mut peer = Interest::empty();
        peer.insert(
            1u8,
            Entry::whole(BTreeMap::from([(1u8, Ranges::range(0, 4))])),
        );
        peer.insert(
            2u8,
            Entry::whole(BTreeMap::from([(1u8, Ranges::range(0, 6))])),
        );
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: peer,
                },
            )
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmWantTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireWant).unwrap();
        let want = sent_want(&fx).expect("a Want is sent");
        assert!(
            want.get(&1u8).is_none(),
            "channel 1: the peer asks for the same tail"
        );
        assert_eq!(
            want.get(&2u8),
            Some(&Entry::whole(BTreeMap::from([(1u8, Ranges::range(0, 4))]))),
            "channel 2: the peer asks for 6.., I still need 4..6"
        );
    }

    /// Fragments of one Want, scoped by author prefix, combine at the
    /// receiver as one ask with no reassembly: a listed author in one scope
    /// is answered by what it lacks, an unlisted author in the other scope
    /// wholesale.
    #[test]
    fn scoped_fragments_combine_as_one_ask() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let b1 = Pair::new(1, 2);
        let s = RouterState::new(
            0u32,
            held(&[(a1, Ranges::range(0, 10)), (b1, Ranges::range(0, 10))]),
        );
        let low = Interest::single(
            1u8,
            Entry::new(0, p(1), BTreeMap::from([(1u8, Ranges::range(0, 5))])).unwrap(),
        );
        let high = Interest::single(1u8, Entry::new(p(2), u32::MAX, BTreeMap::new()).unwrap());
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: low,
                },
            )
            .unwrap();
        assert_eq!(
            s.next_have(),
            held(&[(a1, Ranges::range(5, 10))]),
            "only the low scope so far"
        );
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: high,
                },
            )
            .unwrap();
        assert_eq!(
            s.next_have(),
            held(&[(a1, Ranges::range(5, 10)), (b1, Ranges::range(0, 10))]),
        );
        assert_eq!(s.seen.len(), 2);
        let (s, _) = m.transition(s, A::Tick(ms(501))).unwrap();
        assert!(s.seen.is_empty(), "every fragment expires after want_ttl");
    }

    /// Final review F1, restated: this node's own Want, echoed back by a
    /// relay, contributes nothing to what it answers, because it holds
    /// everything the echo claims. So the echo needs no telling apart from
    /// a foreign Want, and a channel wanter's pure want is still answered
    /// with both logs wholesale.
    #[test]
    fn own_echo_contributes_nothing_and_is_not_relayed() {
        let m = machine();
        let (b, c) = (1u32, 2u32);
        let x = Pair::new(1, 1);
        let y = Pair::new(1, 2);
        let s = RouterState::new(
            c,
            held(&[(x, Ranges::range(0, 4)), (y, Ranges::range(0, 3))]),
        );
        let (s, _) = m.transition(s, A::Open(BTreeSet::from([1u8]))).unwrap();
        // C's own Want goes out.
        let (s, _) = m
            .transition(s, A::ArmWantTimer(Duration::ZERO.into()))
            .unwrap();
        let (s, fx) = m.transition(s, A::FireWant).unwrap();
        let own = sent_want(&fx).expect("C wants the tails of x and y");
        // ... and comes back, re-signed by relay B.
        let (s, fx) = m
            .transition(
                s,
                A::RecvWant {
                    from: b,
                    interest: own,
                },
            )
            .unwrap();
        assert!(
            sent_want(&fx).is_none(),
            "an echo is covered by the own emission"
        );
        assert!(s.next_have().is_empty(), "an echo asks for nothing C holds");
        assert!(
            m.transition(s.clone(), A::ArmHaveTimer(Duration::ZERO.into()))
                .is_err(),
            "so there is nothing to arm for"
        );
        // A's pure want for the channel, via B: both logs wholesale.
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: b,
                    interest: entry(1, &[]),
                },
            )
            .unwrap();
        assert_eq!(
            s.next_have(),
            held(&[(x, Ranges::range(0, 4)), (y, Ranges::range(0, 3))]),
            "both logs go wholesale to the channel wanter"
        );
    }

    /// Each seen Want dies `want_ttl` after its own arrival, so a stale one
    /// falls out while a later one from the same peer lives on.
    #[test]
    fn stale_wants_expire_independently() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let s = RouterState::new(0u32, held(&[(a1, Ranges::range(0, 10))]));
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: entry(1, &[(1, Ranges::range(0, 5))]),
                },
            )
            .unwrap();
        let (s, _) = m.transition(s, A::Tick(ms(250))).unwrap();
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    interest: entry(1, &[(1, Ranges::range(0, 8))]),
                },
            )
            .unwrap();
        assert_eq!(s.seen.len(), 2, "both live");
        assert_eq!(s.network_ask().get(&a1), Some(&Ranges::range(5, 10)));
        let (s, _) = m.transition(s, A::Tick(ms(260))).unwrap();
        assert_eq!(s.seen.len(), 1, "the first expired");
        assert_eq!(
            s.network_ask().get(&a1),
            Some(&Ranges::range(8, 10)),
            "only the later one remains"
        );
    }
}

//! Scenario tests for `NodeMachine`: the glue that composes the storage-less
//! router with the relay/ext stores into a single routing table over the
//! wire. See task-4-brief.md.

use std::collections::BTreeSet;

use dash_router_core::{
    EvictableStorage, LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, Ranges,
    RouterAction, RouterConfig, Storage, Units, WireBody, WireMessage,
};
use polestar::{id::UpTo, prelude::*, time::FiniteTime};

type N = UpTo<3>;
type L = UpTo<2>;
type T = FiniteTime<4, 1000>;
type Node = NodeMachine<N, L, T>;

fn t(n: usize) -> T {
    UpTo::new(n).into()
}
fn n(i: usize) -> N {
    UpTo::new(i)
}
fn l(i: usize) -> L {
    UpTo::new(i)
}

fn lr(pairs: impl IntoIterator<Item = (usize, Ranges)>) -> LogRanges<L> {
    LogRanges::from_pairs(pairs.into_iter().map(|(l, r)| (UpTo::new(l), r)))
}

fn machine() -> Node {
    NodeMachine::new(
        RouterConfig {
            want_ttl: t(2),
            have_ttl: t(2),
        },
        100,
    )
}

fn tiny(cap: Units) -> Node {
    NodeMachine::new(
        RouterConfig {
            want_ttl: t(2),
            have_ttl: t(2),
        },
        cap,
    )
}

/// Receiving a Have: bytes routed by subscription, novel subscribed ops
/// delivered, the flood re-broadcast fully hydrated.
#[test]
fn recv_have_routes_bytes_delivers_and_rebroadcasts_hydrated() {
    let m = machine(); // want_ttl/have_ttl = t(2), cap = 100
    let s = NodeState::new(n(0), m.clone(), [l(0)]); // subscribed to log 0 only
    let op0 = Op {
        header: vec![7],
        payload: Some(vec![7]),
    };
    let op1 = Op {
        header: vec![8],
        payload: Some(vec![8]),
    };
    let wire = WireMessage::have(
        n(1),
        vec![
            (l(0), vec![(0, op0.clone())]),
            (l(1), vec![(0, op1.clone())]),
        ],
    );
    let (s, fx) = m.transition(s, NodeAction::Recv(wire)).unwrap();

    assert_eq!(
        s.ext.0.held_all(),
        lr([(0, Ranges::from_seqs([0]))]),
        "subscribed op → ext"
    );
    assert_eq!(
        s.relay.0.held_all(),
        lr([(1, Ranges::from_seqs([0]))]),
        "unsubscribed op → relay"
    );
    assert!(fx.contains(&NodeEffect::Deliver(l(0), 0)));
    assert!(
        !fx.contains(&NodeEffect::Deliver(l(1), 0)),
        "relay ops are not delivered"
    );
    let broadcast = fx
        .iter()
        .find_map(|e| match e {
            NodeEffect::Broadcast(w) => Some(w),
            _ => None,
        })
        .unwrap();
    assert_eq!(broadcast.sender, n(0), "re-signed");
    assert_eq!(
        broadcast.body,
        WireBody::Have(vec![(l(0), vec![(0, op0)]), (l(1), vec![(0, op1)])]),
        "relay hydrated from the just-ingested bytes"
    );
    assert_eq!(s.router.held, s.held_union(), "router view reconciled");
}

/// Authoring: ingest to ext, Push, hydrated broadcast; duplicate delivery
/// of the same op later teaches nothing.
#[test]
fn authored_ops_push_and_the_echo_is_absorbed() {
    let m = machine();
    let s = NodeState::new(n(0), m.clone(), [l(0)]);
    let op = Op {
        header: vec![7],
        payload: Some(vec![7]),
    };
    let (s, fx) = m
        .transition(s, NodeAction::Authored(l(0), 0, op.clone()))
        .unwrap();
    assert_eq!(s.ext.0.held_all(), lr([(0, Ranges::from_seqs([0]))]));
    assert!(matches!(&fx[..], [NodeEffect::Broadcast(w)]
        if w.body == WireBody::Have(vec![(l(0), vec![(0, op.clone())])])));
    // The flood comes back from a neighbour: no re-broadcast, no delivery.
    let (_, fx) = m
        .transition(
            s,
            NodeAction::Recv(WireMessage::have(n(1), vec![(l(0), vec![(0, op)])])),
        )
        .unwrap();
    assert!(fx.is_empty());
}

/// Subscribe migrates relay bytes to ext without changing held.
#[test]
fn subscribe_migrates_and_preserves_held() {
    let m = machine();
    let s = NodeState::new(n(0), m.clone(), [l(0)]);
    let op = Op {
        header: vec![7],
        payload: Some(vec![7]),
    };
    let (s, _) = m
        .transition(
            s,
            NodeAction::Recv(WireMessage::have(n(1), vec![(l(1), vec![(0, op.clone())])])),
        )
        .unwrap();
    let held_before = s.router.held.clone();
    let (s, fx) = m.transition(s, NodeAction::Subscribe(l(1))).unwrap();
    assert!(s.relay.0.held_all().is_empty(), "relay side emptied");
    assert_eq!(
        s.ext.0.fetch(&s.ext.0.held_all()),
        vec![(l(1), 0, op)],
        "bytes moved"
    );
    assert_eq!(
        s.router.held, held_before,
        "spec §5: migration never changes held"
    );
    assert!(fx.is_empty(), "no traffic from a subscription change");
}

/// The relay sheds ingests at cap; eviction reopens room and shrinks held.
#[test]
fn relay_cap_sheds_then_eviction_reopens() {
    let m = tiny(2); // cap = 2 units
    let s = NodeState::new(n(0), m.clone(), []); // pure relay
    let full = |b: u8| Op {
        header: vec![b],
        payload: Some(vec![b]),
    };
    let (s, fx) = m
        .transition(
            s,
            NodeAction::Recv(WireMessage::have(
                n(1),
                vec![(l(0), vec![(0, full(1)), (1, full(2))])],
            )),
        )
        .unwrap();
    assert_eq!(
        s.relay.0.held_all(),
        lr([(0, Ranges::from_seqs([0]))]),
        "second op shed"
    );
    // The relay decision is unconditional (it does not check storage), but
    // the re-broadcast is hydrated FROM storage, so the shed op is absent
    // from it: only the stored op goes out, not both.
    let broadcast = fx
        .iter()
        .find_map(|e| match e {
            NodeEffect::Broadcast(w) => Some(w),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        broadcast.body,
        WireBody::Have(vec![(l(0), vec![(0, full(1))])]),
        "truncated broadcast: the shed op is dropped from the flood"
    );
    let (s, _) = m
        .transition(s, NodeAction::RelayEvict(lr([(0, Ranges::from_seqs([0]))])))
        .unwrap();
    assert!(s.relay.0.held_all().is_empty());
    assert_eq!(s.router.held, s.held_union(), "shrink reconciled");
}

/// Fix for the cap-arithmetic divergence: the glue's shed check must use the
/// same delta the storage machine's own cap `ensure!` uses. At cap, a
/// payload upgrade over a held header-only op costs only 1 unit (not 2, as
/// if the slot were vacant), so it must fit and upgrade rather than being
/// shed.
#[test]
fn relay_cap_upgrade_over_header_only_fits_within_the_true_delta() {
    let m = tiny(2); // cap = 2 units
    let mut s = NodeState::new(n(0), m.clone(), []); // pure relay, unsubscribed
    let header_only = Op {
        header: vec![1],
        payload: None,
    };
    // Arrange the held-header-only state directly on storage, then
    // reconcile the router's snapshot via a no-op evict (public API).
    s.relay.state_mut().0.ingest(l(0), 0, header_only);
    let (s, _) = m
        .transition(s, NodeAction::RelayEvict(LogRanges::empty()))
        .unwrap();
    assert_eq!(s.relay.0.usage(), 1, "header-only op costs 1 unit");

    let full = Op {
        header: vec![1],
        payload: Some(vec![9]),
    };
    let (s, _) = m
        .transition(
            s,
            NodeAction::Recv(WireMessage::have(
                n(1),
                vec![(l(0), vec![(0, full.clone())])],
            )),
        )
        .unwrap();
    assert_eq!(
        s.relay.0.fetch(&s.relay.0.held_all()),
        vec![(l(0), 0, full)],
        "the 1-unit upgrade fits at cap 2 (1 held + 1 delta), so it must not be shed"
    );
}

/// No test at any layer previously exercised `Unsubscribe`. Pins both halves
/// of the current semantics (a controller ruling, not a behavior change):
/// unsubscribing does not migrate or forget data, so the node keeps
/// advertising what it already holds in `ext` AND keeps wanting the log's
/// open tail.
#[test]
fn unsubscribe_keeps_advertising_and_keeps_wanting() {
    let m = machine();
    let s = NodeState::new(n(0), m.clone(), [l(0)]);
    let op = Op {
        header: vec![1],
        payload: Some(vec![1]),
    };
    let (s, _) = m.transition(s, NodeAction::Authored(l(0), 0, op)).unwrap();

    let (s, _) = m.transition(s, NodeAction::Unsubscribe(l(0))).unwrap();

    // (a) keep advertising: `held` still contains the log's stored ranges.
    assert!(
        s.router.held.get(&l(0)).is_some_and(|r| r.contains(0)),
        "unsubscribe must not stop advertising ext-held data (spec §5 option 1)"
    );

    // (b) keep wanting: the log's open tail is still armed/fired as a Want.
    let (s, _) = m
        .transition(s, NodeAction::Router(RouterAction::ArmWantTimer(t(0))))
        .unwrap();
    let (_, fx) = m
        .transition(s, NodeAction::Router(RouterAction::FireWant))
        .unwrap();
    let want = fx
        .iter()
        .find_map(|e| match e {
            NodeEffect::Broadcast(w) => Some(w),
            _ => None,
        })
        .unwrap();
    match &want.body {
        WireBody::Want { ranges, .. } => assert!(
            ranges.get(&l(0)).is_some_and(|r| r.contains(1)),
            "still wants the log's open tail after unsubscribe"
        ),
        other => panic!("expected a Want, got {other:?}"),
    }
}

/// Native sync and AppGc reach the router as snapshots; direct router
/// Recv actions are the network's business and not enabled here.
#[test]
fn ext_spontaneity_reconciles_and_smuggled_recvs_are_disabled() {
    let m = machine();
    let s = NodeState::new(n(0), m.clone(), [l(0)]);
    let (s, _) = m
        .transition(s, NodeAction::NativeSync(l(0), 0, Op::default()))
        .unwrap();
    assert!(s.router.held.contains(&l(0), 0));
    let (s, _) = m
        .transition(s, NodeAction::AppGc(lr([(0, Ranges::full())])))
        .unwrap();
    assert!(!s.router.held.contains(&l(0), 0));
    assert!(
        m.transition(
            s.clone(),
            NodeAction::Router(RouterAction::RecvWant {
                from: n(1),
                origin: n(1),
                ranges: lr([(0, Ranges::full())]),
                prefixes: BTreeSet::new(),
            })
        )
        .is_err()
    );
    // `Held` and `Push` are equally the glue's business (reconcile_held /
    // Authored are the only legitimate writers): a smuggled `Held` would
    // desync `router.held` from storage, and a smuggled `Push` would grow
    // `held` for data the node doesn't actually hold.
    assert!(
        m.transition(
            s.clone(),
            NodeAction::Router(RouterAction::Held(lr([(0, Ranges::full())])))
        )
        .is_err(),
        "smuggled Held must be disabled"
    );
    assert!(
        m.transition(
            s,
            NodeAction::Router(RouterAction::Push(lr([(0, Ranges::full())])))
        )
        .is_err(),
        "smuggled Push must be disabled"
    );
}

/// When relay and ext both hold a copy of the same (log, seq), SendHave
/// hydration must prefer the payload-bearing copy: broadcasting the
/// header-only one would degrade an op the node actually has in full.
#[test]
fn send_have_hydration_prefers_the_payload_bearing_copy() {
    let m = machine();
    let full = Op {
        header: vec![1],
        payload: Some(vec![1]),
    };
    let header_only = Op {
        header: vec![1],
        payload: None,
    };

    // Build the collision directly: ext holds the full op, relay holds a
    // header-only copy of the very same (log, seq). States are plain
    // values, so mutating the stores' `OpsMap`s directly (via the public
    // `Storage` trait) is the simplest way to arrange this.
    let mut s = NodeState::new(n(0), m.clone(), [l(0)]);
    s.ext.state_mut().0.ingest(l(0), 0, full.clone());
    s.relay.state_mut().0.ingest(l(0), 0, header_only);

    // Reconcile the router's held snapshot against the stores we just
    // mutated by hand (NativeSync-ing the same op again is idempotent).
    let (s, _) = m
        .transition(s, NodeAction::NativeSync(l(0), 0, full.clone()))
        .unwrap();

    // Force a SendHave via a Want/ArmHaveTimer/FireHave round trip. A
    // RecvWant reaches the router only through NodeAction::Recv.
    let (s, _) = m
        .transition(
            s,
            NodeAction::Recv(WireMessage::want(
                n(1),
                n(1),
                lr([(0, Ranges::full())]),
                BTreeSet::new(),
            )),
        )
        .unwrap();
    let (s, _) = m
        .transition(s, NodeAction::Router(RouterAction::ArmHaveTimer(t(0))))
        .unwrap();
    let (_, fx) = m
        .transition(s, NodeAction::Router(RouterAction::FireHave))
        .unwrap();

    let broadcast = fx
        .iter()
        .find_map(|e| match e {
            NodeEffect::Broadcast(w) => Some(w),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        broadcast.body,
        WireBody::Have(vec![(l(0), vec![(0, full)])]),
        "the richer copy must win, not whichever store sorted first"
    );
}

/// Payload eviction is reachable from the node level: candidates spare
/// recently-wanted ranges, evicting frees units, headers keep advertising.
#[test]
fn relay_evict_payloads_frees_units_and_keeps_advertising() {
    let m = machine();
    let s = NodeState::new(n(0), m.clone(), std::iter::empty()); // no subscriptions: bytes land in the relay
    let op = |h: u8| Op {
        header: vec![h],
        payload: Some(vec![h; 4]),
    };
    let have = WireMessage::have(n(1), vec![(l(1), vec![(0, op(10)), (1, op(11))])]);
    let (s, _) = m.transition(s, NodeAction::Recv(have)).unwrap();
    assert_eq!(s.relay.0.usage(), 4);

    // Peer 2 wants seq 0: its payload must survive to answer the Want.
    let want = WireMessage::want(n(2), n(2), lr([(1, Ranges::range(0, 1))]), BTreeSet::new());
    let (s, _) = m.transition(s, NodeAction::Recv(want)).unwrap();
    let candidates = s.eviction_candidates();
    assert_eq!(
        candidates,
        lr([(1, Ranges::range(1, 2))]),
        "the wanted seq 0 is spared; only seq 1's payload is a candidate"
    );

    let (s, fx) = m
        .transition(s, NodeAction::RelayEvictPayloads(candidates))
        .unwrap();
    assert!(
        fx.is_empty(),
        "headers survive: nothing broadcast or delivered"
    );
    assert_eq!(s.relay.0.usage(), 3, "one payload unit freed");
    assert_eq!(s.relay.0.held_payloads(), lr([(1, Ranges::range(0, 1))]));
    assert_eq!(
        s.router.held.get(&l(1)),
        Some(&Ranges::range(0, 2)),
        "both seqs still advertised (headers held)"
    );
}

/// With no outstanding Wants, every relay payload is a candidate.
#[test]
fn eviction_candidates_cover_everything_when_nothing_is_wanted() {
    let m = machine();
    let s = NodeState::new(n(0), m.clone(), std::iter::empty());
    let op = Op {
        header: vec![9],
        payload: Some(vec![9]),
    };
    let have = WireMessage::have(n(1), vec![(l(0), vec![(0, op)])]);
    let (s, _) = m.transition(s, NodeAction::Recv(have)).unwrap();
    assert_eq!(s.eviction_candidates(), s.relay.0.held_payloads());
}

mod prefix {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use dash_router_core::{
        LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, Pair, Ranges, RouterConfig,
        WireBody, WireMessage,
    };
    use dash_router_core::{RouterAction, Storage};
    use polestar::prelude::*;
    use polestar::time::RealTime;

    type M = NodeMachine<u32, Pair, RealTime>;

    fn machine() -> M {
        NodeMachine::new(
            RouterConfig {
                want_ttl: Duration::from_millis(500).into(),
                have_ttl: Duration::from_millis(500).into(),
            },
            1 << 20,
        )
    }

    fn op(b: u8) -> Op {
        Op {
            header: vec![b],
            payload: Some(vec![b; 4]),
        }
    }

    fn have(from: u32, log: Pair, seqs: &[u32]) -> WireMessage<u32, Pair> {
        WireMessage::have(
            from,
            vec![(log, seqs.iter().map(|&q| (q, op(q as u8))).collect())],
        )
    }

    /// Review focus 1: a never-seen author under a subscribed prefix is ext data.
    #[test]
    fn have_for_new_author_under_subscribed_prefix_is_delivered() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), [1u8]);
        let new_author = Pair::new(1, 42);
        let (s, fx) = m
            .transition(s, NodeAction::Recv(have(7, new_author, &[0, 1])))
            .unwrap();
        assert!(fx.contains(&NodeEffect::Deliver(new_author, 0)));
        assert!(fx.contains(&NodeEffect::Deliver(new_author, 1)));
        assert_eq!(
            s.ext.0.held_all().get(&new_author),
            Some(&Ranges::range(0, 2))
        );
        assert!(s.relay.0.held_all().get(&new_author).is_none());
    }

    /// Subscribe(prefix) migrates every author's relay log under it to ext.
    #[test]
    fn subscribe_prefix_migrates_all_relay_logs_under_it() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), []); // pure relay
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let other = Pair::new(2, 1);
        let (s, _) = m
            .transition(s, NodeAction::Recv(have(7, a1, &[0])))
            .unwrap();
        let (s, _) = m
            .transition(s, NodeAction::Recv(have(7, a2, &[0, 1])))
            .unwrap();
        let (s, _) = m
            .transition(s, NodeAction::Recv(have(7, other, &[0])))
            .unwrap();
        let (s, _) = m.transition(s, NodeAction::Subscribe(1u8)).unwrap();
        assert!(s.is_subscribed(&a1) && s.is_subscribed(&a2) && !s.is_subscribed(&other));
        assert_eq!(s.ext.0.held_all().get(&a1), Some(&Ranges::range(0, 1)));
        assert_eq!(s.ext.0.held_all().get(&a2), Some(&Ranges::range(0, 2)));
        assert!(s.relay.0.held_all().get(&a1).is_none());
        assert!(s.relay.0.held_all().get(&a2).is_none());
        assert_eq!(s.relay.0.held_all().get(&other), Some(&Ranges::range(0, 1)));
        assert_eq!(s.router.open, BTreeSet::from([1u8]));
    }

    /// Review focus 3.
    #[test]
    fn unsubscribe_prefix_parks_later_haves_in_relay() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), [1u8]);
        let (s, _) = m.transition(s, NodeAction::Unsubscribe(1u8)).unwrap();
        assert!(s.router.open.is_empty());
        let a1 = Pair::new(1, 1);
        let (s, fx) = m
            .transition(s, NodeAction::Recv(have(7, a1, &[0])))
            .unwrap();
        assert!(!fx.iter().any(|e| matches!(e, NodeEffect::Deliver(..))));
        assert_eq!(s.relay.0.held_all().get(&a1), Some(&Ranges::range(0, 1)));
    }

    /// The prefix Want replaces the known-but-empty marker: a subscription
    /// with nothing stored sends `prefixes`, not a full range for a log.
    #[test]
    fn empty_subscription_wants_by_prefix_not_marker() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), [1u8]);
        assert!(s.held_union().is_empty(), "no marker");
        let (s, _) = m
            .transition(
                s,
                NodeAction::Router(RouterAction::ArmWantTimer(Duration::ZERO.into())),
            )
            .unwrap();
        let (_, fx) = m
            .transition(s, NodeAction::Router(RouterAction::FireWant))
            .unwrap();
        let want = fx.iter().find_map(|e| match e {
            NodeEffect::Broadcast(WireMessage {
                body:
                    WireBody::Want {
                        origin,
                        ranges,
                        prefixes,
                    },
                ..
            }) => Some((*origin, ranges.clone(), prefixes.clone())),
            _ => None,
        });
        assert_eq!(
            want,
            Some((0u32, LogRanges::empty(), BTreeSet::from([1u8]))),
            "an own Want names this node as its origin"
        );
    }

    /// Final review F1: a relayer re-signs a Want but keeps its origin, so
    /// the answerer keys it by the wanter and recognises its own echoes.
    #[test]
    fn relayed_want_keeps_its_origin() {
        let m = machine();
        let s = NodeState::new(1u32, m.clone(), []);
        let (s, fx) = m
            .transition(
                s,
                NodeAction::Recv(WireMessage::want(
                    0u32,
                    0u32,
                    LogRanges::empty(),
                    BTreeSet::from([1u8]),
                )),
            )
            .unwrap();
        let relayed: Vec<_> = fx
            .iter()
            .filter_map(|e| match e {
                NodeEffect::Broadcast(m) => Some(m.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            relayed,
            vec![WireMessage::want(
                1u32,
                0u32,
                LogRanges::empty(),
                BTreeSet::from([1u8])
            )]
        );
        assert!(s.router.wants.contains_key(&0), "keyed by the origin");
        // The same Want, relayed back to its origin by this node: no record.
        let s0 = NodeState::new(0u32, m.clone(), [1u8]);
        let (s0, _) = m
            .transition(s0, NodeAction::Recv(relayed[0].clone()))
            .unwrap();
        assert!(
            s0.router.wants.is_empty(),
            "an echo of an own Want is ignored"
        );
    }
}

//! Scenario tests for `NodeMachine`: the glue that composes the storage-less
//! router with the relay/ext stores into a single routing table over the
//! wire. See task-4-brief.md.

use dash_router_core::{
    LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, Ranges, RouterAction,
    RouterConfig, Storage, Units, WireBody, WireMessage,
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
    let s = NodeState::new(n(0), [l(0)]); // subscribed to log 0 only
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
        vec![(l(0), vec![(0, op0.clone())]), (l(1), vec![(0, op1.clone())])],
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
    let s = NodeState::new(n(0), [l(0)]);
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
    let s = NodeState::new(n(0), [l(0)]);
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
    assert_eq!(s.router.held, held_before, "spec §5: migration never changes held");
    assert!(fx.is_empty(), "no traffic from a subscription change");
}

/// The relay sheds ingests at cap; eviction reopens room and shrinks held.
#[test]
fn relay_cap_sheds_then_eviction_reopens() {
    let m = tiny(2); // cap = 2 units
    let s = NodeState::new(n(0), []); // pure relay
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
    // The Have still floods in full: relaying is not conditioned on storing.
    assert!(matches!(fx.last().unwrap(), NodeEffect::Broadcast(_)));
    let (s, _) = m
        .transition(
            s,
            NodeAction::RelayEvict(lr([(0, Ranges::from_seqs([0]))])),
        )
        .unwrap();
    assert!(s.relay.0.held_all().is_empty());
    assert_eq!(s.router.held, s.held_union(), "shrink reconciled");
}

/// Native sync and AppGc reach the router as snapshots; direct router
/// Recv actions are the network's business and not enabled here.
#[test]
fn ext_spontaneity_reconciles_and_smuggled_recvs_are_disabled() {
    let m = machine();
    let s = NodeState::new(n(0), [l(0)]);
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
            s,
            NodeAction::Router(RouterAction::RecvWant {
                from: n(1),
                ranges: lr([(0, Ranges::full())]),
            })
        )
        .is_err()
    );
}

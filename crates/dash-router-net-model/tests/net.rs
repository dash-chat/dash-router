//! Scenario tests driving whole networks, with bounded model types.

use std::{collections::BTreeSet, sync::Arc};

use dash_router_core::{
    LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, RouterAction, RouterConfig,
};
use dash_router_net_model::{Fair, NetAction, NetMachine, NetState, Topology};
use polestar::{StateMachine, prelude::*, time::FiniteTime};

type N = UpTo<4>;
type L = UpTo<2>;
type T = FiniteTime<4, 1000>;
const K: usize = 8;
type Net = NetMachine<N, L, T, K>;
type A = NetAction<N, L, T, K>;
type State = NetState<N, L, T>;

fn t(n: usize) -> T {
    UpTo::new(n).into()
}
fn n(i: usize) -> N {
    UpTo::new(i)
}
fn l(i: usize) -> L {
    UpTo::new(i)
}
fn i(x: usize) -> UpTo<K> {
    UpTo::new(x)
}
fn op(byte: u8) -> Op {
    Op {
        header: vec![byte],
        payload: Some(vec![byte, byte]),
    }
}

fn router_config() -> RouterConfig<T> {
    RouterConfig {
        want_ttl: t(2),
        have_ttl: t(2),
    }
}

/// One shared `NodeMachine`, generously capped so shedding never fires in
/// these tests (they exercise routing, not the relay's eviction policy).
fn node_machine() -> NodeMachine<N, L, T> {
    NodeMachine::new(router_config(), 100)
}

fn machine(topology: Topology<N>) -> Arc<Net> {
    Arc::new(NetMachine::new(topology, node_machine()))
}

/// One node per id, all subscribed to log 0.
fn nodes(m: NodeMachine<N, L, T>, count: usize) -> impl Iterator<Item = NodeState<N, L, T>> {
    (0..count).map(move |id| NodeState::new(n(id), m.clone(), [l(0)]))
}

/// A network where every node subscribes to log 0.
fn network(m: &Arc<Net>, count: usize) -> StateMachine<Net> {
    m.state_machine(State::new(nodes(m.node_machine.clone(), count)))
}

fn disabled(m: &Net, s: &State, action: A) {
    assert!(
        m.transition(s.clone(), action.clone()).is_err(),
        "{action:?} should not be enabled"
    );
}

/// Deliver everything in flight, in queue order, until the network is
/// quiet. Panics if the flood does not terminate.
fn drain(net: &mut StateMachine<Net>) -> Vec<(N, NodeEffect<N, L>)> {
    let mut fx = vec![];
    for _ in 0..64 {
        if net.inflight.is_empty() {
            return fx;
        }
        fx.extend(net.step(A::Deliver(i(0))).unwrap());
    }
    panic!("flood did not terminate");
}

fn holders(net: &StateMachine<Net>, log: L, seq: u32) -> Vec<N> {
    net.nodes
        .values()
        .filter(|node| node.holds(&log, seq))
        .map(|node| node.router.id)
        .collect()
}

/// The end-to-end version of the core's relay tests: one append at the
/// end of a line must reach every node, hop by hop, and the seen-set
/// must quiesce the network rather than cycle it.
#[test]
fn a_flood_covers_a_path_and_the_network_quiesces() {
    let m = machine(Topology::path([n(0), n(1), n(2), n(3)]));
    let mut net = network(&m, 4);

    net.step(A::Node(n(0), NodeAction::Authored(l(0), 0, op(7))))
        .unwrap();
    assert_eq!(net.inflight.len(), 1, "0 can only reach 1");

    let fx = drain(&mut net);
    assert_eq!(holders(&net, l(0), 0), vec![n(0), n(1), n(2), n(3)]);
    let deliveries: Vec<N> = fx
        .iter()
        .filter(|(_, e)| matches!(e, NodeEffect::Deliver(..)))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(deliveries, vec![n(1), n(2), n(3)]);
}

/// The same flood must cover any connected topology, whatever its shape.
#[test]
fn a_flood_covers_random_trees() {
    for seed in 0..8 {
        let topo = Topology::random_tree(&[n(0), n(1), n(2), n(3)], seed);
        assert!(topo.is_connected());
        let m = machine(topo);
        let mut net = network(&m, 4);
        net.step(A::Node(n(2), NodeAction::Authored(l(0), 0, op(7))))
            .unwrap();
        drain(&mut net);
        assert_eq!(
            holders(&net, l(0), 0).len(),
            4,
            "seed {seed}: flood did not cover the tree"
        );
    }
}

/// A dropped flood message is repaired by the Want/Have exchange, and
/// the repair travels the same hops the flood would have.
#[test]
fn a_dropped_flood_is_backfilled_by_want_then_have() {
    let m = machine(Topology::path([n(0), n(1), n(2)]));
    let mut net = network(&m, 3);

    net.step(A::Node(n(0), NodeAction::Authored(l(0), 0, op(7))))
        .unwrap();
    net.step(A::Deliver(i(0))).unwrap(); // 1 stores and relays to 0 and 2
    net.step(A::Deliver(i(0))).unwrap(); // 0 hears the echo, flood stops there
    net.step(A::Drop(i(0))).unwrap(); // the copy for 2 is lost
    assert!(net.inflight.is_empty());
    assert_eq!(holders(&net, l(0), 0), vec![n(0), n(1)]);

    // While the flood is still "recently circulating" in 1's eyes, 1 will
    // not offer it again; the repair happens after that record expires.
    net.step(A::Node(n(1), NodeAction::Router(RouterAction::Tick(t(2)))))
        .unwrap();

    // 2 asks for everything it lacks; 1 hears it and floods it onward,
    // which is how a Want crosses hops the asker cannot reach.
    net.step(A::Node(
        n(2),
        NodeAction::Router(RouterAction::ArmWantTimer(t(0))),
    ))
    .unwrap();
    net.step(A::Node(n(2), NodeAction::Router(RouterAction::FireWant)))
        .unwrap();
    net.step(A::Deliver(i(0))).unwrap();
    assert_eq!(
        net.inflight.len(),
        2,
        "1 relays the Want to both neighbours"
    );

    // 1 answers with a non-fresh Have, heard by both neighbours.
    net.step(A::Node(
        n(1),
        NodeAction::Router(RouterAction::ArmHaveTimer(t(0))),
    ))
    .unwrap();
    net.step(A::Node(n(1), NodeAction::Router(RouterAction::FireHave)))
        .unwrap();
    assert_eq!(net.inflight.len(), 4);
    let fx = drain(&mut net);
    assert_eq!(holders(&net, l(0), 0), vec![n(0), n(1), n(2)]);
    let deliveries: Vec<N> = fx
        .iter()
        .filter(|(_, e)| matches!(e, NodeEffect::Deliver(..)))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(deliveries, vec![n(2)], "only 2 learned anything");
}

/// Duplicated packets are absorbed: delivering the copy changes no
/// node's state and triggers no further sends.
#[test]
fn a_duplicated_delivery_is_idempotent() {
    let m = machine(Topology::path([n(0), n(1)]));
    let mut net = network(&m, 2);

    net.step(A::Node(n(0), NodeAction::Authored(l(0), 0, op(7))))
        .unwrap();
    net.step(A::Duplicate(i(0))).unwrap();
    assert_eq!(net.inflight.len(), 2);
    assert_eq!(net.inflight[0], net.inflight[1]);

    net.step(A::Deliver(i(0))).unwrap(); // 1 stores, echoes back to 0
    let before = net.clone();
    let fx = net.step(A::Deliver(i(1))).unwrap(); // the duplicate arrives
    assert_eq!(fx, vec![], "nothing stored, delivered or sent");
    assert_eq!(net.nodes, before.nodes);
    assert_eq!(net.inflight[..], before.inflight[..1]);
}

#[test]
fn absent_messages_and_smuggled_recvs_are_disabled() {
    // Hub 0 with three leaves: one append puts three copies in flight.
    let m = machine(Topology::star([n(0), n(1), n(2), n(3)]));
    let mut net = network(&m, 4);

    disabled(&m, &net, A::Deliver(i(0))); // nothing in flight
    disabled(&m, &net, A::Drop(i(0)));
    disabled(&m, &net, A::Duplicate(i(0)));

    net.step(A::Node(n(0), NodeAction::Authored(l(0), 0, op(1))))
        .unwrap();
    assert_eq!(net.inflight.len(), 3);

    // Receiving is the network's business: `Recv` and the router's raw
    // receive actions are both smuggled paths, both disabled.
    let smuggled = net.inflight[0].message.clone();
    disabled(&m, &net, A::Node(n(1), NodeAction::Recv(smuggled)));
    disabled(
        &m,
        &net,
        A::Node(
            n(1),
            NodeAction::Router(RouterAction::RecvWant {
                from: n(0),
                origin: n(0),
                ranges: LogRanges::empty(),
                prefixes: BTreeSet::new(),
            }),
        ),
    );

    // The bare machine imposes no fairness: it drops as often as asked.
    for _ in 0..3 {
        net.step(A::Drop(i(0))).unwrap();
    }
    assert!(net.inflight.is_empty());
    disabled(&m, &net, A::Deliver(i(0))); // empty again, still disabled
}

/// Fairness is a harness's choice, made by wrapping, not the network's.
#[test]
fn the_fair_wrapper_bounds_consecutive_drops() {
    let node_machine = node_machine();
    let m = Arc::new(Fair::new(
        NetMachine::<N, L, T, K>::new(
            Topology::star([n(0), n(1), n(2), n(3)]),
            node_machine.clone(),
        ),
        |a| matches!(a, A::Drop(_)),
        |a| matches!(a, A::Deliver(_)),
        2,
    ));
    let mut net = m.state_machine((State::new(nodes(node_machine, 4)), 0));

    net.step(A::Node(n(0), NodeAction::Authored(l(0), 0, op(1))))
        .unwrap();
    net.step(A::Drop(i(0))).unwrap();
    net.step(A::Drop(i(0))).unwrap();
    assert!(
        m.transition(net.state().clone(), A::Drop(i(0))).is_err(),
        "a third consecutive drop should not be enabled"
    );
    net.step(A::Deliver(i(0))).unwrap();
    net.step(A::Drop(i(0))).unwrap(); // the streak was reset
}

#[test]
fn the_inflight_cap_disables_overflowing_actions() {
    type TinyNet = NetMachine<N, L, T, 2>;
    type TinyA = NetAction<N, L, T, 2>;
    let node_machine = node_machine();
    let m = Arc::new(TinyNet::new(
        Topology::star([n(0), n(1), n(2), n(3)]),
        node_machine.clone(),
    ));
    let mut net = m.state_machine(NetState::new(nodes(node_machine, 4)));

    // Fan-out of 3 cannot fit in a bag of 2.
    assert!(
        m.transition(
            net.state().clone(),
            TinyA::Node(n(0), NodeAction::Authored(l(0), 0, op(1)))
        )
        .is_err(),
        "overflowing send should not be enabled"
    );

    // A leaf's append fans out to 1; duplicating fills the bag exactly.
    net.step(TinyA::Node(n(1), NodeAction::Authored(l(0), 0, op(1))))
        .unwrap();
    net.step(TinyA::Duplicate(UpTo::new(0))).unwrap();
    assert!(
        m.transition(net.state().clone(), TinyA::Duplicate(UpTo::new(0)))
            .is_err(),
        "duplicate past the cap should not be enabled"
    );
}

/// Final review F1, end to end: prefix subscribers behind a relay, with
/// two-level logs so one prefix holds several logs.
mod prefix_behind_relay {
    use std::{sync::Arc, time::Duration};

    use dash_router_core::{
        NodeAction, NodeMachine, NodeState, Op, Pair, Ranges, RouterAction, RouterConfig,
    };
    use dash_router_net_model::{NetAction, NetMachine, NetState, Topology};
    use polestar::{StateMachine, prelude::*, time::RealTime};

    const K: usize = 64;
    type Net = NetMachine<u32, Pair, RealTime, K>;
    type A = NetAction<u32, Pair, RealTime, K>;

    const X: Pair = Pair::new(1, 1);
    const Y: Pair = Pair::new(1, 2);
    const SUB: u32 = 0;
    const RELAY: u32 = 1;
    const HOLDER: u32 = 2;

    fn ms(n: u64) -> RealTime {
        Duration::from_millis(n).into()
    }

    fn op(b: u8) -> Op {
        Op {
            header: vec![b],
            payload: Some(vec![b; 4]),
        }
    }

    fn drain(net: &mut StateMachine<Net>) {
        for _ in 0..1000 {
            if net.inflight.is_empty() {
                return;
            }
            net.step(A::Deliver(UpTo::new(0))).unwrap();
        }
        panic!("flood did not terminate");
    }

    fn router(net: &mut StateMachine<Net>, id: u32, action: RouterAction<u32, Pair, RealTime>) {
        net.step(A::Node(id, NodeAction::Router(action))).unwrap();
    }

    /// Every node fires a Want, then every node that witnessed one fires a
    /// Have, each fully delivered; then time moves on well inside `want_ttl`.
    fn round(net: &mut StateMachine<Net>) {
        let ids = [SUB, RELAY, HOLDER];
        for id in ids {
            router(net, id, RouterAction::ArmWantTimer(ms(0)));
            router(net, id, RouterAction::FireWant);
            drain(net);
        }
        for id in ids {
            if !net.nodes[&id].router.wants.is_empty() {
                router(net, id, RouterAction::ArmHaveTimer(ms(0)));
                router(net, id, RouterAction::FireHave);
                drain(net);
            }
        }
        for id in ids {
            router(net, id, RouterAction::Tick(ms(200)));
        }
    }

    /// Path SUB — RELAY — HOLDER. SUB subscribes to prefix 1; RELAY is a
    /// pure relay; HOLDER authored `1/1` seqs 0..4 and `1/2` seqs 0..3 with
    /// the flood lost. HOLDER's own Want comes back to it relayed by RELAY,
    /// which must not stop it answering SUB's prefix with the logs SUB did
    /// not name. `sub_holds` is what SUB already holds of `1/1`.
    fn run(sub_holds: u32) {
        let nm = NodeMachine::new(
            RouterConfig {
                want_ttl: ms(500),
                have_ttl: ms(500),
            },
            10_000,
        );
        let m = Arc::new(Net::new(Topology::path([SUB, RELAY, HOLDER]), nm.clone()));
        let mut net = m.state_machine(NetState::new([
            NodeState::new(SUB, nm.clone(), [1u8]),
            NodeState::new(RELAY, nm.clone(), []),
            NodeState::new(HOLDER, nm.clone(), [1u8]),
        ]));
        for (log, n) in [(X, 4), (Y, 3)] {
            for seq in 0..n {
                net.step(A::Node(
                    HOLDER,
                    NodeAction::Authored(log, seq, op(seq as u8)),
                ))
                .unwrap();
                while !net.inflight.is_empty() {
                    net.step(A::Drop(UpTo::new(0))).unwrap();
                }
            }
        }
        for seq in 0..sub_holds {
            net.step(A::Node(SUB, NodeAction::NativeSync(X, seq, op(seq as u8))))
                .unwrap();
        }
        for _ in 0..4 {
            round(&mut net);
        }
        let held = &net.nodes[&SUB].router.held;
        assert_eq!(
            held.get(&X),
            Some(&Ranges::range(0, 4)),
            "SUB holding {sub_holds} of 1/1: all of 1/1"
        );
        assert_eq!(
            held.get(&Y),
            Some(&Ranges::range(0, 3)),
            "SUB holding {sub_holds} of 1/1: the unknown author 1/2 wholesale"
        );
    }

    #[test]
    fn late_subscriber_behind_a_relay_receives_unknown_authors() {
        for sub_holds in [0, 1, 4] {
            run(sub_holds);
        }
    }
}

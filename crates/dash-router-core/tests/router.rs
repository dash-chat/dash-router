//! Scenario tests driving `RouterMachine` by hand, with bounded model types.
//! Messages are passed between nodes explicitly here; a network model with
//! in-flight messages, loss and reordering is a separate crate's job.

use std::{collections::BTreeMap, sync::Arc};

use dash_router_core::{
    Effect, LogRanges, Message, MessageEnvelope, Op, Ranges, RouterAction as A, RouterConfig,
    RouterMachine, RouterState,
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
fn l(i: usize) -> L {
    UpTo::new(i)
}
fn op(byte: u8) -> Op {
    Op {
        header: vec![byte],
        payload: Some(vec![byte, byte]),
    }
}

fn machine() -> Arc<Router> {
    Arc::new(RouterMachine::new(RouterConfig {
        want_ttl: t(2),
        have_ttl: t(2),
        relay_cap: 3,
    }))
}

fn node(m: &Arc<Router>, id: usize, subscriptions: impl IntoIterator<Item = L>) -> Node {
    m.state_machine(State::new(n(id), subscriptions))
}

fn disabled(m: &Router, s: &State, action: A<N, L, T>) {
    assert!(
        m.transition(s.clone(), action.clone()).is_err(),
        "{action:?} should not be enabled"
    );
}

fn sends(fx: &[Effect<N, L>]) -> Vec<MessageEnvelope<N, L>> {
    fx.iter()
        .filter_map(|e| match e {
            Effect::Send(m) => Some(m.clone()),
            _ => None,
        })
        .collect()
}

fn delivered(fx: &[Effect<N, L>]) -> Vec<(L, u32)> {
    fx.iter()
        .filter_map(|e| match e {
            Effect::Deliver(log, seq) => Some((*log, *seq)),
            _ => None,
        })
        .collect()
}

fn want(from: N, log: L, ranges: Ranges) -> MessageEnvelope<N, L> {
    MessageEnvelope::want(from, LogRanges::from_pairs([(log, ranges)]))
}

#[test]
fn fresh_have_is_relayed_exactly_once_per_hop() {
    let m = machine();
    let mut a = node(&m, 0, [l(0)]);
    let mut b = node(&m, 1, []); // pure relay
    let mut c = node(&m, 2, [l(0)]);

    let fx = a.step(A::Append(l(0), op(7))).unwrap();
    let [fresh] = sends(&fx).try_into().unwrap();
    assert!(matches!(fresh.message, Message::Have { fresh: true, .. }));

    // B doesn't subscribe, but stores and forwards immediately, as itself.
    let fx = b.step(A::Recv(fresh.clone())).unwrap();
    assert_eq!(delivered(&fx), vec![]);
    assert_eq!(b.holds(&l(0), 0), Some(&op(7)));
    let [forwarded] = sends(&fx).try_into().unwrap();
    assert_eq!(forwarded.from, n(1));
    assert!(matches!(
        forwarded.message,
        Message::Have { fresh: true, .. }
    ));

    // C gets it via B and delivers it to the app, forwarding once more.
    let fx = c.step(A::Recv(forwarded.clone())).unwrap();
    assert_eq!(delivered(&fx), vec![(l(0), 0)]);
    assert_eq!(sends(&fx).len(), 1);

    // The author sees B's forward: nothing new, so the flood stops here.
    let fx = a.step(A::Recv(forwarded)).unwrap();
    assert_eq!(sends(&fx), vec![]);
    let fx = b.step(A::Recv(fresh)).unwrap();
    assert_eq!(sends(&fx), vec![]);
}

/// DESIGN.md: every received Have floods onward, fresh or not, novel to
/// this node or not — its neighbours may still need it, and only this node
/// can reach them. The seen-set alone ends the flood.
#[test]
fn every_have_is_relayed_once_regardless_of_freshness_or_novelty() {
    let m = machine();
    let mut b = node(&m, 1, [l(0)]);

    // B learns op 0 from a non-fresh Have answering someone's Want; even
    // that is passed on.
    let fx = b
        .step(A::Recv(MessageEnvelope::have(n(0), fresh_ops(), false)))
        .unwrap();
    assert_eq!(delivered(&fx), vec![(l(0), 0)]);
    let [forwarded] = sends(&fx).try_into().unwrap();
    assert_eq!(forwarded, MessageEnvelope::have(n(1), fresh_ops(), false));

    // The author's fresh flood arrives late with the same op: the range is
    // already in the seen-set, so it is not relayed again — the seen-set,
    // not novelty or freshness, is what ends the flood.
    let fresh = MessageEnvelope::have(n(2), fresh_ops(), true);
    let fx = b.step(A::Recv(fresh.clone())).unwrap();
    assert_eq!(delivered(&fx), vec![], "nothing new to deliver");
    assert_eq!(sends(&fx), vec![]);

    // Once the seen-set expires, B floods it again.
    b.step(A::Tick(t(2))).unwrap();
    let fx = b.step(A::Recv(fresh)).unwrap();
    assert_eq!(sends(&fx).len(), 1);
}

fn fresh_ops() -> BTreeMap<L, BTreeMap<u32, Op>> {
    BTreeMap::from([(l(0), BTreeMap::from([(0, op(5))]))])
}

/// DESIGN.md: Wants flood too — this is how a request crosses hops to reach
/// a node that actually holds the data. Once per hop, and never back to a
/// node that already emitted it.
#[test]
fn wants_flood_once_per_hop_and_never_echo() {
    let m = machine();
    let mut a = node(&m, 0, [l(0)]);
    let mut b = node(&m, 1, []);
    let mut c = node(&m, 2, []);

    a.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = a.step(A::FireWant).unwrap();
    let [asked] = sends(&fx).try_into().unwrap();

    // B relays the Want re-signed, exactly once.
    let fx = b.step(A::Recv(asked.clone())).unwrap();
    let [relayed] = sends(&fx).try_into().unwrap();
    assert_eq!(relayed, want(n(1), l(0), Ranges::full()));
    let fx = b.step(A::Recv(asked)).unwrap();
    assert_eq!(sends(&fx), vec![], "a repeated Want is not re-relayed");

    // C, two hops out, hears it via B and relays it again: the request
    // reaches nodes the asker cannot.
    let fx = c.step(A::Recv(relayed.clone())).unwrap();
    assert_eq!(sends(&fx), vec![want(n(2), l(0), Ranges::full())]);

    // The author hears the echo and lets it die: its own emission is in
    // its seen-set.
    let fx = a.step(A::Recv(relayed)).unwrap();
    assert_eq!(sends(&fx), vec![]);
}

/// Each flood adds its own seen-set record with its own TTL. A single
/// accumulating record whose TTL refreshes on every update would let steady
/// traffic keep old ranges suppressed forever.
#[test]
fn seen_set_records_expire_independently() {
    let m = machine(); // have_ttl = 2
    let mut b = node(&m, 1, []);
    let have = |seq: u32| {
        MessageEnvelope::have(
            n(0),
            BTreeMap::from([(l(0), BTreeMap::from([(seq, op(seq as u8))]))]),
            false,
        )
    };

    let fx = b.step(A::Recv(have(0))).unwrap();
    assert_eq!(sends(&fx).len(), 1);
    b.step(A::Tick(t(1))).unwrap();
    // A second flood must not extend the first record's life.
    let fx = b.step(A::Recv(have(1))).unwrap();
    assert_eq!(sends(&fx).len(), 1);
    b.step(A::Tick(t(1))).unwrap();

    // Op 0's record has expired: it floods again. Op 1's has not.
    let fx = b.step(A::Recv(have(0))).unwrap();
    assert_eq!(sends(&fx).len(), 1, "expired range floods again");
    let fx = b.step(A::Recv(have(1))).unwrap();
    assert_eq!(sends(&fx), vec![], "younger record still suppresses");
}

#[test]
fn want_timer_follows_the_fetch_timed_idiom() {
    let m = machine();
    let mut a = node(&m, 0, [l(0)]);

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
    assert_eq!(
        sends(&fx),
        vec![want(n(0), l(0), Ranges::full())],
        "an empty subscribed log wants everything"
    );
    assert!(a.want_timer.is_none(), "must re-arm explicitly");
}

#[test]
fn others_wants_suppress_own_want_until_ttl_expires() {
    let m = machine();
    let mut a = node(&m, 0, [l(0)]);
    a.step(A::Append(l(0), op(1))).unwrap();
    a.step(A::Append(l(0), op(2))).unwrap();
    assert_eq!(
        a.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(2))])
    );

    // B already asked for 2.. so A holds back; A still asks for what B didn't.
    a.step(A::Recv(want(n(1), l(0), Ranges::from(2)))).unwrap();
    assert!(a.next_want().is_empty());
    a.step(A::Recv(want(n(1), l(0), Ranges::range(2, 5))))
        .unwrap();
    assert_eq!(
        a.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(5))])
    );

    // want_ttl is 2: after 1 tick still remembered, after 2 forgotten.
    a.step(A::Tick(t(1))).unwrap();
    assert!(!a.wants.is_empty());
    a.step(A::Tick(t(1))).unwrap();
    assert!(a.wants.is_empty());
    assert_eq!(
        a.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(2))])
    );
}

#[test]
fn have_answers_wants_minus_what_is_already_circulating() {
    let m = machine();
    let mut a = node(&m, 0, [l(0)]);
    for i in 0..3 {
        a.step(A::Append(l(0), op(i))).unwrap();
    }

    disabled(&m, &a, A::ArmHaveTimer(t(1))); // no Want witnessed yet
    a.step(A::Recv(want(n(1), l(0), Ranges::full()))).unwrap();
    // C already sent seq 1, so A need not.
    a.step(A::Recv(MessageEnvelope::have(
        n(2),
        BTreeMap::from([(l(0), BTreeMap::from([(1, op(1))]))]),
        false,
    )))
    .unwrap();

    a.step(A::ArmHaveTimer(t(0))).unwrap();
    let fx = a.step(A::FireHave).unwrap();
    let [envelope]: [MessageEnvelope<N, L>; 1] = sends(&fx).try_into().unwrap();
    let Message::Have { ops, fresh: false } = envelope.message else {
        panic!("expected one non-fresh Have")
    };
    assert_eq!(ops[&l(0)].keys().copied().collect::<Vec<_>>(), vec![0, 2]);

    // A remembers its own Have, so re-arming does not repeat it.
    a.step(A::ArmHaveTimer(t(0))).unwrap();
    let fx = a.step(A::FireHave).unwrap();
    assert_eq!(sends(&fx), vec![]);
    assert!(a.have_timer.is_none());
}

#[test]
fn want_then_have_backfills_a_late_subscriber() {
    let m = machine();
    let mut a = node(&m, 0, [l(0)]);
    let mut b = node(&m, 1, [l(0)]);
    for i in 0..2 {
        a.step(A::Append(l(0), op(i))).unwrap();
    }

    b.step(A::ArmWantTimer(t(0))).unwrap();
    let fx = b.step(A::FireWant).unwrap();
    let [want] = sends(&fx).try_into().unwrap();

    a.step(A::Recv(want)).unwrap();
    a.step(A::ArmHaveTimer(t(1))).unwrap();
    a.step(A::Tick(t(1))).unwrap();
    let fx = a.step(A::FireHave).unwrap();
    let [have] = sends(&fx).try_into().unwrap();

    let fx = b.step(A::Recv(have)).unwrap();
    assert_eq!(delivered(&fx), vec![(l(0), 0), (l(0), 1)]);
    assert_eq!(sends(&fx).len(), 1, "the backfill floods onward too");
    assert_eq!(a.held(), b.held());
    assert_eq!(
        b.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(2))])
    );
}

#[test]
fn relay_gc_drops_payloads_before_headers_and_spares_subscriptions() {
    let m = machine(); // relay_cap = 3 units
    let mut b = node(&m, 1, [l(0)]);
    b.step(A::Append(l(0), op(9))).unwrap();

    let have = |seqs: Vec<u32>| {
        MessageEnvelope::have(
            n(0),
            BTreeMap::from([(l(1), seqs.into_iter().map(|s| (s, op(s as u8))).collect())]),
            false,
        )
    };

    // Three full ops = 6 units; dropping all three payloads reaches 3.
    b.step(A::Recv(have(vec![0, 1, 2]))).unwrap();
    assert_eq!(b.relay_usage(), 3);
    for s in 0..3 {
        assert_eq!(b.holds(&l(1), s).unwrap().payload, None);
    }

    // One more full op: its payload goes first, then the oldest header.
    b.step(A::Recv(have(vec![3]))).unwrap();
    assert_eq!(b.relay_usage(), 3);
    assert_eq!(b.holds(&l(1), 0), None);
    assert_eq!(b.holds(&l(1), 3).unwrap().payload, None);
    assert_eq!(
        b.held().get(&l(1)),
        Some(&Ranges::range(1, 4)),
        "headers still count as held"
    );

    // The subscribed log is untouched.
    assert_eq!(b.holds(&l(0), 0), Some(&op(9)));

    // A re-sent payload for a header-only op is accepted as an improvement.
    b.step(A::Recv(have(vec![3]))).unwrap();
    assert_eq!(b.relay_usage(), 3);
}

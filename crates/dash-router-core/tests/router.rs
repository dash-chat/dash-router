//! Scenario tests driving `RouterMachine` by hand, with bounded model types.
//! Messages are passed between nodes explicitly here; a network model with
//! in-flight messages, loss and reordering is a separate crate's job.

use std::{collections::BTreeMap, sync::Arc};

use dash_router_core::{
    Effect, LogRanges, Message, MessageEnvelope, Op, Ranges, RouterAction as A, RouterConfig,
    RouterMachine, RouterState,
};
use polestar::{prelude::*, time::FiniteTime};

type N = UpTo<3>;
type L = UpTo<2>;
type T = FiniteTime<4, 1000>;
type Router = RouterMachine<N, L, T>;
type State = RouterState<N, L, T>;

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

fn machine() -> Router {
    RouterMachine::new(RouterConfig {
        want_ttl: t(2),
        have_ttl: t(2),
        relay_cap: 3,
    })
}

/// Apply an action, panicking with the machine's reason if it isn't enabled.
fn step(m: &Router, s: &mut State, action: A<N, L, T>) -> Vec<Effect<N, L>> {
    let (next, fx) = m
        .transition(s.clone(), action.clone())
        .unwrap_or_else(|e| panic!("{action:?} not enabled: {e}"));
    *s = next;
    fx
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
    let m = Arc::new(machine());
    let mut a = m.state_machine(State::new(n(0), [l(0)]));
    let mut b = State::new(n(1), []); // pure relay
    let mut c = State::new(n(2), [l(0)]);

    let fx = a.step(A::Append(l(0), op(7))).unwrap();
    // let fx = step(&m, &mut a, A::Append(l(0), op(7)));
    let [fresh] = sends(&fx).try_into().unwrap();
    assert!(matches!(fresh.message, Message::Have { fresh: true, .. }));

    // B doesn't subscribe, but stores and forwards immediately, as itself.
    let fx = step(&m, &mut b, A::Recv(fresh.clone()));
    assert_eq!(delivered(&fx), vec![]);
    assert_eq!(b.holds(&l(0), 0), Some(&op(7)));
    let [forwarded] = sends(&fx).try_into().unwrap();
    assert_eq!(forwarded.from, n(1));
    assert!(matches!(
        forwarded.message,
        Message::Have { fresh: true, .. }
    ));

    // C gets it via B and delivers it to the app, forwarding once more.
    let fx = step(&m, &mut c, A::Recv(forwarded.clone()));
    assert_eq!(delivered(&fx), vec![(l(0), 0)]);
    assert_eq!(sends(&fx).len(), 1);

    // The author sees B's forward: nothing new, so the flood stops here.
    let fx = step(&m, &mut a, A::Recv(forwarded));
    assert_eq!(sends(&fx), vec![]);
    let fx = step(&m, &mut b, A::Recv(fresh));
    assert_eq!(sends(&fx), vec![]);
}

/// The flood must not stop at a node that happens to already hold the data:
/// its neighbours may not, and only this node can reach them.
#[test]
fn a_fresh_have_is_relayed_even_when_nothing_in_it_is_new() {
    let m = machine();
    let mut b = State::new(n(1), [l(0)]);

    // B learns op 0 the slow way, from a Have answering someone's Want.
    let fx = step(
        &m,
        &mut b,
        A::Recv(MessageEnvelope::have(
            n(0),
            BTreeMap::from([(l(0), BTreeMap::from([(0, op(5))]))]),
            false,
        )),
    );
    assert_eq!(delivered(&fx), vec![(l(0), 0)]);
    assert_eq!(sends(&fx), vec![], "non-fresh Haves are not relayed");

    // The author's flood reaches B late. B has nothing to learn, but must
    // still pass it on.
    let fresh = MessageEnvelope::have(
        n(2),
        BTreeMap::from([(l(0), BTreeMap::from([(0, op(5))]))]),
        true,
    );
    let fx = step(&m, &mut b, A::Recv(fresh.clone()));
    assert_eq!(delivered(&fx), vec![], "nothing new to deliver");
    let [forwarded] = sends(&fx).try_into().unwrap();
    assert_eq!(forwarded, MessageEnvelope::have(n(1), fresh_ops(), true));

    // But only once: the seen-set, not novelty, is what ends the flood.
    let fx = step(&m, &mut b, A::Recv(fresh.clone()));
    assert_eq!(sends(&fx), vec![]);

    // Once the seen-set expires, B would flood it again.
    step(&m, &mut b, A::Tick(t(2)));
    let fx = step(&m, &mut b, A::Recv(fresh));
    assert_eq!(sends(&fx).len(), 1);
}

fn fresh_ops() -> BTreeMap<L, BTreeMap<u32, Op>> {
    BTreeMap::from([(l(0), BTreeMap::from([(0, op(5))]))])
}

#[test]
fn want_timer_follows_the_fetch_timed_idiom() {
    let m = machine();
    let mut a = State::new(n(0), [l(0)]);

    disabled(&m, &a, A::FireWant); // nothing armed
    disabled(&m, &a, A::Tick(t(0))); // zero tick is not a transition
    step(&m, &mut a, A::ArmWantTimer(t(2)));
    disabled(&m, &a, A::ArmWantTimer(t(1))); // already armed
    disabled(&m, &a, A::FireWant); // not due
    disabled(&m, &a, A::Tick(t(3))); // would skip past due
    step(&m, &mut a, A::Tick(t(1)));
    disabled(&m, &a, A::FireWant);
    step(&m, &mut a, A::Tick(t(1)));
    disabled(&m, &a, A::Tick(t(1))); // due: must fire before time moves on

    let fx = step(&m, &mut a, A::FireWant);
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
    let mut a = State::new(n(0), [l(0)]);
    step(&m, &mut a, A::Append(l(0), op(1)));
    step(&m, &mut a, A::Append(l(0), op(2)));
    assert_eq!(
        a.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(2))])
    );

    // B already asked for 2.. so A holds back; A still asks for what B didn't.
    step(&m, &mut a, A::Recv(want(n(1), l(0), Ranges::from(2))));
    assert!(a.next_want().is_empty());
    step(&m, &mut a, A::Recv(want(n(1), l(0), Ranges::range(2, 5))));
    assert_eq!(
        a.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(5))])
    );

    // want_ttl is 2: after 1 tick still remembered, after 2 forgotten.
    step(&m, &mut a, A::Tick(t(1)));
    assert!(!a.wants.is_empty());
    step(&m, &mut a, A::Tick(t(1)));
    assert!(a.wants.is_empty());
    assert_eq!(
        a.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(2))])
    );
}

#[test]
fn have_answers_wants_minus_what_is_already_circulating() {
    let m = machine();
    let mut a = State::new(n(0), [l(0)]);
    for i in 0..3 {
        step(&m, &mut a, A::Append(l(0), op(i)));
    }

    disabled(&m, &a, A::ArmHaveTimer(t(1))); // no Want witnessed yet
    step(&m, &mut a, A::Recv(want(n(1), l(0), Ranges::full())));
    // C already sent seq 1, so A need not.
    step(
        &m,
        &mut a,
        A::Recv(MessageEnvelope::have(
            n(2),
            BTreeMap::from([(l(0), BTreeMap::from([(1, op(1))]))]),
            false,
        )),
    );

    step(&m, &mut a, A::ArmHaveTimer(t(0)));
    let fx = step(&m, &mut a, A::FireHave);
    let [envelope]: [MessageEnvelope<N, L>; 1] = sends(&fx).try_into().unwrap();
    let Message::Have { ops, fresh: false } = envelope.message else {
        panic!("expected one non-fresh Have")
    };
    assert_eq!(ops[&l(0)].keys().copied().collect::<Vec<_>>(), vec![0, 2]);

    // A remembers its own Have, so re-arming does not repeat it.
    step(&m, &mut a, A::ArmHaveTimer(t(0)));
    let fx = step(&m, &mut a, A::FireHave);
    assert_eq!(sends(&fx), vec![]);
    assert!(a.have_timer.is_none());
}

#[test]
fn want_then_have_backfills_a_late_subscriber() {
    let m = machine();
    let mut a = State::new(n(0), [l(0)]);
    let mut b = State::new(n(1), [l(0)]);
    for i in 0..2 {
        step(&m, &mut a, A::Append(l(0), op(i)));
    }

    step(&m, &mut b, A::ArmWantTimer(t(0)));
    let fx = step(&m, &mut b, A::FireWant);
    let [want] = sends(&fx).try_into().unwrap();

    step(&m, &mut a, A::Recv(want));
    step(&m, &mut a, A::ArmHaveTimer(t(1)));
    step(&m, &mut a, A::Tick(t(1)));
    let fx = step(&m, &mut a, A::FireHave);
    let [have] = sends(&fx).try_into().unwrap();

    let fx = step(&m, &mut b, A::Recv(have));
    assert_eq!(delivered(&fx), vec![(l(0), 0), (l(0), 1)]);
    assert_eq!(sends(&fx), vec![], "non-fresh Haves are not forwarded");
    assert_eq!(a.held(), b.held());
    assert_eq!(
        b.next_want(),
        LogRanges::from_pairs([(l(0), Ranges::from(2))])
    );
}

#[test]
fn relay_gc_drops_payloads_before_headers_and_spares_subscriptions() {
    let m = machine(); // relay_cap = 3 units
    let mut b = State::new(n(1), [l(0)]);
    step(&m, &mut b, A::Append(l(0), op(9)));

    let have = |seqs: Vec<u32>| {
        MessageEnvelope::have(
            n(0),
            BTreeMap::from([(l(1), seqs.into_iter().map(|s| (s, op(s as u8))).collect())]),
            false,
        )
    };

    // Three full ops = 6 units; dropping all three payloads reaches 3.
    step(&m, &mut b, A::Recv(have(vec![0, 1, 2])));
    assert_eq!(b.relay_usage(), 3);
    for s in 0..3 {
        assert_eq!(b.holds(&l(1), s).unwrap().payload, None);
    }

    // One more full op: its payload goes first, then the oldest header.
    step(&m, &mut b, A::Recv(have(vec![3])));
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
    step(&m, &mut b, A::Recv(have(vec![3])));
    assert_eq!(b.relay_usage(), 3);
}

//! Two real shells over the loopback transport: the whole §2 select loop,
//! end to end, no p2panda.

use std::collections::BTreeSet;
use std::time::Duration;

use dash_router_core::{Op, OpsMap, RouterConfig, Storage, Units};
// The relay store is a plain OpsMap through the blanket sync bridge:
// MemStore is the *watchable ext* store and implements no eviction.
use dash_router::{CoreConfig, LoopbackHub, MemStore, PolicyIntervals, RouterEvent, spawn};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use rand::SeedableRng;

fn config() -> CoreConfig {
    CoreConfig {
        router: RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
        },
        relay_cap: 1024 as Units,
        evict_at: 0.75,
        debounce: PushDebouncePolicy {
            window_ms: 50,
            max_latency_ms: 200,
        },
        max_wire_bytes: dash_router::pack::DEFAULT_MAX_WIRE_BYTES,
    }
}

fn intervals(seed: u64) -> PolicyIntervals {
    PolicyIntervals {
        want: IntervalPolicy::Fixed {
            min_ms: 100.0,
            max_ms: 200.0,
        },
        have: IntervalPolicy::Fixed {
            min_ms: 20.0,
            max_ms: 60.0,
        },
        n: 2,
        rng: rand::rngs::StdRng::seed_from_u64(seed),
    }
}

#[allow(clippy::never_loop)] // intentional: loops past non-Delivered events, returns on the first Delivered
async fn next_delivery(events: &mut tokio::sync::mpsc::Receiver<RouterEvent<u8>>) -> (u8, u32) {
    loop {
        match tokio::time::timeout(Duration::from_secs(60), events.recv())
            .await
            .expect("delivery within virtual 60s")
            .expect("event stream open")
        {
            RouterEvent::Delivered(l, s) => return (l, s),
            RouterEvent::StorageError(e) => panic!("unexpected storage error: {e:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn push_reaches_the_other_shell() {
    let hub = LoopbackHub::new();
    let b_ext = MemStore::<u8>::new();
    let (a, _a_events, _a_task) = {
        let (h, e, t) = spawn(
            1u32,
            config(),
            Duration::from_secs(1),
            BTreeSet::from([0u8]),
            MemStore::<u8>::new(),
            OpsMap::default(),
            hub.join("192.168.0.1".parse().unwrap()),
            intervals(1),
        );
        (h, e, t)
    };
    let (_b, mut b_events, _b_task) = spawn(
        2u32,
        config(),
        Duration::from_secs(1),
        BTreeSet::from([0u8]),
        b_ext.clone(),
        OpsMap::default(),
        hub.join("192.168.0.2".parse().unwrap()),
        intervals(2),
    );

    let op = Op {
        header: vec![1],
        payload: Some(vec![9; 16]),
    };
    a.append(0, 0, op.clone()).await.unwrap();
    assert_eq!(next_delivery(&mut b_events).await, (0, 0));
    assert!(
        Storage::held_all(&b_ext.snapshot()).contains(&0, 0),
        "bytes are in B's own store"
    );
}

#[tokio::test(start_paused = true)]
async fn late_joiner_repairs_via_want() {
    let hub = LoopbackHub::new();
    let (a, _a_events, _a_task) = spawn(
        1u32,
        config(),
        Duration::from_secs(1),
        BTreeSet::from([0u8]),
        MemStore::<u8>::new(),
        OpsMap::default(),
        hub.join("192.168.0.1".parse().unwrap()),
        intervals(1),
    );
    a.append(
        0,
        0,
        Op {
            header: vec![1],
            payload: Some(vec![7]),
        },
    )
    .await
    .unwrap();
    a.append(
        0,
        1,
        Op {
            header: vec![2],
            payload: Some(vec![8]),
        },
    )
    .await
    .unwrap();
    // Let the push flood into an empty room.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // C joins afterwards: only Want/repair can teach it.
    let (_c, mut c_events, _c_task) = spawn(
        3u32,
        config(),
        Duration::from_secs(1),
        BTreeSet::from([0u8]),
        MemStore::<u8>::new(),
        OpsMap::default(),
        hub.join("192.168.0.3".parse().unwrap()),
        intervals(3),
    );
    let mut got = BTreeSet::new();
    got.insert(next_delivery(&mut c_events).await);
    got.insert(next_delivery(&mut c_events).await);
    assert_eq!(got, BTreeSet::from([(0u8, 0u32), (0, 1)]));
}

/// Finding 3: the degrade-and-report counters must be observable, not just
/// maintained internally. A freshly spawned, idle node's stats snapshot
/// comes back `Ok` with all-zero counters (nothing has degraded yet).
#[tokio::test(start_paused = true)]
async fn stats_reports_a_zeroed_snapshot_for_a_fresh_node() {
    let hub = LoopbackHub::new();
    let (a, _a_events, _a_task) = spawn(
        1u32,
        config(),
        Duration::from_secs(1),
        BTreeSet::from([0u8]),
        MemStore::<u8>::new(),
        OpsMap::default(),
        hub.join("192.168.0.1".parse().unwrap()),
        intervals(1),
    );
    let snapshot = a.stats().await.expect("stats command round-trips");
    assert_eq!(snapshot.dropped_msgs, 0);
    assert_eq!(snapshot.relay_errors, 0);
    assert_eq!(snapshot.relay_store_errors, 0);
}

/// Final review F1 in the real shell: a late joiner subscribing to a prefix
/// under which it knows no author is served every log under it within one
/// Want/Have cycle, not only after `want_ttl`. The holder's own Want is
/// relayed back to it by the joiner; that echo must not count as the
/// joiner naming the holder's logs.
#[tokio::test(start_paused = true)]
async fn late_joiner_under_prefix_is_served_before_want_ttl() {
    use dash_router_core::Pair;

    let want_ttl = Duration::from_secs(6);
    let config = || CoreConfig {
        router: RouterConfig {
            want_ttl: want_ttl.into(),
            have_ttl: want_ttl.into(),
        },
        ..config()
    };
    let intervals = |seed: u64| PolicyIntervals {
        want: IntervalPolicy::Fixed {
            min_ms: 500.0,
            max_ms: 1500.0,
        },
        have: IntervalPolicy::Fixed {
            min_ms: 50.0,
            max_ms: 250.0,
        },
        n: 2,
        rng: rand::rngs::StdRng::seed_from_u64(seed),
    };
    for seed in 0..4u64 {
        let hub = LoopbackHub::new();
        let (holder, _holder_events, _holder_task) = spawn(
            1u32,
            config(),
            Duration::from_secs(1),
            BTreeSet::from([1u8]),
            MemStore::<Pair>::new(),
            OpsMap::<Pair>::default(),
            hub.join("192.168.0.1".parse().unwrap()),
            intervals(seed * 2 + 1),
        );
        let mut all = BTreeSet::new();
        for author in [1u8, 2] {
            for seq in 0..3u32 {
                let op = Op {
                    header: vec![author, seq as u8],
                    payload: Some(vec![author; 8]),
                };
                holder.append(Pair::new(1, author), seq, op).await.unwrap();
                all.insert((Pair::new(1, author), seq));
            }
        }
        // Let the push flood into an empty room and the holder settle.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let (_joiner, mut events, _joiner_task) = spawn(
            2u32,
            config(),
            Duration::from_secs(1),
            BTreeSet::from([1u8]),
            MemStore::<Pair>::new(),
            OpsMap::<Pair>::default(),
            hub.join("192.168.0.2".parse().unwrap()),
            intervals(seed * 2 + 2),
        );
        // One joiner Want (≤ 1.5 s) plus one holder Have (≤ 0.25 s), with
        // room to spare, and well under `want_ttl`.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut got = BTreeSet::new();
        while got != all {
            match tokio::time::timeout_at(deadline, events.recv()).await {
                Ok(Some(RouterEvent::Delivered(log, seq))) => {
                    got.insert((log, seq));
                }
                Ok(Some(RouterEvent::StorageError(e))) => panic!("storage error: {e:?}"),
                Ok(None) => panic!("event stream closed"),
                Err(_) => panic!("seed {seed}: joiner got only {got:?} within 3 s"),
            }
        }
    }
}

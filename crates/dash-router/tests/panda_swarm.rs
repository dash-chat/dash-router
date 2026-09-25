//! Fifty real shells over the real p2panda transport (spec §6.1): real UDP
//! sockets, real mDNS, one process. Ignored by default because it binds
//! sockets and multicasts on the host; run manually with
//!
//! ```text
//! cargo test -p dash-router --features p2panda --test panda_swarm -- --ignored --nocapture
//! ```
//!
//! One swarm, two claims:
//!
//! 1. **The overlay is not a clique.** Every node's set of direct gossip
//!    neighbours -- the peers a `broadcast` actually hands bytes to -- is
//!    non-empty and a strict subset of the other N-1 nodes. HyParView caps
//!    the active view (5 by default), so with N = 50 every wire message
//!    must travel multiple gossip hops to reach everyone. Checked before
//!    and after the traffic in claim 2, so the overlay can't quietly turn
//!    into a clique under load.
//!
//! 2. **Full replication over that sparse overlay.** Each node authors one
//!    op on its own log (log `i` belongs to node `i`) while subscribed to
//!    all N logs, and every node's ext store ends up holding all N ops.
#![cfg(feature = "p2panda")]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use dash_router::panda::{GossipConfig, PandaTransport, spawn_panda};
use dash_router::{CoreConfig, MemStore, PolicyIntervals, RouterEvent, RouterHandle, spawn};
use dash_router_core::{Op, OpsMap, RouterConfig, Storage};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use p2panda_core::{SigningKey, VerifyingKey};
use rand::SeedableRng;
use tokio::sync::{mpsc, watch};

const N: usize = 50;
/// Every node must have joined the overlay within this long.
const JOIN_DEADLINE: Duration = Duration::from_secs(90);
/// Every node must hold every op within this long after the appends.
const REPLICATION_DEADLINE: Duration = Duration::from_secs(180);

type Log = u8;

fn config() -> CoreConfig {
    CoreConfig {
        router: RouterConfig {
            want_ttl: Duration::from_secs(2).into(),
            have_ttl: Duration::from_secs(2).into(),
        },
        relay_cap: 1 << 20,
        evict_at: 0.75,
        debounce: PushDebouncePolicy {
            window_ms: 50,
            max_latency_ms: 200,
        },
        max_wire_bytes: None,
    }
}

fn intervals(seed: u64) -> PolicyIntervals {
    PolicyIntervals {
        want: IntervalPolicy::Fixed {
            min_ms: 500.0,
            max_ms: 1500.0,
        },
        have: IntervalPolicy::Fixed {
            min_ms: 50.0,
            max_ms: 250.0,
        },
        n: N,
        rng: rand::rngs::StdRng::seed_from_u64(seed),
    }
}

/// The op node `i` authors on log `i`: a header that names the author and
/// a small payload, so a Have bundling all N logs stays well under
/// p2panda's 4 KiB max gossip message size.
fn authored_op(i: usize) -> Op {
    Op {
        header: vec![i as u8],
        payload: Some(vec![i as u8; 8]),
    }
}

struct Node {
    key: VerifyingKey,
    neighbours: watch::Receiver<BTreeSet<VerifyingKey>>,
    handle: RouterHandle<Log>,
    events: mpsc::Receiver<RouterEvent<Log>>,
    ext: MemStore<Log>,
}

async fn spawn_node(i: usize) -> Node {
    let (transport, key): (PandaTransport, VerifyingKey) =
        spawn_panda(SigningKey::generate(), GossipConfig::default())
            .await
            .unwrap_or_else(|e| panic!("spawn p2panda node {i}: {e:#}"));
    let neighbours = transport.neighbours();
    let ext = MemStore::<Log>::new();
    // The wire identity `N` is the node's p2panda public key (spec §6),
    // exactly as a real node would run.
    let (handle, events, _task) = spawn(
        key,
        config(),
        Duration::from_secs(5),
        (0..N as Log).collect(),
        ext.clone(),
        OpsMap::default(),
        transport,
        intervals(i as u64),
    );
    Node {
        key,
        neighbours,
        handle,
        events,
        ext,
    }
}

/// Claim 1: every node's direct-neighbour set is non-empty and a strict
/// subset of the other N-1 nodes. Returns the per-node neighbour counts
/// for the report.
fn assert_sparse_overlay(nodes: &[Node], when: &str) -> Vec<usize> {
    let all: BTreeSet<VerifyingKey> = nodes.iter().map(|n| n.key).collect();
    let mut counts = Vec::with_capacity(nodes.len());
    for (i, node) in nodes.iter().enumerate() {
        let neighbours = node.neighbours.borrow().clone();
        assert!(
            !neighbours.contains(&node.key),
            "{when}: node {i} lists itself as a neighbour"
        );
        assert!(
            neighbours.is_subset(&all),
            "{when}: node {i} has a neighbour outside the swarm: {:?}",
            neighbours.difference(&all).collect::<Vec<_>>()
        );
        assert!(
            !neighbours.is_empty(),
            "{when}: node {i} has no gossip neighbours (never joined the overlay)"
        );
        assert!(
            neighbours.len() < nodes.len() - 1,
            "{when}: node {i} is directly connected to every other node ({}); \
             the overlay is a clique, so this test isn't exercising multi-hop gossip",
            neighbours.len()
        );
        counts.push(neighbours.len());
    }
    counts
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "binds 50 real sockets and mDNS; run manually: cargo test -p dash-router --features p2panda --test panda_swarm -- --ignored --nocapture"]
async fn fifty_nodes_form_a_sparse_overlay_and_fully_replicate() {
    let t0 = Instant::now();
    let mut nodes = Vec::with_capacity(N);
    for i in 0..N {
        nodes.push(spawn_node(i).await);
    }
    eprintln!("spawned {N} nodes in {:.1?}", t0.elapsed());

    // --- Wait for the overlay: every node has at least one neighbour. ---
    let join_deadline = Instant::now() + JOIN_DEADLINE;
    loop {
        let unjoined: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.neighbours.borrow().is_empty())
            .map(|(i, _)| i)
            .collect();
        if unjoined.is_empty() {
            break;
        }
        assert!(
            Instant::now() < join_deadline,
            "{} nodes never joined the gossip overlay within {JOIN_DEADLINE:?}: {unjoined:?}",
            unjoined.len()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("all {N} nodes joined the overlay at {:.1?}", t0.elapsed());

    // --- Claim 1 (before traffic). ---
    let counts = assert_sparse_overlay(&nodes, "before appends");
    eprintln!(
        "direct neighbours per node before appends: min {} / max {} / mean {:.1}",
        counts.iter().min().unwrap(),
        counts.iter().max().unwrap(),
        counts.iter().sum::<usize>() as f64 / N as f64
    );

    // --- Claim 2: every node authors one op on its own log. ---
    for (i, node) in nodes.iter().enumerate() {
        node.handle
            .append(i as Log, 0, authored_op(i))
            .await
            .unwrap_or_else(|e| panic!("node {i} append: {e:#}"));
    }
    let appended_at = Instant::now();

    // Drain Delivered events until every node holds all N ops (the ext
    // store is the ground truth; events are just the wake-up signal).
    let want: BTreeSet<(Log, u32)> = (0..N as Log).map(|l| (l, 0)).collect();
    let mut delivered: BTreeMap<usize, BTreeSet<(Log, u32)>> = BTreeMap::new();
    let mut incomplete: BTreeSet<usize> = (0..N).collect();
    let replication_deadline = appended_at + REPLICATION_DEADLINE;
    while !incomplete.is_empty() {
        let mut progressed = false;
        for i in incomplete.clone() {
            let node = &mut nodes[i];
            // Non-blocking drain of whatever arrived since the last pass.
            while let Ok(ev) = node.events.try_recv() {
                match ev {
                    RouterEvent::Delivered(l, s) => {
                        delivered.entry(i).or_default().insert((l, s));
                        progressed = true;
                    }
                    RouterEvent::StorageError(e) => {
                        panic!("node {i} reported a storage error: {e:?}")
                    }
                }
            }
            let held = Storage::held_all(&node.ext.snapshot());
            if want.iter().all(|(l, s)| held.contains(&l, *s)) {
                incomplete.remove(&i);
                progressed = true;
            }
        }
        if incomplete.is_empty() {
            break;
        }
        if Instant::now() >= replication_deadline {
            let mut missing = BTreeMap::new();
            for &i in &incomplete {
                let held = Storage::held_all(&nodes[i].ext.snapshot());
                let m: Vec<Log> = want
                    .iter()
                    .filter(|(l, s)| !held.contains(&l, *s))
                    .map(|(l, _)| *l)
                    .collect();
                missing.insert(i, m);
            }
            panic!(
                "{} of {N} nodes still missing ops {REPLICATION_DEADLINE:?} after the appends; \
                 (node -> missing logs) {missing:?}",
                incomplete.len()
            );
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    eprintln!(
        "all {N} nodes hold all {N} ops {:.1?} after the appends ({:.1?} total)",
        appended_at.elapsed(),
        t0.elapsed()
    );

    // Every node's ext store holds exactly the swarm's N ops, byte for byte.
    for (i, node) in nodes.iter().enumerate() {
        let snapshot = node.ext.snapshot();
        for j in 0..N {
            let got = Storage::fetch(
                &snapshot,
                &dash_router_core::LogRanges::from_pairs([(
                    j as Log,
                    dash_router_core::Ranges::from_seqs([0]),
                )]),
            );
            assert_eq!(
                got,
                vec![(j as Log, 0, authored_op(j))],
                "node {i} holds the wrong bytes for node {j}'s op"
            );
        }
    }

    // Delivered must not lie in the other direction either: every op a
    // node did not author itself arrived as a Delivered event. The shell
    // emits Delivered *after* the ext write, so the store can be complete
    // a beat before the last events land; keep receiving until they do.
    for (i, node) in nodes.iter_mut().enumerate() {
        let expected: BTreeSet<(Log, u32)> = want
            .iter()
            .copied()
            .filter(|(l, _)| *l as usize != i)
            .collect();
        let got = delivered.entry(i).or_default();
        while !got.is_superset(&expected) {
            match tokio::time::timeout(Duration::from_secs(5), node.events.recv()).await {
                Ok(Some(RouterEvent::Delivered(l, s))) => {
                    got.insert((l, s));
                }
                Ok(Some(RouterEvent::StorageError(e))) => {
                    panic!("node {i} reported a storage error: {e:?}")
                }
                Ok(None) => panic!("node {i}'s event stream closed"),
                Err(_) => break,
            }
        }
        assert_eq!(
            *got, expected,
            "node {i}'s Delivered events don't match the N-1 foreign ops"
        );
    }

    // --- Claim 1 (after traffic). ---
    let counts = assert_sparse_overlay(&nodes, "after replication");
    eprintln!(
        "direct neighbours per node after replication: min {} / max {} / mean {:.1}",
        counts.iter().min().unwrap(),
        counts.iter().max().unwrap(),
        counts.iter().sum::<usize>() as f64 / N as f64
    );

    // Nothing degraded along the way.
    for (i, node) in nodes.iter().enumerate() {
        let stats = node.handle.stats().await.expect("stats");
        assert_eq!(stats.relay_errors, 0, "node {i} had relay errors");
        assert_eq!(
            stats.relay_store_errors, 0,
            "node {i} had relay store errors"
        );
    }
}

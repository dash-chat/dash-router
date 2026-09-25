//! Delay tolerance over the real p2panda transport (real UDP sockets, real
//! mDNS, one process), in the style of `panda_swarm`. Ignored by default
//! because it binds sockets and multicasts on the host; run manually with
//!
//! ```text
//! cargo test -p dash-router --features p2panda --test panda_delay_tolerance -- --ignored --nocapture
//! ```
//!
//! N nodes, indexed 0..N. Node 0 is A and node N-1 is Z. A and Z subscribe
//! to the same channel and each author one op on their own log under it
//! (`Pair { channel, author: 0 }` and `Pair { channel, author: N-1 }`), but
//! they are never on the LAN at the same time. Every other node subscribes
//! to nothing. The claim: A's op still reaches Z, and Z's op still reaches
//! A, carried hop by hop through the unsubscribed nodes' relay stores.
//!
//! The LAN is modelled as "which shells are currently alive": a node is on
//! the LAN exactly while its shell task and p2panda transport exist.
//! Leaving is `shutdown` (which drops the transport, tearing down mDNS,
//! discovery and gossip); rejoining re-spawns the shell with the same
//! signing key, the same ext store and the same relay store, exactly as a
//! phone coming back into range would.
//!
//! Schedule, with only nodes `n` and `n+1` on the LAN at each step:
//!
//! - forward, n = 0 ..= N-2: wait until `n+1` holds A's op (Z: in its ext
//!   store; anyone else: in its relay store);
//! - backward, n = N-2 ..= 0: wait until `n` holds Z's op likewise.
//!
//! Then Z's ext store holds A's op byte for byte, and A's holds Z's.
#![cfg(feature = "p2panda")]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dash_router::panda::{GossipConfig, PandaTransport, spawn_panda};
use dash_router::{CoreConfig, MemStore, PolicyIntervals, RouterEvent, RouterHandle, spawn};
use dash_router_core::{
    EvictableStorage, LogRanges, Op, OpsMap, Pair, Ranges, RouterConfig, Seq, Storage, Units,
};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use p2panda_core::{SigningKey, VerifyingKey};
use rand::SeedableRng;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const N: usize = 6;
const A: usize = 0;
const Z: usize = N - 1;
/// The one channel A and Z share.
const CHANNEL: u8 = 7;
/// Each step (two nodes alone on the LAN) must complete within this long.
const STEP_DEADLINE: Duration = Duration::from_secs(60);

type Log = Pair;

const A_LOG: Log = Pair::new(CHANNEL, A as u8);
const Z_LOG: Log = Pair::new(CHANNEL, Z as u8);

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
        // Only ever two nodes on the LAN at once.
        n: 2,
        rng: rand::rngs::StdRng::seed_from_u64(seed),
    }
}

/// The op node `i` authors: a header naming the author and a small payload.
fn authored_op(i: usize) -> Op {
    Op {
        header: vec![i as u8],
        payload: Some(vec![i as u8; 8]),
    }
}

/// A relay store that survives its shell: `spawn` takes the relay store by
/// value and moves it into the node task, so to re-spawn a node with the
/// relay contents it accumulated last time it was on the LAN, the store
/// has to live behind a shared handle. Same shape as `MemStore`, minus the
/// change hints (the relay store has no writers of its own).
#[derive(Clone, Default)]
struct SharedRelay(Arc<Mutex<OpsMap<Log>>>);

impl SharedRelay {
    fn snapshot(&self) -> OpsMap<Log> {
        self.0.lock().expect("relay poisoned").clone()
    }
}

impl Storage<Log> for SharedRelay {
    fn held_of(&self, logs: &BTreeSet<Log>) -> LogRanges<Log> {
        Storage::held_of(&*self.0.lock().expect("relay poisoned"), logs)
    }
    fn held_all(&self) -> LogRanges<Log> {
        Storage::held_all(&*self.0.lock().expect("relay poisoned"))
    }
    fn fetch(&self, ranges: &LogRanges<Log>) -> Vec<(Log, Seq, Op)> {
        Storage::fetch(&*self.0.lock().expect("relay poisoned"), ranges)
    }
    fn ingest(&mut self, log: Log, seq: Seq, op: Op) {
        Storage::ingest(&mut *self.0.lock().expect("relay poisoned"), log, seq, op)
    }
}

impl EvictableStorage<Log> for SharedRelay {
    fn usage(&self) -> Units {
        EvictableStorage::usage(&*self.0.lock().expect("relay poisoned"))
    }
    fn held_payloads(&self) -> LogRanges<Log> {
        EvictableStorage::held_payloads(&*self.0.lock().expect("relay poisoned"))
    }
    fn evict_payloads(&mut self, ranges: &LogRanges<Log>) {
        EvictableStorage::evict_payloads(&mut *self.0.lock().expect("relay poisoned"), ranges)
    }
    fn evict(&mut self, ranges: &LogRanges<Log>) {
        EvictableStorage::evict(&mut *self.0.lock().expect("relay poisoned"), ranges)
    }
    fn ingest_delta(&self, log: &Log, seq: Seq, op: &Op) -> Units {
        EvictableStorage::ingest_delta(&*self.0.lock().expect("relay poisoned"), log, seq, op)
    }
}

/// What a node keeps while it is off the LAN: its identity, its
/// subscriptions and both stores.
struct Node {
    signing_key: SigningKey,
    key: VerifyingKey,
    subscriptions: BTreeSet<u8>,
    ext: MemStore<Log>,
    relay: SharedRelay,
    /// `Some` while the node is on the LAN.
    live: Option<Live>,
}

/// The parts that exist only while a node is on the LAN.
struct Live {
    handle: RouterHandle<Log>,
    events: mpsc::Receiver<RouterEvent<Log>>,
    task: JoinHandle<anyhow::Result<()>>,
    neighbours: watch::Receiver<BTreeSet<VerifyingKey>>,
}

fn name(i: usize) -> String {
    match i {
        A => "A".to_string(),
        Z => "Z".to_string(),
        _ => i.to_string(),
    }
}

impl Node {
    fn new(i: usize) -> Self {
        let signing_key = SigningKey::generate();
        let key = signing_key.verifying_key();
        let subscriptions = if i == A || i == Z {
            BTreeSet::from([CHANNEL])
        } else {
            BTreeSet::new()
        };
        Self {
            signing_key,
            key,
            subscriptions,
            ext: MemStore::new(),
            relay: SharedRelay::default(),
            live: None,
        }
    }

    /// Bring the node onto the LAN: a fresh p2panda stack and a fresh
    /// shell over the node's persistent key and stores.
    async fn join(&mut self, i: usize) {
        assert!(self.live.is_none(), "{} is already on the LAN", name(i));
        let (transport, key): (PandaTransport, VerifyingKey) =
            spawn_panda(self.signing_key.clone(), GossipConfig::default())
                .await
                .unwrap_or_else(|e| panic!("spawn p2panda node {}: {e:#}", name(i)));
        assert_eq!(key, self.key);
        let neighbours = transport.neighbours();
        let (handle, events, task) = spawn(
            key,
            config(),
            Duration::from_secs(5),
            self.subscriptions.clone(),
            self.ext.clone(),
            self.relay.clone(),
            transport,
            intervals(i as u64),
        );
        self.live = Some(Live {
            handle,
            events,
            task,
            neighbours,
        });
    }

    /// Take the node off the LAN: stop the shell and wait for its task to
    /// exit, which drops the transport and with it the whole p2panda stack.
    async fn leave(&mut self, i: usize) {
        let Some(live) = self.live.take() else {
            return;
        };
        live.handle.shutdown().await.expect("shutdown");
        tokio::time::timeout(Duration::from_secs(10), live.task)
            .await
            .unwrap_or_else(|_| panic!("{}'s shell did not exit after shutdown", name(i)))
            .expect("shell task panicked")
            .unwrap_or_else(|e| panic!("{}'s shell exited with an error: {e:#}", name(i)));
    }

    fn live(&mut self, i: usize) -> &mut Live {
        self.live
            .as_mut()
            .unwrap_or_else(|| panic!("{} is not on the LAN", name(i)))
    }

    /// Does this node hold `(log, 0)`? For a subscriber that means the ext
    /// store; for anyone else it means the relay store.
    fn holds(&self, log: Log) -> bool {
        let store = if self.subscriptions.contains(&log.channel) {
            Storage::held_all(&self.ext.snapshot())
        } else {
            Storage::held_all(&self.relay.snapshot())
        };
        store.contains(&log, 0)
    }
}

/// Make exactly `on` the set of nodes on the LAN.
async fn set_lan(nodes: &mut [Node], on: &[usize]) {
    for (i, node) in nodes.iter_mut().enumerate() {
        if !on.contains(&i) {
            node.leave(i).await;
        }
    }
    for &i in on {
        if nodes[i].live.is_none() {
            nodes[i].join(i).await;
        }
    }
}

/// Wait until node `target` holds `log`, with `peer` the only other node
/// on the LAN. Drains both nodes' events meanwhile (a storage error is
/// fatal); on timeout, reports what each node holds and who it can see.
async fn wait_until_holds(nodes: &mut [Node], target: usize, peer: usize, log: Log, what: &str) {
    let t0 = Instant::now();
    let deadline = t0 + STEP_DEADLINE;
    loop {
        for i in [target, peer] {
            let live = nodes[i].live(i);
            while let Ok(ev) = live.events.try_recv() {
                if let RouterEvent::StorageError(e) = ev {
                    panic!("{} reported a storage error: {e:?}", name(i));
                }
            }
        }
        if nodes[target].holds(log) {
            eprintln!(
                "{} holds {what} {:.1?} after joining {}",
                name(target),
                t0.elapsed(),
                name(peer)
            );
            return;
        }
        if Instant::now() >= deadline {
            let describe = |nodes: &mut [Node], i: usize| {
                let keys: Vec<VerifyingKey> = nodes.iter().map(|n| n.key).collect();
                let seen: Vec<String> = nodes[i]
                    .live(i)
                    .neighbours
                    .borrow()
                    .iter()
                    .map(|k| {
                        keys.iter()
                            .position(|x| x == k)
                            .map(name)
                            .unwrap_or_else(|| "?".into())
                    })
                    .collect();
                format!(
                    "{}: neighbours {seen:?}, ext {:?}, relay {:?}",
                    name(i),
                    Storage::held_all(&nodes[i].ext.snapshot()),
                    Storage::held_all(&nodes[i].relay.snapshot()),
                )
            };
            let t = describe(nodes, target);
            let p = describe(nodes, peer);
            panic!(
                "{} did not get {what} within {STEP_DEADLINE:?} of being alone on the LAN with {}\n  {t}\n  {p}",
                name(target),
                name(peer),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "binds real sockets and mDNS; run manually: cargo test -p dash-router --features p2panda --test panda_delay_tolerance -- --ignored --nocapture"]
async fn ops_cross_a_lan_that_never_holds_both_ends_at_once() {
    let t0 = Instant::now();
    let mut nodes: Vec<Node> = (0..N).map(Node::new).collect();

    // A and Z each author one op before anyone is on the LAN, via the
    // embedder's native write path; the shell picks it up on spawn.
    nodes[A].ext.insert_out_of_band(A_LOG, 0, authored_op(A));
    nodes[Z].ext.insert_out_of_band(Z_LOG, 0, authored_op(Z));

    // --- Forward: carry A's op towards Z. ---
    for n in 0..N - 1 {
        set_lan(&mut nodes, &[n, n + 1]).await;
        eprintln!(
            "[{:.1?}] LAN = {{{}, {}}}",
            t0.elapsed(),
            name(n),
            name(n + 1)
        );
        wait_until_holds(&mut nodes, n + 1, n, A_LOG, "A's op").await;
    }
    assert!(nodes[Z].holds(A_LOG), "Z never got A's op");

    // --- Backward: carry Z's op towards A. ---
    for n in (0..N - 1).rev() {
        set_lan(&mut nodes, &[n, n + 1]).await;
        eprintln!(
            "[{:.1?}] LAN = {{{}, {}}}",
            t0.elapsed(),
            name(n),
            name(n + 1)
        );
        wait_until_holds(&mut nodes, n, n + 1, Z_LOG, "Z's op").await;
    }
    assert!(nodes[A].holds(Z_LOG), "A never got Z's op");

    // Both ends hold the other's op byte for byte, in the ext store.
    for (holder, log, author) in [(Z, A_LOG, A), (A, Z_LOG, Z)] {
        let got = Storage::fetch(
            &nodes[holder].ext.snapshot(),
            &LogRanges::from_pairs([(log, Ranges::from_seqs([0]))]),
        );
        assert_eq!(
            got,
            vec![(log, 0, authored_op(author))],
            "{} holds the wrong bytes for {}'s op",
            name(holder),
            name(author)
        );
    }

    // Nothing degraded along the way.
    for (i, node) in nodes.iter().enumerate() {
        if let Some(live) = &node.live {
            let stats = live.handle.stats().await.expect("stats");
            assert_eq!(stats.relay_errors, 0, "{} had relay errors", name(i));
            assert_eq!(
                stats.relay_store_errors,
                0,
                "{} had relay store errors",
                name(i)
            );
        }
    }
    set_lan(&mut nodes, &[]).await;
    eprintln!("done in {:.1?}", t0.elapsed());
}

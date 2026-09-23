//! The tokio shell: one node task owning pure `RouterMachine` transitions,
//! async storage behind traits, and a pluggable LAN transport. See
//! docs/superpowers/specs/2026-09-21-real-world-shell-design.md.

pub mod disk;
pub mod handle;
pub mod lan;
pub mod mem;
#[cfg(feature = "p2panda")]
pub mod panda;
pub mod shell;
pub mod storage;
pub mod transport;

pub use disk::{DiskRelayStore, LogKey};
pub use handle::{Command, RouterEvent, RouterHandle, StatsSnapshot, StorageErrorReport};
pub use lan::is_lan;
pub use mem::MemStore;
pub use shell::{CoreConfig, IntervalSource, NodeCore, Out, PolicyIntervals, spawn};
pub use storage::{AsyncEvictableStorage, AsyncStorage, WatchableStorage};
pub use transport::{
    GossipPublisher, GossipSubscription, GossipTransport, Incoming, LoopbackHub, LoopbackTransport,
    PeerIdentity, PeerKey, Transport,
};

/// The pure protocol core, re-exported so an embedder depends on one crate.
pub use dash_router_core as core;
/// The interval/debounce policies, likewise.
pub use dash_router_policy as policy;

//! The tokio shell: one node task owning pure `RouterMachine` transitions,
//! async storage behind traits, and a pluggable LAN transport. See
//! docs/superpowers/specs/2026-09-21-real-world-shell-design.md.

pub mod disk;
pub mod handle;
pub mod lan;
pub mod mem;
pub mod shell;
pub mod storage;
pub mod transport;

pub use disk::{DiskRelayStore, LogKey};
pub use handle::{RouterEvent, StorageErrorReport};
pub use lan::is_lan;
pub use mem::MemStore;
pub use shell::{CoreConfig, IntervalSource, NodeCore, Out, PolicyIntervals};
pub use storage::{AsyncEvictableStorage, AsyncStorage, WatchableStorage};
pub use transport::{Incoming, LoopbackHub, LoopbackTransport, Transport};

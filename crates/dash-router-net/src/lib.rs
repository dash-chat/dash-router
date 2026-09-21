//! The tokio shell: one node task owning pure `RouterMachine` transitions,
//! async storage behind traits, and a pluggable LAN transport. See
//! docs/superpowers/specs/2026-09-21-real-world-shell-design.md.

pub mod disk;
pub mod mem;
pub mod storage;

pub use disk::{DiskRelayStore, LogKey};
pub use mem::MemStore;
pub use storage::{AsyncEvictableStorage, AsyncStorage, WatchableStorage};

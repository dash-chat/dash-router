//! The embedding API's data types (spec §5): the command channel and
//! `RouterHandle` that Task 10's `spawn` returns alongside the node task.

use dash_router_core::{Op, Seq};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouterEvent<L> {
    /// Novel subscribed data landed in the ext store. No bytes: the
    /// embedder owns that store; handing bytes again would invite a
    /// second source of truth [approved].
    Delivered(L, Seq),
    /// The ext store (the embedder's data path) failed; the node keeps
    /// gossiping from what it has [approved: degrade, don't crash].
    StorageError(StorageErrorReport),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageErrorReport {
    pub context: &'static str,
    pub message: String,
}

/// A request into the node task's select loop (spec §5). `Shutdown` drains
/// nothing: durable state is already in the stores, and router state is
/// deliberately ephemeral.
pub enum Command<L> {
    Append {
        log: L,
        seq: Seq,
        op: Op,
        reply: oneshot::Sender<anyhow::Result<()>>,
    },
    Subscribe {
        log: L,
        reply: oneshot::Sender<anyhow::Result<()>>,
    },
    Unsubscribe {
        log: L,
        reply: oneshot::Sender<anyhow::Result<()>>,
    },
    /// Finding 3 (spec §3's degrade-and-report posture, made observable):
    /// snapshot the node's degrade counters. No `now`/state mutation — a
    /// plain read of already-maintained counters.
    Stats {
        reply: oneshot::Sender<StatsSnapshot>,
    },
    Shutdown,
}

/// A point-in-time read of the degrade counters a spawned node task
/// maintains (spec §3): dropped wire messages, relay-store call failures
/// the shell degraded from, and the relay store's own internally-swallowed
/// errors (e.g. `DiskRelayStore::io_errors`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub dropped_msgs: u64,
    pub relay_errors: u64,
    pub relay_store_errors: u64,
}

/// The embedder's handle to a spawned node task (spec §5). Cloning shares
/// the command channel, so any number of embedders can drive the same node.
#[derive(Clone)]
pub struct RouterHandle<L> {
    tx: mpsc::Sender<Command<L>>,
}

impl<L> RouterHandle<L> {
    pub(crate) fn new(tx: mpsc::Sender<Command<L>>) -> Self {
        Self { tx }
    }
}

impl<L: Send> RouterHandle<L> {
    async fn call(
        &self,
        make: impl FnOnce(oneshot::Sender<anyhow::Result<()>>) -> Command<L>,
    ) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make(reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("router task is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("router task dropped the reply"))?
    }

    pub async fn append(&self, log: L, seq: Seq, op: Op) -> anyhow::Result<()> {
        self.call(|reply| Command::Append {
            log,
            seq,
            op,
            reply,
        })
        .await
    }

    pub async fn subscribe(&self, log: L) -> anyhow::Result<()> {
        self.call(|reply| Command::Subscribe { log, reply }).await
    }

    pub async fn unsubscribe(&self, log: L) -> anyhow::Result<()> {
        self.call(|reply| Command::Unsubscribe { log, reply }).await
    }

    /// Finding 3: read the node task's degrade counters.
    pub async fn stats(&self) -> anyhow::Result<StatsSnapshot> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Command::Stats { reply: reply_tx })
            .await
            .map_err(|_| anyhow::anyhow!("router task is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("router task dropped the reply"))
    }

    /// A closed channel means the task is already down — that's not a
    /// failure to shut down, it's the goal already met.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.tx.send(Command::Shutdown).await;
        Ok(())
    }
}

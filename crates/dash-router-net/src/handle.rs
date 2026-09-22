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
    Shutdown,
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
        self.call(|reply| Command::Append { log, seq, op, reply }).await
    }

    pub async fn subscribe(&self, log: L) -> anyhow::Result<()> {
        self.call(|reply| Command::Subscribe { log, reply }).await
    }

    pub async fn unsubscribe(&self, log: L) -> anyhow::Result<()> {
        self.call(|reply| Command::Unsubscribe { log, reply }).await
    }

    /// A closed channel means the task is already down — that's not a
    /// failure to shut down, it's the goal already met.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.tx.send(Command::Shutdown).await;
        Ok(())
    }
}

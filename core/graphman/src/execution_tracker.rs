use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use graphman_store::ExecutionId;
use graphman_store::GraphmanStore;
use tokio::sync::Notify;

/// The execution status is updated at this interval.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

/// Used with long-running command executions to maintain their status as active.
pub struct GraphmanExecutionTracker<S> {
    id: ExecutionId,
    heartbeat_stopper: Arc<Notify>,
    store: Arc<S>,
}

impl<S> GraphmanExecutionTracker<S>
where
    S: GraphmanStore + Send + Sync + 'static,
{
    /// Creates a new execution tracker that spawns a separate background task that keeps
    /// the execution active by periodically updating its status.
    pub fn new(store: Arc<S>, id: ExecutionId) -> Self {
        let heartbeat_stopper = Arc::new(Notify::new());

        let tracker = Self {
            id,
            store,
            heartbeat_stopper,
        };

        tracker.spawn_heartbeat();
        tracker
    }

    fn spawn_heartbeat(&self) {
        let id = self.id;
        let heartbeat_stopper = self.heartbeat_stopper.clone();
        let store = self.store.clone();

        graph::spawn(async move {
            store.mark_execution_as_running(id).unwrap();

            let stop_heartbeat = heartbeat_stopper.notified();
            tokio::pin!(stop_heartbeat);

            loop {
                tokio::select! {
                    biased;

                    _ = &mut stop_heartbeat => {
                        break;
                    },

                    _ = tokio::time::sleep(DEFAULT_HEARTBEAT_INTERVAL) => {
                        store.mark_execution_as_running(id).unwrap();
                    },
                }
            }
        });
    }

    /// Completes the execution with an error.
    pub fn track_failure(self, error_message: String) -> Result<()> {
        self.heartbeat_stopper.notify_one();

        self.store.mark_execution_as_failed(self.id, error_message)
    }

    /// Completes the execution with a success.
    pub fn track_success(self) -> Result<()> {
        self.heartbeat_stopper.notify_one();

        self.store.mark_execution_as_succeeded(self.id)
    }
}

impl<S> Drop for GraphmanExecutionTracker<S> {
    fn drop(&mut self) {
        self.heartbeat_stopper.notify_one();
    }
}

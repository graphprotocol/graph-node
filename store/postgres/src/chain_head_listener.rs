use std::collections::BTreeMap;

use diesel::RunQueryDsl;
use lazy_static::lazy_static;
use tokio::sync::{mpsc::Receiver, watch, RwLock};

use crate::{
    connection_pool::ConnectionPool,
    notification_listener::{JsonNotification, NotificationListener, SafeChannelName},
};
use graph::prelude::serde_json::{self, json};
use graph::prelude::*;
use graph::tokio_stream::wrappers::WatchStream;
use graph_chain_ethereum::BlockIngestorMetrics;

lazy_static! {
    pub static ref CHANNEL_NAME: SafeChannelName =
        SafeChannelName::i_promise_this_is_safe("chain_head_updates");
}

struct Watcher {
    sender: watch::Sender<()>,
    receiver: watch::Receiver<()>,
}

impl Watcher {
    fn new() -> Self {
        let (sender, receiver) = watch::channel(());
        Watcher { sender, receiver }
    }

    fn send(&self) {
        // Unwrap: `self` holds a receiver.
        self.sender.send(()).unwrap()
    }
}

pub struct ChainHeadUpdateListener {
    /// Update watchers keyed by network.
    watchers: Arc<RwLock<BTreeMap<String, Watcher>>>,
    _listener: NotificationListener,
}

/// Sender for messages that the `ChainHeadUpdateListener` on other nodes
/// will receive. The sender is specific to a particular chain.
pub(crate) struct ChainHeadUpdateSender {
    pool: ConnectionPool,
    chain_name: String,
}

impl ChainHeadUpdateListener {
    pub fn new(logger: &Logger, registry: Arc<dyn MetricsRegistry>, postgres_url: String) -> Self {
        let logger = logger.new(o!("component" => "ChainHeadUpdateListener"));
        let ingestor_metrics = Arc::new(BlockIngestorMetrics::new(registry.clone()));

        // Create a Postgres notification listener for chain head updates
        let (mut listener, receiver) =
            NotificationListener::new(&logger, postgres_url, CHANNEL_NAME.clone());
        let watchers = Arc::new(RwLock::new(BTreeMap::new()));

        Self::listen(
            logger,
            ingestor_metrics,
            &mut listener,
            receiver,
            watchers.cheap_clone(),
        );

        ChainHeadUpdateListener {
            watchers,

            // We keep the listener around to tie its stream's lifetime to
            // that of the chain head update listener and prevent it from
            // terminating early
            _listener: listener,
        }
    }

    fn listen(
        logger: Logger,
        metrics: Arc<BlockIngestorMetrics>,
        listener: &mut NotificationListener,
        mut receiver: Receiver<JsonNotification>,
        watchers: Arc<RwLock<BTreeMap<String, Watcher>>>,
    ) {
        // Process chain head updates in a dedicated task
        graph::spawn(async move {
            while let Some(notification) = receiver.recv().await {
                // Create ChainHeadUpdate from JSON
                let update: ChainHeadUpdate =
                    match serde_json::from_value(notification.payload.clone()) {
                        Ok(update) => update,
                        Err(e) => {
                            crit!(
                                logger,
                                "invalid chain head update received from database";
                                "payload" => format!("{:?}", notification.payload),
                                "error" => e.to_string()
                            );
                            continue;
                        }
                    };

                // Observe the latest chain head for each network to monitor block ingestion
                metrics
                    .set_chain_head_number(&update.network_name, *&update.head_block_number as i64);

                // If there are subscriptions for this network, notify them.
                if let Some(watcher) = watchers.read().await.get(&update.network_name) {
                    watcher.send()
                }
            }
        });

        // We're ready, start listening to chain head updates
        listener.start();
    }

    pub async fn subscribe(&self, network_name: String) -> ChainHeadUpdateStream {
        let update_receiver = {
            let existing = {
                let watchers = self.watchers.read().await;
                watchers.get(&network_name).map(|w| w.receiver.clone())
            };

            if let Some(watcher) = existing {
                watcher
            } else {
                // This is the first subscription for this network, a write lock is required.
                let watcher = Watcher::new();
                let receiver = watcher.receiver.clone();
                self.watchers.write().await.insert(network_name, watcher);
                receiver
            }
        };

        Box::new(
            WatchStream::new(update_receiver)
                .map(Result::<_, ()>::Ok)
                .boxed()
                .compat(),
        )
    }
}

impl ChainHeadUpdateSender {
    pub fn new(pool: ConnectionPool, network_name: String) -> Self {
        Self {
            pool,
            chain_name: network_name,
        }
    }

    pub fn send(&self, hash: &str, number: i64) -> Result<(), StoreError> {
        use crate::functions::pg_notify;

        let msg = json! ({
            "network_name": &self.chain_name,
            "head_block_hash": hash,
            "head_block_number": number
        });

        let conn = self.pool.get()?;
        diesel::select(pg_notify(CHANNEL_NAME.as_str(), &msg.to_string()))
            .execute(&conn)
            .map_err(StoreError::from)
            .map(|_| ())
    }
}

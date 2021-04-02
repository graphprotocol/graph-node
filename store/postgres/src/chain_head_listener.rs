use diesel::RunQueryDsl;
use lazy_static::lazy_static;
use tokio::sync::watch;
use web3::types::H256;

use crate::{
    connection_pool::ConnectionPool,
    notification_listener::{NotificationListener, SafeChannelName},
};
use graph::prelude::serde_json::{self, json};
use graph::prelude::{ChainHeadUpdateListener as ChainHeadUpdateListenerTrait, *};
use graph::tokio_stream::wrappers::WatchStream;
use graph_chain_ethereum::BlockIngestorMetrics;

lazy_static! {
    pub static ref CHANNEL_NAME: SafeChannelName =
        SafeChannelName::i_promise_this_is_safe("chain_head_updates");
}

pub struct ChainHeadUpdateListener {
    /// A receiver that gets all chain head updates for all networks. We
    /// filter notifications to the desired network in `subscribe`. Using
    /// a `watch::Receiver` here has the downside that the notification for
    /// one network can make the notification for another network disappear
    /// if events aren't processed fast enough. If that happens, the update
    /// for the preempted network will happen on the next block. Since even
    /// the fastest network generates new blocks a few seconds apart, the
    /// risk for collisions, and in particular sustained collisions is
    /// very low
    update_receiver: watch::Receiver<ChainHeadUpdate>,
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
        let mut listener = NotificationListener::new(&logger, postgres_url, CHANNEL_NAME.clone());

        let none_update = ChainHeadUpdate {
            network_name: "none".to_owned(),
            head_block_hash: H256::zero(),
            head_block_number: 0,
        };
        let (update_sender, update_receiver) = watch::channel(none_update);
        Self::listen(ingestor_metrics, &mut listener, update_sender);

        ChainHeadUpdateListener {
            update_receiver,

            // We keep the listener around to tie its stream's lifetime to
            // that of the chain head update listener and prevent it from
            // terminating early
            _listener: listener,
        }
    }

    fn listen(
        metrics: Arc<BlockIngestorMetrics>,
        listener: &mut NotificationListener,
        update_sender: watch::Sender<ChainHeadUpdate>,
    ) {
        // Process chain head updates in a dedicated task
        graph::spawn(
            listener
                .take_event_stream()
                .unwrap()
                .compat()
                .try_filter_map(move |notification| {
                    // Create ChainHeadUpdate from JSON
                    let update: ChainHeadUpdate =
                        serde_json::from_value(notification.payload.clone()).unwrap_or_else(|_| {
                            panic!(
                                "invalid chain head update received from database: {:?}",
                                notification.payload
                            )
                        });

                    // Observe the latest chain_head_number for each network in order to monitor
                    // block ingestion
                    metrics.set_chain_head_number(
                        &update.network_name,
                        *&update.head_block_number as i64,
                    );
                    futures03::future::ok(Some(update))
                })
                .try_for_each(move |update| {
                    futures03::future::ready(update_sender.send(update).map_err(|_| ()))
                }),
        );

        // We're ready, start listening to chain head updates
        listener.start();
    }
}

impl ChainHeadUpdateListenerTrait for ChainHeadUpdateListener {
    fn subscribe(&self, network_name: String) -> ChainHeadUpdateStream {
        let f = move |update: ChainHeadUpdate| {
            if update.network_name == network_name {
                futures03::future::ready(Some(()))
            } else {
                futures03::future::ready(None)
            }
        };
        Box::new(
            WatchStream::new(self.update_receiver.clone())
                .filter_map(f)
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

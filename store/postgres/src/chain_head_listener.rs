use tokio::sync::watch;
use web3::types::H256;

use crate::notification_listener::{NotificationListener, SafeChannelName};
use graph::prelude::serde_json;
use graph::prelude::{ChainHeadUpdateListener as ChainHeadUpdateListenerTrait, *};
use graph_chain_ethereum::BlockIngestorMetrics;

pub struct ChainHeadUpdateListener {
    update_receiver: watch::Receiver<ChainHeadUpdate>,
    _listener: NotificationListener,
}

impl ChainHeadUpdateListener {
    pub fn new(logger: &Logger, registry: Arc<dyn MetricsRegistry>, postgres_url: String) -> Self {
        let logger = logger.new(o!("component" => "ChainHeadUpdateListener"));
        let ingestor_metrics = Arc::new(BlockIngestorMetrics::new(registry.clone()));

        // Create a Postgres notification listener for chain head updates
        let mut listener = NotificationListener::new(
            &logger,
            postgres_url,
            SafeChannelName::i_promise_this_is_safe("chain_head_updates"),
        );

        let none_update = ChainHeadUpdate {
            network_name: "none".to_owned(),
            head_block_hash: H256::zero(),
            head_block_number: 0,
        };
        let (update_sender, update_receiver) = watch::channel(none_update);
        Self::listen(logger, ingestor_metrics, &mut listener, update_sender);

        ChainHeadUpdateListener {
            update_receiver,

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
        update_sender: watch::Sender<ChainHeadUpdate>,
    ) {
        let logger = logger.clone();

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
                    debug!(
                        logger.clone(),
                        "Received chain head update";
                        "network" => &update.network_name,
                        "head_block_hash" => format!("{}", update.head_block_hash),
                        "head_block_number" => &update.head_block_number,
                    );

                    futures03::future::ready(update_sender.broadcast(update).map_err(|_| ()))
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
            self.update_receiver
                .clone()
                .filter_map(f)
                .map(Result::<_, ()>::Ok)
                .boxed()
                .compat(),
        )
    }
}

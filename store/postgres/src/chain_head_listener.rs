use tokio::sync::watch;

use crate::notification_listener::{NotificationListener, SafeChannelName};
use graph::prelude::serde_json;
use graph::prelude::{ChainHeadUpdateListener as ChainHeadUpdateListenerTrait, *};
use graph_chain_ethereum::BlockIngestorMetrics;

pub struct ChainHeadUpdateListener {
    update_receiver: watch::Receiver<()>,
    _listener: NotificationListener,
}

impl ChainHeadUpdateListener {
    pub fn new(
        logger: &Logger,
        ingestor_metrics: Arc<BlockIngestorMetrics>,
        postgres_url: String,
        network_name: String,
    ) -> Self {
        let logger = logger.new(o!("component" => "ChainHeadUpdateListener"));

        // Create a Postgres notification listener for chain head updates
        let mut listener = NotificationListener::new(
            &logger,
            postgres_url,
            SafeChannelName::i_promise_this_is_safe("chain_head_updates"),
        );

        let (update_sender, update_receiver) = watch::channel(());
        Self::listen(
            logger,
            ingestor_metrics,
            &mut listener,
            network_name,
            update_sender,
        );

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
        network_name: String,
        update_sender: watch::Sender<()>,
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

                    // Only include update if it is for the network we're interested in
                    futures03::future::ok(if update.network_name == network_name {
                        Some(update)
                    } else {
                        None
                    })
                })
                .try_for_each(move |update| {
                    debug!(
                        logger.clone(),
                        "Received chain head update";
                        "network" => &update.network_name,
                        "head_block_hash" => format!("{}", update.head_block_hash),
                        "head_block_number" => &update.head_block_number,
                    );

                    futures03::future::ready(
                        update_sender
                            .broadcast(())
                            .map_err(|_| panic!("no listeners for chain head updates")),
                    )
                }),
        );

        // We're ready, start listening to chain head updates
        listener.start();
    }
}

impl ChainHeadUpdateListenerTrait for ChainHeadUpdateListener {
    fn subscribe(&self) -> ChainHeadUpdateStream {
        Box::new(self.update_receiver.clone().map(Ok).compat())
    }
}

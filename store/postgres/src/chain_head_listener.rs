use crate::notification_listener::{NotificationListener, SafeChannelName};
use graph::prelude::{ChainHeadUpdateListener as ChainHeadUpdateListenerTrait, *};
use graph::serde_json;

pub struct ChainHeadUpdateListener {
    notification_listener: NotificationListener,
    network_name: String,
}

impl ChainHeadUpdateListener {
    pub fn new(logger: &Logger, postgres_url: String, network_name: String) -> Self {
        ChainHeadUpdateListener {
            notification_listener: NotificationListener::new(
                logger,
                postgres_url,
                SafeChannelName::i_promise_this_is_safe("chain_head_updates"),
            ),
            network_name,
        }
    }
}

impl ChainHeadUpdateListenerTrait for ChainHeadUpdateListener {
    fn start(&mut self) {
        self.notification_listener.start()
    }
}

impl EventProducer<ChainHeadUpdate> for ChainHeadUpdateListener {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = ChainHeadUpdate, Error = ()> + Send>> {
        let network_name = self.network_name.clone();

        self.notification_listener.take_event_stream().map(
            move |stream| -> Box<Stream<Item = _, Error = _> + Send> {
                Box::new(stream.filter_map(move |notification| {
                    // Create ChainHeadUpdate from JSON
                    let update: ChainHeadUpdate =
                        serde_json::from_value(notification.payload.clone()).unwrap_or_else(|_| {
                            panic!(
                                "invalid chain head update received from database: {:?}",
                                notification.payload
                            )
                        });

                    // Only include update if about the right network
                    if update.network_name == network_name {
                        Some(update)
                    } else {
                        None
                    }
                }))
            },
        )
    }
}

use crate::notification_listener::{NotificationListener, SafeChannelName};
use graph::prelude::serde_json;
use graph::prelude::*;

pub struct StoreEventListener {
    notification_listener: NotificationListener,
}

impl StoreEventListener {
    pub fn new(logger: &Logger, postgres_url: String) -> Self {
        StoreEventListener {
            notification_listener: NotificationListener::new(
                logger,
                postgres_url,
                SafeChannelName::i_promise_this_is_safe("store_events"),
            ),
        }
    }

    pub fn start(&mut self) {
        self.notification_listener.start()
    }
}

impl EventProducer<StoreEvent> for StoreEventListener {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = StoreEvent, Error = ()> + Send>> {
        self.notification_listener.take_event_stream().map(
            |stream| -> Box<dyn Stream<Item = _, Error = _> + Send> {
                Box::new(stream.map(|notification| {
                    // Create StoreEvent from JSON
                    let change: StoreEvent = serde_json::from_value(notification.payload.clone())
                        .unwrap_or_else(|_| {
                            panic!(
                                "invalid store event received from database: {:?}",
                                notification.payload
                            )
                        });

                    change
                }))
            },
        )
    }
}

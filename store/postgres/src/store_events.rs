use graph::prelude::*;
use graph::serde_json;
use notification_listener::{NotificationListener, SafeChannelName};

pub struct StoreEventListener {
    notification_listener: NotificationListener,
}

impl StoreEventListener {
    pub fn new(postgres_url: String) -> Self {
        StoreEventListener {
            notification_listener: NotificationListener::new(
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
    fn take_event_stream(&mut self) -> Option<StoreEventStreamBox> {
        self.notification_listener.take_event_stream().map(
            |stream| -> Box<Stream<Item = _, Error = _> + Send> {
                Box::new(stream.map(|notification| {
                    // Parse notification as JSON
                    let value: serde_json::Value = serde_json::from_str(&notification.payload)
                        .expect(&format!(
                            "invalid JSON store event received from database: '{}'",
                            &notification.payload
                        ));

                    // Create StoreEvent from JSON
                    let change: StoreEvent =
                        serde_json::from_value(value.clone()).unwrap_or_else(|_| {
                            panic!("invalid store event received from database: {:?}", value)
                        });

                    change
                }))
            },
        )
    }
}

use graph::prelude::*;
use graph::serde_json;
use notification_listener::{NotificationListener, SafeChannelName};

pub struct EntityChangeListener {
    notification_listener: NotificationListener,
}

impl EntityChangeListener {
    pub fn new(postgres_url: String) -> Self {
        EntityChangeListener {
            notification_listener: NotificationListener::new(
                postgres_url,
                SafeChannelName::i_promise_this_is_safe("entity_changes"),
            ),
        }
    }

    pub fn start(&mut self) {
        self.notification_listener.start()
    }
}

impl EventProducer<EntityChange> for EntityChangeListener {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = EntityChange, Error = ()> + Send>> {
        self.notification_listener.take_event_stream().map(
            |stream| -> Box<Stream<Item = _, Error = _> + Send> {
                Box::new(stream.map(|notification| {
                    // Parse notification as JSON
                    let value: serde_json::Value = serde_json::from_str(&notification.payload)
                        .expect("invalid JSON entity change received from database");

                    // Create EntityChange from JSON
                    let change: EntityChange = serde_json::from_value(value.clone())
                        .unwrap_or_else(|_| {
                            panic!("invalid entity change received from database: {:?}", value)
                        });

                    change
                }))
            },
        )
    }
}

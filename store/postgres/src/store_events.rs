use graph::prelude::*;
use graph::serde_json;
use notification_listener::{NotificationListener, SafeChannelName};
use store::StoreEvent;

pub struct StoreEventListener {
    notification_listener: NotificationListener,
}

impl StoreEventListener {
    pub fn new(postgres_url: String) -> Self {
        StoreEventListener {
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

impl EventProducer<StoreEvent> for StoreEventListener {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = StoreEvent, Error = ()> + Send>> {
        self.notification_listener.take_event_stream().map(
            |stream| -> Box<Stream<Item = _, Error = _> + Send> {
                Box::new(stream.map(|notification| {
                    // Parse notification as JSON
                    let value: serde_json::Value = serde_json::from_str(&notification.payload)
                        .expect("invalid JSON entity change received from database");
                    // The JSON has a 'block' field for a BlockTick. If it doesn't,
                    // it must be an EntityChange.
                    // FIXME: We should really send an entire StoreEvent rather
                    // than its two components, but that requires changing the
                    // triggers on the entities table.
                    match value.get("block") {
                        Some(_) => {
                            let block: BlockTick = serde_json::from_value(value.clone())
                                .unwrap_or_else(|_| {
                                    panic!(
                                        "invalid block change received from database: {:?}",
                                        value
                                    )
                                });
                            StoreEvent::Tick(block)
                        }
                        None => {
                            let change: EntityChange = serde_json::from_value(value.clone())
                                .unwrap_or_else(|_| {
                                    panic!(
                                        "invalid entity change received from database: {:?}",
                                        value
                                    )
                                });

                            StoreEvent::Change(change)
                        }
                    }
                }))
            },
        )
    }
}

use futures::sync::mpsc::{channel, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use uuid::Uuid;

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

/// Manage subscriptions to the `StoreEvent` stream. Keep a list of
/// currently active subscribers and forward new events to each of them
pub struct SubscriptionManager {
    subscriptions: Arc<RwLock<HashMap<String, Sender<Arc<StoreEvent>>>>>,

    /// listen to StoreEvents generated when applying entity operations
    listener: Mutex<StoreEventListener>,
}

impl SubscriptionManager {
    pub fn new(logger: Logger, postgres_url: String) -> Self {
        let mut listener = StoreEventListener::new(&logger, postgres_url);
        let store_events = listener
            .take_event_stream()
            .expect("Failed to listen to entity change events in Postgres");

        let manager = SubscriptionManager {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            listener: Mutex::new(listener),
        };

        // Deal with store subscriptions
        manager.handle_store_events(store_events);
        manager.periodically_clean_up_stale_subscriptions();

        let mut listener = manager.listener.lock().unwrap();
        listener.start();
        drop(listener);

        manager
    }

    /// Receive store events from Postgres and send them to all active
    /// subscriptions. Detect stale subscriptions in the process and
    /// close them.
    fn handle_store_events(
        &self,
        store_events: Box<dyn Stream<Item = StoreEvent, Error = ()> + Send>,
    ) {
        let subscriptions = self.subscriptions.clone();

        // This channel is constantly receiving things and there are locks involved,
        // so it's best to use a blocking task.
        graph::spawn_blocking(
            store_events
                .for_each(move |event| {
                    let senders = subscriptions.read().unwrap().clone();
                    let subscriptions = subscriptions.clone();
                    let event = Arc::new(event);

                    // Write change to all matching subscription streams; remove subscriptions
                    // whose receiving end has been dropped
                    stream::iter_ok::<_, ()>(senders).for_each(move |(id, sender)| {
                        let subscriptions = subscriptions.clone();

                        sender.send(event.cheap_clone()).then(move |result| {
                            match result {
                                Err(_send_error) => {
                                    // Receiver was dropped
                                    subscriptions.write().unwrap().remove(&id);
                                    Ok(())
                                }
                                Ok(_sender) => Ok(()),
                            }
                        })
                    })
                })
                .compat(),
        );
    }

    fn periodically_clean_up_stale_subscriptions(&self) {
        use futures03::stream::StreamExt;

        let subscriptions = self.subscriptions.clone();

        // Clean up stale subscriptions every 5s
        graph::spawn(
            tokio::time::interval(Duration::from_secs(5)).for_each(move |_| {
                let mut subscriptions = subscriptions.write().unwrap();

                // Obtain IDs of subscriptions whose receiving end has gone
                let stale_ids = subscriptions
                    .iter_mut()
                    .filter_map(|(id, sender)| match sender.poll_ready() {
                        Err(_) => Some(id.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                // Remove all stale subscriptions
                for id in stale_ids {
                    subscriptions.remove(&id);
                }

                futures03::future::ready(())
            }),
        );
    }

    pub fn subscribe(&self, entities: Vec<SubscriptionFilter>) -> StoreEventStreamBox {
        let id = Uuid::new_v4().to_string();

        // Prepare the new subscription by creating a channel and a subscription object
        let (sender, receiver) = channel(100);

        // Add the new subscription
        self.subscriptions.write().unwrap().insert(id, sender);

        // Return the subscription ID and entity change stream
        StoreEventStream::new(Box::new(receiver)).filter_by_entities(entities)
    }
}

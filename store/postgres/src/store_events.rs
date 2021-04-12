use futures::sync::mpsc::{channel, Sender};
use futures03::TryStreamExt;
use std::sync::{atomic::Ordering, Arc, RwLock};
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use uuid::Uuid;

use crate::notification_listener::{NotificationListener, SafeChannelName};
use graph::components::store::SubscriptionManager as SubscriptionManagerTrait;
use graph::prelude::serde_json;
use graph::prelude::*;

pub struct StoreEventListener {
    notification_listener: NotificationListener,
}

impl StoreEventListener {
    pub fn new(
        logger: Logger,
        postgres_url: String,
    ) -> (Self, Box<dyn Stream<Item = StoreEvent, Error = ()> + Send>) {
        let (notification_listener, receiver) = NotificationListener::new(
            &logger,
            postgres_url,
            SafeChannelName::i_promise_this_is_safe("store_events"),
        );

        let event_stream = Box::new(receiver.map(Result::<_, ()>::Ok).compat().filter_map(
            move |notification| {
                // When graph-node is starting up, it is possible that
                // Postgres still has old messages queued up that we
                // can't decode anymore. It is safe to skip them; once
                // We've seen 10 valid messages, we can assume that
                // whatever old messages Postgres had queued have been
                // cleared. Seeing an invalid message after that
                // definitely indicates trouble.
                let num_valid = AtomicUsize::new(0);
                serde_json::from_value(notification.payload.clone()).map_or_else(
                    |_err| {
                        error!(
                            &logger,
                            "invalid store event received from database: {:?}",
                            notification.payload
                        );
                        if num_valid.load(Ordering::SeqCst) > 10 {
                            panic!(
                                "invalid store event received from database: {:?}",
                                notification.payload
                            );
                        }
                        None
                    },
                    |change| {
                        num_valid.fetch_add(1, Ordering::SeqCst);
                        Some(change)
                    },
                )
            },
        ));

        (
            StoreEventListener {
                notification_listener,
            },
            event_stream,
        )
    }

    pub fn start(&mut self) {
        self.notification_listener.start()
    }
}

/// Manage subscriptions to the `StoreEvent` stream. Keep a list of
/// currently active subscribers and forward new events to each of them
pub struct SubscriptionManager {
    subscriptions: Arc<RwLock<HashMap<String, Sender<Arc<StoreEvent>>>>>,

    /// Keep the notification listener alive
    listener: StoreEventListener,
}

impl SubscriptionManager {
    pub fn new(logger: Logger, postgres_url: String) -> Self {
        let (listener, store_events) = StoreEventListener::new(logger, postgres_url);

        let mut manager = SubscriptionManager {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            listener,
        };

        // Deal with store subscriptions
        manager.handle_store_events(store_events);
        manager.periodically_clean_up_stale_subscriptions();

        manager.listener.start();

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
        let subscriptions = self.subscriptions.clone();

        // Clean up stale subscriptions every 5s
        graph::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
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
            }
        });
    }
}

impl SubscriptionManagerTrait for SubscriptionManager {
    fn subscribe(&self, entities: Vec<SubscriptionFilter>) -> StoreEventStreamBox {
        let id = Uuid::new_v4().to_string();

        // Prepare the new subscription by creating a channel and a subscription object
        let (sender, receiver) = channel(100);

        // Add the new subscription
        self.subscriptions.write().unwrap().insert(id, sender);

        // Return the subscription ID and entity change stream
        StoreEventStream::new(Box::new(receiver)).filter_by_entities(entities)
    }
}

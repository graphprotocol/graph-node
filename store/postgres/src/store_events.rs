use graph::futures01::Stream;
use graph::futures03::compat::Stream01CompatExt;
use graph::futures03::stream::StreamExt;
use graph::futures03::TryStreamExt;
use graph::tokio_stream::wrappers::ReceiverStream;
use std::sync::{atomic::Ordering, Arc, RwLock};
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tokio::sync::mpsc::{channel, Sender};

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
        registry: Arc<MetricsRegistry>,
    ) -> (Self, Box<dyn Stream<Item = StoreEvent, Error = ()> + Send>) {
        let channel = SafeChannelName::i_promise_this_is_safe("store_events");
        let (notification_listener, receiver) =
            NotificationListener::new(&logger, postgres_url, channel.clone());

        let counter = registry
            .global_counter_vec(
                "notification_queue_recvd",
                "Number of messages received through Postgres LISTEN",
                vec!["channel", "network"].as_slice(),
            )
            .unwrap()
            .with_label_values(&[channel.as_str(), "none"]);

        let event_stream = Box::new(
            ReceiverStream::new(receiver)
                .map(Result::<_, ()>::Ok)
                .compat()
                .filter_map(move |notification| {
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
                            counter.inc();
                            Some(change)
                        },
                    )
                }),
        );

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
    subscriptions: Arc<RwLock<HashMap<usize, Sender<Arc<StoreEvent>>>>>,

    /// Keep the notification listener alive
    listener: StoreEventListener,

    logger: Logger,
}

impl SubscriptionManager {
    pub fn new(logger: Logger, postgres_url: String, registry: Arc<MetricsRegistry>) -> Self {
        let logger = logger.new(o!("component" => "StoreEventListener"));

        let (listener, store_events) =
            StoreEventListener::new(logger.cheap_clone(), postgres_url, registry);

        let mut manager = SubscriptionManager {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            listener,
            logger,
        };

        // Deal with store subscriptions
        manager.handle_store_events(store_events);
        manager.periodically_clean_up_stale_subscriptions();

        manager.listener.start();

        manager
    }

    async fn broadcast_event(
        logger: &Logger,
        subscriptions: &Arc<RwLock<HashMap<usize, Sender<Arc<StoreEvent>>>>>,
        event: StoreEvent,
    ) {
        let event = Arc::new(event);

        // Send to `subscriptions`.
        {
            let senders = subscriptions.read().unwrap().clone();

            // Write change to all matching subscription streams; remove subscriptions
            // whose receiving end has been dropped
            for (id, sender) in senders {
                if let Err(e) = sender.send(event.cheap_clone()).await {
                    error!(
                        logger,
                        "Failed to send store event to subscriber {}: {}", id, e
                    );
                    // Receiver was dropped
                    subscriptions.write().unwrap().remove(&id);
                }
            }
        }
    }

    /// Receive store events from Postgres and send them to all active
    /// subscriptions. Detect stale subscriptions in the process and
    /// close them.
    fn handle_store_events(
        &self,
        store_events: Box<dyn Stream<Item = StoreEvent, Error = ()> + Send>,
    ) {
        let subscriptions = self.subscriptions.cheap_clone();
        let mut store_events = store_events.compat();
        let logger = self.logger.cheap_clone();

        // This channel is constantly receiving things and there are locks involved,
        // so it's best to use a blocking task.
        graph::spawn_blocking(async move {
            loop {
                match store_events.next().await {
                    Some(Ok(event)) => {
                        Self::broadcast_event(&logger, &subscriptions, event).await;
                    }
                    Some(Err(_)) => {
                        error!(logger, "Error receiving store event");
                    }
                    None => {
                        error!(logger, "Store event stream ended");
                        break;
                    }
                }
            }
        });
    }

    fn periodically_clean_up_stale_subscriptions(&self) {
        let subscriptions = self.subscriptions.cheap_clone();
        let logger = self.logger.cheap_clone();

        // Clean up stale subscriptions every 5s
        graph::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;

                // Cleanup `subscriptions`.
                {
                    let mut subscriptions = subscriptions.write().unwrap();

                    // Obtain IDs of subscriptions whose receiving end has gone
                    let stale_ids = subscriptions
                        .iter_mut()
                        .filter_map(|(id, sender)| match sender.is_closed() {
                            true => Some(id.clone()),
                            false => None,
                        })
                        .collect::<Vec<_>>();

                    // Remove all stale subscriptions
                    for id in stale_ids {
                        warn!(logger, "Removing stale subscription {}", id);
                        subscriptions.remove(&id);
                    }
                }
            }
        });
    }
}

impl SubscriptionManagerTrait for SubscriptionManager {
    fn subscribe(&self) -> StoreEventStreamBox {
        static SUBSCRIPTION_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = SUBSCRIPTION_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Prepare the new subscription by creating a channel and a subscription object
        let (sender, receiver) = channel(100);

        // Add the new subscription
        self.subscriptions.write().unwrap().insert(id, sender);

        // Return the subscription ID and entity change stream
        Box::new(ReceiverStream::new(receiver).map(Ok).compat())
    }
}

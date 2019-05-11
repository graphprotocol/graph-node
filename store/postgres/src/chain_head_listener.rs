use futures::sync::mpsc::{channel, Sender};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use graph::prelude::{ChainHeadUpdateListener as ChainHeadUpdateListenerTrait, *};
use graph::serde_json;

use crate::notification_listener::{NotificationListener, SafeChannelName};

type ChainHeadUpdateSubscribers = Arc<RwLock<HashMap<String, Sender<ChainHeadUpdate>>>>;

pub struct ChainHeadUpdateListener {
    logger: Logger,
    subscribers: ChainHeadUpdateSubscribers,
    _listener: NotificationListener,
}

impl ChainHeadUpdateListener {
    pub fn new(logger: &Logger, postgres_url: String, network_name: String) -> Self {
        let logger = logger.new(o!("component" => "ChainHeadUpdateListener"));
        let subscribers = Arc::new(RwLock::new(HashMap::new()));

        // Create a Postgres notification listener for chain head updates
        let mut listener = NotificationListener::new(
            &logger,
            postgres_url,
            SafeChannelName::i_promise_this_is_safe("chain_head_updates"),
        );

        Self::listen(&logger, &mut listener, network_name, subscribers.clone());

        ChainHeadUpdateListener {
            logger,
            subscribers,

            // We keep the listener around to tie its stream's lifetime to
            // that of the chain head update listener and prevent it from
            // terminating early
            _listener: listener,
        }
    }

    fn listen(
        logger: &Logger,
        listener: &mut NotificationListener,
        network_name: String,
        subscribers: ChainHeadUpdateSubscribers,
    ) {
        let logger = logger.clone();

        // Process chain head updates in a dedicated task
        tokio::spawn(
            listener
                .take_event_stream()
                .unwrap()
                .filter_map(move |notification| {
                    // Create ChainHeadUpdate from JSON
                    let update: ChainHeadUpdate =
                        serde_json::from_value(notification.payload.clone()).unwrap_or_else(|_| {
                            panic!(
                                "invalid chain head update received from database: {:?}",
                                notification.payload
                            )
                        });

                    // Only include update if it is for the network we're interested in
                    if update.network_name == network_name {
                        Some(update)
                    } else {
                        None
                    }
                })
                .for_each(move |update| {
                    let logger = logger.clone();
                    let senders = subscribers.read().unwrap().clone();
                    let subscribers = subscribers.clone();

                    debug!(
                        logger,
                        "Received chain head update";
                        "network" => &update.network_name,
                        "head_block_hash" => format!("{}", update.head_block_hash),
                        "head_block_number" => &update.head_block_number,
                    );

                    // Forward update to all susbcribers
                    stream::iter_ok::<_, ()>(senders).for_each(move |(id, mut sender)| {
                        let logger = logger.clone();
                        let subscribers = subscribers.clone();

                        // A subgraph that's syncing will let chain head updates
                        // pile up in the channel. So we don't wait for room in
                        // the channel and instead skip it, it will have the
                        // opportunity to grab future updates.
                        match sender.try_send(update.clone()) {
                            // Move on to the next subscriber
                            Ok(()) => (),
                            Err(ref e) if e.is_full() => {
                                // Temporary log, feel free to remove if noisy.
                                debug!(logger, "Full chain head update channel"; "id" => &id);
                            }
                            Err(ref e) if e.is_disconnected() => {
                                // Remove disconnected subscribers.
                                debug!(logger, "Unsubscribe"; "id" => &id);
                                subscribers.write().unwrap().remove(&id);
                            }
                            Err(e) => warn!(logger, "Unexpected send error"; "e" => e.to_string()),
                        }
                        Ok(())
                    })
                }),
        );

        // We're ready, start listening to chain head updaates
        listener.start();
    }
}

impl ChainHeadUpdateListenerTrait for ChainHeadUpdateListener {
    fn subscribe(&self) -> ChainHeadUpdateStream {
        // Generate a new (unique) UUID; we're looping just to be sure we avoid collisions
        let mut id = Uuid::new_v4().to_string();
        while self.subscribers.read().unwrap().contains_key(&id) {
            id = Uuid::new_v4().to_string();
        }

        debug!(self.logger, "Subscribe"; "id" => &id);

        // Create a subscriber and return the receiving end
        let (sender, receiver) = channel(100);
        self.subscribers.write().unwrap().insert(id, sender);
        Box::new(receiver)
    }
}

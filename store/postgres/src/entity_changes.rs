use fallible_iterator::FallibleIterator;
use postgres::{Connection, TlsMode};
use slog::Logger;
use std::thread;

use futures::sync::mpsc::{channel, Receiver};
use graph::prelude::*;
use graph::serde_json;

pub struct EntityChangeListener {
    output: Option<Receiver<EntityChange>>,
}

impl EntityChangeListener {
    pub fn new(logger: Logger, url: String) -> Self {
        let logger = logger.new(o!("component" => "EntityChangeListener"));
        let receiver = Self::listen(logger, url);

        EntityChangeListener {
            output: Some(receiver),
        }
    }

    fn listen(logger: Logger, url: String) -> Receiver<EntityChange> {
        let (sender, receiver) = channel(100);

        let error_logger = logger.clone();

        thread::spawn(move || {
            let conn = Connection::connect(url, TlsMode::None)
                .expect("failed to connect entity change listener to Postgres");

            let notifications = conn.notifications();
            let iter = notifications.blocking_iter();

            conn.execute("LISTEN entity_changes", &[])
                .expect("failed to listen to entity changes in Postgres");

            iter.iterator()
                .filter_map(|notification| match notification {
                    Ok(notification) => Some(notification),
                    Err(_) => None,
                })
                .filter(|notification| notification.channel == String::from("entity_changes"))
                .map(|notification| notification.payload)
                .filter_map(|payload: String| -> Option<EntityChange> {
                    serde_json::from_str(payload.as_str())
                        .map_err(|e| {
                            error!(error_logger, "Invalid entity change received from Postgres";
                                   "error" => format!("{}", e).as_str());
                        })
                        .ok()
                })
                .for_each(|change| {
                    sender
                        .clone()
                        .send(change)
                        .wait()
                        .expect("failed to pass entity change event along");
                });
        });

        receiver
    }
}

impl EventProducer<EntityChange> for EntityChangeListener {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = EntityChange, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = EntityChange, Error = ()> + Send>)
    }
}

use fallible_iterator::FallibleIterator;
use postgres::{Connection, TlsMode};
use std::thread;

use futures::sync::mpsc::{channel, Receiver};
use graph::prelude::*;
use graph::serde_json;

pub struct EntityChangeListener {
    output: Option<Receiver<EntityChange>>,
}

impl EntityChangeListener {
    pub fn new(url: String) -> Self {
        let receiver = Self::listen(url);

        EntityChangeListener {
            output: Some(receiver),
        }
    }

    fn listen(url: String) -> Receiver<EntityChange> {
        let (sender, receiver) = channel(100);

        thread::spawn(move || {
            let conn = Connection::connect(url, TlsMode::None)
                .expect("failed to connect entity change listener to Postgres");

            let notifications = conn.notifications();
            let iter = notifications.blocking_iter();

            conn.execute("LISTEN entity_changes", &[])
                .expect("failed to listen to entity changes in Postgres");

            let changes = iter
                .iterator()
                .filter_map(Result::ok)
                .filter(|notification| notification.channel == String::from("entity_changes"))
                .map(|notification| notification.payload)
                .map(|payload: String| -> EntityChange {
                    let value: serde_json::Value = serde_json::from_str(payload.as_str())
                        .expect("Invalid JSON entity change data received from database");

                    serde_json::from_value(value.clone()).expect(
                        format!(
                            "Invalid entity change received from the database: {:?}",
                            value
                        ).as_str(),
                    )
                });

            // Read notifications as long as the Postgres connection is alive;
            // which can be "forever"
            for change in changes {
                // We'll assume here that if sending fails, this means that the
                // entity change listener has already been dropped, the receiving
                // is gone and we should terminate the listener loop
                if sender.clone().send(change).wait().is_err() {
                    break;
                }
            }
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

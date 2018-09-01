use fallible_iterator::FallibleIterator;
use postgres::{Connection, TlsMode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use futures::sync::mpsc::{channel, Receiver};
use graph::prelude::*;
use graph::serde_json;

pub struct EntityChangeListener {
    output: Option<Receiver<EntityChange>>,
    worker_handle: Option<thread::JoinHandle<()>>,
    terminate_worker: Arc<AtomicBool>,
    worker_barrier: Arc<Barrier>,
    started: bool,
}

impl EntityChangeListener {
    pub fn new(url: String, logger: &slog::Logger) -> Self {
        let logger = logger.new(o!("component" => "EntityChangeListener"));

        // Listen to Postgres notifications in a worker thread
        let (receiver, worker_handle, terminate_worker, worker_barrier) = Self::listen(url, logger);

        EntityChangeListener {
            output: Some(receiver),
            worker_handle: Some(worker_handle),
            terminate_worker,
            worker_barrier,
            started: false,
        }
    }

    /// Begins processing notifications coming in from Postgres.
    pub fn start(&mut self) {
        if !self.started {
            self.worker_barrier.wait();
            self.started = true;
        }
    }

    fn listen(
        url: String,
        logger: slog::Logger,
    ) -> (
        Receiver<EntityChange>,
        thread::JoinHandle<()>,
        Arc<AtomicBool>,
        Arc<Barrier>,
    ) {
        // Create two ends of a boolean variable for signalling when the worker
        // thread should be terminated
        let terminate = Arc::new(AtomicBool::new(false));
        let terminate_worker = terminate.clone();
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();

        // Create a channel for entity changes
        let (sender, receiver) = channel(100);

        let worker_handle = thread::spawn(move || {
            // Connect to Postgres
            let conn = Connection::connect(url, TlsMode::None)
                .expect("failed to connect entity change listener to Postgres");

            // Subscribe to the "entity_changes" notification channel in Postgres
            conn.execute("LISTEN entity_changes", &[])
                .expect("failed to listen to entity changes in Postgres");

            // Wait until the listener has been started
            barrier.wait();

            // Read notifications until the thread is to be terminated
            while !terminate.load(Ordering::SeqCst) {
                // Obtain a notifications iterator from Postgres
                let notifications = conn.notifications();

                // Read notifications until there hasn't been one for 500ms
                for notification in notifications
                    .timeout_iter(Duration::from_millis(500))
                    .iterator()
                    .filter_map(Result::ok)
                    .filter(|notification| notification.channel == "entity_changes")
                {
                    // Terminate the thread if desired
                    if terminate.load(Ordering::SeqCst) {
                        return;
                    }

                    // Parse payload into an entity change
                    let value: serde_json::Value =
                        serde_json::from_str(notification.payload.as_str())
                            .expect("Invalid JSON entity change data received from database");
                    let change: EntityChange = serde_json::from_value(value.clone()).expect(
                        format!(
                            "Invalid entity change received from the database: {:?}",
                            value
                        ).as_str(),
                    );

                    // We'll assume here that if sending fails, this means that the
                    // entity change listener has already been dropped, the receiving
                    // is gone and we should terminate the listener loop
                    if sender.clone().send(change).wait().is_err() {
                        break;
                    }
                }
            }
        });

        (receiver, worker_handle, terminate_worker, worker_barrier)
    }
}

impl Drop for EntityChangeListener {
    fn drop(&mut self) {
        // When dropping the change listener, also make sure we signal termination
        // to the worker and wait for it to shut down
        self.terminate_worker.store(true, Ordering::SeqCst);
        self.worker_handle
            .take()
            .unwrap()
            .join()
            .expect("failed to terminate EntityChangeListener thread");
    }
}

impl EventProducer<EntityChange> for EntityChangeListener {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = EntityChange, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = EntityChange, Error = ()> + Send>)
    }
}

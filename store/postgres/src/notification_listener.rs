use fallible_iterator::FallibleIterator;
use postgres::notification::Notification;
use postgres::{Connection, TlsMode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use futures::sync::mpsc::{channel, Receiver};
use graph::prelude::*;

/// This newtype exists to make it hard to misuse the `NotificationListener` API in a way that
/// could impact security.
pub struct SafeChannelName(String);

impl SafeChannelName {
    /// Channel names must be valid Postgres SQL identifiers.
    /// If a channel name is provided that is not a valid identifier,
    /// then there is a security risk due to SQL injection.
    /// Unfortunately, it is difficult to properly validate a channel name.
    /// (A blacklist would have to include SQL keywords, for example)
    ///
    /// The caller of this method is promising that the supplied channel name
    /// is a valid Postgres identifier and cannot be supplied (even partially)
    /// by an attacker.
    pub fn i_promise_this_is_safe(channel_name: impl Into<String>) -> Self {
        SafeChannelName(channel_name.into())
    }
}

pub struct NotificationListener {
    output: Option<Receiver<Notification>>,
    worker_handle: Option<thread::JoinHandle<()>>,
    terminate_worker: Arc<AtomicBool>,
    worker_barrier: Arc<Barrier>,
    started: bool,
}

impl NotificationListener {
    /// Connect to the specified database and listen for Postgres notifications on the specified
    /// channel.
    ///
    /// Must call `.start()` to begin receiving notifications.
    pub fn new(postgres_url: String, channel_name: SafeChannelName) -> Self {
        // Listen to Postgres notifications in a worker thread
        let (receiver, worker_handle, terminate_worker, worker_barrier) =
            Self::listen(postgres_url, channel_name);

        NotificationListener {
            output: Some(receiver),
            worker_handle: Some(worker_handle),
            terminate_worker,
            worker_barrier,
            started: false,
        }
    }

    /// Start accepting notifications.
    /// Must be called for any notifications to be received.
    pub fn start(&mut self) {
        if !self.started {
            self.worker_barrier.wait();
            self.started = true;
        }
    }

    fn listen(
        postgres_url: String,
        channel_name: SafeChannelName,
    ) -> (
        Receiver<Notification>,
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

        // Create a channel for notifications
        let (sender, receiver) = channel(1024);

        let worker_handle = thread::spawn(move || {
            // Connect to Postgres
            let conn = Connection::connect(postgres_url, TlsMode::None)
                .expect("failed to connect notification listener to Postgres");

            // Subscribe to the notification channel in Postgres
            conn.execute(&format!("LISTEN {}", channel_name.0), &[])
                .expect("failed to listen to Postgres notifications");

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
                    .filter(|notification| notification.channel == channel_name.0)
                {
                    // Terminate the thread if desired
                    if terminate.load(Ordering::SeqCst) {
                        break;
                    }

                    // We'll assume here that if sending fails, this means that the
                    // listener has already been dropped, the receiving
                    // end is gone and we should terminate the listener loop
                    if sender.clone().send(notification).wait().is_err() {
                        break;
                    }
                }
            }
        });

        (receiver, worker_handle, terminate_worker, worker_barrier)
    }
}

impl Drop for NotificationListener {
    fn drop(&mut self) {
        // When dropping the listener, also make sure we signal termination
        // to the worker and wait for it to shut down
        self.terminate_worker.store(true, Ordering::SeqCst);
        self.worker_handle
            .take()
            .unwrap()
            .join()
            .expect("failed to terminate NotificationListener thread");
    }
}

impl EventProducer<Notification> for NotificationListener {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = Notification, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = Notification, Error = ()> + Send>)
    }
}

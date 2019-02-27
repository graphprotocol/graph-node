use diesel::pg::PgConnection;
use diesel::select;
use fallible_iterator::FallibleIterator;
use functions::pg_notify;
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

                    let sent = if notification.payload.starts_with(LARGE_NOTIFICATION_MARKER) {
                        // FIXME: These error messages need to contain more detail
                        // of what we were given, like the rest and the pid
                        let (_, rest) = notification.payload.split_at(1);
                        let pid: i32 = rest.parse().expect("Invalid id for large_notification");
                        let rows = conn
                            .query(
                                "select payload from large_notifications where id = $1",
                                &[&pid],
                            )
                            .expect("Failed to load large_notification");
                        let notification = Notification {
                            process_id: notification.process_id,
                            payload: rows.get(0).get(0),
                            channel: notification.channel,
                        };
                        sender.clone().send(notification)
                    } else {
                        sender.clone().send(notification)
                    };
                    // We'll assume here that if sending fails, this means that the
                    // listener has already been dropped, the receiving
                    // end is gone and we should terminate the listener loop
                    if sent.wait().is_err() {
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

// A utility to send notifications larger than 8000 bytes through Postgres
// by putting notifications that are bigger than that into the
// large_notifications table.
//
// We do not need to map the structure of the large_notifications table
pub struct LargeNotification {}

// Any payload bigger than this is considered large.
static LARGE_NOTIFICATION_THRESHOLD: usize = 7800;

// When we need to send a large payload, we stick it into the
// large_notifications table and send a message containing the id of the payload
// prefixed with '<'. We choose this marker as it is likely to freak out any JSON
// parser that unwittingly runs across this payload before it got unpacked
// by the NotificationListener
static LARGE_NOTIFICATION_MARKER: &str = "<";

impl LargeNotification {
    pub fn send_json(channel: &str, msg: &str, conn: &PgConnection) -> Result<(), StoreError> {
        use db_schema::large_notifications::dsl::*;
        use diesel::ExpressionMethods;
        use diesel::RunQueryDsl;

        if msg.len() <= LARGE_NOTIFICATION_THRESHOLD {
            select(pg_notify(channel, msg)).execute(conn)?;
        } else {
            let row: i32 = diesel::insert_into(large_notifications)
                .values(payload.eq(&msg))
                .returning(id)
                .get_result(conn)?;
            let msg = format!("{}{}", LARGE_NOTIFICATION_MARKER, row);
            select(pg_notify(channel, msg)).execute(conn)?;
            // Delete any notifications older than 5 minutes
            diesel::sql_query(
                "delete from large_notifications
                where created_at < current_timestamp() - interval '300s'",
            )
            .execute(conn)?;
        }
        Ok(())
    }
}

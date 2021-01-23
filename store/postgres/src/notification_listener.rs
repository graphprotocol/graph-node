use crate::functions::pg_notify;
use diesel::pg::PgConnection;
use diesel::select;
use fallible_iterator::FallibleIterator;
use lazy_static::lazy_static;
use postgres::notification::Notification;
use postgres::{Connection, TlsMode};
use std::env;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use futures::sync::mpsc::{channel, Receiver};
use graph::prelude::serde_json;
use graph::prelude::*;

lazy_static! {
    static ref LARGE_NOTIFICATION_CLEANUP_INTERVAL: Duration =
        env::var("LARGE_NOTIFICATION_CLEANUP_INTERVAL")
            .ok()
            .map(
                |s| Duration::from_secs(u64::from_str(&s).unwrap_or_else(|_| panic!(
                    "failed to parse env var LARGE_NOTIFICATION_CLEANUP_INTERVAL"
                )))
            )
            .unwrap_or(Duration::from_secs(300));
}

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
    output: Option<Receiver<JsonNotification>>,
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
    pub fn new(logger: &Logger, postgres_url: String, channel_name: SafeChannelName) -> Self {
        // Listen to Postgres notifications in a worker thread
        let (receiver, worker_handle, terminate_worker, worker_barrier) =
            Self::listen(logger, postgres_url, channel_name);

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
        logger: &Logger,
        postgres_url: String,
        channel_name: SafeChannelName,
    ) -> (
        Receiver<JsonNotification>,
        thread::JoinHandle<()>,
        Arc<AtomicBool>,
        Arc<Barrier>,
    ) {
        let logger = logger.new(o!(
            "component" => "NotificationListener",
            "channel" => channel_name.0.clone()
        ));

        debug!(
            logger,
            "Cleaning up large notifications after about {}s",
            LARGE_NOTIFICATION_CLEANUP_INTERVAL.as_secs()
        );

        // Create two ends of a boolean variable for signalling when the worker
        // thread should be terminated
        let terminate = Arc::new(AtomicBool::new(false));
        let terminate_worker = terminate.clone();
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();

        // Create a channel for notifications
        let (sender, receiver) = channel(100);

        let worker_handle = thread::spawn(move || {
            // We exit the process on panic so unwind safety is irrelevant.
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
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
                        .filter_map(|item| match item {
                            Ok(msg) => Some(msg),
                            Err(e) => {
                                let msg = format!("{}", e);
                                crit!(logger, "Error receiving message"; "error" => &msg);
                                eprintln!(
                                    "Connection to Postgres lost while listening for events. \
                                 Aborting to avoid inconsistent state. ({})",
                                    msg
                                );
                                std::process::abort();
                            }
                        })
                        .filter(|notification| notification.channel == channel_name.0)
                    {
                        // Terminate the thread if desired
                        if terminate.load(Ordering::SeqCst) {
                            break;
                        }

                        match JsonNotification::parse(&notification, &conn) {
                            Ok(json_notification) => {
                                // We'll assume here that if sending fails, this means that the
                                // listener has already been dropped, the receiving
                                // end is gone and we should terminate the listener loop
                                if sender.clone().send(json_notification).wait().is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                crit!(
                                    logger,
                                    "Failed to parse database notification";
                                    "notification" => format!("{:?}", notification),
                                    "error" => format!("{}", e),
                                );
                                continue;
                            }
                        }
                    }
                }
            }))
            .unwrap_or_else(|_| std::process::exit(1))
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

impl EventProducer<JsonNotification> for NotificationListener {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = JsonNotification, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<dyn Stream<Item = JsonNotification, Error = ()> + Send>)
    }
}

mod public {
    table! {
        large_notifications(id) {
            id -> Integer,
            payload -> Text,
            created_at -> Timestamp,
        }
    }
}

// A utility to send JSON notifications that may be larger than the
// 8000 bytes limit for Postgres NOTIFY payloads. Large notifications
// are written to the `large_notifications` table and their ID is sent
// via NOTIFY in place of the actual payload. Consumers of large
// notifications are then responsible to fetch the actual payload from
// the `large_notifications` table.
#[derive(Debug)]
pub struct JsonNotification {
    pub process_id: i32,
    pub channel: String,
    pub payload: serde_json::Value,
}

// Any payload bigger than this is considered large. Any notification larger
// than this will be put into the `large_notifications` table, and only
// its id in the table will be sent via `notify`
static LARGE_NOTIFICATION_THRESHOLD: usize = 7800;

impl JsonNotification {
    pub fn parse(
        notification: &Notification,
        conn: &Connection,
    ) -> Result<JsonNotification, StoreError> {
        let value = serde_json::from_str(&notification.payload)?;

        match value {
            serde_json::Value::Number(n) => {
                let payload_id: i64 = n.as_i64().ok_or_else(|| {
                    anyhow!("Invalid notification ID, not compatible with i64: {}", n)
                })?;

                if payload_id < (i32::min_value() as i64) || payload_id > (i32::max_value() as i64)
                {
                    Err(anyhow!(
                        "Invalid notification ID, value exceeds i32: {}",
                        payload_id
                    ))?;
                }

                let payload_rows = conn
                    .query(
                        "SELECT payload FROM large_notifications WHERE id = $1",
                        &[&(payload_id as i32)],
                    )
                    .map_err(|e| {
                        anyhow!(
                            "Error retrieving payload for notification {}: {}",
                            payload_id,
                            e
                        )
                    })?;

                if payload_rows.is_empty() || payload_rows.get(0).is_empty() {
                    return Err(anyhow!("No payload found for notification {}", payload_id))?;
                }
                let payload: String = payload_rows.get(0).get(0);

                Ok(JsonNotification {
                    process_id: notification.process_id,
                    channel: notification.channel.clone(),
                    payload: serde_json::from_str(&payload)?,
                })
            }
            serde_json::Value::Object(_) => Ok(JsonNotification {
                process_id: notification.process_id,
                channel: notification.channel.clone(),
                payload: value,
            }),
            _ => Err(anyhow!("JSON notifications must be numbers or objects"))?,
        }
    }

    pub fn send(
        channel: &str,
        data: &serde_json::Value,
        conn: &PgConnection,
    ) -> Result<(), StoreError> {
        use diesel::ExpressionMethods;
        use diesel::RunQueryDsl;
        use public::large_notifications::dsl::*;

        let msg = data.to_string();

        if msg.len() <= LARGE_NOTIFICATION_THRESHOLD {
            select(pg_notify(channel, &msg)).execute(conn)?;
        } else {
            // Write the notification payload to the large_notifications table
            let payload_id: i32 = diesel::insert_into(large_notifications)
                .values(payload.eq(&msg))
                .returning(id)
                .get_result(conn)?;

            // Use the large_notifications row ID as the payload for NOTIFY
            select(pg_notify(channel, &payload_id.to_string())).execute(conn)?;

            // Prune old large_notifications. We want to keep the size of the
            // table manageable, but there's a lot of latitude in how often
            // we need to clean up before old notifications slow down
            // data access.
            //
            // To avoid checking whether cleanup is needed too often, we
            // only check once every LARGE_NOTIFICATION_CLEANUP_INTERVAL
            // (per graph-node process) It would be even better to do this
            // cleanup only once for all graph-node processes accessing the
            // same database, but that requires a lot more infrastructure
            lazy_static! {
                static ref LAST_CLEANUP_CHECK: Mutex<Instant> = Mutex::new(Instant::now());
            }

            // If we can't get the lock, another thread in this process is
            // already checking, and we can just skip checking
            if let Some(mut last_check) = LAST_CLEANUP_CHECK.try_lock().ok() {
                if last_check.elapsed() > *LARGE_NOTIFICATION_CLEANUP_INTERVAL {
                    diesel::sql_query(format!(
                        "delete from large_notifications
                         where created_at < current_timestamp - interval '{}s'",
                        LARGE_NOTIFICATION_CLEANUP_INTERVAL.as_secs()
                    ))
                    .execute(conn)?;
                    *last_check = Instant::now();
                }
            }
        }
        Ok(())
    }
}

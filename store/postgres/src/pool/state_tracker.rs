//! Event/error handlers for our r2d2 pools

use diesel::r2d2::{self, event as e, HandleEvent};

use graph::prelude::error;
use graph::prelude::Counter;
use graph::prelude::Gauge;
use graph::prelude::MetricsRegistry;
use graph::prelude::PoolWaitStats;
use graph::slog::Logger;

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Track whether a database is available or not using the event and error
/// handlers from this module. The pool must be set up with these handlers
/// when it is created
#[derive(Clone)]
pub(super) struct StateTracker {
    available: Arc<AtomicBool>,
    ignore_timeout: Arc<AtomicBool>,
}

impl StateTracker {
    pub(super) fn new() -> Self {
        Self {
            available: Arc::new(AtomicBool::new(true)),
            ignore_timeout: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn mark_available(&self) {
        self.available.store(true, Ordering::Relaxed);
    }

    fn mark_unavailable(&self) {
        self.available.store(false, Ordering::Relaxed);
    }

    pub(super) fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    fn timeout_is_ignored(&self) -> bool {
        self.ignore_timeout.load(Ordering::Relaxed)
    }

    pub(super) fn ignore_timeout<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.ignore_timeout.store(true, Ordering::Relaxed);
        let res = f();
        self.ignore_timeout.store(false, Ordering::Relaxed);
        res
    }
}

#[derive(Clone)]
pub(super) struct ErrorHandler {
    logger: Logger,
    counter: Counter,
    state_tracker: StateTracker,
}

impl ErrorHandler {
    pub(super) fn new(logger: Logger, counter: Counter, state_tracker: StateTracker) -> Self {
        Self {
            logger,
            counter,
            state_tracker,
        }
    }
}
impl std::fmt::Debug for ErrorHandler {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Result::Ok(())
    }
}

impl r2d2::HandleError<r2d2::Error> for ErrorHandler {
    fn handle_error(&self, error: r2d2::Error) {
        let msg = brief_error_msg(&error);

        // Don't count canceling statements for timeouts etc. as a
        // connection error. Unfortunately, we only have the textual error
        // and need to infer whether the error indicates that the database
        // is down or if something else happened. When querying a replica,
        // these messages indicate that a query was canceled because it
        // conflicted with replication, but does not indicate that there is
        // a problem with the database itself.
        //
        // This check will break if users run Postgres (or even graph-node)
        // in a locale other than English. In that case, their database will
        // be marked as unavailable even though it is perfectly fine.
        if msg.contains("canceling statement")
            || msg.contains("terminating connection due to conflict with recovery")
        {
            return;
        }

        self.counter.inc();
        if self.state_tracker.is_available() {
            error!(self.logger, "Postgres connection error"; "error" => msg);
        }
        self.state_tracker.mark_unavailable();
    }
}

#[derive(Clone)]
pub(super) struct EventHandler {
    logger: Logger,
    count_gauge: Gauge,
    wait_gauge: Gauge,
    size_gauge: Gauge,
    wait_stats: PoolWaitStats,
    state_tracker: StateTracker,
}

impl EventHandler {
    pub(super) fn new(
        logger: Logger,
        registry: Arc<MetricsRegistry>,
        wait_stats: PoolWaitStats,
        const_labels: HashMap<String, String>,
        state_tracker: StateTracker,
    ) -> Self {
        let count_gauge = registry
            .global_gauge(
                "store_connection_checkout_count",
                "The number of Postgres connections currently checked out",
                const_labels.clone(),
            )
            .expect("failed to create `store_connection_checkout_count` counter");
        let wait_gauge = registry
            .global_gauge(
                "store_connection_wait_time_ms",
                "Average connection wait time",
                const_labels.clone(),
            )
            .expect("failed to create `store_connection_wait_time_ms` counter");
        let size_gauge = registry
            .global_gauge(
                "store_connection_pool_size_count",
                "Overall size of the connection pool",
                const_labels,
            )
            .expect("failed to create `store_connection_pool_size_count` counter");
        EventHandler {
            logger,
            count_gauge,
            wait_gauge,
            wait_stats,
            size_gauge,
            state_tracker,
        }
    }

    fn add_conn_wait_time(&self, duration: Duration) {
        self.wait_stats
            .write()
            .unwrap()
            .add_and_register(duration, &self.wait_gauge);
    }
}

impl std::fmt::Debug for EventHandler {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Result::Ok(())
    }
}

impl HandleEvent for EventHandler {
    fn handle_acquire(&self, _: e::AcquireEvent) {
        self.size_gauge.inc();
        self.state_tracker.mark_available();
    }

    fn handle_release(&self, _: e::ReleaseEvent) {
        self.size_gauge.dec();
    }

    fn handle_checkout(&self, event: e::CheckoutEvent) {
        self.count_gauge.inc();
        self.add_conn_wait_time(event.duration());
        self.state_tracker.mark_available();
    }

    fn handle_timeout(&self, event: e::TimeoutEvent) {
        if self.state_tracker.timeout_is_ignored() {
            return;
        }
        self.add_conn_wait_time(event.timeout());
        if self.state_tracker.is_available() {
            error!(self.logger, "Connection checkout timed out";
               "wait_ms" => event.timeout().as_millis()
            )
        }
        self.state_tracker.mark_unavailable();
    }

    fn handle_checkin(&self, _: e::CheckinEvent) {
        self.count_gauge.dec();
    }
}

fn brief_error_msg(error: &dyn std::error::Error) -> String {
    // For 'Connection refused' errors, Postgres includes the IP and
    // port number in the error message. We want to suppress that and
    // only use the first line from the error message. For more detailed
    // analysis, 'Connection refused' manifests as a
    // `ConnectionError(BadConnection("could not connect to server:
    // Connection refused.."))`
    error
        .to_string()
        .split('\n')
        .next()
        .unwrap_or("no error details provided")
        .to_string()
}

//! Connection management for Postgres connection pools
//!
//! This module provides helpers for collecting metrics for a pool and
//! tracking availability of the underlying database

use deadpool::managed::Hook;
use diesel::r2d2;

use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use graph::prelude::error;
use graph::prelude::Counter;
use graph::prelude::Gauge;
use graph::prelude::MetricsRegistry;
use graph::prelude::MovingStats;
use graph::prelude::PoolWaitStats;
use graph::slog::info;
use graph::slog::Logger;

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use crate::pool::AsyncPool;

/// Track whether a database is available or not
#[derive(Clone)]
pub(super) struct StateTracker {
    logger: Logger,
    available: Arc<AtomicBool>,
    ignore_timeout: Arc<AtomicBool>,
}

impl StateTracker {
    pub(super) fn new(logger: Logger) -> Self {
        Self {
            logger,
            available: Arc::new(AtomicBool::new(true)),
            ignore_timeout: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn mark_available(&self) {
        if !self.is_available() {
            info!(self.logger, "Conection checkout"; "event" => "available");
        }
        self.available.store(true, Ordering::Relaxed);
    }

    pub(super) fn mark_unavailable(&self, waited: Duration) {
        if self.is_available() {
            if waited.as_nanos() > 0 {
                error!(self.logger, "Connection checkout timed out";
                   "event" => "unavailable",
                   "wait_ms" => waited.as_millis()
                )
            } else {
                error!(self.logger, "Connection checkout"; "event" => "unavailable");
            }
        }
        self.available.store(false, Ordering::Relaxed);
    }

    pub(super) fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    pub(super) fn timeout_is_ignored(&self) -> bool {
        self.ignore_timeout.load(Ordering::Relaxed)
    }

    /// Run the given async function while ignoring timeouts; if `f` causes
    /// a timeout, the database is not marked as unavailable
    pub(super) async fn ignore_timeout<F, R>(&self, f: F) -> R
    where
        F: AsyncFnOnce() -> R,
    {
        self.ignore_timeout.store(true, Ordering::Relaxed);
        let res = f().await;
        self.ignore_timeout.store(false, Ordering::Relaxed);
        res
    }

    /// Return a deadpool hook that marks the database as available
    pub(super) fn mark_available_hook(
        &self,
    ) -> Hook<AsyncDieselConnectionManager<AsyncPgConnection>> {
        let state_tracker = self.clone();
        Hook::async_fn(move |_conn, _metrics| {
            let state_tracker = state_tracker.clone();
            Box::pin(async move {
                state_tracker.mark_available();
                Ok(())
            })
        })
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
        self.state_tracker.mark_unavailable(Duration::from_secs(0));
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

pub(crate) fn spawn_size_stat_collector(
    pool: AsyncPool,
    registry: &MetricsRegistry,
    const_labels: HashMap<String, String>,
) {
    let count_gauge = registry
        .global_gauge(
            "store_connection_checkout_count",
            "The number of Postgres connections currently checked out",
            const_labels.clone(),
        )
        .expect("failed to create `store_connection_checkout_count` counter");
    let size_gauge = registry
        .global_gauge(
            "store_connection_pool_size_count",
            "Overall size of the connection pool",
            const_labels,
        )
        .expect("failed to create `store_connection_pool_size_count` counter");
    tokio::task::spawn(async move {
        loop {
            let status = pool.status();
            count_gauge.set((status.size - status.available) as f64);
            size_gauge.set(status.size as f64);
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    });
}

pub(crate) struct WaitMeter {
    wait_gauge: Gauge,
    pub(crate) wait_stats: PoolWaitStats,
}

impl WaitMeter {
    pub(crate) fn new(registry: &MetricsRegistry, const_labels: HashMap<String, String>) -> Self {
        let wait_gauge = registry
            .global_gauge(
                "store_connection_wait_time_ms",
                "Average connection wait time",
                const_labels,
            )
            .expect("failed to create `store_connection_wait_time_ms` counter");
        let wait_stats = Arc::new(RwLock::new(MovingStats::default()));

        Self {
            wait_gauge,
            wait_stats,
        }
    }

    pub(crate) fn add_conn_wait_time(&self, duration: Duration) {
        self.wait_stats
            .write()
            .unwrap()
            .add_and_register(duration, &self.wait_gauge);
    }
}

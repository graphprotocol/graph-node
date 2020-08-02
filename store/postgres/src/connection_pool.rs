use diesel::pg::PgConnection;
use diesel::r2d2::{self, event as e, ConnectionManager, HandleEvent, Pool};

use graph::prelude::*;
use graph::util::security::SafeDisplay;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

struct ErrorHandler(Logger, Counter);

impl Debug for ErrorHandler {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Result::Ok(())
    }
}

impl r2d2::HandleError<r2d2::Error> for ErrorHandler {
    fn handle_error(&self, error: r2d2::Error) {
        self.1.inc();
        error!(self.0, "Postgres connection error"; "error" => error.to_string());
    }
}

struct EventHandler {
    logger: Logger,
    count_gauge: Gauge,
    wait_gauge: Gauge,
    wait_stats: PoolWaitStats,
}

impl EventHandler {
    fn new(
        logger: Logger,
        registry: Arc<dyn MetricsRegistry>,
        wait_stats: PoolWaitStats,
        const_labels: HashMap<String, String>,
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
        EventHandler {
            logger,
            count_gauge,
            wait_gauge,
            wait_stats,
        }
    }

    fn add_wait_time(&self, duration: Duration) {
        let wait_avg = {
            let mut wait_stats = self.wait_stats.write().unwrap();
            wait_stats.add(duration);
            wait_stats.average()
        };
        let wait_avg = wait_avg.map(|wait_avg| wait_avg.as_millis()).unwrap_or(0);
        self.wait_gauge.set(wait_avg as f64);
    }
}

impl Debug for EventHandler {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Result::Ok(())
    }
}

impl HandleEvent for EventHandler {
    fn handle_acquire(&self, _: e::AcquireEvent) {}
    fn handle_release(&self, _: e::ReleaseEvent) {}
    fn handle_checkout(&self, event: e::CheckoutEvent) {
        self.count_gauge.inc();
        self.add_wait_time(event.duration());
    }
    fn handle_timeout(&self, event: e::TimeoutEvent) {
        self.add_wait_time(event.timeout());
        error!(self.logger, "Connection checkout timed out";
               "wait_ms" => event.timeout().as_millis())
    }
    fn handle_checkin(&self, _: e::CheckinEvent) {
        self.count_gauge.dec();
    }
}

pub fn create_connection_pool(
    pool_name: &str,
    postgres_url: String,
    pool_size: u32,
    logger: &Logger,
    registry: Arc<dyn MetricsRegistry>,
    wait_time: PoolWaitStats,
) -> Pool<ConnectionManager<PgConnection>> {
    let logger_store = logger.new(o!("component" => "Store"));
    let logger_pool = logger.new(o!("component" => "PostgresConnectionPool"));
    let const_labels = {
        let mut map = HashMap::new();
        map.insert("pool".to_owned(), pool_name.to_owned());
        map
    };
    let error_counter = registry
        .global_counter(
            "store_connection_error_count",
            "The number of Postgres connections errors",
            HashMap::new(),
        )
        .expect("failed to create `store_connection_error_count` counter");
    let error_handler = Box::new(ErrorHandler(logger_pool.clone(), error_counter));
    let event_handler = Box::new(EventHandler::new(
        logger_pool.clone(),
        registry,
        wait_time,
        const_labels,
    ));

    // Connect to Postgres
    let conn_manager = ConnectionManager::new(postgres_url.clone());
    // Set the time we wait for a connection to 6h. The default is 30s
    // which can be too little if database connections are highly
    // contended; if we don't get a connection within the timeout,
    // ultimately subgraphs get marked as failed. This effectively
    // turns off this timeout and makes it possible that work needing
    // a database connection blocks for a very long time
    //
    // When running tests however, use the default of 30 seconds.
    // There should not be a lot of contention when running tests,
    // and this can help debug the issue faster when a test appears
    // to be hanging but really there is just no connection to postgres
    // available.
    let timeout_seconds = if cfg!(test) { 30 } else { 6 * 60 * 60 };
    let pool = Pool::builder()
        .error_handler(error_handler)
        .event_handler(event_handler)
        .connection_timeout(Duration::from_secs(timeout_seconds))
        .max_size(pool_size)
        .build(conn_manager)
        .unwrap();
    info!(
        logger_store,
        "Connected to Postgres";
        "pool_name" => pool_name,
        "url" => SafeDisplay(postgres_url.as_str())
    );
    pool
}

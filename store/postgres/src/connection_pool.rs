use diesel::r2d2::{self, event as e, ConnectionManager, HandleEvent, Pool, PooledConnection};
use diesel::{connection::SimpleConnection, pg::PgConnection, sql_query, RunQueryDsl};

use graph::prelude::{
    debug, error, info, o, tokio::sync::Semaphore, CancelGuard, CancelHandle, CancelToken as _,
    CancelableError, Counter, Gauge, Logger, MetricsRegistry, MovingStats, PoolWaitStats,
    StoreError,
};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, sync::RwLock};

//use postgres::config::Config;

#[derive(Clone)]
pub struct ConnectionPool {
    logger: Logger,
    pool: Pool<ConnectionManager<PgConnection>>,
    limiter: Arc<Semaphore>,
    pub(crate) wait_stats: PoolWaitStats,
}

struct ErrorHandler(Logger, Counter);

impl std::fmt::Debug for ErrorHandler {
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

impl std::fmt::Debug for EventHandler {
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
           "wait_ms" => event.timeout().as_millis(),
           "backtrace" => format!("{:?}", backtrace::Backtrace::new()),
        )
    }
    fn handle_checkin(&self, _: e::CheckinEvent) {
        self.count_gauge.dec();
    }
}

impl std::ops::Deref for ConnectionPool {
    type Target = Pool<ConnectionManager<PgConnection>>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl ConnectionPool {
    pub fn create(
        shard_name: &str,
        pool_name: &str,
        postgres_url: String,
        pool_size: u32,
        logger: &Logger,
        registry: Arc<dyn MetricsRegistry>,
    ) -> ConnectionPool {
        let logger_store = logger.new(o!("component" => "Store"));
        let logger_pool = logger.new(o!("component" => "ConnectionPool"));
        let const_labels = {
            let mut map = HashMap::new();
            map.insert("pool".to_owned(), pool_name.to_owned());
            map.insert("shard".to_string(), shard_name.to_owned());
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
        let wait_stats = Arc::new(RwLock::new(MovingStats::default()));
        let event_handler = Box::new(EventHandler::new(
            logger_pool.clone(),
            registry,
            wait_stats.clone(),
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
        let limiter = Arc::new(Semaphore::new(pool_size as usize));
        info!(logger_store, "Pool successfully connected to Postgres");
        ConnectionPool {
            logger: logger_pool,
            pool,
            limiter,
            wait_stats,
        }
    }

    /// Execute a closure with a connection to the database.
    ///
    /// # API
    ///   The API of using a closure to bound the usage of the connection serves several
    ///   purposes:
    ///
    ///   * Moves blocking database access out of the `Future::poll`. Within
    ///     `Future::poll` (which includes all `async` methods) it is illegal to
    ///     perform a blocking operation. This includes all accesses to the
    ///     database, acquiring of locks, etc. Calling a blocking operation can
    ///     cause problems with `Future` combinators (including but not limited
    ///     to select, timeout, and FuturesUnordered) and problems with
    ///     executors/runtimes. This method moves the database work onto another
    ///     thread in a way which does not block `Future::poll`.
    ///
    ///   * Limit the total number of connections. Because the supplied closure
    ///     takes a reference, we know the scope of the usage of all entity
    ///     connections and can limit their use in a non-blocking way.
    ///
    /// # Cancellation
    ///   The normal pattern for futures in Rust is drop to cancel. Once we
    ///   spawn the database work in a thread though, this expectation no longer
    ///   holds because the spawned task is the independent of this future. So,
    ///   this method provides a cancel token which indicates that the `Future`
    ///   has been dropped. This isn't *quite* as good as drop on cancel,
    ///   because a drop on cancel can do things like cancel http requests that
    ///   are in flight, but checking for cancel periodically is a significant
    ///   improvement.
    ///
    ///   The implementation of the supplied closure should check for cancel
    ///   between every operation that is potentially blocking. This includes
    ///   any method which may interact with the database. The check can be
    ///   conveniently written as `token.check_cancel()?;`. It is low overhead
    ///   to check for cancel, so when in doubt it is better to have too many
    ///   checks than too few.
    ///
    /// # Panics:
    ///   * This task will panic if the supplied closure panics
    ///   * This task will panic if the supplied closure returns Err(Cancelled)
    ///     when the supplied cancel token is not cancelled.
    pub(crate) async fn with_conn<T: Send + 'static>(
        &self,
        f: impl 'static
            + Send
            + FnOnce(
                &PooledConnection<ConnectionManager<PgConnection>>,
                &CancelHandle,
            ) -> Result<T, CancelableError<StoreError>>,
    ) -> Result<T, StoreError> {
        let _permit = self.limiter.acquire().await;
        let pool = self.clone();

        let cancel_guard = CancelGuard::new();
        let cancel_handle = cancel_guard.handle();

        let result = graph::spawn_blocking_allow_panic(move || {
            // It is possible time has passed between scheduling on the
            // threadpool and being executed. Time to check for cancel.
            cancel_handle.check_cancel()?;

            // A failure to establish a connection is propagated as though the
            // closure failed.
            let conn = pool
                .get()
                .map_err(|e| CancelableError::Error(StoreError::Unknown(e.into())))?;

            // It is possible time has passed while establishing a connection.
            // Time to check for cancel.
            cancel_handle.check_cancel()?;

            f(&conn, &cancel_handle)
        })
        .await
        .unwrap(); // Propagate panics, though there shouldn't be any.

        drop(cancel_guard);

        // Finding cancel isn't technically unreachable, since there is nothing
        // stopping the supplied closure from returning Canceled even if the
        // supplied handle wasn't canceled. That would be very unexpected, the
        // doc comment for this function says we will panic in this scenario.
        match result {
            Ok(t) => Ok(t),
            Err(CancelableError::Error(e)) => Err(e),
            Err(CancelableError::Cancel) => panic!("The closure supplied to with_entity_conn must not return Err(Canceled) unless the supplied token was canceled."),
        }
    }

    pub fn get_with_timeout_warning(
        &self,
        logger: &Logger,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, graph::prelude::Error> {
        loop {
            match self.get_timeout(Duration::from_secs(60)) {
                Ok(conn) => return Ok(conn),
                Err(e) => error!(logger, "Error checking out connection, retrying";
                   "error" => e.to_string(),
                ),
            }
        }
    }

    /// Run any pending schema migrations for this database.
    ///
    /// # Panics
    ///
    /// If any errors happen during the migration, the process panics
    pub async fn migrate_schema(&self) {
        #[derive(QueryableByName)]
        struct LockResult {
            #[sql_type = "diesel::sql_types::Bool"]
            migrate: bool,
        }

        let pool = self.clone();
        self.with_conn(move |conn, _| {
            // Get an advisory session-level lock. The graph-node process
            // that acquires the lock will run the migration. Everybody else
            // will skip running migrations.
            //
            // We use two locks: one so we can tell whether we were the
            // lucky ones that should run the migration, and one that blocks
            // every process that is not running migrations until the
            // migration is finished
            let lock = sql_query("select pg_try_advisory_lock(1) as migrate, pg_advisory_lock(2)")
                .get_result::<LockResult>(conn)
                .expect("we can try to get advisory locks 1 and 2");
            let result = if lock.migrate {
                migrate_schema(&pool.logger, conn)
            } else {
                Ok(())
            };
            sql_query("select pg_advisory_unlock(1), pg_advisory_unlock(2)")
                .execute(conn)
                .expect("we can unlock the advisory locks");
            result.expect("migration succeeds");
            Ok(())
        })
        .await
        .expect("migrations are never canceled");
    }
}

embed_migrations!("./migrations");

/// Run all schema migrations.
///
/// When multiple `graph-node` processes start up at the same time, we ensure
/// that they do not run migrations in parallel by using `blocking_conn` to
/// serialize them. The `conn` is used to run the actual migration.
fn migrate_schema(
    logger: &Logger,
    conn: &PgConnection,
) -> Result<(), diesel::migration::RunMigrationsError> {
    // Collect migration logging output
    let mut output = vec![];

    info!(
        logger,
        "Waiting for other graph-node instances to finish migrating"
    );
    let result = embedded_migrations::run_with_output(conn, &mut output);
    info!(logger, "Migrations finished");

    // If there was any migration output, log it now
    let has_output = !output.is_empty();
    if has_output {
        let msg = String::from_utf8(output).unwrap_or_else(|_| String::from("<unreadable>"));
        if result.is_err() {
            error!(logger, "Postgres migration output"; "output" => msg);
            return result;
        } else {
            debug!(logger, "Postgres migration output"; "output" => msg);
        }
    }

    if has_output {
        // We take getting output as a signal that a migration was actually
        // run, which is not easy to tell from the Diesel API, and reset the
        // query statistics since a schema change makes them not all that
        // useful. An error here is not serious and can be ignored.
        conn.batch_execute("select pg_stat_statements_reset()").ok();
    }

    Ok(())
}

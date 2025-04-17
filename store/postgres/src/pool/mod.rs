use diesel::r2d2::Builder;
use diesel::{connection::SimpleConnection, pg::PgConnection};
use diesel::{
    r2d2::{self, event as e, ConnectionManager, HandleEvent, Pool, PooledConnection},
    Connection,
};
use diesel::{sql_query, RunQueryDsl};

use diesel_migrations::{EmbeddedMigrations, HarnessWithOutput};
use graph::cheap_clone::CheapClone;
use graph::components::store::QueryPermit;
use graph::derive::CheapClone;
use graph::futures03::future::join_all;
use graph::futures03::FutureExt as _;
use graph::internal_error;
use graph::prelude::tokio::time::Instant;
use graph::prelude::{
    anyhow::anyhow, crit, debug, error, info, o, tokio::sync::Semaphore, CancelGuard, CancelHandle,
    CancelToken as _, CancelableError, Counter, Gauge, Logger, MovingStats, PoolWaitStats,
    StoreError, ENV_VARS,
};
use graph::prelude::{tokio, MetricsRegistry};
use graph::slog::warn;
use graph::util::timed_rw_lock::TimedMutex;

use std::fmt::{self};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, sync::RwLock};

use crate::advisory_lock::with_migration_lock;
use crate::catalog;
use crate::primary::{self, Mirror, Namespace};
use crate::{Shard, PRIMARY_SHARD};

mod foreign_server;

pub use foreign_server::ForeignServer;

/// The namespace under which the `PRIMARY_TABLES` are mapped into each
/// shard
pub(crate) const PRIMARY_PUBLIC: &'static str = "primary_public";

/// Tables that we map from the primary into `primary_public` in each shard
const PRIMARY_TABLES: [&str; 3] = ["deployment_schemas", "chains", "active_copies"];

/// The namespace under which we create views in the primary that union all
/// the `SHARDED_TABLES`
pub(crate) const CROSS_SHARD_NSP: &'static str = "sharded";

/// Tables that we map from each shard into each other shard into the
/// `shard_<name>_subgraphs` namespace
const SHARDED_TABLES: [(&str, &[&str]); 2] = [
    ("public", &["ethereum_networks"]),
    (
        "subgraphs",
        &[
            "copy_state",
            "copy_table_state",
            "dynamic_ethereum_contract_data_source",
            "subgraph_deployment",
            "subgraph_error",
            "subgraph_manifest",
            "table_stats",
            "subgraph",
            "subgraph_version",
            "subgraph_deployment_assignment",
        ],
    ),
];

/// Make sure that the tables that `jobs::MirrorJob` wants to mirror are
/// actually mapped into the various shards. A failure here is simply a
/// coding mistake
fn check_mirrored_tables() {
    for table in Mirror::PUBLIC_TABLES {
        if !PRIMARY_TABLES.contains(&table) {
            panic!("table {} is not in PRIMARY_TABLES", table);
        }
    }

    let subgraphs_tables = *SHARDED_TABLES
        .iter()
        .find(|(nsp, _)| *nsp == "subgraphs")
        .map(|(_, tables)| tables)
        .unwrap();

    for table in Mirror::SUBGRAPHS_TABLES {
        if !subgraphs_tables.contains(&table) {
            panic!("table {} is not in SHARDED_TABLES[subgraphs]", table);
        }
    }
}

/// How long to keep connections in the `fdw_pool` around before closing
/// them on idle. This is much shorter than the default of 10 minutes.
const FDW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

enum PoolStateInner {
    /// A connection pool, and all the servers for which we need to
    /// establish fdw mappings when we call `setup` on the pool
    Created(Arc<PoolInner>, Arc<PoolCoordinator>),
    /// The pool has been successfully set up
    Ready(Arc<PoolInner>),
}

/// A pool goes through several states, and this struct tracks what state we
/// are in, together with the `state_tracker` field on `ConnectionPool`.
/// When first created, the pool is in state `Created`; once we successfully
/// called `setup` on it, it moves to state `Ready`. During use, we use the
/// r2d2 callbacks to determine if the database is available or not, and set
/// the `available` field accordingly. Tracking that allows us to fail fast
/// and avoids having to wait for a connection timeout every time we need a
/// database connection. That avoids overall undesirable states like buildup
/// of queries; instead of queueing them until the database is available,
/// they return almost immediately with an error
#[derive(Clone, CheapClone)]
struct PoolState {
    logger: Logger,
    inner: Arc<TimedMutex<PoolStateInner>>,
}

impl PoolState {
    fn new(logger: Logger, inner: PoolStateInner, name: String) -> Self {
        let pool_name = format!("pool-{}", name);
        Self {
            logger,
            inner: Arc::new(TimedMutex::new(inner, pool_name)),
        }
    }

    fn created(pool: Arc<PoolInner>, coord: Arc<PoolCoordinator>) -> Self {
        let logger = pool.logger.clone();
        let name = pool.shard.to_string();
        let inner = PoolStateInner::Created(pool, coord);
        Self::new(logger, inner, name)
    }

    fn ready(pool: Arc<PoolInner>) -> Self {
        let logger = pool.logger.clone();
        let name = pool.shard.to_string();
        let inner = PoolStateInner::Ready(pool);
        Self::new(logger, inner, name)
    }

    fn set_ready(&self) {
        use PoolStateInner::*;

        let mut guard = self.inner.lock(&self.logger);
        match &*guard {
            Created(pool, _) => *guard = Ready(pool.clone()),
            Ready(_) => { /* nothing to do */ }
        }
    }

    /// Get a connection pool that is ready, i.e., has been through setup
    /// and running migrations
    fn get_ready(&self) -> Result<Arc<PoolInner>, StoreError> {
        // We have to be careful here that we do not hold a lock when we
        // call `setup_bg`, otherwise we will deadlock
        let (pool, coord) = {
            let guard = self.inner.lock(&self.logger);

            use PoolStateInner::*;
            match &*guard {
                Created(pool, coord) => (pool.cheap_clone(), coord.cheap_clone()),
                Ready(pool) => return Ok(pool.clone()),
            }
        };

        // self is `Created` and needs to have setup run
        coord.setup_bg(self.cheap_clone())?;

        // We just tried to set up the pool; if it is still not set up and
        // we didn't have an error, it means the database is not available
        if self.needs_setup() {
            return Err(StoreError::DatabaseUnavailable);
        } else {
            Ok(pool)
        }
    }

    /// Get the inner pool, regardless of whether it has been set up or not.
    /// Most uses should use `get_ready` instead
    fn get_unready(&self) -> Arc<PoolInner> {
        use PoolStateInner::*;

        match &*self.inner.lock(&self.logger) {
            Created(pool, _) | Ready(pool) => pool.cheap_clone(),
        }
    }

    fn needs_setup(&self) -> bool {
        let guard = self.inner.lock(&self.logger);

        use PoolStateInner::*;
        match &*guard {
            Created(_, _) => true,
            Ready(_) => false,
        }
    }
}
#[derive(Clone)]
pub struct ConnectionPool {
    inner: PoolState,
    pub shard: Shard,
    state_tracker: PoolStateTracker,
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("shard", &self.shard)
            .finish()
    }
}

/// The role of the pool, mostly for logging, and what purpose it serves.
/// The main pool will always be called `main`, and can be used for reading
/// and writing. Replica pools can only be used for reading, and don't
/// require any setup (migrations etc.)
pub enum PoolRole {
    Main,
    Replica(String),
}

impl PoolRole {
    fn as_str(&self) -> &str {
        match self {
            PoolRole::Main => "main",
            PoolRole::Replica(name) => name,
        }
    }

    fn is_replica(&self) -> bool {
        match self {
            PoolRole::Main => false,
            PoolRole::Replica(_) => true,
        }
    }
}

#[derive(Clone)]
struct PoolStateTracker {
    available: Arc<AtomicBool>,
    ignore_timeout: Arc<AtomicBool>,
}

impl PoolStateTracker {
    fn new() -> Self {
        Self {
            available: Arc::new(AtomicBool::new(true)),
            ignore_timeout: Arc::new(AtomicBool::new(false)),
        }
    }

    fn mark_available(&self) {
        self.available.store(true, Ordering::Relaxed);
    }

    fn mark_unavailable(&self) {
        self.available.store(false, Ordering::Relaxed);
    }

    fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    fn timeout_is_ignored(&self) -> bool {
        self.ignore_timeout.load(Ordering::Relaxed)
    }

    fn ignore_timeout<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.ignore_timeout.store(true, Ordering::Relaxed);
        let res = f();
        self.ignore_timeout.store(false, Ordering::Relaxed);
        res
    }
}

impl ConnectionPool {
    fn create(
        shard_name: &str,
        pool_name: PoolRole,
        postgres_url: String,
        pool_size: u32,
        fdw_pool_size: Option<u32>,
        logger: &Logger,
        registry: Arc<MetricsRegistry>,
        coord: Arc<PoolCoordinator>,
    ) -> ConnectionPool {
        let state_tracker = PoolStateTracker::new();
        let shard =
            Shard::new(shard_name.to_string()).expect("shard_name is a valid name for a shard");
        let inner = {
            let pool = PoolInner::create(
                shard.clone(),
                pool_name.as_str(),
                postgres_url,
                pool_size,
                fdw_pool_size,
                logger,
                registry,
                state_tracker.clone(),
            );
            if pool_name.is_replica() {
                PoolState::ready(Arc::new(pool))
            } else {
                PoolState::created(Arc::new(pool), coord)
            }
        };
        ConnectionPool {
            inner,
            shard,
            state_tracker,
        }
    }

    /// This is only used for `graphman` to ensure it doesn't run migrations
    /// or other setup steps
    pub fn skip_setup(&self) {
        self.inner.set_ready();
    }

    /// Return a pool that is ready, i.e., connected to the database. If the
    /// pool has not been set up yet, call `setup`. If there are any errors
    /// or the pool is marked as unavailable, return
    /// `StoreError::DatabaseUnavailable`
    fn get_ready(&self) -> Result<Arc<PoolInner>, StoreError> {
        if !self.state_tracker.is_available() {
            // We know that trying to use this pool is pointless since the
            // database is not available, and will only lead to other
            // operations having to wait until the connection timeout is
            // reached.
            return Err(StoreError::DatabaseUnavailable);
        }

        match self.inner.get_ready() {
            Ok(pool) => {
                self.state_tracker.mark_available();
                Ok(pool)
            }
            Err(e) => Err(e),
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
                &mut PooledConnection<ConnectionManager<PgConnection>>,
                &CancelHandle,
            ) -> Result<T, CancelableError<StoreError>>,
    ) -> Result<T, StoreError> {
        let pool = self.get_ready()?;
        pool.with_conn(f).await
    }

    pub fn get(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError> {
        self.get_ready()?.get()
    }

    /// Get a connection from the pool for foreign data wrapper access;
    /// since that pool can be very contended, periodically log that we are
    /// still waiting for a connection
    ///
    /// The `timeout` is called every time we time out waiting for a
    /// connection. If `timeout` returns `true`, `get_fdw` returns with that
    /// error, otherwise we try again to get a connection.
    pub fn get_fdw<F>(
        &self,
        logger: &Logger,
        timeout: F,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError>
    where
        F: FnMut() -> bool,
    {
        self.get_ready()?.get_fdw(logger, timeout)
    }

    /// Get a connection from the pool for foreign data wrapper access if
    /// one is available
    pub fn try_get_fdw(
        &self,
        logger: &Logger,
        timeout: Duration,
    ) -> Option<PooledConnection<ConnectionManager<PgConnection>>> {
        let Ok(inner) = self.get_ready() else {
            return None;
        };
        self.state_tracker
            .ignore_timeout(|| inner.try_get_fdw(logger, timeout))
    }

    pub(crate) async fn query_permit(&self) -> QueryPermit {
        let pool = self.inner.get_unready();
        let start = Instant::now();
        let permit = pool.query_permit().await;
        QueryPermit {
            permit,
            wait: start.elapsed(),
        }
    }

    pub(crate) fn wait_stats(&self) -> PoolWaitStats {
        self.inner.get_unready().wait_stats.cheap_clone()
    }

    /// Mirror key tables from the primary into our own schema. We do this
    /// by manually inserting or deleting rows through comparing it with the
    /// table on the primary. Once we drop support for PG 9.6, we can
    /// simplify all this and achieve the same result with logical
    /// replication.
    pub(crate) async fn mirror_primary_tables(&self) -> Result<(), StoreError> {
        let pool = self.get_ready()?;
        pool.mirror_primary_tables().await
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

#[derive(Clone)]
struct ErrorHandler {
    logger: Logger,
    counter: Counter,
    state_tracker: PoolStateTracker,
}

impl ErrorHandler {
    fn new(logger: Logger, counter: Counter, state_tracker: PoolStateTracker) -> Self {
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
struct EventHandler {
    logger: Logger,
    count_gauge: Gauge,
    wait_gauge: Gauge,
    size_gauge: Gauge,
    wait_stats: PoolWaitStats,
    state_tracker: PoolStateTracker,
}

impl EventHandler {
    fn new(
        logger: Logger,
        registry: Arc<MetricsRegistry>,
        wait_stats: PoolWaitStats,
        const_labels: HashMap<String, String>,
        state_tracker: PoolStateTracker,
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

#[derive(Clone)]
pub struct PoolInner {
    logger: Logger,
    pub shard: Shard,
    pool: Pool<ConnectionManager<PgConnection>>,
    // A separate pool for connections that will use foreign data wrappers.
    // Once such a connection accesses a foreign table, Postgres keeps a
    // connection to the foreign server until the connection is closed.
    // Normal pooled connections live quite long (up to 10 minutes) and can
    // therefore keep a lot of connections into foreign databases open. We
    // mitigate this by using a separate small pool with a much shorter
    // connection lifetime. Starting with postgres_fdw 1.1 in Postgres 14,
    // this will no longer be needed since it will then be possible to
    // explicitly close connections to foreign servers when a connection is
    // returned to the pool.
    fdw_pool: Option<Pool<ConnectionManager<PgConnection>>>,
    limiter: Arc<Semaphore>,
    postgres_url: String,
    pub(crate) wait_stats: PoolWaitStats,

    // Limits the number of graphql queries that may execute concurrently. Since one graphql query
    // may require multiple DB queries, it is useful to organize the queue at the graphql level so
    // that waiting queries consume few resources. Still this is placed here because the semaphore
    // is sized acording to the DB connection pool size.
    query_semaphore: Arc<tokio::sync::Semaphore>,
    semaphore_wait_stats: Arc<RwLock<MovingStats>>,
    semaphore_wait_gauge: Box<Gauge>,
}

impl PoolInner {
    fn create(
        shard: Shard,
        pool_name: &str,
        postgres_url: String,
        pool_size: u32,
        fdw_pool_size: Option<u32>,
        logger: &Logger,
        registry: Arc<MetricsRegistry>,
        state_tracker: PoolStateTracker,
    ) -> PoolInner {
        check_mirrored_tables();

        let logger_store = logger.new(o!("component" => "Store"));
        let logger_pool = logger.new(o!("component" => "ConnectionPool"));
        let const_labels = {
            let mut map = HashMap::new();
            map.insert("pool".to_owned(), pool_name.to_owned());
            map.insert("shard".to_string(), shard.to_string());
            map
        };
        let error_counter = registry
            .global_counter(
                "store_connection_error_count",
                "The number of Postgres connections errors",
                const_labels.clone(),
            )
            .expect("failed to create `store_connection_error_count` counter");
        let error_handler = Box::new(ErrorHandler::new(
            logger_pool.clone(),
            error_counter,
            state_tracker.clone(),
        ));
        let wait_stats = Arc::new(RwLock::new(MovingStats::default()));
        let event_handler = Box::new(EventHandler::new(
            logger_pool.clone(),
            registry.cheap_clone(),
            wait_stats.clone(),
            const_labels.clone(),
            state_tracker,
        ));

        // Connect to Postgres
        let conn_manager = ConnectionManager::new(postgres_url.clone());
        let min_idle = ENV_VARS.store.connection_min_idle.filter(|min_idle| {
            if *min_idle <= pool_size {
                true
            } else {
                warn!(
                    logger_pool,
                    "Configuration error: min idle {} exceeds pool size {}, ignoring min idle",
                    min_idle,
                    pool_size
                );
                false
            }
        });
        let builder: Builder<ConnectionManager<PgConnection>> = Pool::builder()
            .error_handler(error_handler.clone())
            .event_handler(event_handler.clone())
            .connection_timeout(ENV_VARS.store.connection_timeout)
            .max_size(pool_size)
            .min_idle(min_idle)
            .idle_timeout(Some(ENV_VARS.store.connection_idle_timeout));
        let pool = builder.build_unchecked(conn_manager);
        let fdw_pool = fdw_pool_size.map(|pool_size| {
            let conn_manager = ConnectionManager::new(postgres_url.clone());
            let builder: Builder<ConnectionManager<PgConnection>> = Pool::builder()
                .error_handler(error_handler)
                .event_handler(event_handler)
                .connection_timeout(ENV_VARS.store.connection_timeout)
                .max_size(pool_size)
                .min_idle(Some(1))
                .idle_timeout(Some(FDW_IDLE_TIMEOUT));
            builder.build_unchecked(conn_manager)
        });

        let max_concurrent_queries = pool_size as usize + ENV_VARS.store.extra_query_permits;
        let limiter = Arc::new(Semaphore::new(max_concurrent_queries));
        info!(logger_store, "Pool successfully connected to Postgres");

        let semaphore_wait_gauge = registry
            .new_gauge(
                "query_semaphore_wait_ms",
                "Moving average of time spent on waiting for postgres query semaphore",
                const_labels,
            )
            .expect("failed to create `query_effort_ms` counter");
        let query_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_queries));
        PoolInner {
            logger: logger_pool,
            shard,
            postgres_url,
            pool,
            fdw_pool,
            limiter,
            wait_stats,
            semaphore_wait_stats: Arc::new(RwLock::new(MovingStats::default())),
            query_semaphore,
            semaphore_wait_gauge,
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
                &mut PooledConnection<ConnectionManager<PgConnection>>,
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
            let mut conn = pool
                .get()
                .map_err(|_| CancelableError::Error(StoreError::DatabaseUnavailable))?;

            // It is possible time has passed while establishing a connection.
            // Time to check for cancel.
            cancel_handle.check_cancel()?;

            f(&mut conn, &cancel_handle)
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

    pub fn get(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError> {
        self.pool.get().map_err(|_| StoreError::DatabaseUnavailable)
    }

    /// Get the pool for fdw connections. It is an error if none is configured
    fn fdw_pool(
        &self,
        logger: &Logger,
    ) -> Result<&Pool<ConnectionManager<PgConnection>>, StoreError> {
        let pool = match &self.fdw_pool {
            Some(pool) => pool,
            None => {
                const MSG: &str =
                    "internal error: trying to get fdw connection on a pool that doesn't have any";
                error!(logger, "{}", MSG);
                return Err(internal_error!(MSG));
            }
        };
        Ok(pool)
    }

    /// Get a connection from the pool for foreign data wrapper access;
    /// since that pool can be very contended, periodically log that we are
    /// still waiting for a connection
    ///
    /// The `timeout` is called every time we time out waiting for a
    /// connection. If `timeout` returns `true`, `get_fdw` returns with that
    /// error, otherwise we try again to get a connection.
    pub fn get_fdw<F>(
        &self,
        logger: &Logger,
        mut timeout: F,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError>
    where
        F: FnMut() -> bool,
    {
        let pool = self.fdw_pool(logger)?;
        loop {
            match pool.get() {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    if timeout() {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    /// Get a connection from the fdw pool if one is available. We wait for
    /// `timeout` for a connection which should be set just big enough to
    /// allow establishing a connection
    pub fn try_get_fdw(
        &self,
        logger: &Logger,
        timeout: Duration,
    ) -> Option<PooledConnection<ConnectionManager<PgConnection>>> {
        // Any error trying to get a connection is treated as "couldn't get
        // a connection in time". If there is a serious error with the
        // database, e.g., because it's not available, the next database
        // operation will run into it and report it.
        let Ok(fdw_pool) = self.fdw_pool(logger) else {
            return None;
        };
        let Ok(conn) = fdw_pool.get_timeout(timeout) else {
            return None;
        };
        Some(conn)
    }

    pub fn connection_detail(&self) -> Result<ForeignServer, StoreError> {
        ForeignServer::new(self.shard.clone(), &self.postgres_url).map_err(|e| e.into())
    }

    /// Check that we can connect to the database
    pub fn check(&self) -> bool {
        self.pool
            .get()
            .ok()
            .map(|mut conn| sql_query("select 1").execute(&mut conn).is_ok())
            .unwrap_or(false)
    }

    fn locale_check(
        &self,
        logger: &Logger,
        mut conn: PooledConnection<ConnectionManager<PgConnection>>,
    ) -> Result<(), StoreError> {
        Ok(
            if let Err(msg) = catalog::Locale::load(&mut conn)?.suitable() {
                if &self.shard == &*PRIMARY_SHARD && primary::is_empty(&mut conn)? {
                    const MSG: &str =
                    "Database does not use C locale. \
                    Please check the graph-node documentation for how to set up the database locale";

                    crit!(logger, "{}: {}", MSG, msg);
                    panic!("{}: {}", MSG, msg);
                } else {
                    warn!(logger, "{}.\nPlease check the graph-node documentation for how to set up the database locale", msg);
                }
            },
        )
    }

    pub(crate) async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        let start = Instant::now();
        let permit = self.query_semaphore.cheap_clone().acquire_owned().await;
        self.semaphore_wait_stats
            .write()
            .unwrap()
            .add_and_register(start.elapsed(), &self.semaphore_wait_gauge);
        permit.unwrap()
    }

    fn configure_fdw(&self, servers: &[ForeignServer]) -> Result<(), StoreError> {
        info!(&self.logger, "Setting up fdw");
        let mut conn = self.get()?;
        conn.batch_execute("create extension if not exists postgres_fdw")?;
        conn.transaction(|conn| {
            let current_servers: Vec<String> = crate::catalog::current_servers(conn)?;
            for server in servers.iter().filter(|server| server.shard != self.shard) {
                if current_servers.contains(&server.name) {
                    server.update(conn)?;
                } else {
                    server.create(conn)?;
                }
            }
            Ok(())
        })
    }

    /// Do the part of database setup that only affects this pool. Those
    /// steps are
    /// 1. Configuring foreign servers and user mappings for talking to the
    ///    other shards
    /// 2. Migrating the schema to the latest version
    /// 3. Checking that the locale is set to C
    async fn migrate(
        self: Arc<Self>,
        servers: &[ForeignServer],
    ) -> Result<MigrationCount, StoreError> {
        self.configure_fdw(servers)?;
        let mut conn = self.get()?;
        let (this, count) = conn.transaction(|conn| -> Result<_, StoreError> {
            let count = migrate_schema(&self.logger, conn)?;
            Ok((self, count))
        })?;

        this.locale_check(&this.logger, conn)?;

        Ok(count)
    }

    /// If this is the primary shard, drop the namespace `CROSS_SHARD_NSP`
    fn drop_cross_shard_views(&self) -> Result<(), StoreError> {
        if self.shard != *PRIMARY_SHARD {
            return Ok(());
        }

        info!(&self.logger, "Dropping cross-shard views");
        let mut conn = self.get()?;
        conn.transaction(|conn| {
            let query = format!("drop schema if exists {} cascade", CROSS_SHARD_NSP);
            conn.batch_execute(&query)?;
            Ok(())
        })
    }

    /// If this is the primary shard, create the namespace `CROSS_SHARD_NSP`
    /// and populate it with tables that union various imported tables
    fn create_cross_shard_views(&self, servers: &[ForeignServer]) -> Result<(), StoreError> {
        fn shard_nsp_pairs<'a>(
            current: &Shard,
            local_nsp: &str,
            servers: &'a [ForeignServer],
        ) -> Vec<(&'a str, String)> {
            servers
                .into_iter()
                .map(|server| {
                    let nsp = if &server.shard == current {
                        local_nsp.to_string()
                    } else {
                        ForeignServer::metadata_schema(&server.shard)
                    };
                    (server.shard.as_str(), nsp)
                })
                .collect::<Vec<_>>()
        }

        if self.shard != *PRIMARY_SHARD {
            return Ok(());
        }

        let mut conn = self.get()?;
        let sharded = Namespace::special(CROSS_SHARD_NSP);
        if catalog::has_namespace(&mut conn, &sharded)? {
            // We dropped the namespace before, but another node must have
            // recreated it in the meantime so we don't need to do anything
            return Ok(());
        }

        info!(&self.logger, "Creating cross-shard views");
        conn.transaction(|conn| {
            let query = format!("create schema {}", CROSS_SHARD_NSP);
            conn.batch_execute(&query)?;
            for (src_nsp, src_tables) in SHARDED_TABLES {
                // Pairs of (shard, nsp) for all servers
                let nsps = shard_nsp_pairs(&self.shard, src_nsp, servers);
                for src_table in src_tables {
                    let create_view = catalog::create_cross_shard_view(
                        conn,
                        src_nsp,
                        src_table,
                        CROSS_SHARD_NSP,
                        &nsps,
                    )?;
                    conn.batch_execute(&create_view)?;
                }
            }
            Ok(())
        })
    }

    /// Copy the data from key tables in the primary into our local schema
    /// so it can be used as a fallback when the primary goes down
    pub async fn mirror_primary_tables(&self) -> Result<(), StoreError> {
        if self.shard == *PRIMARY_SHARD {
            return Ok(());
        }
        self.with_conn(|conn, handle| {
            conn.transaction(|conn| {
                primary::Mirror::refresh_tables(conn, handle).map_err(CancelableError::from)
            })
        })
        .await
    }

    /// The foreign server `server` had schema changes, and we therefore
    /// need to remap anything that we are importing via fdw to make sure we
    /// are using this updated schema
    pub fn remap(&self, server: &ForeignServer) -> Result<(), StoreError> {
        if &server.shard == &*PRIMARY_SHARD {
            info!(&self.logger, "Mapping primary");
            let mut conn = self.get()?;
            conn.transaction(|conn| ForeignServer::map_primary(conn, &self.shard))?;
        }
        if &server.shard != &self.shard {
            info!(
                &self.logger,
                "Mapping metadata from {}",
                server.shard.as_str()
            );
            let mut conn = self.get()?;
            conn.transaction(|conn| server.map_metadata(conn))?;
        }
        Ok(())
    }

    pub fn needs_remap(&self, server: &ForeignServer) -> Result<bool, StoreError> {
        if &server.shard == &self.shard {
            return Ok(false);
        }

        let mut conn = self.get()?;
        server.needs_remap(&mut conn)
    }
}

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

struct MigrationCount {
    old: usize,
    new: usize,
}

impl MigrationCount {
    fn had_migrations(&self) -> bool {
        self.old != self.new
    }
}

/// Run all schema migrations.
///
/// When multiple `graph-node` processes start up at the same time, we ensure
/// that they do not run migrations in parallel by using `blocking_conn` to
/// serialize them. The `conn` is used to run the actual migration.
fn migrate_schema(logger: &Logger, conn: &mut PgConnection) -> Result<MigrationCount, StoreError> {
    use diesel_migrations::MigrationHarness;

    // Collect migration logging output
    let mut output = vec![];

    let old_count = catalog::migration_count(conn)?;
    let mut harness = HarnessWithOutput::new(conn, &mut output);

    info!(logger, "Running migrations");
    let result = harness.run_pending_migrations(MIGRATIONS);
    info!(logger, "Migrations finished");

    if let Err(e) = result {
        let msg = String::from_utf8(output).unwrap_or_else(|_| String::from("<unreadable>"));
        let mut msg = msg.trim().to_string();
        if !msg.is_empty() {
            msg = msg.replace('\n', " ");
        }

        error!(logger, "Postgres migration error"; "output" => msg);
        return Err(StoreError::Unknown(anyhow!(e.to_string())));
    } else {
        let msg = String::from_utf8(output).unwrap_or_else(|_| String::from("<unreadable>"));
        let mut msg = msg.trim().to_string();
        if !msg.is_empty() {
            msg = msg.replace('\n', " ");
        }
        debug!(logger, "Postgres migration output"; "output" => msg);
    }

    let migrations = catalog::migration_count(conn)?;

    Ok(MigrationCount {
        new: migrations,
        old: old_count,
    })
}

/// Helper to coordinate propagating schema changes from the database that
/// changes schema to all other shards so they can update their fdw mappings
/// of tables imported from that shard
pub struct PoolCoordinator {
    logger: Logger,
    pools: Mutex<HashMap<Shard, PoolState>>,
    servers: Arc<Vec<ForeignServer>>,
}

impl PoolCoordinator {
    pub fn new(logger: &Logger, servers: Arc<Vec<ForeignServer>>) -> Self {
        let logger = logger.new(o!("component" => "ConnectionPool", "component" => "Coordinator"));
        Self {
            logger,
            pools: Mutex::new(HashMap::new()),
            servers,
        }
    }

    pub fn create_pool(
        self: Arc<Self>,
        logger: &Logger,
        name: &str,
        pool_name: PoolRole,
        postgres_url: String,
        pool_size: u32,
        fdw_pool_size: Option<u32>,
        registry: Arc<MetricsRegistry>,
    ) -> ConnectionPool {
        let is_writable = !pool_name.is_replica();

        let pool = ConnectionPool::create(
            name,
            pool_name,
            postgres_url,
            pool_size,
            fdw_pool_size,
            logger,
            registry,
            self.cheap_clone(),
        );

        // Ignore non-writable pools (replicas), there is no need (and no
        // way) to coordinate schema changes with them
        if is_writable {
            self.pools
                .lock()
                .unwrap()
                .insert(pool.shard.clone(), pool.inner.cheap_clone());
        }

        pool
    }

    /// Propagate changes to the schema in `shard` to all other pools. Those
    /// other pools will then recreate any tables that they imported from
    /// `shard`. If `pool` is a new shard, we also map all other shards into
    /// it.
    ///
    /// This tries to take the migration lock and must therefore be run from
    /// code that does _not_ hold the migration lock as it will otherwise
    /// deadlock
    fn propagate(&self, pool: &PoolInner, count: MigrationCount) -> Result<(), StoreError> {
        // We need to remap all these servers into `pool` if the list of
        // tables that are mapped have changed from the code of the previous
        // version. Since dropping and recreating the foreign table
        // definitions can slow the startup of other nodes down because of
        // locking, we try to only do this when it is actually needed
        for server in self.servers.iter() {
            if pool.needs_remap(server)? {
                pool.remap(server)?;
            }
        }

        // pool had schema changes, refresh the import from pool into all
        // other shards. This makes sure that schema changes to
        // already-mapped tables are propagated to all other shards. Since
        // we run `propagate` after migrations have been applied to `pool`,
        // we can be sure that these mappings use the correct schema
        if count.had_migrations() {
            let server = self.server(&pool.shard)?;
            for pool in self.pools.lock().unwrap().values() {
                let pool = pool.get_unready();
                let remap_res = pool.remap(server);
                if let Err(e) = remap_res {
                    error!(pool.logger, "Failed to map imports from {}", server.shard; "error" => e.to_string());
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Return a list of all pools, regardless of whether they are ready or
    /// not.
    pub fn pools(&self) -> Vec<Arc<PoolInner>> {
        self.pools
            .lock()
            .unwrap()
            .values()
            .map(|state| state.get_unready())
            .collect::<Vec<_>>()
    }

    pub fn servers(&self) -> Arc<Vec<ForeignServer>> {
        self.servers.clone()
    }

    fn server(&self, shard: &Shard) -> Result<&ForeignServer, StoreError> {
        self.servers
            .iter()
            .find(|server| &server.shard == shard)
            .ok_or_else(|| internal_error!("unknown shard {shard}"))
    }

    fn primary(&self) -> Result<Arc<PoolInner>, StoreError> {
        let map = self.pools.lock().unwrap();
        let pool_state = map.get(&*&PRIMARY_SHARD).ok_or_else(|| {
            internal_error!("internal error: primary shard not found in pool coordinator")
        })?;

        Ok(pool_state.get_unready())
    }

    /// Setup all pools the coordinator knows about and return the number of
    /// pools that were successfully set up.
    ///
    /// # Panics
    ///
    /// If any errors besides a database not being available happen during
    /// the migration, the process panics
    pub async fn setup_all(&self, logger: &Logger) -> usize {
        let pools = self
            .pools
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let res = self.setup(pools).await;

        match res {
            Ok(count) => {
                info!(logger, "Setup finished"; "shards" => count);
                count
            }
            Err(e) => {
                crit!(logger, "database setup failed"; "error" => format!("{e}"));
                panic!("database setup failed: {}", e);
            }
        }
    }

    /// A helper to call `setup` from a non-async context. Returns `true` if
    /// the setup was actually run, i.e. if `pool` was available
    fn setup_bg(self: Arc<Self>, pool: PoolState) -> Result<bool, StoreError> {
        let migrated = graph::spawn_thread("database-setup", move || {
            graph::block_on(self.setup(vec![pool.clone()]))
        })
        .join()
        // unwrap: propagate panics
        .unwrap()?;
        Ok(migrated == 1)
    }

    /// Setup all pools by doing the following steps:
    /// 1. Get the migration lock in the primary. This makes sure that only
    ///    one node runs migrations
    /// 2. Remove the views in `sharded` as they might interfere with
    ///    running migrations
    /// 3. In parallel, do the following in each pool:
    ///    1. Configure fdw servers
    ///    2. Run migrations in all pools in parallel
    /// 4. In parallel, do the following in each pool:
    ///    1. Create/update the mappings in `shard_<shard>_subgraphs` and in
    ///       `primary_public`
    /// 5. Create the views in `sharded` again
    /// 6. Release the migration lock
    ///
    /// This method tolerates databases that are not available and will
    /// simply ignore them. The returned count is the number of pools that
    /// were successfully set up.
    ///
    /// When this method returns, the entries from `states` that were
    /// successfully set up will be marked as ready. The method returns the
    /// number of pools that were set up
    async fn setup(&self, states: Vec<PoolState>) -> Result<usize, StoreError> {
        type MigrationCounts = Vec<(PoolState, MigrationCount)>;

        /// Filter out pools that are not available. We don't want to fail
        /// because one of the pools is not available. We will just ignore
        /// them and continue with the others.
        fn filter_unavailable<T>(
            (state, res): (PoolState, Result<T, StoreError>),
        ) -> Option<Result<(PoolState, T), StoreError>> {
            if let Err(StoreError::DatabaseUnavailable) = res {
                error!(
                    state.logger,
                    "migrations failed because database was unavailable"
                );
                None
            } else {
                Some(res.map(|count| (state, count)))
            }
        }

        /// Migrate all pools in parallel
        async fn migrate(
            pools: &[PoolState],
            servers: &[ForeignServer],
        ) -> Result<MigrationCounts, StoreError> {
            let futures = pools
                .iter()
                .map(|state| {
                    state
                        .get_unready()
                        .cheap_clone()
                        .migrate(servers)
                        .map(|res| (state.cheap_clone(), res))
                })
                .collect::<Vec<_>>();
            join_all(futures)
                .await
                .into_iter()
                .filter_map(filter_unavailable)
                .collect::<Result<Vec<_>, _>>()
        }

        /// Propagate the schema changes to all other pools in parallel
        async fn propagate(
            this: &PoolCoordinator,
            migrated: MigrationCounts,
        ) -> Result<Vec<PoolState>, StoreError> {
            let futures = migrated
                .into_iter()
                .map(|(state, count)| async move {
                    let pool = state.get_unready();
                    let res = this.propagate(&pool, count);
                    (state.cheap_clone(), res)
                })
                .collect::<Vec<_>>();
            join_all(futures)
                .await
                .into_iter()
                .filter_map(filter_unavailable)
                .map(|res| res.map(|(state, ())| state))
                .collect::<Result<Vec<_>, _>>()
        }

        let primary = self.primary()?;

        let mut pconn = primary.get().map_err(|_| StoreError::DatabaseUnavailable)?;

        let states: Vec<_> = states
            .into_iter()
            .filter(|pool| pool.needs_setup())
            .collect();
        if states.is_empty() {
            return Ok(0);
        }

        // Everything here happens under the migration lock. Anything called
        // from here should not try to get that lock, otherwise the process
        // will deadlock
        debug!(self.logger, "Waiting for migration lock");
        let res = with_migration_lock(&mut pconn, |_| async {
            debug!(self.logger, "Migration lock acquired");

            // While we were waiting for the migration lock, another thread
            // might have already run this
            let states: Vec<_> = states
                .into_iter()
                .filter(|pool| pool.needs_setup())
                .collect();
            if states.is_empty() {
                debug!(self.logger, "No pools to set up");
                return Ok(0);
            }

            primary.drop_cross_shard_views()?;

            let migrated = migrate(&states, self.servers.as_ref()).await?;

            let propagated = propagate(&self, migrated).await?;

            primary.create_cross_shard_views(&self.servers)?;

            for state in &propagated {
                state.set_ready();
            }
            Ok(propagated.len())
        })
        .await;
        debug!(self.logger, "Database setup finished");

        res
    }
}

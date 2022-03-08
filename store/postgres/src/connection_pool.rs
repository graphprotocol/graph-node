use diesel::r2d2::Builder;
use diesel::{connection::SimpleConnection, pg::PgConnection};
use diesel::{
    r2d2::{self, event as e, ConnectionManager, HandleEvent, Pool, PooledConnection},
    Connection,
};
use diesel::{sql_query, RunQueryDsl};

use graph::cheap_clone::CheapClone;
use graph::constraint_violation;
use graph::prelude::tokio;
use graph::prelude::tokio::time::Instant;
use graph::util::timed_rw_lock::TimedMutex;
use graph::{
    prelude::{
        anyhow::{self, anyhow, bail},
        crit, debug, error, info, o,
        tokio::sync::Semaphore,
        CancelGuard, CancelHandle, CancelToken as _, CancelableError, Counter, Gauge, Logger,
        MetricsRegistry, MovingStats, PoolWaitStats, StoreError,
    },
    util::security::SafeDisplay,
};

use std::fmt::{self, Write};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, sync::RwLock};

use postgres::config::{Config, Host};

use crate::primary::{self, NAMESPACE_PUBLIC};
use crate::{advisory_lock, catalog};
use crate::{Shard, PRIMARY_SHARD};

lazy_static::lazy_static! {
    // There is typically no need to configure this. But this can be used to effectivey disable the
    // query semaphore by setting it to a high number.
    static ref EXTRA_QUERY_PERMITS: usize = {
        std::env::var("GRAPH_EXTRA_QUERY_PERMITS")
            .ok()
            .map(|s| {
                usize::from_str(&s).unwrap_or_else(|_| {
                    panic!("GRAPH_EXTRA_QUERY_PERMITS must be a number, but is `{}`", s)
                })
            })
            .unwrap_or(0)
    };

    // These environment variables should really be set through the
    // configuration file; especially for min_idle and idle_timeout, it's
    // likely that they should be configured differently for each pool

    static ref CONNECTION_TIMEOUT: Duration = {
        std::env::var("GRAPH_STORE_CONNECTION_TIMEOUT").ok().map(|s| Duration::from_millis(u64::from_str(&s).unwrap_or_else(|_| {
            panic!("GRAPH_STORE_CONNECTION_TIMEOUT must be a positive number, but is `{}`", s)
        }))).unwrap_or(Duration::from_secs(5))
    };
    static ref MIN_IDLE: Option<u32> = {
        std::env::var("GRAPH_STORE_CONNECTION_MIN_IDLE").ok().map(|s| u32::from_str(&s).unwrap_or_else(|_| {
           panic!("GRAPH_STORE_CONNECTION_MIN_IDLE must be a positive number but is `{}`", s)
        }))
    };
    static ref IDLE_TIMEOUT: Duration = {
        std::env::var("GRAPH_STORE_CONNECTION_IDLE_TIMEOUT").ok().map(|s| Duration::from_secs(u64::from_str(&s).unwrap_or_else(|_| {
            panic!("GRAPH_STORE_CONNECTION_IDLE_TIMEOUT must be a positive number, but is `{}`", s)
        }))).unwrap_or(Duration::from_secs(600))
    };
    // A fallback in case the logic to remember database availability goes
    // wrong; when this is set, we always try to get a connection and never
    // use the availability state we remembered
    static ref TRY_ALWAYS: bool = {
        std::env::var("GRAPH_STORE_CONNECTION_TRY_ALWAYS").ok().map(|_| true).unwrap_or(false)
    };
}

pub struct ForeignServer {
    pub name: String,
    pub shard: Shard,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub dbname: String,
}

impl ForeignServer {
    pub(crate) const PRIMARY_PUBLIC: &'static str = "primary_public";

    /// The name of the foreign server under which data for `shard` is
    /// accessible
    pub fn name(shard: &Shard) -> String {
        format!("shard_{}", shard.as_str())
    }

    /// The name of the schema under which the `subgraphs` schema for `shard`
    /// is accessible in shards that are not `shard`
    pub fn metadata_schema(shard: &Shard) -> String {
        format!("{}_subgraphs", Self::name(shard))
    }

    pub fn new_from_raw(shard: String, postgres_url: &str) -> Result<Self, anyhow::Error> {
        Self::new(Shard::new(shard)?, postgres_url)
    }

    pub fn new(shard: Shard, postgres_url: &str) -> Result<Self, anyhow::Error> {
        let config: Config = match postgres_url.parse() {
            Ok(config) => config,
            Err(e) => panic!(
                "failed to parse Postgres connection string `{}`: {}",
                SafeDisplay(postgres_url),
                e
            ),
        };

        let host = match config.get_hosts().get(0) {
            Some(Host::Tcp(host)) => host.to_string(),
            _ => bail!("can not find host name in `{}`", SafeDisplay(postgres_url)),
        };

        let user = config
            .get_user()
            .ok_or_else(|| anyhow!("could not find user in `{}`", SafeDisplay(postgres_url)))?
            .to_string();
        let password = String::from_utf8(
            config
                .get_password()
                .ok_or_else(|| anyhow!("could not find user in `{}`", SafeDisplay(postgres_url)))?
                .into(),
        )?;
        let port = config.get_ports().get(0).cloned().unwrap_or(5432u16);
        let dbname = config
            .get_dbname()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("could not find user in `{}`", SafeDisplay(postgres_url)))?;

        Ok(Self {
            name: Self::name(&shard),
            shard,
            user,
            password,
            host,
            port,
            dbname,
        })
    }

    /// Create a new foreign server and user mapping on `conn` for this foreign
    /// server
    fn create(&self, conn: &PgConnection) -> Result<(), StoreError> {
        let query = format!(
            "\
        create server \"{name}\"
               foreign data wrapper postgres_fdw
               options (host '{remote_host}', port '{remote_port}', dbname '{remote_db}', updatable 'false');
        create user mapping
               for current_user server \"{name}\"
               options (user '{remote_user}', password '{remote_password}');",
            name = self.name,
            remote_host = self.host,
            remote_port = self.port,
            remote_db = self.dbname,
            remote_user = self.user,
            remote_password = self.password,
        );
        Ok(conn.batch_execute(&query)?)
    }

    /// Update an existing user mapping with possibly new details
    fn update(&self, conn: &PgConnection) -> Result<(), StoreError> {
        let options = catalog::server_options(conn, &self.name)?;
        let set_or_add = |option: &str| -> &'static str {
            if options.contains_key(option) {
                "set"
            } else {
                "add"
            }
        };

        let query = format!(
            "\
        alter server \"{name}\"
              options (set host '{remote_host}', {set_port} port '{remote_port}', set dbname '{remote_db}');
        alter user mapping
              for current_user server \"{name}\"
              options (set user '{remote_user}', set password '{remote_password}');",
            name = self.name,
            remote_host = self.host,
            set_port = set_or_add("port"),
            remote_port = self.port,
            remote_db = self.dbname,
            remote_user = self.user,
            remote_password = self.password,
        );
        Ok(conn.batch_execute(&query)?)
    }

    /// Map key tables from the primary into our local schema. If we are the
    /// primary, set them up as views.
    ///
    /// We recreate this mapping on every server start so that migrations that
    /// change one of the mapped tables actually show up in the imported tables
    fn map_primary(conn: &PgConnection, shard: &Shard) -> Result<(), StoreError> {
        catalog::recreate_schema(conn, Self::PRIMARY_PUBLIC)?;

        let mut query = String::new();
        for table_name in ["deployment_schemas", "chains", "active_copies"] {
            let create_stmt = if shard == &*PRIMARY_SHARD {
                format!(
                    "create view {nsp}.{table_name} as select * from public.{table_name};",
                    nsp = Self::PRIMARY_PUBLIC,
                    table_name = table_name
                )
            } else {
                catalog::create_foreign_table(
                    conn,
                    NAMESPACE_PUBLIC,
                    table_name,
                    Self::PRIMARY_PUBLIC,
                    Self::name(&*PRIMARY_SHARD).as_str(),
                )?
            };
            write!(query, "{}", create_stmt)?;
        }
        conn.batch_execute(&query)?;
        Ok(())
    }

    /// Map the `subgraphs` schema from the foreign server `self` into the
    /// database accessible through `conn`
    fn map_metadata(&self, conn: &PgConnection) -> Result<(), StoreError> {
        let nsp = Self::metadata_schema(&self.shard);
        catalog::recreate_schema(conn, &nsp)?;
        let mut query = String::new();
        for table_name in [
            "subgraph_error",
            "dynamic_ethereum_contract_data_source",
            "table_stats",
            "subgraph_deployment_assignment",
            "subgraph",
            "subgraph_version",
        ] {
            let create_stmt =
                catalog::create_foreign_table(conn, "subgraphs", table_name, &nsp, &self.name)?;
            write!(query, "{}", create_stmt)?;
        }
        Ok(conn.batch_execute(&query)?)
    }
}

/// How long to keep connections in the `fdw_pool` around before closing
/// them on idle. This is much shorter than the default of 10 minutes.
const FDW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A pool goes through several states, and this enum tracks what state we
/// are in, together with the `state_tracker` field on `ConnectionPool`.
/// When first created, the pool is in state `Created`; once we successfully
/// called `setup` on it, it moves to state `Ready`. During use, we use the
/// r2d2 callbacks to determine if the database is available or not, and set
/// the `available` field accordingly. Tracking that allows us to fail fast
/// and avoids having to wait for a connection timeout every time we need a
/// database connection. That avoids overall undesirable states like buildup
/// of queries; instead of queueing them until the database is available,
/// they return almost immediately with an error
enum PoolState {
    /// A connection pool, and all the servers for which we need to
    /// establish fdw mappings when we call `setup` on the pool
    Created(Arc<PoolInner>, Arc<Vec<ForeignServer>>),
    /// The pool has been successfully set up
    Ready(Arc<PoolInner>),
}

#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<TimedMutex<PoolState>>,
    logger: Logger,
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

/// The name of the pool, mostly for logging, and what purpose it serves.
/// The main pool will always be called `main`, and can be used for reading
/// and writing. Replica pools can only be used for reading, and don't
/// require any setup (migrations etc.)
pub enum PoolName {
    Main,
    Replica(String),
}

impl PoolName {
    fn as_str(&self) -> &str {
        match self {
            PoolName::Main => "main",
            PoolName::Replica(name) => name,
        }
    }

    fn is_replica(&self) -> bool {
        match self {
            PoolName::Main => false,
            PoolName::Replica(_) => true,
        }
    }
}

#[derive(Clone)]
struct PoolStateTracker {
    available: Arc<AtomicBool>,
}

impl PoolStateTracker {
    fn new() -> Self {
        Self {
            available: Arc::new(AtomicBool::new(true)),
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
}

impl ConnectionPool {
    pub fn create(
        shard_name: &str,
        pool_name: PoolName,
        postgres_url: String,
        pool_size: u32,
        fdw_pool_size: Option<u32>,
        logger: &Logger,
        registry: Arc<dyn MetricsRegistry>,
        servers: Arc<Vec<ForeignServer>>,
    ) -> ConnectionPool {
        let state_tracker = PoolStateTracker::new();
        let pool = PoolInner::create(
            shard_name,
            pool_name.as_str(),
            postgres_url,
            pool_size,
            fdw_pool_size,
            logger,
            registry,
            state_tracker.clone(),
        );
        let shard = pool.shard.clone();
        let pool_state = if pool_name.is_replica() {
            PoolState::Ready(Arc::new(pool))
        } else {
            PoolState::Created(Arc::new(pool), servers)
        };
        ConnectionPool {
            inner: Arc::new(TimedMutex::new(pool_state, format!("pool-{}", shard_name))),
            logger: logger.clone(),
            shard,
            state_tracker,
        }
    }

    /// This is only used for `graphman` to ensure it doesn't run migrations
    /// or other setup steps
    pub fn skip_setup(&self) {
        let mut guard = self.inner.lock(&self.logger);
        match &*guard {
            PoolState::Created(pool, _) => *guard = PoolState::Ready(pool.clone()),
            PoolState::Ready(_) => { /* nothing to do */ }
        }
    }

    /// Return a pool that is ready, i.e., connected to the database. If the
    /// pool has not been set up yet, call `setup`. If there are any errors
    /// or the pool is marked as unavailable, return
    /// `StoreError::DatabaseUnavailable`
    fn get_ready(&self) -> Result<Arc<PoolInner>, StoreError> {
        let mut guard = self.inner.lock(&self.logger);
        if !self.state_tracker.is_available() && !*TRY_ALWAYS {
            // We know that trying to use this pool is pointless since the
            // database is not available, and will only lead to other
            // operations having to wait until the connection timeout is
            // reached. `TRY_ALWAYS` allows users to force us to try
            // regardless.
            return Err(StoreError::DatabaseUnavailable);
        }

        match &*guard {
            PoolState::Created(pool, servers) => {
                pool.setup(servers.clone())?;
                let pool2 = pool.clone();
                *guard = PoolState::Ready(pool.clone());
                self.state_tracker.mark_available();
                Ok(pool2)
            }
            PoolState::Ready(pool) => Ok(pool.clone()),
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

    pub fn connection_detail(&self) -> Result<ForeignServer, StoreError> {
        let pool = self.get_ready()?;
        ForeignServer::new(pool.shard.clone(), &pool.postgres_url).map_err(|e| e.into())
    }

    /// Check that we can connect to the database
    pub fn check(&self) -> bool {
        true
    }

    /// Setup the database for this pool. This includes configuring foreign
    /// data wrappers for cross-shard communication, and running any pending
    /// schema migrations for this database.
    ///
    /// # Panics
    ///
    /// If any errors happen during the migration, the process panics
    pub async fn setup(&self) {
        let pool = self.clone();
        graph::spawn_blocking_allow_panic(move || {
            pool.get_ready().ok();
        })
        .await
        // propagate panics
        .unwrap();
    }

    pub(crate) async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        let pool = match &*self.inner.lock(&self.logger) {
            PoolState::Created(pool, _) | PoolState::Ready(pool) => pool.clone(),
        };
        pool.query_permit().await
    }

    pub(crate) fn wait_stats(&self) -> PoolWaitStats {
        match &*self.inner.lock(&self.logger) {
            PoolState::Created(pool, _) | PoolState::Ready(pool) => pool.wait_stats.clone(),
        }
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
            || msg.contains("no connection to the server")
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
        registry: Arc<dyn MetricsRegistry>,
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
    shard: Shard,
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
        shard_name: &str,
        pool_name: &str,
        postgres_url: String,
        pool_size: u32,
        fdw_pool_size: Option<u32>,
        logger: &Logger,
        registry: Arc<dyn MetricsRegistry>,
        state_tracker: PoolStateTracker,
    ) -> PoolInner {
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
        let builder: Builder<ConnectionManager<PgConnection>> = Pool::builder()
            .error_handler(error_handler.clone())
            .event_handler(event_handler.clone())
            .connection_timeout(*CONNECTION_TIMEOUT)
            .max_size(pool_size)
            .min_idle(*MIN_IDLE)
            .idle_timeout(Some(*IDLE_TIMEOUT));
        let pool = builder.build_unchecked(conn_manager);
        let fdw_pool = fdw_pool_size.map(|pool_size| {
            let conn_manager = ConnectionManager::new(postgres_url.clone());
            let builder: Builder<ConnectionManager<PgConnection>> = Pool::builder()
                .error_handler(error_handler)
                .event_handler(event_handler)
                .connection_timeout(*CONNECTION_TIMEOUT)
                .max_size(pool_size)
                .min_idle(Some(1))
                .idle_timeout(Some(FDW_IDLE_TIMEOUT));
            builder.build_unchecked(conn_manager)
        });

        let limiter = Arc::new(Semaphore::new(pool_size as usize));
        info!(logger_store, "Pool successfully connected to Postgres");

        let semaphore_wait_gauge = registry
            .new_gauge(
                "query_semaphore_wait_ms",
                "Moving average of time spent on waiting for postgres query semaphore",
                const_labels,
            )
            .expect("failed to create `query_effort_ms` counter");
        let max_concurrent_queries = pool_size as usize + *EXTRA_QUERY_PERMITS;
        let query_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_queries));
        PoolInner {
            logger: logger_pool,
            shard: Shard::new(shard_name.to_string())
                .expect("shard_name is a valid name for a shard"),
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
                .map_err(|_| CancelableError::Error(StoreError::DatabaseUnavailable))?;

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

    pub fn get(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError> {
        self.pool.get().map_err(|_| StoreError::DatabaseUnavailable)
    }

    pub fn get_with_timeout_warning(
        &self,
        logger: &Logger,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError> {
        loop {
            match self.pool.get_timeout(*CONNECTION_TIMEOUT) {
                Ok(conn) => return Ok(conn),
                Err(e) => error!(logger, "Error checking out connection, retrying";
                   "error" => brief_error_msg(&e),
                ),
            }
        }
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
        let pool = match &self.fdw_pool {
            Some(pool) => pool,
            None => {
                const MSG: &str =
                    "internal error: trying to get fdw connection on a pool that doesn't have any";
                error!(logger, "{}", MSG);
                return Err(constraint_violation!(MSG));
            }
        };
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

    pub fn connection_detail(&self) -> Result<ForeignServer, StoreError> {
        ForeignServer::new(self.shard.clone(), &self.postgres_url).map_err(|e| e.into())
    }

    /// Check that we can connect to the database
    pub fn check(&self) -> bool {
        self.pool
            .get()
            .ok()
            .map(|conn| sql_query("select 1").execute(&conn).is_ok())
            .unwrap_or(false)
    }

    /// Setup the database for this pool. This includes configuring foreign
    /// data wrappers for cross-shard communication, and running any pending
    /// schema migrations for this database.
    ///
    /// Returns `StoreError::DatabaseUnavailable` if we can't connect to the
    /// database. Any other error causes a panic.
    ///
    /// # Panics
    ///
    /// If any errors happen during the migration, the process panics
    pub fn setup(&self, servers: Arc<Vec<ForeignServer>>) -> Result<(), StoreError> {
        fn die(logger: &Logger, msg: &'static str, err: &dyn std::fmt::Display) -> ! {
            crit!(logger, "{}", msg; "error" => err.to_string());
            panic!("{}: {}", msg, err);
        }

        let pool = self.clone();
        let conn = self.get().map_err(|_| StoreError::DatabaseUnavailable)?;

        advisory_lock::lock_migration(&conn)
            .unwrap_or_else(|err| die(&pool.logger, "failed to get migration lock", &err));
        let result = pool
            .configure_fdw(servers.as_ref())
            .and_then(|()| migrate_schema(&pool.logger, &conn))
            .and_then(|()| pool.map_primary())
            .and_then(|()| pool.map_metadata(servers.as_ref()));
        advisory_lock::unlock_migration(&conn).unwrap_or_else(|err| {
            die(&pool.logger, "failed to release migration lock", &err);
        });
        result.unwrap_or_else(|err| die(&pool.logger, "migrations failed", &err));
        Ok(())
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
        let conn = self.get()?;
        conn.batch_execute("create extension if not exists postgres_fdw")?;
        conn.transaction(|| {
            let current_servers: Vec<String> = crate::catalog::current_servers(&conn)?;
            for server in servers.iter().filter(|server| server.shard != self.shard) {
                if current_servers.contains(&server.name) {
                    server.update(&conn)?;
                } else {
                    server.create(&conn)?;
                }
            }
            Ok(())
        })
    }

    /// Map key tables from the primary into our local schema. If we are the
    /// primary, set them up as views.
    ///
    /// We recreate this mapping on every server start so that migrations that
    /// change one of the mapped tables actually show up in the imported tables
    fn map_primary(&self) -> Result<(), StoreError> {
        info!(&self.logger, "Mapping primary");
        let conn = self.get()?;
        conn.transaction(|| ForeignServer::map_primary(&conn, &self.shard))
    }

    /// Copy the data from key tables in the primary into our local schema
    /// so it can be used as a fallback when the primary goes down
    pub async fn mirror_primary_tables(&self) -> Result<(), StoreError> {
        if self.shard == *PRIMARY_SHARD {
            return Ok(());
        }
        self.with_conn(|conn, handle| {
            conn.transaction(|| {
                primary::Mirror::refresh_tables(conn, handle).map_err(CancelableError::from)
            })
        })
        .await
    }

    // Map some tables from the `subgraphs` metadata schema from foreign
    // servers to ourselves. The mapping is recreated on every server start
    // so that we pick up possible schema changes in the mappings
    fn map_metadata(&self, servers: &[ForeignServer]) -> Result<(), StoreError> {
        let conn = self.get()?;
        conn.transaction(|| {
            for server in servers.iter().filter(|server| server.shard != self.shard) {
                server.map_metadata(&conn)?;
            }
            Ok(())
        })
    }
}

embed_migrations!("./migrations");

/// Run all schema migrations.
///
/// When multiple `graph-node` processes start up at the same time, we ensure
/// that they do not run migrations in parallel by using `blocking_conn` to
/// serialize them. The `conn` is used to run the actual migration.
fn migrate_schema(logger: &Logger, conn: &PgConnection) -> Result<(), StoreError> {
    // Collect migration logging output
    let mut output = vec![];

    info!(logger, "Running migrations");
    let result = embedded_migrations::run_with_output(conn, &mut output);
    info!(logger, "Migrations finished");

    // If there was any migration output, log it now
    let msg = String::from_utf8(output).unwrap_or_else(|_| String::from("<unreadable>"));
    let msg = msg.trim();
    let has_output = !msg.is_empty();
    if has_output {
        let msg = msg.replace('\n', " ");
        if let Err(e) = result {
            error!(logger, "Postgres migration error"; "output" => msg);
            return Err(StoreError::Unknown(e.into()));
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

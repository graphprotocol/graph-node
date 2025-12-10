use deadpool::managed::{PoolError, Timeouts};
use deadpool::Runtime;
use diesel::sql_query;
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::{AsyncConnection as _, RunQueryDsl, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, HarnessWithOutput};

use graph::cheap_clone::CheapClone;
use graph::components::store::QueryPermit;
use graph::derive::CheapClone;
use graph::internal_error;
use graph::prelude::tokio::time::Instant;
use graph::prelude::{
    anyhow::anyhow, crit, debug, error, info, o, Gauge, Logger, MovingStats, PoolWaitStats,
    StoreError, ENV_VARS,
};
use graph::prelude::{tokio, MetricsRegistry};
use graph::slog::warn;
use graph::util::timed_rw_lock::TimedMutex;

use std::fmt::{self};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, sync::RwLock};

use crate::catalog;
use crate::pool::manager::{ConnectionManager, WaitMeter};
use crate::primary::{self, Mirror, Namespace};
use crate::{Shard, PRIMARY_SHARD};

mod coordinator;
mod foreign_server;
mod manager;

pub use diesel_async::scoped_futures::ScopedFutureExt;

pub use coordinator::PoolCoordinator;
pub use foreign_server::ForeignServer;
use manager::StateTracker;

type AsyncPool = deadpool::managed::Pool<ConnectionManager>;
/// A database connection for asynchronous diesel operations
pub type AsyncPgConnection = deadpool::managed::Object<ConnectionManager>;

/// A database connection bundled with a semaphore permit.
/// The permit is held for the lifetime of the connection, providing
/// backpressure to prevent pool exhaustion during mass operations.
pub struct PermittedConnection {
    conn: AsyncPgConnection,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Deref for PermittedConnection {
    type Target = AsyncPgConnection;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for PermittedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

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
            "head",
            "deployment",
            "subgraph_error",
            "subgraph_manifest",
            "table_stats",
            "subgraph",
            "subgraph_version",
            "subgraph_deployment_assignment",
            "prune_state",
            "prune_table_state",
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
pub(super) struct PoolState {
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
            error!(self.logger, "Database is not available, setup did not work");
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
    state_tracker: StateTracker,
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
        let shard =
            Shard::new(shard_name.to_string()).expect("shard_name is a valid name for a shard");
        let (inner, state_tracker) = {
            let pool = PoolInner::create(
                shard.clone(),
                pool_name.as_str(),
                postgres_url,
                pool_size,
                fdw_pool_size,
                logger,
                registry,
            );
            let state_tracker = pool.state_tracker.clone();
            if pool_name.is_replica() {
                (PoolState::ready(Arc::new(pool)), state_tracker)
            } else {
                (PoolState::created(Arc::new(pool), coord), state_tracker)
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

    pub async fn get(&self) -> Result<AsyncPgConnection, StoreError> {
        self.get_ready()?.get().await
    }

    /// Get a connection with backpressure via semaphore permit. Use this
    /// for indexing operations to prevent pool exhaustion. This method will
    /// wait indefinitely until a permit, and with that, a connection is
    /// available.
    pub async fn get_permitted(&self) -> Result<PermittedConnection, StoreError> {
        self.get_ready()?.get_permitted().await
    }

    /// Get a connection from the pool for foreign data wrapper access;
    /// since that pool can be very contended, periodically log that we are
    /// still waiting for a connection
    ///
    /// The `timeout` is called every time we time out waiting for a
    /// connection. If `timeout` returns `true`, `get_fdw` returns with that
    /// error, otherwise we try again to get a connection.
    pub async fn get_fdw<F>(
        &self,
        logger: &Logger,
        timeout: F,
    ) -> Result<AsyncPgConnection, StoreError>
    where
        F: FnMut() -> bool,
    {
        self.get_ready()?.get_fdw(logger, timeout).await
    }

    /// Get a connection from the pool for foreign data wrapper access if
    /// one is available
    pub async fn try_get_fdw(
        &self,
        logger: &Logger,
        timeout: Duration,
    ) -> Option<AsyncPgConnection> {
        let Ok(inner) = self.get_ready() else {
            return None;
        };
        self.state_tracker
            .ignore_timeout(|| inner.try_get_fdw(logger, timeout))
            .await
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
        self.inner.get_unready().wait_meter.wait_stats.cheap_clone()
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

pub struct PoolInner {
    logger: Logger,
    pub shard: Shard,
    pool: AsyncPool,
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
    fdw_pool: Option<AsyncPool>,
    postgres_url: String,
    /// Measures how long we spend getting connections from the main `pool`
    wait_meter: WaitMeter,
    state_tracker: StateTracker,

    // Limits the number of graphql queries that may execute concurrently. Since one graphql query
    // may require multiple DB queries, it is useful to organize the queue at the graphql level so
    // that waiting queries consume few resources. Still this is placed here because the semaphore
    // is sized acording to the DB connection pool size.
    query_semaphore: Arc<tokio::sync::Semaphore>,
    semaphore_wait_stats: Arc<RwLock<MovingStats>>,
    semaphore_wait_gauge: Box<Gauge>,

    // Limits concurrent indexing operations to prevent pool exhaustion
    // during mass subgraph startup or high write load. Provides
    // backpressure similar to the old `with_conn` limiter that was removed
    // during diesel-async migration. It also avoids timeouts because of
    // pool exhaustion when getting a connection.
    indexing_semaphore: Arc<tokio::sync::Semaphore>,
    indexing_semaphore_wait_stats: Arc<RwLock<MovingStats>>,
    indexing_semaphore_wait_gauge: Box<Gauge>,
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

        let state_tracker = StateTracker::new(logger_pool.cheap_clone());

        // Connect to Postgres
        let conn_manager = ConnectionManager::new(
            logger_pool.clone(),
            postgres_url.clone(),
            state_tracker.clone(),
            &registry,
            const_labels.clone(),
        );

        let timeouts = Timeouts {
            wait: Some(ENV_VARS.store.connection_timeout),
            create: Some(ENV_VARS.store.connection_timeout),
            recycle: Some(ENV_VARS.store.connection_timeout),
        };

        // The post_create and post_recycle hooks are only called when
        // create and recycle succeed; we can therefore mark the pool
        // available
        let pool = AsyncPool::builder(conn_manager.clone())
            .max_size(pool_size as usize)
            .timeouts(timeouts)
            .runtime(Runtime::Tokio1)
            .post_create(state_tracker.mark_available_hook())
            .post_recycle(state_tracker.mark_available_hook())
            .build()
            .expect("failed to create connection pool");

        manager::spawn_size_stat_collector(pool.clone(), &registry, const_labels.clone());

        let wait_meter = WaitMeter::new(&registry, const_labels.clone());

        manager::spawn_connection_reaper(
            pool.clone(),
            ENV_VARS.store.connection_idle_timeout,
            Some(wait_meter.wait_gauge.clone()),
        );

        let fdw_pool = fdw_pool_size.map(|pool_size| {
            let fdw_timeouts = Timeouts {
                wait: Some(ENV_VARS.store.connection_timeout),
                create: None,
                recycle: Some(FDW_IDLE_TIMEOUT),
            };

            let fdw_pool = AsyncPool::builder(conn_manager)
                .max_size(pool_size as usize)
                .timeouts(fdw_timeouts)
                .runtime(Runtime::Tokio1)
                .post_create(state_tracker.mark_available_hook())
                .post_recycle(state_tracker.mark_available_hook())
                .build()
                .expect("failed to create fdw connection pool");

            manager::spawn_connection_reaper(fdw_pool.clone(), FDW_IDLE_TIMEOUT, None);
            fdw_pool
        });

        info!(logger_store, "Pool successfully connected to Postgres");

        let max_concurrent_queries = pool_size as usize + ENV_VARS.store.extra_query_permits;

        // Query semaphore for GraphQL queries
        let semaphore_wait_gauge = registry
            .new_gauge(
                "query_semaphore_wait_ms",
                "Moving average of time spent on waiting for postgres query semaphore",
                const_labels.clone(),
            )
            .expect("failed to create `query_effort_ms` counter");
        let query_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_queries));

        // Indexing semaphore for indexing/write operations
        let indexing_semaphore_wait_gauge = registry
            .new_gauge(
                "indexing_semaphore_wait_ms",
                "Moving average of time spent waiting for indexing semaphore",
                const_labels,
            )
            .expect("failed to create indexing_semaphore_wait_ms gauge");
        let indexing_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_queries));

        PoolInner {
            logger: logger_pool,
            shard,
            postgres_url,
            pool,
            fdw_pool,
            wait_meter,
            state_tracker,
            semaphore_wait_stats: Arc::new(RwLock::new(MovingStats::default())),
            query_semaphore,
            semaphore_wait_gauge,
            indexing_semaphore,
            indexing_semaphore_wait_stats: Arc::new(RwLock::new(MovingStats::default())),
            indexing_semaphore_wait_gauge,
        }
    }

    /// Helper so that getting a connection from the main pool and the
    /// fdw_pool collect the same metrics.
    ///
    /// If `timeouts` is `None`, the default pool timeouts are used.
    ///
    /// On error, returns `StoreError::DatabaseUnavailable` and marks the
    /// pool as unavailable if we can tell that the error is due to the pool
    /// being closed. Returns `StoreError::StatementTimeout` if the error is
    /// due to a timeout.
    async fn get_from_pool(
        &self,
        pool: &AsyncPool,
        timeouts: Option<Timeouts>,
    ) -> Result<AsyncPgConnection, StoreError> {
        let start = Instant::now();
        let res = match timeouts {
            Some(timeouts) => pool.timeout_get(&timeouts).await,
            None => pool.get().await,
        };
        let elapsed = start.elapsed();
        self.wait_meter.add_conn_wait_time(elapsed);
        match res {
            Ok(conn) => {
                self.state_tracker.mark_available();
                return Ok(conn);
            }
            Err(PoolError::Closed) | Err(PoolError::Backend(_)) => {
                self.state_tracker.mark_unavailable(Duration::from_nanos(0));
                return Err(StoreError::DatabaseUnavailable);
            }
            Err(PoolError::Timeout(_)) => {
                if !self.state_tracker.timeout_is_ignored() {
                    self.state_tracker.mark_unavailable(elapsed);
                }
                return Err(StoreError::StatementTimeout);
            }
            Err(PoolError::NoRuntimeSpecified) | Err(PoolError::PostCreateHook(_)) => {
                let e = res.err().unwrap();
                unreachable!("impossible error {e}");
            }
        }
    }

    async fn get(&self) -> Result<AsyncPgConnection, StoreError> {
        self.get_from_pool(&self.pool, None).await
    }

    /// Get the pool for fdw connections. It is an error if none is configured
    fn fdw_pool(&self, logger: &Logger) -> Result<&AsyncPool, StoreError> {
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
    async fn get_fdw<F>(
        &self,
        logger: &Logger,
        mut timeout: F,
    ) -> Result<AsyncPgConnection, StoreError>
    where
        F: FnMut() -> bool,
    {
        let pool = self.fdw_pool(logger)?;
        loop {
            match self.get_from_pool(&pool, None).await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    if timeout() {
                        return Err(anyhow!("timeout in get_fdw: {e}").into());
                    }
                }
            }
        }
    }

    /// Get a connection from the fdw pool if one is available. We wait for
    /// `timeout` for a connection which should be set just big enough to
    /// allow establishing a connection
    async fn try_get_fdw(&self, logger: &Logger, timeout: Duration) -> Option<AsyncPgConnection> {
        // Any error trying to get a connection is treated as "couldn't get
        // a connection in time". If there is a serious error with the
        // database, e.g., because it's not available, the next database
        // operation will run into it and report it.
        let Ok(fdw_pool) = self.fdw_pool(logger) else {
            return None;
        };
        let timeouts = Timeouts {
            wait: Some(timeout),
            create: None,
            recycle: None,
        };
        let Ok(conn) = self.get_from_pool(fdw_pool, Some(timeouts)).await else {
            return None;
        };
        Some(conn)
    }

    pub fn connection_detail(&self) -> Result<ForeignServer, StoreError> {
        ForeignServer::new(self.shard.clone(), &self.postgres_url).map_err(|e| e.into())
    }

    /// Check that we can connect to the database
    pub async fn check(&self) -> bool {
        let Ok(mut conn) = self.get().await else {
            return false;
        };

        sql_query("select 1").execute(&mut conn).await.is_ok()
    }

    async fn locale_check(&self, logger: &Logger) -> Result<(), StoreError> {
        let mut conn = self.get().await?;
        Ok(
            if let Err(msg) = catalog::Locale::load(&mut conn).await?.suitable() {
                if &self.shard == &*PRIMARY_SHARD && primary::is_empty(&mut conn).await? {
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

    /// Acquire a permit for indexing operations. This provides backpressure
    /// to prevent connection pool exhaustion during mass subgraph startup
    /// or high write load.
    async fn indexing_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        let start = Instant::now();
        let permit = self.indexing_semaphore.cheap_clone().acquire_owned().await;
        self.indexing_semaphore_wait_stats
            .write()
            .unwrap()
            .add_and_register(start.elapsed(), &self.indexing_semaphore_wait_gauge);
        permit.unwrap()
    }

    /// Get a connection with backpressure via semaphore permit. Use this
    /// for indexing operations to prevent pool exhaustion. This method will
    /// wait indefinitely until a permit, and with that, a connection is
    /// available.
    pub(crate) async fn get_permitted(&self) -> Result<PermittedConnection, StoreError> {
        let permit = self.indexing_permit().await;
        let conn = self.get().await?;
        Ok(PermittedConnection {
            conn,
            _permit: permit,
        })
    }

    async fn configure_fdw(&self, servers: &[ForeignServer]) -> Result<(), StoreError> {
        info!(&self.logger, "Setting up fdw");
        let mut conn = self.get().await?;
        conn.batch_execute("create extension if not exists postgres_fdw")
            .await?;
        conn.transaction(|conn| {
            async {
                let current_servers: Vec<String> = crate::catalog::current_servers(conn).await?;
                for server in servers.iter().filter(|server| server.shard != self.shard) {
                    if current_servers.contains(&server.name) {
                        server.update(conn).await?;
                    } else {
                        server.create(conn).await?;
                    }
                }
                Ok(())
            }
            .scope_boxed()
        })
        .await
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
        self.locale_check(&self.logger).await?;

        self.configure_fdw(servers).await?;

        // We use AsyncConnectionWrapper here since diesel_async doesn't
        // offer a truly async way to run migrations, and we need to be very
        // careful that block_on only gets called on a blocking thread to
        // avoid errors from the tokio runtime
        let logger = self.logger.cheap_clone();
        let mut conn = self.get().await.map(AsyncConnectionWrapper::from)?;

        tokio::task::spawn_blocking(move || {
            diesel::Connection::transaction::<_, StoreError, _>(&mut conn, |conn| {
                migrate_schema(&logger, conn)
            })
        })
        .await
        .expect("migration task panicked")
    }

    /// If this is the primary shard, drop the namespace `CROSS_SHARD_NSP`
    async fn drop_cross_shard_views(&self) -> Result<(), StoreError> {
        if self.shard != *PRIMARY_SHARD {
            return Ok(());
        }

        info!(&self.logger, "Dropping cross-shard views");
        let mut conn = self.get().await?;
        conn.transaction(|conn| {
            async {
                let query = format!("drop schema if exists {} cascade", CROSS_SHARD_NSP);
                conn.batch_execute(&query).await?;
                Ok(())
            }
            .scope_boxed()
        })
        .await
    }

    /// If this is the primary shard, create the namespace `CROSS_SHARD_NSP`
    /// and populate it with tables that union various imported tables
    async fn create_cross_shard_views(&self, servers: &[ForeignServer]) -> Result<(), StoreError> {
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

        let mut conn = self.get().await?;
        let sharded = Namespace::special(CROSS_SHARD_NSP);
        if catalog::has_namespace(&mut conn, &sharded).await? {
            // We dropped the namespace before, but another node must have
            // recreated it in the meantime so we don't need to do anything
            return Ok(());
        }

        info!(&self.logger, "Creating cross-shard views");
        conn.transaction(|conn| {
            async {
                let query = format!("create schema {}", CROSS_SHARD_NSP);
                conn.batch_execute(&query).await?;
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
                        )
                        .await?;
                        conn.batch_execute(&create_view).await?;
                    }
                }
                Ok(())
            }
            .scope_boxed()
        })
        .await
    }

    /// Copy the data from key tables in the primary into our local schema
    /// so it can be used as a fallback when the primary goes down
    pub async fn mirror_primary_tables(&self) -> Result<(), StoreError> {
        if self.shard == *PRIMARY_SHARD {
            return Ok(());
        }
        let mut conn = self.get().await?;
        conn.transaction(|conn| primary::Mirror::refresh_tables(conn).scope_boxed())
            .await
    }

    /// The foreign server `server` had schema changes, and we therefore
    /// need to remap anything that we are importing via fdw to make sure we
    /// are using this updated schema
    pub async fn remap(&self, server: &ForeignServer) -> Result<(), StoreError> {
        if &server.shard == &*PRIMARY_SHARD {
            info!(&self.logger, "Mapping primary");
            let mut conn = self.get().await?;
            conn.transaction(|conn| ForeignServer::map_primary(conn, &self.shard).scope_boxed())
                .await?;
        }
        if &server.shard != &self.shard {
            info!(
                &self.logger,
                "Mapping metadata from {}",
                server.shard.as_str()
            );
            let mut conn = self.get().await?;
            conn.transaction(|conn| server.map_metadata(conn).scope_boxed())
                .await?;
        }
        Ok(())
    }

    pub async fn needs_remap(&self, server: &ForeignServer) -> Result<bool, StoreError> {
        if &server.shard == &self.shard {
            return Ok(false);
        }

        let mut conn = self.get().await?;
        server.needs_remap(&mut conn).await
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
fn migrate_schema(
    logger: &Logger,
    conn: &mut AsyncConnectionWrapper<AsyncPgConnection>,
) -> Result<MigrationCount, StoreError> {
    use diesel_migrations::MigrationHarness;

    // Collect migration logging output
    let mut output = vec![];

    let old_count = graph::block_on(catalog::migration_count(conn))?;
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

    let migrations = graph::block_on(catalog::migration_count(conn))?;

    Ok(MigrationCount {
        new: migrations,
        old: old_count,
    })
}

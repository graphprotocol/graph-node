use diesel::{connection::SimpleConnection, pg::PgConnection};
use diesel::{
    r2d2::{self, event as e, ConnectionManager, HandleEvent, Pool, PooledConnection},
    Connection,
};

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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, sync::RwLock};

use postgres::config::{Config, Host};

use crate::advisory_lock;
use crate::{Shard, PRIMARY_SHARD};

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
    const PRIMARY_PUBLIC: &'static str = "primary_public";

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

    fn new(shard: Shard, postgres_url: &str) -> Result<Self, anyhow::Error> {
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
               options (host '{remote_host}', dbname '{remote_db}', updatable 'false');
        create user mapping
               for current_user server \"{name}\"
               options (user '{remote_user}', password '{remote_password}');",
            name = self.name,
            remote_host = self.host,
            remote_db = self.dbname,
            remote_user = self.user,
            remote_password = self.password,
        );
        Ok(conn.batch_execute(&query)?)
    }

    /// Update an existing user mapping with possibly new details
    fn update(&self, conn: &PgConnection) -> Result<(), StoreError> {
        let query = format!(
            "\
        alter server \"{name}\"
              options (set host '{remote_host}', set dbname '{remote_db}');
        alter user mapping
              for current_user server \"{name}\"
              options (set user '{remote_user}', set password '{remote_password}');",
            name = self.name,
            remote_host = self.host,
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
        let query = format!(
            "drop schema if exists {nsp} cascade;
            create schema {nsp};",
            nsp = Self::PRIMARY_PUBLIC
        );
        conn.batch_execute(&query)?;

        let query = if shard == &*PRIMARY_SHARD {
            format!(
                "create view {nsp}.deployment_schemas as
                        select * from public.deployment_schemas;
                 create view {nsp}.chains as
                        select * from public.chains;
                 create view {nsp}.active_copies as
                        select * from public.active_copies;",
                nsp = Self::PRIMARY_PUBLIC
            )
        } else {
            format!(
                "import foreign schema public
                        limit to (deployment_schemas, chains, active_copies) \
                        from server {shard} into {nsp};",
                shard = Self::name(&*PRIMARY_SHARD),
                nsp = Self::PRIMARY_PUBLIC
            )
        };
        conn.batch_execute(&query)?;
        Ok(())
    }

    /// Map the `subgraphs` schema from the foreign server `self` into the
    /// database accessible through `conn`
    fn map_metadata(&self, conn: &PgConnection) -> Result<(), StoreError> {
        let query = format!(
            "drop schema if exists {nsp} cascade;\
             create schema {nsp};
             import foreign schema subgraphs from server {srvname} into {nsp};",
            nsp = Self::metadata_schema(&self.shard),
            srvname = self.name
        );
        Ok(conn.batch_execute(&query)?)
    }
}

/// How long to keep connections in the `fdw_pool` around before closing
/// them on idle. This is much shorter than the default of 10 minutes.
const FDW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct ConnectionPool {
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
}

#[derive(Clone)]
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

#[derive(Clone)]
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
        fdw_pool_size: Option<u32>,
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
            .error_handler(error_handler.clone())
            .event_handler(event_handler.clone())
            .connection_timeout(Duration::from_secs(timeout_seconds))
            .max_size(pool_size)
            .build(conn_manager)
            .unwrap();
        let fdw_pool = fdw_pool_size.map(|pool_size| {
            let conn_manager = ConnectionManager::new(postgres_url.clone());
            Pool::builder()
                .error_handler(error_handler)
                .event_handler(event_handler)
                .max_size(pool_size)
                .min_idle(Some(1))
                .idle_timeout(Some(FDW_IDLE_TIMEOUT))
                .build(conn_manager)
                .unwrap()
        });

        let limiter = Arc::new(Semaphore::new(pool_size as usize));
        info!(logger_store, "Pool successfully connected to Postgres");
        ConnectionPool {
            logger: logger_pool,
            shard: Shard::new(shard_name.to_string())
                .expect("shard_name is a valid name for a shard"),
            postgres_url: postgres_url.clone(),
            pool,
            fdw_pool,
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
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, graph::prelude::Error>
    where
        F: FnMut() -> bool,
    {
        let pool = match &self.fdw_pool {
            Some(pool) => pool,
            None => {
                const MSG: &str =
                    "internal error: trying to get fdw connection on a pool that doesn't have any";
                error!(logger, "{}", MSG);
                bail!(MSG)
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

    /// Setup the database for this pool. This includes configuring foreign
    /// data wrappers for cross-shard communication, and running any pending
    /// schema migrations for this database.
    ///
    /// # Panics
    ///
    /// If any errors happen during the migration, the process panics
    pub async fn setup(&self, servers: Arc<Vec<ForeignServer>>) {
        fn die(logger: &Logger, msg: &'static str, err: &dyn std::fmt::Display) -> ! {
            crit!(logger, "{}", msg; "error" => err.to_string());
            panic!("{}: {}", msg, err);
        }

        let pool = self.clone();
        let res = self
            .with_conn(move |conn, _| {
                advisory_lock::lock_migration(conn)
                    .unwrap_or_else(|err| die(&pool.logger, "failed to get migration lock", &err));
                let result = pool
                    .configure_fdw(servers.as_ref())
                    .and_then(|()| migrate_schema(&pool.logger, conn))
                    .and_then(|()| pool.map_primary())
                    .and_then(|()| pool.map_metadata(servers.as_ref()));
                advisory_lock::unlock_migration(conn).unwrap_or_else(|err| {
                    die(&pool.logger, "failed to release migration lock", &err);
                });
                result.unwrap_or_else(|err| die(&pool.logger, "migrations failed", &err));
                Ok(())
            })
            .await;
        res.unwrap_or_else(|err| die(&self.logger, "migrations were canceled", &err))
    }

    fn configure_fdw(&self, servers: &Vec<ForeignServer>) -> Result<(), StoreError> {
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

    // Map the `subgraphs` metadata schema from foreign servers to
    // ourselves. The mapping is recreated on every server start so that we
    // pick up possible schema changes in the mappings
    fn map_metadata(&self, servers: &Vec<ForeignServer>) -> Result<(), StoreError> {
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

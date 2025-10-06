use graph::cheap_clone::CheapClone;
use graph::futures03::future::join_all;
use graph::futures03::FutureExt as _;
use graph::internal_error;
use graph::prelude::MetricsRegistry;
use graph::prelude::{crit, debug, error, info, o, StoreError};
use graph::slog::Logger;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::advisory_lock::with_migration_lock;
use crate::{Shard, PRIMARY_SHARD};

use super::{ConnectionPool, ForeignServer, MigrationCount, PoolInner, PoolRole, PoolState};

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
    async fn propagate(&self, pool: &PoolInner, count: MigrationCount) -> Result<(), StoreError> {
        // We need to remap all these servers into `pool` if the list of
        // tables that are mapped have changed from the code of the previous
        // version. Since dropping and recreating the foreign table
        // definitions can slow the startup of other nodes down because of
        // locking, we try to only do this when it is actually needed
        for server in self.servers.iter() {
            if pool.needs_remap(server).await? {
                pool.remap(server).await?;
            }
        }

        // pool had schema changes, refresh the import from pool into all
        // other shards. This makes sure that schema changes to
        // already-mapped tables are propagated to all other shards. Since
        // we run `propagate` after migrations have been applied to `pool`,
        // we can be sure that these mappings use the correct schema
        if count.had_migrations() {
            let server = self.server(&pool.shard)?;
            for pool in self.pools() {
                let remap_res = pool.remap(&server).await;
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
    pub(crate) fn setup_bg(self: Arc<Self>, pool: PoolState) -> Result<bool, StoreError> {
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
                    let res = this.propagate(&pool, count).await;
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

        let mut pconn = primary.get().await?;

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

            primary.drop_cross_shard_views().await?;

            let migrated = migrate(&states, self.servers.as_ref()).await?;

            let propagated = propagate(&self, migrated).await?;

            primary.create_cross_shard_views(&self.servers).await?;

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

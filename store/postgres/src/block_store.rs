use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::anyhow;
use diesel::{
    query_dsl::methods::FilterDsl as _,
    r2d2::{ConnectionManager, PooledConnection},
    sql_query, ExpressionMethods as _, PgConnection, RunQueryDsl,
};
use graph::{
    blockchain::ChainIdentifier,
    components::{
        adapter::ChainId,
        store::{BlockStore as BlockStoreTrait, QueryPermit},
    },
    prelude::{error, info, BlockNumber, BlockPtr, Logger, ENV_VARS},
    slog::o,
};
use graph::{constraint_violation, prelude::CheapClone};
use graph::{prelude::StoreError, util::timed_cache::TimedCache};

use crate::{
    chain_head_listener::ChainHeadUpdateSender,
    chain_store::{ChainStoreMetrics, Storage},
    connection_pool::ConnectionPool,
    primary::Mirror as PrimaryMirror,
    ChainStore, NotificationSender, Shard, PRIMARY_SHARD,
};

use self::primary::Chain;

#[cfg(debug_assertions)]
pub const FAKE_NETWORK_SHARED: &str = "fake_network_shared";

/// The status of a chain: whether we can only read from the chain, or
/// whether it is ok to ingest from it, too
#[derive(Copy, Clone)]
pub enum ChainStatus {
    ReadOnly,
    Ingestible,
}

pub mod primary {
    use std::convert::TryFrom;

    use diesel::{
        delete, insert_into,
        r2d2::{ConnectionManager, PooledConnection},
        update, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
    };
    use graph::{
        blockchain::{BlockHash, ChainIdentifier},
        constraint_violation,
        prelude::StoreError,
    };

    use crate::chain_store::Storage;
    use crate::{connection_pool::ConnectionPool, Shard};

    table! {
            chains(id) {
                id                 -> Integer,
                name               -> Text,
                net_version        -> Text,
                genesis_block_hash -> Text,
                shard              -> Text,
                namespace          -> Text,
        }
    }

    /// Information about the mapping of chains to storage shards. We persist
    /// this information in the database to make it possible to detect a
    /// change in the configuration file that doesn't match what is in the database
    #[derive(Clone, Queryable)]
    pub struct Chain {
        pub id: i32,
        pub name: String,
        pub net_version: String,
        pub genesis_block: String,
        pub shard: Shard,
        pub storage: Storage,
    }

    impl Chain {
        pub fn network_identifier(&self) -> Result<ChainIdentifier, StoreError> {
            Ok(ChainIdentifier {
                net_version: self.net_version.clone(),
                genesis_block_hash: BlockHash::try_from(self.genesis_block.as_str()).map_err(
                    |e| {
                        constraint_violation!(
                            "the genesis block hash `{}` for chain `{}` is not a valid hash: {}",
                            self.genesis_block,
                            self.name,
                            e
                        )
                    },
                )?,
            })
        }
    }

    pub fn load_chains(conn: &mut PgConnection) -> Result<Vec<Chain>, StoreError> {
        Ok(chains::table.load(conn)?)
    }

    pub fn find_chain(conn: &mut PgConnection, name: &str) -> Result<Option<Chain>, StoreError> {
        Ok(chains::table
            .filter(chains::name.eq(name))
            .first(conn)
            .optional()?)
    }

    pub fn add_chain(
        conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
        name: &str,
        shard: &Shard,
        ident: ChainIdentifier,
    ) -> Result<Chain, StoreError> {
        // For tests, we want to have a chain that still uses the
        // shared `ethereum_blocks` table
        #[cfg(debug_assertions)]
        if name == super::FAKE_NETWORK_SHARED {
            insert_into(chains::table)
                .values((
                    chains::name.eq(name),
                    chains::namespace.eq("public"),
                    chains::net_version.eq(&ident.net_version),
                    chains::genesis_block_hash.eq(ident.genesis_block_hash.hash_hex()),
                    chains::shard.eq(shard.as_str()),
                ))
                .returning(chains::namespace)
                .get_result::<Storage>(conn)
                .map_err(StoreError::from)?;
            return Ok(chains::table.filter(chains::name.eq(name)).first(conn)?);
        }

        insert_into(chains::table)
            .values((
                chains::name.eq(name),
                chains::net_version.eq(&ident.net_version),
                chains::genesis_block_hash.eq(ident.genesis_block_hash.hash_hex()),
                chains::shard.eq(shard.as_str()),
            ))
            .returning(chains::namespace)
            .get_result::<Storage>(conn)
            .map_err(StoreError::from)?;
        Ok(chains::table.filter(chains::name.eq(name)).first(conn)?)
    }

    pub(super) fn drop_chain(pool: &ConnectionPool, name: &str) -> Result<(), StoreError> {
        let mut conn = pool.get()?;

        delete(chains::table.filter(chains::name.eq(name))).execute(&mut conn)?;
        Ok(())
    }

    // update chain name where chain name is 'name'
    pub fn update_chain_name(
        conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
        name: &str,
        new_name: &str,
    ) -> Result<(), StoreError> {
        update(chains::table.filter(chains::name.eq(name)))
            .set(chains::name.eq(new_name))
            .execute(conn)?;
        Ok(())
    }

    pub fn update_chain_genesis_hash(
        conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
        name: &str,
        hash: BlockHash,
    ) -> Result<(), StoreError> {
        update(chains::table.filter(chains::name.eq(name)))
            .set(chains::genesis_block_hash.eq(hash.hash_hex()))
            .execute(conn)?;
        Ok(())
    }
}

/// The store that chains use to maintain their state and cache often used
/// data from a chain. The `BlockStore` maintains information about all
/// configured chains, and serves as a directory of chains, whereas the
/// [ChainStore] manages chain-specific data. The `BlockStore` is sharded so
/// that each chain can use, depending on configuration, a separate database.
/// Regardless of configuration, the data for each chain is stored in its
/// own database namespace, though for historical reasons, the code also deals
/// with a shared table for the block and call cache for chains in the
/// `public` namespace.
///
/// The `BlockStore` uses the table `public.chains` to keep immutable
/// information about each chain, including a mapping from the chain name
/// to the shard holding the chain specific data, and the database namespace
/// for that chain.
///
/// Chains are identified by the name with which they are configured in the
/// configuration file. Once a chain has been used with the system, it is
/// not possible to change its configuration, in particular, the database
/// shard and namespace, and the genesis block and net version must not
/// change between runs of `graph-node`
pub struct BlockStore {
    logger: Logger,
    /// Map chain names to the corresponding store. This map is updated
    /// dynamically with new chains if an operation would require a chain
    /// that is not yet in `stores`. It is initialized with all chains
    /// known to the system at startup, either from configuration or from
    /// previous state in the database.
    stores: RwLock<HashMap<String, Arc<ChainStore>>>,
    // We keep this information so we can create chain stores during startup
    shards: Vec<(String, Shard)>,
    pools: HashMap<Shard, ConnectionPool>,
    sender: Arc<NotificationSender>,
    mirror: PrimaryMirror,
    chain_head_cache: TimedCache<String, HashMap<String, BlockPtr>>,
    chain_store_metrics: Arc<ChainStoreMetrics>,
}

impl BlockStore {
    /// Create a new `BlockStore` by creating a `ChainStore` for each entry
    /// in `networks`. The creation process checks that the configuration for
    /// existing chains has not changed from the last time `graph-node` ran, and
    /// creates new entries in its chain directory for chains we had not used
    /// previously. It also creates a `ChainStore` for each chain that was used
    /// in previous runs of the node, regardless of whether it is mentioned in
    /// `chains` to ensure that queries against such chains will succeed.
    ///
    /// Each entry in `chains` gives the chain name, the network identifier,
    /// and the name of the database shard for the chain. The `ChainStore` for
    /// a chain uses the pool from `pools` for the given shard.
    pub fn new(
        logger: Logger,
        // (network, shard)
        shards: Vec<(String, Shard)>,
        // shard -> pool
        pools: HashMap<Shard, ConnectionPool>,
        sender: Arc<NotificationSender>,
        chain_store_metrics: Arc<ChainStoreMetrics>,
    ) -> Result<Self, StoreError> {
        // Cache chain head pointers for this long when returning
        // information from `chain_head_pointers`
        const CHAIN_HEAD_CACHE_TTL: Duration = Duration::from_secs(2);

        let mirror = PrimaryMirror::new(&pools);
        let existing_chains = mirror.read(|conn| primary::load_chains(conn))?;
        let chain_head_cache = TimedCache::new(CHAIN_HEAD_CACHE_TTL);
        let chains = shards.clone();

        let block_store = Self {
            logger,
            stores: RwLock::new(HashMap::new()),
            shards,
            pools,
            sender,
            mirror,
            chain_head_cache,
            chain_store_metrics,
        };

        /// Check that the configuration for `chain` hasn't changed so that
        /// it is ok to ingest from it
        fn chain_ingestible(
            logger: &Logger,
            chain: &primary::Chain,
            shard: &Shard,
            // ident: &ChainIdentifier,
        ) -> bool {
            if &chain.shard != shard {
                error!(
                    logger,
                    "the chain {} is stored in shard {} but is configured for shard {}",
                    chain.name,
                    chain.shard,
                    shard
                );
                return false;
            }
            true
        }

        // For each configured chain, add a chain store
        for (chain_name, shard) in chains {
            match existing_chains
                .iter()
                .find(|chain| chain.name == chain_name)
            {
                Some(chain) => {
                    let status = if chain_ingestible(&block_store.logger, chain, &shard) {
                        ChainStatus::Ingestible
                    } else {
                        ChainStatus::ReadOnly
                    };
                    block_store.add_chain_store(chain, status, false)?;
                }
                None => {}
            };
        }

        // There might be chains we have in the database that are not yet/
        // no longer configured. Add a chain store for each of them, too
        let configured_chains = block_store
            .stores
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for chain in existing_chains
            .iter()
            .filter(|chain| !configured_chains.contains(&chain.name))
        {
            block_store.add_chain_store(chain, ChainStatus::ReadOnly, false)?;
        }
        Ok(block_store)
    }

    pub(crate) async fn query_permit_primary(&self) -> QueryPermit {
        self.mirror
            .primary()
            .query_permit()
            .await
            .expect("the primary is never disabled")
    }

    pub fn allocate_chain(
        conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
        name: &String,
        shard: &Shard,
        ident: &ChainIdentifier,
    ) -> Result<Chain, StoreError> {
        #[derive(QueryableByName, Debug)]
        struct ChainIdSeq {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            last_value: i64,
        }

        // Fetch the current last_value from the sequence
        let result =
            sql_query("SELECT last_value FROM chains_id_seq").get_result::<ChainIdSeq>(conn)?;

        let last_val = result.last_value;

        let next_val = last_val + 1;
        let namespace = format!("chain{}", next_val);
        let storage =
            Storage::new(namespace.to_string()).map_err(|e| StoreError::Unknown(anyhow!(e)))?;

        let chain = Chain {
            id: next_val as i32,
            name: name.clone(),
            shard: shard.clone(),
            net_version: ident.net_version.clone(),
            genesis_block: ident.genesis_block_hash.hash_hex(),
            storage: storage.clone(),
        };

        Ok(chain)
    }

    pub fn add_chain_store(
        &self,
        chain: &primary::Chain,
        status: ChainStatus,
        create: bool,
    ) -> Result<Arc<ChainStore>, StoreError> {
        let pool = self
            .pools
            .get(&chain.shard)
            .ok_or_else(|| constraint_violation!("there is no pool for shard {}", chain.shard))?
            .clone();
        let sender = ChainHeadUpdateSender::new(
            self.mirror.primary().clone(),
            chain.name.clone(),
            self.sender.clone(),
        );
        let ident = chain.network_identifier()?;
        let logger = self.logger.new(o!("network" => chain.name.clone()));
        let store = ChainStore::new(
            logger,
            chain.name.clone(),
            chain.storage.clone(),
            status,
            sender,
            pool,
            ENV_VARS.store.recent_blocks_cache_capacity,
            self.chain_store_metrics.clone(),
        );
        if create {
            store.create(&ident)?;
        }
        let store = Arc::new(store);
        self.stores
            .write()
            .unwrap()
            .insert(chain.name.clone(), store.clone());
        Ok(store)
    }

    /// Return a map from network name to the network's chain head pointer.
    /// The information is cached briefly since this method is used heavily
    /// by the indexing status API
    pub fn chain_head_pointers(&self) -> Result<HashMap<String, BlockPtr>, StoreError> {
        let mut map = HashMap::new();
        for (shard, pool) in &self.pools {
            let cached = match self.chain_head_cache.get(shard.as_str()) {
                Some(cached) => cached,
                None => {
                    let mut conn = match pool.get() {
                        Ok(conn) => conn,
                        Err(StoreError::DatabaseUnavailable) => continue,
                        Err(e) => return Err(e),
                    };
                    let heads = Arc::new(ChainStore::chain_head_pointers(&mut conn)?);
                    self.chain_head_cache.set(shard.to_string(), heads.clone());
                    heads
                }
            };
            map.extend(
                cached
                    .iter()
                    .map(|(chain, ptr)| (chain.clone(), ptr.clone())),
            );
        }
        Ok(map)
    }

    pub fn chain_head_block(&self, chain: &str) -> Result<Option<BlockNumber>, StoreError> {
        let store = self
            .store(chain)
            .ok_or_else(|| constraint_violation!("unknown network `{}`", chain))?;
        store.chain_head_block(chain)
    }

    fn lookup_chain<'a>(&'a self, chain: &'a str) -> Result<Option<Arc<ChainStore>>, StoreError> {
        // See if we have that chain in the database even if it wasn't one
        // of the configured chains
        self.mirror.read(|conn| {
            primary::find_chain(conn, chain).and_then(|chain| {
                chain
                    .map(|chain| self.add_chain_store(&chain, ChainStatus::ReadOnly, false))
                    .transpose()
            })
        })
    }

    fn store(&self, chain: &str) -> Option<Arc<ChainStore>> {
        let store = self
            .stores
            .read()
            .unwrap()
            .get(chain)
            .map(CheapClone::cheap_clone);
        if store.is_some() {
            return store;
        }
        // The chain is not in the cache yet, load it from the database; we
        // suppress errors here since it will be very rare that we look up
        // a chain from the database as most of them will be set up when
        // the block store is created
        self.lookup_chain(chain).unwrap_or_else(|e| {
                error!(&self.logger, "Error getting chain from store"; "network" => chain, "error" => e.to_string());
                None
            })
    }

    pub fn drop_chain(&self, chain: &str) -> Result<(), StoreError> {
        let chain_store = self
            .store(chain)
            .ok_or_else(|| constraint_violation!("unknown chain {}", chain))?;

        // Delete from the primary first since that's where
        // deployment_schemas has a fk constraint on chains
        primary::drop_chain(self.mirror.primary(), chain)?;

        chain_store.drop_chain()?;

        self.stores.write().unwrap().remove(chain);

        Ok(())
    }

    // cleanup_ethereum_shallow_blocks will delete cached blocks previously produced by firehose on
    // an ethereum chain that is not currently configured to use firehose provider.
    //
    // This is to prevent an issue where firehose stores "shallow" blocks (with null data) in `chainX.blocks`
    // table but RPC provider requires those blocks to be full.
    //
    // - This issue only affects ethereum chains.
    // - This issue only happens when switching providers from firehose back to RPC. it is gated by
    // the presence of a cursor in the public.ethereum_networks table for a chain configured without firehose.
    // - Only the shallow blocks close to HEAD need to be deleted, the older blocks don't need data.
    // - Deleting everything or creating an index on empty data would cause too much performance
    // hit on graph-node startup.
    //
    // Discussed here: https://github.com/graphprotocol/graph-node/pull/4790
    pub fn cleanup_ethereum_shallow_blocks(
        &self,
        eth_rpc_only_nets: Vec<String>,
    ) -> Result<(), StoreError> {
        for store in self.stores.read().unwrap().values() {
            if !eth_rpc_only_nets.contains(&&store.chain) {
                continue;
            };

            if let Some(head_block) = store.remove_cursor(&&store.chain)? {
                let lower_bound = head_block.saturating_sub(ENV_VARS.reorg_threshold * 2);
                info!(&self.logger, "Removed cursor for non-firehose chain, now cleaning shallow blocks"; "network" => &store.chain, "lower_bound" => lower_bound);
                store.cleanup_shallow_blocks(lower_bound)?;
            }
        }
        Ok(())
    }

    fn truncate_block_caches(&self) -> Result<(), StoreError> {
        for store in self.stores.read().unwrap().values() {
            store.truncate_block_cache()?
        }
        Ok(())
    }

    pub fn update_db_version(&self) -> Result<(), StoreError> {
        use crate::primary::db_version as dbv;
        use diesel::prelude::*;

        let primary_pool = self.pools.get(&*PRIMARY_SHARD).unwrap();
        let mut conn = primary_pool.get()?;
        let version: i64 = dbv::table.select(dbv::version).get_result(&mut conn)?;
        if version < 3 {
            self.truncate_block_caches()?;
            diesel::update(dbv::table)
                .set(dbv::version.eq(3))
                .execute(&mut conn)?;
        };
        Ok(())
    }

    /// Updates the chains table of the primary shard. This table is replicated to other shards and
    /// has to be refreshed afterwards for the update to be reflected.
    pub fn set_chain_identifier(
        &self,
        chain_id: ChainId,
        ident: &ChainIdentifier,
    ) -> Result<(), StoreError> {
        use primary::chains as c;

        let primary_pool = self.pools.get(&*PRIMARY_SHARD).unwrap();
        let mut conn = primary_pool.get()?;

        diesel::update(c::table.filter(c::name.eq(chain_id.as_str())))
            .set((
                c::genesis_block_hash.eq(ident.genesis_block_hash.hash_hex()),
                c::net_version.eq(&ident.net_version),
            ))
            .execute(&mut conn)?;

        Ok(())
    }
}

impl BlockStoreTrait for BlockStore {
    type ChainStore = ChainStore;

    fn chain_store(&self, network: &str) -> Option<Arc<Self::ChainStore>> {
        self.store(network)
    }

    fn create_chain_store(
        &self,
        network: &str,
        ident: ChainIdentifier,
    ) -> anyhow::Result<Arc<Self::ChainStore>> {
        match self.store(network) {
            Some(chain_store) => {
                return Ok(chain_store);
            }
            None => {}
        }

        let mut conn = self.mirror.primary().get()?;
        let shard = self
            .shards
            .iter()
            .find_map(|(chain_id, shard)| {
                if chain_id.as_str().eq(network) {
                    Some(shard)
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow!("unable to find shard for network {}", network))?;
        let chain = primary::add_chain(&mut conn, &network, &shard, ident)?;
        self.add_chain_store(&chain, ChainStatus::Ingestible, true)
            .map_err(anyhow::Error::from)
    }
}

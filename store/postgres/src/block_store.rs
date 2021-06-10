use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
    sync::{Arc, RwLock},
};

use graph::prelude::StoreError;
use graph::{
    components::store::BlockStore as BlockStoreTrait,
    prelude::{error, warn, BlockNumber, BlockPtr, EthereumNetworkIdentifier, Logger},
};
use graph::{
    constraint_violation,
    prelude::{anyhow, CheapClone},
};

use crate::{
    chain_head_listener::ChainHeadUpdateSender, connection_pool::ConnectionPool, ChainStore,
};
use crate::{subgraph_store::PRIMARY_SHARD, Shard};

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
    use std::str::FromStr;

    use diesel::{
        delete, insert_into, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl,
        RunQueryDsl,
    };
    use graph::{
        constraint_violation,
        prelude::{web3::types::H256, EthereumNetworkIdentifier, StoreError},
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
        pub fn network_identifier(&self) -> Result<EthereumNetworkIdentifier, StoreError> {
            Ok(EthereumNetworkIdentifier {
                net_version: self.net_version.clone(),
                genesis_block_hash: H256::from_str(&self.genesis_block).map_err(|e| {
                    constraint_violation!(
                        "the genesis block hash `{}` for chain `{}` is not a valid hash: {}",
                        self.genesis_block,
                        self.name,
                        e
                    )
                })?,
            })
        }
    }

    pub fn load_chains(pool: &ConnectionPool) -> Result<Vec<Chain>, StoreError> {
        let conn = pool.get()?;
        Ok(chains::table.load(&conn)?)
    }

    pub fn find_chain(conn: &PgConnection, name: &str) -> Result<Option<Chain>, StoreError> {
        Ok(chains::table
            .filter(chains::name.eq(name))
            .first(conn)
            .optional()?)
    }

    pub fn add_chain(
        pool: &ConnectionPool,
        name: &str,
        ident: &EthereumNetworkIdentifier,
        shard: &Shard,
    ) -> Result<Chain, StoreError> {
        let conn = pool.get()?;

        // For tests, we want to have a chain that still uses the
        // shared `ethereum_blocks` table
        #[cfg(debug_assertions)]
        if name == super::FAKE_NETWORK_SHARED {
            insert_into(chains::table)
                .values((
                    chains::name.eq(name),
                    chains::namespace.eq("public"),
                    chains::net_version.eq(&ident.net_version),
                    chains::genesis_block_hash.eq(format!("{:x}", &ident.genesis_block_hash)),
                    chains::shard.eq(shard.as_str()),
                ))
                .returning(chains::namespace)
                .get_result::<Storage>(&conn)
                .map_err(StoreError::from)?;
            return Ok(chains::table.filter(chains::name.eq(name)).first(&conn)?);
        }

        insert_into(chains::table)
            .values((
                chains::name.eq(name),
                chains::net_version.eq(&ident.net_version),
                chains::genesis_block_hash.eq(format!("{:x}", &ident.genesis_block_hash)),
                chains::shard.eq(shard.as_str()),
            ))
            .returning(chains::namespace)
            .get_result::<Storage>(&conn)
            .map_err(StoreError::from)?;
        Ok(chains::table.filter(chains::name.eq(name)).first(&conn)?)
    }

    pub(super) fn drop_chain(pool: &ConnectionPool, name: &str) -> Result<(), StoreError> {
        let conn = pool.get()?;

        delete(chains::table.filter(chains::name.eq(name))).execute(&conn)?;
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
    pools: HashMap<Shard, ConnectionPool>,
    primary: ConnectionPool,
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
        // (network, ident, shard)
        chains: Vec<(String, Vec<EthereumNetworkIdentifier>, Shard)>,
        // shard -> pool
        pools: HashMap<Shard, ConnectionPool>,
    ) -> Result<Self, StoreError> {
        let primary = pools
            .get(&PRIMARY_SHARD)
            .expect("we always have a primary pool")
            .clone();
        let existing_chains = primary::load_chains(&primary)?;

        let block_store = Self {
            logger,
            stores: RwLock::new(HashMap::new()),
            pools,
            primary,
        };

        fn reduce_idents(
            chain_name: &str,
            idents: Vec<EthereumNetworkIdentifier>,
        ) -> Result<Option<EthereumNetworkIdentifier>, StoreError> {
            let mut idents: HashSet<EthereumNetworkIdentifier> =
                HashSet::from_iter(idents.into_iter());
            match idents.len() {
                0 => Ok(None),
                1 => Ok(idents.drain().next()),
                _ => Err(anyhow!(
                    "conflicting network identifiers for chain {}: {:?}",
                    chain_name,
                    idents
                )
                .into()),
            }
        }

        /// Check that the configuration for `chain` hasn't changed so that
        /// it is ok to ingest from it
        fn chain_ingestible(
            logger: &Logger,
            chain: &primary::Chain,
            shard: &Shard,
            ident: &Option<EthereumNetworkIdentifier>,
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
            match ident {
                Some(ident) => {
                    if chain.net_version != ident.net_version {
                        error!(logger,
                        "the net version for chain {} has changed from {} to {} since the last time we ran",
                        chain.name,
                        chain.net_version,
                        ident.net_version
                    );
                        return false;
                    }
                    if &chain.genesis_block != &format!("{:x}", ident.genesis_block_hash) {
                        error!(logger,
                        "the genesis block hash for chain {} has changed from {} to {:x} since the last time we ran",
                        chain.name,
                        chain.genesis_block,
                        ident.genesis_block_hash
                    );
                        return false;
                    }
                    return true;
                }
                None => {
                    warn!(logger, "Failed to get net version and genesis hash from provider. Assuming it has not changed");
                    return true;
                }
            }
        }

        // For each configured chain, add a chain store
        for (chain_name, idents, shard) in chains {
            let ident = reduce_idents(&chain_name, idents)?;
            match (
                existing_chains
                    .iter()
                    .find(|chain| chain.name == chain_name),
                ident,
            ) {
                (Some(chain), ident) => {
                    let status = if chain_ingestible(&block_store.logger, chain, &shard, &ident) {
                        ChainStatus::Ingestible
                    } else {
                        ChainStatus::ReadOnly
                    };
                    block_store.add_chain_store(&chain, status, false)?;
                }
                (None, Some(ident)) => {
                    let chain =
                        primary::add_chain(&block_store.primary, &chain_name, &ident, &shard)?;
                    block_store.add_chain_store(&chain, ChainStatus::Ingestible, true)?;
                }
                (None, None) => {
                    error!(
                        &block_store.logger,
                        " the chain {} is new but we could not get a network identifier for it",
                        chain_name
                    );
                }
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
            block_store.add_chain_store(&chain, ChainStatus::ReadOnly, false)?;
        }
        Ok(block_store)
    }

    fn add_chain_store(
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
        let sender = ChainHeadUpdateSender::new(self.primary.clone(), chain.name.clone());
        let ident = chain.network_identifier()?;
        let store = ChainStore::new(
            chain.name.clone(),
            chain.storage.clone(),
            &ident,
            status,
            sender,
            pool,
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

    pub fn chain_head_pointers(&self) -> Result<HashMap<String, BlockPtr>, StoreError> {
        let mut map = HashMap::new();
        let stores = self
            .stores
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for store in stores {
            map.extend(store.chain_head_pointers()?);
        }
        Ok(map)
    }

    pub fn chain_head_block(&self, chain: &str) -> Result<Option<BlockNumber>, StoreError> {
        let store = self
            .store(chain)
            .ok_or_else(|| constraint_violation!("unknown network `{}`", chain))?;
        store.chain_head_block(chain)
    }

    fn lookup_chain(&self, chain: &str) -> Result<Option<Arc<ChainStore>>, StoreError> {
        // See if we have that chain in the database even if it wasn't one
        // of the configured chains
        let conn = self.primary.get()?;
        primary::find_chain(&conn, chain)?
            .map(|chain| self.add_chain_store(&chain, ChainStatus::ReadOnly, false))
            .transpose()
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
        primary::drop_chain(&self.primary, chain)?;

        chain_store.drop_chain()?;

        self.stores.write().unwrap().remove(chain);

        Ok(())
    }
}

impl BlockStoreTrait for BlockStore {
    type ChainStore = ChainStore;

    fn chain_store(&self, network: &str) -> Option<Arc<Self::ChainStore>> {
        self.store(network)
    }
}

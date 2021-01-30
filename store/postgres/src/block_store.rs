use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use graph::{
    components::store::BlockStore as BlockStoreTrait,
    prelude::{error, EthereumBlockPointer, EthereumNetworkIdentifier, Logger},
};
use graph::{components::store::CallCache as CallCacheTrait, prelude::StoreError};
use graph::{
    constraint_violation,
    prelude::{anyhow, CheapClone},
};

use crate::{
    chain_head_listener::ChainHeadUpdateSender, connection_pool::ConnectionPool,
    ChainHeadUpdateListener, ChainStore,
};
use crate::{subgraph_store::PRIMARY_SHARD, Shard};

#[cfg(debug_assertions)]
pub const FAKE_NETWORK_SHARED: &str = "fake_network_shared";

mod primary {
    use std::str::FromStr;

    use diesel::{
        insert_into, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
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
}

pub struct BlockStore {
    logger: Logger,
    stores: RwLock<HashMap<String, Arc<ChainStore>>>,
    pools: HashMap<Shard, ConnectionPool>,
    primary: ConnectionPool,
    chain_head_update_listener: Arc<ChainHeadUpdateListener>,
}

impl BlockStore {
    pub fn new(
        logger: Logger,
        // (network, ident, shard)
        chains: Vec<(String, EthereumNetworkIdentifier, Shard)>,
        // shard -> pool
        pools: HashMap<Shard, ConnectionPool>,
        chain_head_update_listener: Arc<ChainHeadUpdateListener>,
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
            chain_head_update_listener,
        };

        // For each configured chain, add a chain store
        for (chain_name, ident, shard) in chains {
            let chain = match existing_chains
                .iter()
                .find(|chain| chain.name == chain_name)
            {
                Some(chain) => {
                    if chain.shard != shard {
                        return Err(StoreError::Unknown(anyhow!(
                            "the chain {} is stored in shard {} but is configured for shard {}",
                            chain.name,
                            chain.shard,
                            shard
                        )));
                    }
                    chain.clone()
                }
                None => primary::add_chain(&block_store.primary, &chain_name, &ident, &shard)?,
            };

            block_store.add_chain_store(&chain)?;
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
            block_store.add_chain_store(&chain)?;
        }
        Ok(block_store)
    }

    fn add_chain_store(&self, chain: &primary::Chain) -> Result<Arc<ChainStore>, StoreError> {
        let pool = self
            .pools
            .get(&chain.shard)
            .ok_or_else(|| constraint_violation!("there is no pool for shard {}", chain.shard))?
            .clone();
        let sender = ChainHeadUpdateSender::new(self.primary.clone(), chain.name.clone());
        let store = ChainStore::new(
            chain.name.clone(),
            chain.storage.clone(),
            chain.network_identifier()?,
            self.chain_head_update_listener.clone(),
            sender,
            pool,
        );
        let store = Arc::new(store);
        self.stores
            .write()
            .unwrap()
            .insert(chain.name.clone(), store.clone());
        Ok(store)
    }

    pub fn chain_head_pointers(&self) -> Result<HashMap<String, EthereumBlockPointer>, StoreError> {
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

    pub fn chain_head_block(&self, chain: &str) -> Result<Option<u64>, StoreError> {
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
            .map(|chain| self.add_chain_store(&chain))
            .transpose()
    }

    fn store(&self, chain: &str) -> Option<Arc<ChainStore>> {
        let store = self
            .stores
            .read()
            .unwrap()
            .get(chain)
            .map(|store| store.cheap_clone());
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
}

impl BlockStoreTrait for BlockStore {
    type ChainStore = ChainStore;

    fn chain_store(&self, network: &str) -> Option<Arc<Self::ChainStore>> {
        self.store(network)
    }
}

impl CallCacheTrait for BlockStore {
    type EthereumCallCache = ChainStore;

    fn ethereum_call_cache(&self, network: &str) -> Option<Arc<Self::EthereumCallCache>> {
        self.store(network)
    }
}

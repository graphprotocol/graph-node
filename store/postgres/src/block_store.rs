use std::{collections::HashMap, sync::Arc};

use graph::{
    components::store::BlockStore as BlockStoreTrait,
    prelude::{EthereumBlockPointer, EthereumNetworkIdentifier},
};
use graph::{components::store::CallCache as CallCacheTrait, prelude::StoreError};
use graph::{
    constraint_violation,
    prelude::{anyhow, CheapClone},
};

use crate::{connection_pool::ConnectionPool, ChainHeadUpdateListener, ChainStore};
use crate::{subgraph_store::PRIMARY_SHARD, Shard};

#[cfg(debug_assertions)]
pub const FAKE_NETWORK_SHARED: &str = "fake_network_shared";

mod primary {
    use diesel::{insert_into, ExpressionMethods, RunQueryDsl};
    use graph::prelude::{EthereumNetworkIdentifier, StoreError};

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
    #[derive(Queryable)]
    pub struct Chain {
        pub id: i32,
        pub name: String,
        pub net_version: String,
        pub genesis_block: String,
        pub shard: Shard,
        pub storage: Storage,
    }

    pub fn load_chains(pool: &ConnectionPool) -> Result<Vec<Chain>, StoreError> {
        let conn = pool.get()?;
        Ok(chains::table.load(&conn)?)
    }

    pub fn add_chain(
        pool: &ConnectionPool,
        name: &str,
        ident: &EthereumNetworkIdentifier,
        shard: &Shard,
    ) -> Result<Storage, StoreError> {
        let conn = pool.get()?;

        // For tests, we want to have a chain that still uses the
        // shared `ethereum_blocks` table
        #[cfg(debug_assertions)]
        if name == super::FAKE_NETWORK_SHARED {
            return insert_into(chains::table)
                .values((
                    chains::name.eq(name),
                    chains::namespace.eq("public"),
                    chains::net_version.eq(&ident.net_version),
                    chains::genesis_block_hash.eq(format!("{:x}", &ident.genesis_block_hash)),
                    chains::shard.eq(shard.as_str()),
                ))
                .returning(chains::namespace)
                .get_result::<Storage>(&conn)
                .map_err(StoreError::from);
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
            .map_err(StoreError::from)
    }
}

pub struct BlockStore {
    stores: HashMap<String, Arc<ChainStore>>,
}

impl BlockStore {
    pub fn new(
        // (network, ident, shard)
        networks: Vec<(String, EthereumNetworkIdentifier, Shard)>,
        // shard -> pool
        pools: &HashMap<Shard, ConnectionPool>,
        chain_head_update_listener: Arc<ChainHeadUpdateListener>,
    ) -> Result<Self, StoreError> {
        let primary = pools
            .get(&PRIMARY_SHARD)
            .expect("we always have a primary pool")
            .clone();
        let chains = primary::load_chains(&primary)?;
        let mut stores = HashMap::new();

        for (network, ident, shard) in networks {
            let pool = pools
                .get(&shard)
                .ok_or_else(|| constraint_violation!("there is no pool for shard {}", shard))?
                .clone();

            let namespace = match chains.iter().find(|chain| chain.name == network) {
                Some(chain) => {
                    if chain.shard != shard {
                        return Err(StoreError::Unknown(anyhow!(
                            "the chain {} is stored in shard {} but is configured for shard {}",
                            chain.name,
                            chain.shard,
                            shard
                        )));
                    }
                    chain.storage.clone()
                }
                None => primary::add_chain(&primary, &network, &ident, &shard)?,
            };

            let store = ChainStore::new(
                network.clone(),
                namespace,
                ident.clone(),
                chain_head_update_listener.clone(),
                pool,
            );
            stores.insert(network.clone(), Arc::new(store));
        }
        Ok(Self { stores })
    }

    pub fn chain_head_pointers(&self) -> Result<HashMap<String, EthereumBlockPointer>, StoreError> {
        let mut map = HashMap::new();
        for store in self.stores.values() {
            map.extend(store.chain_head_pointers()?);
        }
        Ok(map)
    }

    pub fn chain_head_block(&self, network: &str) -> Result<Option<u64>, StoreError> {
        let store = self
            .stores
            .get(network)
            .ok_or_else(|| constraint_violation!("unknown network `{}`", network))?;
        store.chain_head_block(network)
    }
}

impl BlockStoreTrait for BlockStore {
    type ChainStore = ChainStore;

    fn chain_store(&self, network: &str) -> Option<Arc<Self::ChainStore>> {
        self.stores.get(network).map(|store| store.cheap_clone())
    }
}

impl CallCacheTrait for BlockStore {
    type EthereumCallCache = ChainStore;

    fn ethereum_call_cache(&self, network: &str) -> Option<Arc<Self::EthereumCallCache>> {
        self.stores.get(network).map(|store| store.cheap_clone())
    }
}

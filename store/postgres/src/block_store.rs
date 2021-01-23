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

mod primary {
    use diesel::{insert_into, ExpressionMethods, RunQueryDsl};
    use graph::prelude::{EthereumNetworkIdentifier, StoreError};

    use crate::{connection_pool::ConnectionPool, Shard};

    table! {
            chains(id) {
                id                 -> Integer,
                name               -> Text,
                net_version        -> Nullable<Text>,
                genesis_block_hash -> Nullable<Text>,
                shard              -> Text,
        }
    }

    /// Information about the mapping of chains to storage shards. We persist
    /// this information in the database to make it possible to detect a
    /// change in the configuration file that doesn't match what is in the database
    #[derive(Queryable)]
    pub struct Chain {
        pub id: i32,
        pub name: String,
        pub net_version: Option<String>,
        pub genesis_block: Option<String>,
        pub shard: Shard,
    }

    pub fn load_chains(pool: &ConnectionPool) -> Result<Vec<Chain>, StoreError> {
        let conn = pool.get()?;
        Ok(chains::table.load(&conn)?)
    }

    pub fn add_chain(
        pool: &ConnectionPool,
        name: &str,
        ident: EthereumNetworkIdentifier,
        shard: &Shard,
    ) -> Result<(), StoreError> {
        let conn = pool.get()?;
        insert_into(chains::table)
            .values((
                chains::name.eq(name),
                chains::net_version.eq(&ident.net_version),
                chains::genesis_block_hash.eq(format!("{:x}", &ident.genesis_block_hash)),
                chains::shard.eq(shard.as_str()),
            ))
            .execute(&conn)?;
        Ok(())
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
        pools: HashMap<Shard, ConnectionPool>,
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

            let store = ChainStore::new(
                network.clone(),
                ident.clone(),
                chain_head_update_listener.clone(),
                pool,
            );
            stores.insert(network.clone(), Arc::new(store));

            match chains.iter().find(|chain| chain.name == network) {
                Some(chain) => {
                    if chain.shard != shard {
                        return Err(StoreError::Unknown(anyhow!(
                            "the chain {} is stored in shard {} but is configured for shard {}",
                            chain.name,
                            chain.shard,
                            shard
                        )));
                    }
                }
                None => primary::add_chain(&primary, &network, ident, &shard)?,
            }
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

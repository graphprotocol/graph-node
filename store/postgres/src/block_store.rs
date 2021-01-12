use std::{collections::HashMap, sync::Arc};

use graph::components::store::BlockStore as BlockStoreTrait;
use graph::components::store::CallCache as CallCacheTrait;
use graph::prelude::CheapClone;

use crate::ChainStore;

pub struct BlockStore {
    stores: HashMap<String, Arc<ChainStore>>,
}

impl BlockStore {
    pub fn new(stores: HashMap<String, Arc<ChainStore>>) -> Self {
        Self { stores }
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

use mockall::predicate::*;
use mockall::*;
use std::collections::BTreeMap;

use graph::components::store::*;
use graph::data::subgraph::schema::*;
use graph::prelude::*;
use graph_graphql::prelude::api_schema;
use web3::types::{Address, H256};

mock! {
    pub Store {
        fn get_mock(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;
    }

    trait SubgraphDeploymentStore: Send + Sync + 'static {
        fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error>;

        fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error>;

        fn uses_relational_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<bool, Error>;

        fn network_name(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Option<String>, Error>;
    }

    trait ChainStore: Send + Sync + 'static {
        fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error>;

        fn upsert_blocks<B, E>(&self, blocks: B) -> Box<dyn Future<Item = (), Error = E> + Send + 'static>
        where
            B: Stream<Item = EthereumBlock, Error = E> + Send + 'static,
            E: From<Error> + Send + 'static,
            Self: Sized;

        fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), Error>;

        fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error>;

        fn chain_head_updates(&self) -> ChainHeadUpdateStream;

        fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error>;

        fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error>;

        fn ancestor_block(
            &self,
            block_ptr: EthereumBlockPointer,
            offset: u64,
        ) -> Result<Option<EthereumBlock>, Error>;

        fn cleanup_cached_blocks(&self, ancestor_count: u64) -> Result<(BlockNumber, usize), Error>;

        fn block_hashes_by_block_number(&self, number: u64) -> Result<Vec<H256>, Error>;

        fn confirm_block_hash(&self, number: u64, hash: &H256) -> Result<usize, Error>;
    }
}

impl Store for MockStore {
    fn block_ptr(
        &self,
        _subgraph_id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!()
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        self.get_mock(key)
    }

    fn get_many(
        &self,
        _subgraph_id: &SubgraphDeploymentId,
        _ids_for_type: BTreeMap<&str, Vec<&str>>,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        unimplemented!()
    }

    fn supports_proof_of_indexing<'a>(
        &'a self,
        _subgraph_id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool> {
        unimplemented!()
    }

    fn get_proof_of_indexing<'a>(
        &'a self,
        _subgraph_id: &'a SubgraphDeploymentId,
        _indexer: &'a Option<Address>,
        _block_hash: H256,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        unimplemented!()
    }

    fn find(&self, _query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn find_one(&self, _query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn find_ens_name(&self, _hash: &str) -> Result<Option<String>, QueryExecutionError> {
        unimplemented!()
    }

    fn transact_block_operations(
        &self,
        _subgraph_id: SubgraphDeploymentId,
        _block_ptr_to: EthereumBlockPointer,
        _mods: Vec<EntityModification>,
        _stopwatch: StopwatchMetrics,
    ) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn apply_metadata_operations(
        &self,
        _operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn build_entity_attribute_indexes(
        &self,
        _subgraph: &SubgraphDeploymentId,
        _indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        unimplemented!()
    }

    fn revert_block_operations(
        &self,
        _subgraph_id: SubgraphDeploymentId,
        _block_ptr_from: EthereumBlockPointer,
        _block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn subscribe(&self, _entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        unimplemented!()
    }

    fn create_subgraph_deployment(
        &self,
        _schema: &Schema,
        _ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn start_subgraph_deployment(
        &self,
        _logger: &Logger,
        _subgraph_id: &SubgraphDeploymentId,
        _ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn migrate_subgraph_deployment(
        &self,
        _logger: &Logger,
        _subgraph_id: &SubgraphDeploymentId,
        _block_ptr: &EthereumBlockPointer,
    ) {
        unimplemented!()
    }

    fn block_number(
        &self,
        _subgraph_id: &SubgraphDeploymentId,
        _block_hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError> {
        unimplemented!()
    }

    fn query_store(self: Arc<Self>, _: bool) -> Arc<dyn QueryStore + Send + Sync> {
        unimplemented!()
    }
}

pub fn mock_store_with_users_subgraph() -> (Arc<MockStore>, SubgraphDeploymentId) {
    let mut store = MockStore::new();

    let subgraph_id = SubgraphDeploymentId::new("users").unwrap();
    let subgraph_id_for_deployment_entity = subgraph_id.clone();
    let subgraph_id_for_api_schema_match = subgraph_id.clone();
    let subgraph_id_for_api_schema = subgraph_id.clone();

    // Simulate that the "users" subgraph is deployed
    store
        .expect_get_mock()
        .withf(move |key| {
            key == &SubgraphDeploymentEntity::key(subgraph_id_for_deployment_entity.clone())
        })
        .returning(|_| Ok(Some(Entity::from(vec![]))));

    // Simulate an API schema for the "users" subgraph
    store
        .expect_api_schema()
        .withf(move |key| key == &subgraph_id_for_api_schema_match)
        .returning(move |_| {
            const USERS_SCHEMA: &str = "
                type User @entity {
                    id: ID!,
                    name: String,
                }

                # Needed by ipfs_map in runtime/wasm/src/test.rs
                type Thing @entity {
                    id: ID!,
                    value: String,
                    extra: String
                }
            ";

            let mut schema = Schema::parse(USERS_SCHEMA, subgraph_id_for_api_schema.clone())
                .expect("failed to parse users schema");
            schema.document =
                api_schema(&schema.document).expect("failed to generate users API schema");
            Ok(Arc::new(schema))
        });

    store.expect_network_name().returning(|_| Ok(None));

    (Arc::new(store), subgraph_id)
}

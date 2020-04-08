use mockall::predicate::*;
use mockall::*;
use std::collections::BTreeMap;

use graph::components::store::*;
use graph::data::subgraph::schema::*;
use graph::prelude::*;
use graph_graphql::prelude::api_schema;
use web3::types::H256;

mock! {
    pub Store {}

    trait Store: Send + Sync + 'static  {
        fn block_ptr(
            &self,
            subgraph_id: SubgraphDeploymentId,
        ) -> Result<Option<EthereumBlockPointer>, Error>;

        fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

        fn get_many<'a>(
            &self,
            subgraph_id: &SubgraphDeploymentId,
            ids_for_type: BTreeMap<&'a str, Vec<&'a str>>,
        ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError>;

        fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError>;

        fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError>;

        fn find_ens_name(&self, _hash: &str) -> Result<Option<String>, QueryExecutionError>;

        fn transact_block_operations(
            &self,
            subgraph_id: SubgraphDeploymentId,
            block_ptr_to: EthereumBlockPointer,
            mods: Vec<EntityModification>,
            stopwatch: StopwatchMetrics,
        ) -> Result<bool, StoreError>;

        fn apply_metadata_operations(
            &self,
            operations: Vec<MetadataOperation>,
        ) -> Result<(), StoreError>;

        fn build_entity_attribute_indexes(
            &self,
            subgraph: &SubgraphDeploymentId,
            indexes: Vec<AttributeIndexDefinition>,
        ) -> Result<(), SubgraphAssignmentProviderError>;

        fn revert_block_operations(
            &self,
            subgraph_id: SubgraphDeploymentId,
            block_ptr_from: EthereumBlockPointer,
            block_ptr_to: EthereumBlockPointer,
        ) -> Result<(), StoreError>;

        fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox;

        fn create_subgraph_deployment(
            &self,
            schema: &Schema,
            ops: Vec<MetadataOperation>,
        ) -> Result<(), StoreError>;

        fn start_subgraph_deployment(
            &self,
            logger: &Logger,
            subgraph_id: &SubgraphDeploymentId,
            ops: Vec<MetadataOperation>,
        ) -> Result<(), StoreError>;

        fn migrate_subgraph_deployment(
            &self,
            logger: &Logger,
            subgraph_id: &SubgraphDeploymentId,
            block_ptr: &EthereumBlockPointer,
        );

        fn block_number(
            &self,
            subgraph_id: &SubgraphDeploymentId,
            block_hash: H256,
        ) -> Result<Option<BlockNumber>, StoreError>;
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

pub fn mock_store_with_users_subgraph() -> (Arc<MockStore>, SubgraphDeploymentId) {
    let mut store = MockStore::new();

    let subgraph_id = SubgraphDeploymentId::new("users").unwrap();
    let subgraph_id_for_deployment_entity = subgraph_id.clone();
    let subgraph_id_for_api_schema_match = subgraph_id.clone();
    let subgraph_id_for_api_schema = subgraph_id.clone();

    // Simulate that the "users" subgraph is deployed
    store
        .expect_get()
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

    (Arc::new(store), subgraph_id)
}

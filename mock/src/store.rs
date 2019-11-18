use futures::sync::mpsc;
use rand::rngs::OsRng;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;

use graph::components::store::*;
use graph::data::subgraph::schema::*;
use graph::prelude::*;
use graph_graphql::prelude::api_schema;
use web3::types::H256;

#[derive(Debug)]
pub struct MockStore {
    schemas: HashMap<SubgraphDeploymentId, Schema>,

    // Entities by (subgraph ID, entity type, entity ID)
    entities: Mutex<HashMap<SubgraphDeploymentId, HashMap<String, HashMap<String, Entity>>>>,

    subscriptions: Mutex<Vec<(HashSet<SubgraphEntityPair>, mpsc::Sender<StoreEvent>)>>,
}

fn entity_matches_filter(entity: &Entity, filter: &EntityFilter) -> bool {
    match filter {
        EntityFilter::And(subfilters) => subfilters
            .iter()
            .all(|subfilter| entity_matches_filter(entity, subfilter)),
        EntityFilter::Or(subfilters) => subfilters
            .iter()
            .any(|subfilter| entity_matches_filter(entity, subfilter)),
        EntityFilter::Equal(attr_name, attr_value) => {
            entity.get(attr_name).unwrap_or(&Value::Null) == attr_value
        }
        EntityFilter::In(attr_name, allowed_attr_values) => {
            let attr_value = entity.get(attr_name).unwrap_or(&Value::Null);

            allowed_attr_values
                .iter()
                .any(|allowed_attr_value| attr_value == allowed_attr_value)
        }
        _ => unimplemented!(),
    }
}

impl MockStore {
    /// Creates a new mock `Store`.
    pub fn new(schemas: Vec<(SubgraphDeploymentId, Schema)>) -> Self {
        MockStore {
            schemas: schemas.into_iter().collect(),
            entities: Default::default(),
            subscriptions: Default::default(),
        }
    }

    pub fn user_subgraph_id() -> SubgraphDeploymentId {
        SubgraphDeploymentId::new("UserSubgraph").unwrap()
    }

    /// Create a store that uses a very simple `User(id, name)` schema and
    /// puts two users into the store
    pub fn user_store() -> Self {
        const USER_GQL: &str = "
            type User @entity {
                id: ID!,
                name: String,
            }
            # Needed by ipfs_map in runtime/wasm/src/test.rs
            type Thing @entity {
                id: ID!,
                value: String,
                extra: String
            }";
        let subgraph = Self::user_subgraph_id();
        let schema =
            Schema::parse(USER_GQL, subgraph.clone()).expect("Failed to parse user schema");

        // Build a map of entities
        let mut alex = Entity::new();
        alex.set("id", "alex");
        alex.set("name", "Alex");
        let mut steve = Entity::new();
        steve.set("id", "steve");
        steve.set("name", "Steve");
        let mut users = HashMap::new();
        users.insert("alex".to_owned(), alex);
        users.insert("steve".to_owned(), steve);
        let mut entities = HashMap::new();
        entities.insert("User".to_owned(), users);

        let store = Self::new(vec![(subgraph.clone(), schema)]);
        store.entities.lock().unwrap().insert(subgraph, entities);
        store
    }

    fn execute_query(
        &self,
        entities: &HashMap<SubgraphDeploymentId, HashMap<String, HashMap<String, Entity>>>,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let EntityQuery {
            subgraph_id,
            entity_types,
            filter,
            order_by,
            order_direction,
            range: _,
        } = query;

        // List all entities with correct type
        let empty1 = HashMap::default();
        let empty2 = HashMap::default();
        let entities_of_type = entities
            .get(&subgraph_id)
            .unwrap_or(&empty1)
            .get(&entity_types[0]) // This does not support querying interfaces.
            .unwrap_or(&empty2)
            .values();

        // Apply filter, if any
        let filtered_entities: Vec<_> = if let Some(filter) = filter {
            entities_of_type
                .filter(|entity| entity_matches_filter(entity, &filter))
                .collect()
        } else {
            entities_of_type.collect()
        };

        // Sort results
        let sorted_entities = if let Some((order_by_attr_name, _order_by_attr_type)) = order_by {
            if order_by_attr_name == "id" {
                let mut sorted_entities = filtered_entities;
                sorted_entities.sort_by(|a, b| match (a.get("id"), b.get("id")) {
                    (Some(Value::String(a_id)), Some(Value::String(b_id))) => a_id.cmp(&b_id),
                    _ => ::std::cmp::Ordering::Equal,
                });
                sorted_entities
            } else {
                unimplemented!("only ordering by `id` is support in the mock store");
            }
        } else {
            assert_eq!(order_direction, None);

            // Randomize order to help identify bugs where ordering is assumed to be deterministic.
            let mut sorted_entities = filtered_entities;
            sorted_entities.shuffle(&mut OsRng::new().unwrap());
            sorted_entities
        };

        Ok(sorted_entities.into_iter().cloned().collect())
    }
}

impl Store for MockStore {
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        Ok(self
            .entities
            .lock()
            .unwrap()
            .get(&key.subgraph_id)
            .and_then(|entities_in_subgraph| entities_in_subgraph.get(&key.entity_type))
            .and_then(|entities_of_type| entities_of_type.get(&key.entity_id))
            .map(|entity| entity.to_owned()))
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        self.execute_query(&self.entities.lock().unwrap(), query)
    }

    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        Ok(self.find(query)?.pop())
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        let s1 = "dealdrafts".to_string();
        match hash {
            "0x7f0c1b04d1a4926f9c635a030eeb611d4c26e5e73291b32a1c7a4ac56935b5b3" => Ok(Some(s1)),
            _ => Ok(None),
        }
    }

    fn block_ptr(&self, _: SubgraphDeploymentId) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn transact_block_operations(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: Vec<EntityModification>,
        _: StopwatchMetrics,
    ) -> Result<bool, StoreError> {
        unimplemented!();
    }

    fn apply_metadata_operations(&self, ops: Vec<MetadataOperation>) -> Result<(), StoreError> {
        let mut entities_ref = self.entities.lock().unwrap();

        let mut entities: HashMap<_, _> = entities_ref.clone();
        let mut entity_changes = vec![];
        for op in ops {
            match op {
                MetadataOperation::Set { entity, id, data } => {
                    let key = MetadataOperation::entity_key(entity, id);
                    let entities_of_type = entities
                        .entry(key.subgraph_id.clone())
                        .or_default()
                        .entry(key.entity_type.clone())
                        .or_default();

                    if entities_of_type.contains_key(&key.entity_id) {
                        let existing_entity = entities_of_type.get_mut(&key.entity_id).unwrap();
                        existing_entity.merge(data);

                        entity_changes
                            .push(EntityChange::from_key(key, EntityChangeOperation::Set));
                    } else {
                        let mut new_entity = data;
                        new_entity.insert("id".to_owned(), key.entity_id.clone().into());
                        new_entity.retain(|_k, v| *v != Value::Null);
                        entities_of_type.insert(key.entity_id.clone(), new_entity);

                        entity_changes
                            .push(EntityChange::from_key(key, EntityChangeOperation::Set));
                    }
                }
                MetadataOperation::Update { entity, id, data } => {
                    let key = MetadataOperation::entity_key(entity, id);
                    let entities_of_type = entities
                        .entry(key.subgraph_id.clone())
                        .or_default()
                        .entry(key.entity_type.clone())
                        .or_default();

                    if entities_of_type.contains_key(&key.entity_id) {
                        let existing_entity = entities_of_type.get_mut(&key.entity_id).unwrap();
                        existing_entity.merge(data);

                        entity_changes
                            .push(EntityChange::from_key(key, EntityChangeOperation::Set));
                    } else {
                        return Err(TransactionAbortError::AbortUnless {
                            expected_entity_ids: vec![key.entity_id],
                            actual_entity_ids: vec![],
                            description: "update failed because entity does not exist".to_owned(),
                        }
                        .into());
                    }
                }
                MetadataOperation::Remove { entity, id } => {
                    let key = MetadataOperation::entity_key(entity, id);
                    if let Some(in_subgraph) = entities.get_mut(&key.subgraph_id) {
                        if let Some(of_type) = in_subgraph.get_mut(&key.entity_type) {
                            if of_type.remove(&key.entity_id).is_some() {
                                entity_changes.push(EntityChange::from_key(
                                    key,
                                    EntityChangeOperation::Removed,
                                ));
                            }
                        }
                    }
                }
                MetadataOperation::AbortUnless {
                    description,
                    query,
                    entity_ids: mut expected_entity_ids,
                } => {
                    let query_results = self.execute_query(&entities, query.clone()).unwrap();
                    let mut actual_entity_ids = query_results
                        .into_iter()
                        .map(|entity| entity.id().unwrap())
                        .collect::<Vec<_>>();

                    if query.order_by.is_none() {
                        actual_entity_ids.sort();
                        expected_entity_ids.sort();
                    }

                    if actual_entity_ids != expected_entity_ids {
                        return Err(TransactionAbortError::AbortUnless {
                            expected_entity_ids,
                            actual_entity_ids,
                            description,
                        }
                        .into());
                    }
                }
            }
        }

        *entities_ref = entities;
        ::std::mem::drop(entities_ref);

        // Now that the transaction has been committed,
        // send entity changes to subscribers.
        let subscriptions = self.subscriptions.lock().unwrap();
        for entity_change in entity_changes {
            let entity_type = entity_change.subgraph_entity_pair();

            for (entity_types_set, sender) in subscriptions.iter() {
                if entity_types_set.contains(&entity_type) {
                    let entity_change = entity_change.clone();
                    let sender = sender.clone();

                    tokio::spawn(future::lazy(move || {
                        let event = StoreEvent::new(vec![entity_change]);
                        sender
                            .send(event)
                            .map(|_| ())
                            .map_err(|e| panic!("subscription send error: {}", e))
                    }));
                }
            }
        }

        Ok(())
    }

    fn build_entity_attribute_indexes(
        &self,
        _: &SubgraphDeploymentId,
        _: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        Ok(())
    }

    fn revert_block_operations(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!();
    }

    fn subscribe(&self, entity_types: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        let (sender, receiver) = mpsc::channel(100);

        self.subscriptions
            .lock()
            .unwrap()
            .push((entity_types.into_iter().collect(), sender));

        StoreEventStream::new(Box::new(receiver))
    }

    fn create_subgraph_deployment(
        &self,
        _schema: &Schema,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        self.apply_metadata_operations(ops)
    }

    fn start_subgraph_deployment(
        &self,
        _subgraph_id: &SubgraphDeploymentId,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        self.apply_metadata_operations(ops)
    }

    fn migrate_subgraph_deployment(
        &self,
        _: &Logger,
        _: &SubgraphDeploymentId,
        _: &EthereumBlockPointer,
    ) {
    }
}

impl SubgraphDeploymentStore for MockStore {
    fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        if *subgraph_id == *SUBGRAPHS_ID {
            let raw_schema = include_str!("../../store/postgres/src/subgraphs.graphql").to_owned();
            let schema = Schema::parse(&raw_schema, subgraph_id.clone())?;
            return Ok(Arc::new(schema));
        }
        Ok(Arc::new(self.schemas.get(subgraph_id).unwrap().clone()))
    }

    fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        let mut schema = self.input_schema(subgraph_id)?.as_ref().clone();
        schema.document = api_schema(&schema.document)?;
        return Ok(Arc::new(schema));
    }

    fn uses_relational_schema(&self, _: &SubgraphDeploymentId) -> Result<bool, Error> {
        Ok(true)
    }
}

impl ChainStore for MockStore {
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        Ok(EthereumBlockPointer {
            hash: H256::zero(),
            number: 0,
        })
    }

    fn upsert_blocks<'a, B, E>(&self, _: B) -> Box<dyn Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a,
    {
        unimplemented!();
    }

    fn upsert_light_blocks(&self, _: Vec<LightEthereumBlock>) -> Result<(), Error> {
        unimplemented!();
    }

    fn attempt_chain_head_update(&self, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!();
    }

    fn chain_head_updates(&self) -> ChainHeadUpdateStream {
        unimplemented!();
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        Ok(None)
    }

    fn blocks(&self, _: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }
}

impl EthereumCallCache for MockStore {
    fn get_call(
        &self,
        _: ethabi::Address,
        _: &[u8],
        _: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, Error> {
        unimplemented!()
    }

    fn set_call(
        &self,
        _: ethabi::Address,
        _: &[u8],
        _: EthereumBlockPointer,
        _: &[u8],
    ) -> Result<(), Error> {
        unimplemented!()
    }
}

pub struct FakeStore;

impl Store for FakeStore {
    fn get(&self, _: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        Ok(None)
    }

    fn find(&self, _: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!();
    }

    fn find_one(&self, _: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        unimplemented!();
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        let s1 = "dealdrafts".to_string();
        match hash {
            "0x7f0c1b04d1a4926f9c635a030eeb611d4c26e5e73291b32a1c7a4ac56935b5b3" => Ok(Some(s1)),
            _ => Ok(None),
        }
    }

    fn block_ptr(&self, _: SubgraphDeploymentId) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn transact_block_operations(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: Vec<EntityModification>,
        _: StopwatchMetrics,
    ) -> Result<bool, StoreError> {
        unimplemented!();
    }

    fn apply_metadata_operations(&self, _: Vec<MetadataOperation>) -> Result<(), StoreError> {
        Ok(())
    }

    fn build_entity_attribute_indexes(
        &self,
        _: &SubgraphDeploymentId,
        _: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        Ok(())
    }

    fn revert_block_operations(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!();
    }

    fn subscribe(&self, _: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        unimplemented!();
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
        _subgraph_id: &SubgraphDeploymentId,
        _ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn migrate_subgraph_deployment(
        &self,
        _: &Logger,
        _: &SubgraphDeploymentId,
        _: &EthereumBlockPointer,
    ) {
        unimplemented!()
    }
}

impl ChainStore for FakeStore {
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn upsert_blocks<'a, B, E>(&self, _: B) -> Box<dyn Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a,
    {
        unimplemented!();
    }

    fn upsert_light_blocks(&self, _: Vec<LightEthereumBlock>) -> Result<(), Error> {
        unimplemented!()
    }

    fn attempt_chain_head_update(&self, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!();
    }

    fn chain_head_updates(&self) -> ChainHeadUpdateStream {
        unimplemented!();
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn blocks(&self, _: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }
}

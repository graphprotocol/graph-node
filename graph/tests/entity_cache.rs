use async_trait::async_trait;
use graph::blockchain::BlockPtr;
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth};
use graph::prelude::{Schema, StopwatchMetrics, StoreError};
use lazy_static::lazy_static;
use mockall::predicate::*;
use mockall::*;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;

use graph::components::store::{EntityType, StoredDynamicDataSource, WritableStore};
use graph::{
    components::store::{DeploymentId, DeploymentLocator},
    prelude::{DeploymentHash, Entity, EntityCache, EntityKey, EntityModification, Value},
};

lazy_static! {
    static ref SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("entity_cache").unwrap();
    static ref DEPLOYMENT: DeploymentLocator =
        DeploymentLocator::new(DeploymentId::new(-12), SUBGRAPH_ID.clone());
}

mock! {
    pub Store {
        fn get_many_mock<'a>(
            &self,
            _ids_for_type: BTreeMap<&'a EntityType, Vec<&'a str>>,
        ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError>;
    }
}

// The store trait must be implemented manually because mockall does not support async_trait, nor borrowing from arguments.
#[async_trait]
impl WritableStore for MockStore {
    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        unimplemented!()
    }

    fn block_cursor(&self) -> Result<Option<String>, StoreError> {
        unimplemented!()
    }

    fn start_subgraph_deployment(&self, _: &Logger) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn revert_block_operations(&self, _: BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn unfail_deterministic_error(&self, _: &BlockPtr, _: &BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn unfail_non_deterministic_error(&self, _: &BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn fail_subgraph(&self, _: SubgraphError) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn get(&self, _: &EntityKey) -> Result<Option<Entity>, StoreError> {
        unimplemented!()
    }

    fn transact_block_operations(
        &self,
        _: BlockPtr,
        _: Option<String>,
        _: Vec<EntityModification>,
        _: StopwatchMetrics,
        _: Vec<StoredDynamicDataSource>,
        _: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.get_many_mock(ids_for_type)
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        unimplemented!()
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn shard(&self) -> &str {
        unimplemented!()
    }

    async fn health(&self, _: &DeploymentHash) -> Result<SubgraphHealth, StoreError> {
        unimplemented!()
    }

    fn input_schema(&self) -> Arc<Schema> {
        unimplemented!()
    }
}

fn make_band(id: &'static str, data: Vec<(&str, Value)>) -> (EntityKey, Entity) {
    (
        EntityKey::data(SUBGRAPH_ID.clone(), "Band".to_string(), id.into()),
        Entity::from(data),
    )
}

fn sort_by_entity_key(mut mods: Vec<EntityModification>) -> Vec<EntityModification> {
    mods.sort_by_key(|m| m.entity_key().clone());
    mods
}

#[tokio::test]
async fn empty_cache_modifications() {
    let store = Arc::new(MockStore::new());
    let cache = EntityCache::new(store.clone());
    let result = cache.as_modifications();
    assert_eq!(result.unwrap().modifications, vec![]);
}

#[test]
fn insert_modifications() {
    let mut store = MockStore::new();

    // Return no entities from the store, forcing the cache to treat any `set`
    // operation as an insert.
    store
        .expect_get_many_mock()
        .returning(|_| Ok(BTreeMap::new()));

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store.clone());

    let (mogwai_key, mogwai_data) = make_band(
        "mogwai",
        vec![("id", "mogwai".into()), ("name", "Mogwai".into())],
    );
    cache.set(mogwai_key.clone(), mogwai_data.clone());

    let (sigurros_key, sigurros_data) = make_band(
        "sigurros",
        vec![("id", "sigurros".into()), ("name", "Sigur Ros".into())],
    );
    cache.set(sigurros_key.clone(), sigurros_data.clone());

    let result = cache.as_modifications();
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![
            EntityModification::Insert {
                key: mogwai_key,
                data: mogwai_data,
            },
            EntityModification::Insert {
                key: sigurros_key,
                data: sigurros_data,
            }
        ])
    );
}

#[test]
fn overwrite_modifications() {
    let mut store = MockStore::new();

    // Pre-populate the store with entities so that the cache treats
    // every set operation as an overwrite.
    store.expect_get_many_mock().returning(|_| {
        let mut map = BTreeMap::new();

        map.insert(
            EntityType::from("Band"),
            vec![
                make_band(
                    "mogwai",
                    vec![("id", "mogwai".into()), ("name", "Mogwai".into())],
                )
                .1,
                make_band(
                    "sigurros",
                    vec![("id", "sigurros".into()), ("name", "Sigur Ros".into())],
                )
                .1,
            ],
        );

        Ok(map)
    });

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store.clone());

    let (mogwai_key, mogwai_data) = make_band(
        "mogwai",
        vec![
            ("id", "mogwai".into()),
            ("name", "Mogwai".into()),
            ("founded", 1995.into()),
        ],
    );
    cache.set(mogwai_key.clone(), mogwai_data.clone());

    let (sigurros_key, sigurros_data) = make_band(
        "sigurros",
        vec![
            ("id", "sigurros".into()),
            ("name", "Sigur Ros".into()),
            ("founded", 1994.into()),
        ],
    );
    cache.set(sigurros_key.clone(), sigurros_data.clone());

    let result = cache.as_modifications();
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![
            EntityModification::Overwrite {
                key: mogwai_key,
                data: mogwai_data,
            },
            EntityModification::Overwrite {
                key: sigurros_key,
                data: sigurros_data,
            }
        ])
    );
}

#[test]
fn consecutive_modifications() {
    let mut store = MockStore::new();

    // Pre-populate the store with data so that we can test setting a field to
    // `Value::Null`.
    store.expect_get_many_mock().returning(|_| {
        let mut map = BTreeMap::new();

        map.insert(
            EntityType::from("Band"),
            vec![
                make_band(
                    "mogwai",
                    vec![
                        ("id", "mogwai".into()),
                        ("name", "Mogwai".into()),
                        ("label", "Chemikal Underground".into()),
                    ],
                )
                .1,
            ],
        );

        Ok(map)
    });

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store.clone());

    // First, add "founded" and change the "label".
    let (update_key, update_data) = make_band(
        "mogwai",
        vec![
            ("id", "mogwai".into()),
            ("founded", 1995.into()),
            ("label", "Rock Action Records".into()),
        ],
    );
    cache.set(update_key.clone(), update_data.clone());

    // Then, just reset the "label".
    let (update_key, update_data) = make_band(
        "mogwai",
        vec![("id", "mogwai".into()), ("label", Value::Null)],
    );
    cache.set(update_key.clone(), update_data.clone());

    // We expect a single overwrite modification for the above that leaves "id"
    // and "name" untouched, sets "founded" and removes the "label" field.
    let result = cache.as_modifications();
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![EntityModification::Overwrite {
            key: update_key,
            data: Entity::from(vec![
                ("id", "mogwai".into()),
                ("name", "Mogwai".into()),
                ("founded", 1995.into()),
            ]),
        },])
    );
}

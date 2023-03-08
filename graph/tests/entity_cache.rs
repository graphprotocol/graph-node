use async_trait::async_trait;
use graph::blockchain::block_stream::FirehoseCursor;
use graph::blockchain::BlockPtr;
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth};
use graph::data_source::CausalityRegion;
use graph::prelude::{Schema, StopwatchMetrics, StoreError, UnfailOutcome};
use lazy_static::lazy_static;
use slog::Logger;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use graph::components::store::{
    DeploymentCursorTracker, EntityKey, EntityType, ReadStore, StoredDynamicDataSource,
    WritableStore, EntityDerived,
};
use graph::{
    components::store::{DeploymentId, DeploymentLocator},
    prelude::{DeploymentHash, Entity, EntityCache, EntityModification, Value},
};

lazy_static! {
    static ref SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("entity_cache").unwrap();
    static ref DEPLOYMENT: DeploymentLocator =
        DeploymentLocator::new(DeploymentId::new(-12), SUBGRAPH_ID.clone());
    static ref SCHEMA: Arc<Schema> = Arc::new(
        Schema::parse(
            "
            type Band @entity {
                id: ID!
                name: String!
                founded: Int
                label: String
            }
            ",
            SUBGRAPH_ID.clone(),
        )
        .expect("Test schema invalid")
    );
}

struct MockStore {
    get_many_res: BTreeMap<EntityKey, Entity>,
}

impl MockStore {
    fn new(get_many_res: BTreeMap<EntityKey, Entity>) -> Self {
        Self { get_many_res }
    }
}

impl ReadStore for MockStore {
    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        Ok(self.get_many_res.get(key).cloned())
    }

    fn get_many(
        &self,
        _keys: BTreeSet<EntityKey>,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        Ok(self.get_many_res.clone())
    }

    fn get_derived(
        &self,
        _key: &EntityDerived,
    ) -> Result<Vec<Entity>, StoreError> {
        let values: Vec<Entity> = self.get_many_res.clone().into_iter().map(|(_, v)| v).collect();
        Ok(values)
    }

    fn input_schema(&self) -> Arc<Schema> {
        SCHEMA.clone()
    }
}
impl DeploymentCursorTracker for MockStore {
    fn block_ptr(&self) -> Option<BlockPtr> {
        unimplemented!()
    }

    fn firehose_cursor(&self) -> FirehoseCursor {
        unimplemented!()
    }
}

#[async_trait]
impl WritableStore for MockStore {
    async fn start_subgraph_deployment(&self, _: &Logger) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn revert_block_operations(
        &self,
        _: BlockPtr,
        _: FirehoseCursor,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn unfail_deterministic_error(
        &self,
        _: &BlockPtr,
        _: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        unimplemented!()
    }

    fn unfail_non_deterministic_error(&self, _: &BlockPtr) -> Result<UnfailOutcome, StoreError> {
        unimplemented!()
    }

    async fn fail_subgraph(&self, _: SubgraphError) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        unimplemented!()
    }

    async fn transact_block_operations(
        &self,
        _: BlockPtr,
        _: FirehoseCursor,
        _: Vec<EntityModification>,
        _: &StopwatchMetrics,
        _: Vec<StoredDynamicDataSource>,
        _: Vec<SubgraphError>,
        _: Vec<(u32, String)>,
        _: Vec<StoredDynamicDataSource>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn load_dynamic_data_sources(
        &self,
        _manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        unimplemented!()
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn shard(&self) -> &str {
        unimplemented!()
    }

    async fn health(&self) -> Result<SubgraphHealth, StoreError> {
        unimplemented!()
    }

    async fn flush(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn causality_region_curr_val(&self) -> Result<Option<CausalityRegion>, StoreError> {
        unimplemented!()
    }
}

fn make_band(id: &'static str, data: Vec<(&str, Value)>) -> (EntityKey, Entity) {
    (
        EntityKey {
            entity_type: EntityType::new("Band".to_string()),
            entity_id: id.into(),
            causality_region: CausalityRegion::ONCHAIN,
        },
        Entity::from(data),
    )
}

fn sort_by_entity_key(mut mods: Vec<EntityModification>) -> Vec<EntityModification> {
    mods.sort_by_key(|m| m.entity_ref().clone());
    mods
}

#[tokio::test]
async fn empty_cache_modifications() {
    let store = Arc::new(MockStore::new(BTreeMap::new()));
    let cache = EntityCache::new(store);
    let result = cache.as_modifications();
    assert_eq!(result.unwrap().modifications, vec![]);
}

#[test]
fn insert_modifications() {
    // Return no entities from the store, forcing the cache to treat any `set`
    // operation as an insert.
    let store = MockStore::new(BTreeMap::new());

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store);

    let (mogwai_key, mogwai_data) = make_band(
        "mogwai",
        vec![("id", "mogwai".into()), ("name", "Mogwai".into())],
    );
    cache.set(mogwai_key.clone(), mogwai_data.clone()).unwrap();

    let (sigurros_key, sigurros_data) = make_band(
        "sigurros",
        vec![("id", "sigurros".into()), ("name", "Sigur Ros".into())],
    );
    cache
        .set(sigurros_key.clone(), sigurros_data.clone())
        .unwrap();

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

fn entity_version_map(entity_type: &str, entities: Vec<Entity>) -> BTreeMap<EntityKey, Entity> {
    let mut map = BTreeMap::new();
    for entity in entities {
        let key = EntityKey {
            entity_type: EntityType::new(entity_type.to_string()),
            entity_id: entity.id().unwrap().into(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        map.insert(key, entity);
    }
    map
}

#[test]
fn overwrite_modifications() {
    // Pre-populate the store with entities so that the cache treats
    // every set operation as an overwrite.
    let store = {
        let entities = vec![
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
        ];
        MockStore::new(entity_version_map("Band", entities))
    };

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store);

    let (mogwai_key, mogwai_data) = make_band(
        "mogwai",
        vec![
            ("id", "mogwai".into()),
            ("name", "Mogwai".into()),
            ("founded", 1995.into()),
        ],
    );
    cache.set(mogwai_key.clone(), mogwai_data.clone()).unwrap();

    let (sigurros_key, sigurros_data) = make_band(
        "sigurros",
        vec![
            ("id", "sigurros".into()),
            ("name", "Sigur Ros".into()),
            ("founded", 1994.into()),
        ],
    );
    cache
        .set(sigurros_key.clone(), sigurros_data.clone())
        .unwrap();

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
    // Pre-populate the store with data so that we can test setting a field to
    // `Value::Null`.
    let store = {
        let entities = vec![
            make_band(
                "mogwai",
                vec![
                    ("id", "mogwai".into()),
                    ("name", "Mogwai".into()),
                    ("label", "Chemikal Underground".into()),
                ],
            )
            .1,
        ];

        MockStore::new(entity_version_map("Band", entities))
    };

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store);

    // First, add "founded" and change the "label".
    let (update_key, update_data) = make_band(
        "mogwai",
        vec![
            ("id", "mogwai".into()),
            ("founded", 1995.into()),
            ("label", "Rock Action Records".into()),
        ],
    );
    cache.set(update_key, update_data).unwrap();

    // Then, just reset the "label".
    let (update_key, update_data) = make_band(
        "mogwai",
        vec![("id", "mogwai".into()), ("label", Value::Null)],
    );
    cache.set(update_key.clone(), update_data).unwrap();

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

use async_trait::async_trait;
use graph::blockchain::BlockTime;
use graph::blockchain::block_stream::FirehoseCursor;
use graph::components::store::{
    DeploymentCursorTracker, DerivedEntityQuery, EntityLfuCache, GetScope, LoadRelatedRequest,
    ReadStore, StoredDynamicDataSource, WritableStore,
};
use graph::components::subgraph::BlockState;
use graph::data::store::Id;
use graph::data::subgraph::schema::{DeploymentCreate, SubgraphError, SubgraphHealth};
use graph::data_source::CausalityRegion;
use graph::prelude::alloy::primitives::B256;
use graph::schema::{EntityKey, EntityType, InputSchema};
use graph::{
    components::store::{DeploymentId, DeploymentLocator},
    prelude::{DeploymentHash, Entity, EntityCache, EntityModification, Value},
};
use graph::{entity, prelude::*};
use hex_literal::hex;

use graph::semver::Version;
use lazy_static::lazy_static;
use slog::Logger;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;

use graph_store_postgres::SubgraphStore as DieselSubgraphStore;
use test_store::*;

lazy_static! {
    static ref SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("entity_cache").unwrap();
    static ref DEPLOYMENT: DeploymentLocator =
        DeploymentLocator::new(DeploymentId::new(-12), SUBGRAPH_ID.clone());
    static ref SCHEMA: InputSchema = InputSchema::parse_latest(
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
    .expect("Test schema invalid");
}

struct MockStore {
    get_many_res: BTreeMap<EntityKey, Entity>,
}

impl MockStore {
    fn new(get_many_res: BTreeMap<EntityKey, Entity>) -> Self {
        Self { get_many_res }
    }
}

#[async_trait]
impl ReadStore for MockStore {
    async fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        Ok(self.get_many_res.get(key).cloned())
    }

    async fn get_many(
        &self,
        _keys: BTreeSet<EntityKey>,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        Ok(self.get_many_res.clone())
    }

    async fn get_derived(
        &self,
        _key: &DerivedEntityQuery,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        Ok(self.get_many_res.clone())
    }

    fn input_schema(&self) -> InputSchema {
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

    fn input_schema(&self) -> InputSchema {
        todo!()
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

    async fn unfail_non_deterministic_error(
        &self,
        _: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        unimplemented!()
    }

    async fn fail_subgraph(&self, _: SubgraphError) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn transact_block_operations(
        &self,
        _: BlockPtr,
        _: BlockTime,
        _: FirehoseCursor,
        _: Vec<EntityModification>,
        _: &StopwatchMetrics,
        _: Vec<StoredDynamicDataSource>,
        _: Vec<SubgraphError>,
        _: Vec<StoredDynamicDataSource>,
        _: bool,
        _: bool,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn is_deployment_synced(&self) -> bool {
        unimplemented!()
    }

    async fn pause_subgraph(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn load_dynamic_data_sources(
        &self,
        _manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        unimplemented!()
    }

    async fn deployment_synced(&self, _block_ptr: BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn shard(&self) -> &str {
        unimplemented!()
    }

    async fn health(&self) -> Result<SubgraphHealth, StoreError> {
        unimplemented!()
    }

    async fn create_postponed_indexes(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn flush(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn causality_region_curr_val(&self) -> Result<Option<CausalityRegion>, StoreError> {
        unimplemented!()
    }

    async fn restart(self: Arc<Self>) -> Result<Option<Arc<dyn WritableStore>>, StoreError> {
        unimplemented!()
    }
}

fn make_band_key(id: &str) -> EntityKey {
    SCHEMA.entity_type("Band").unwrap().parse_key(id).unwrap()
}

fn sort_by_entity_key(mut mods: Vec<EntityModification>) -> Vec<EntityModification> {
    mods.sort_by_key(|m| m.key().clone());
    mods
}

#[graph::test]
async fn empty_cache_modifications() {
    let store = Arc::new(MockStore::new(BTreeMap::new()));
    let cache = EntityCache::new(store, SeqGenerator::new(0));
    let result = cache.as_modifications(0, &STOPWATCH).await;
    assert_eq!(result.unwrap().modifications, vec![]);
}

#[graph::test]
async fn insert_modifications() {
    // Return no entities from the store, forcing the cache to treat any `set`
    // operation as an insert.
    let store = MockStore::new(BTreeMap::new());

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store, SeqGenerator::new(0));

    let mut mogwai_data = entity! { SCHEMA => id: "mogwai", name: "Mogwai" };
    let mogwai_key = make_band_key("mogwai");
    cache
        .set(mogwai_key.clone(), mogwai_data.clone(), None)
        .await
        .unwrap();

    let mut sigurros_data = entity! { SCHEMA => id: "sigurros", name: "Sigur Ros" };
    let sigurros_key = make_band_key("sigurros");
    cache
        .set(sigurros_key.clone(), sigurros_data.clone(), None)
        .await
        .unwrap();

    mogwai_data.set_vid(100).unwrap();
    sigurros_data.set_vid(101).unwrap();

    let result = cache.as_modifications(0, &STOPWATCH).await;
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![
            EntityModification::insert(mogwai_key, mogwai_data, 0),
            EntityModification::insert(sigurros_key, sigurros_data, 0)
        ])
    );
}

fn entity_version_map(entity_type: &str, entities: Vec<Entity>) -> BTreeMap<EntityKey, Entity> {
    let mut map = BTreeMap::new();
    for entity in entities {
        let key = SCHEMA.entity_type(entity_type).unwrap().key(entity.id());
        map.insert(key, entity);
    }
    map
}

#[graph::test]
async fn overwrite_modifications() {
    // Pre-populate the store with entities so that the cache treats
    // every set operation as an overwrite.
    let store = {
        let entities = vec![
            entity! { SCHEMA => id: "mogwai", name: "Mogwai" },
            entity! { SCHEMA => id: "sigurros", name: "Sigur Ros" },
        ];
        MockStore::new(entity_version_map("Band", entities))
    };

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store, SeqGenerator::new(0));

    let mut mogwai_data = entity! { SCHEMA => id: "mogwai", name: "Mogwai", founded: 1995 };
    let mogwai_key = make_band_key("mogwai");
    cache
        .set(mogwai_key.clone(), mogwai_data.clone(), None)
        .await
        .unwrap();

    let mut sigurros_data = entity! { SCHEMA => id: "sigurros", name: "Sigur Ros", founded: 1994};
    let sigurros_key = make_band_key("sigurros");
    cache
        .set(sigurros_key.clone(), sigurros_data.clone(), None)
        .await
        .unwrap();

    mogwai_data.set_vid(100).unwrap();
    sigurros_data.set_vid(101).unwrap();

    let result = cache.as_modifications(0, &STOPWATCH).await;
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![
            EntityModification::overwrite(mogwai_key, mogwai_data, 0),
            EntityModification::overwrite(sigurros_key, sigurros_data, 0)
        ])
    );
}

#[graph::test]
async fn consecutive_modifications() {
    // Pre-populate the store with data so that we can test setting a field to
    // `Value::Null`.
    let store = {
        let entities =
            vec![entity! { SCHEMA => id: "mogwai", name: "Mogwai", label: "Chemikal Underground" }];

        MockStore::new(entity_version_map("Band", entities))
    };

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store, SeqGenerator::new(0));

    // First, add "founded" and change the "label".
    let update_data =
        entity! { SCHEMA => id: "mogwai", founded: 1995, label: "Rock Action Records" };
    let update_key = make_band_key("mogwai");
    cache.set(update_key, update_data, None).await.unwrap();

    // Then, just reset the "label".
    let update_data = entity! { SCHEMA => id: "mogwai", label: Value::Null };
    let update_key = make_band_key("mogwai");
    cache
        .set(update_key.clone(), update_data, None)
        .await
        .unwrap();

    // We expect a single overwrite modification for the above that leaves "id"
    // and "name" untouched, sets "founded" and removes the "label" field.
    let result = cache.as_modifications(0, &STOPWATCH).await;
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![EntityModification::overwrite(
            update_key,
            entity! { SCHEMA => id: "mogwai", name: "Mogwai", founded: 1995, vid: 101i64 },
            0,
        )])
    );
}

#[graph::test]
async fn check_vid_sequence() {
    let store = MockStore::new(BTreeMap::new());
    let store = Arc::new(store);
    let mut cache = EntityCache::new(store, SeqGenerator::new(0));

    for n in 0..10 {
        let id = (10 - n).to_string();
        let name = "Mogwai".to_string();
        let mogwai_key = make_band_key(id.as_str());
        let mogwai_data = entity! { SCHEMA => id: id, name: name };
        cache
            .set(mogwai_key.clone(), mogwai_data.clone(), None)
            .await
            .unwrap();
    }

    let result = cache.as_modifications(0, &STOPWATCH).await;
    let mods = result.unwrap().modifications;
    for m in mods {
        match m {
            EntityModification::Insert {
                key: _,
                data,
                block: _,
                end: _,
            } => {
                let id = data.id().to_string();
                let insert_order = data.vid() - 100;
                // check that the order of the insertions matches VID order by comparing
                // it to the value of the ID (which is inserted in decreasing order)
                let id_value = 10 - insert_order;
                assert_eq!(id, format!("{}", id_value));
            }
            _ => panic!("wrong entity modification type"),
        }
    }
}

// Test that demonstrates the VID collision bug when multiple offchain triggers
// use separate VidGenerators. Each gets its own sequence starting at RESERVED_VIDS
// (100), so entities in the same block get identical VIDs.
#[graph::test]
async fn offchain_trigger_vid_collision_without_shared_generator() {
    let block: i32 = 2_163_923;

    // Simulate first offchain trigger with its own VidGenerator
    let store1 = Arc::new(MockStore::new(BTreeMap::new()));
    let mut cache1 = EntityCache::new(store1, SeqGenerator::new(block));
    let band1_data = entity! { SCHEMA => id: "band1", name: "First Band" };
    let band1_key = make_band_key("band1");
    cache1
        .set(band1_key.clone(), band1_data, None)
        .await
        .unwrap();
    let result1 = cache1.as_modifications(block, &STOPWATCH).await.unwrap();

    // Simulate second offchain trigger with a SEPARATE VidGenerator
    let store2 = Arc::new(MockStore::new(BTreeMap::new()));
    let mut cache2 = EntityCache::new(store2, SeqGenerator::new(block));
    let band2_data = entity! { SCHEMA => id: "band2", name: "Second Band" };
    let band2_key = make_band_key("band2");
    cache2
        .set(band2_key.clone(), band2_data, None)
        .await
        .unwrap();
    let result2 = cache2.as_modifications(block, &STOPWATCH).await.unwrap();

    let vid1 = match &result1.modifications[0] {
        EntityModification::Insert { data, .. } => data.vid(),
        _ => panic!("expected Insert"),
    };
    let vid2 = match &result2.modifications[0] {
        EntityModification::Insert { data, .. } => data.vid(),
        _ => panic!("expected Insert"),
    };

    // BUG: Both VIDs are identical because each VidGenerator starts at 100
    let expected_vid = ((block as i64) << 32) + 100;
    assert_eq!(vid1, expected_vid);
    assert_eq!(vid2, expected_vid);
    assert_eq!(vid1, vid2, "VIDs collide when using separate VidGenerators");
}

// Test that demonstrates the fix: sharing a single VidGenerator across
// multiple EntityCaches prevents VID collisions.
#[graph::test]
async fn offchain_trigger_vid_no_collision_with_shared_generator() {
    let block: i32 = 2_163_923;
    let vid_gen = SeqGenerator::new(block);

    // First offchain trigger
    let store1 = Arc::new(MockStore::new(BTreeMap::new()));
    let mut cache1 = EntityCache::new(store1, vid_gen.cheap_clone());
    let band1_data = entity! { SCHEMA => id: "band1", name: "First Band" };
    let band1_key = make_band_key("band1");
    cache1
        .set(band1_key.clone(), band1_data, None)
        .await
        .unwrap();
    let result1 = cache1.as_modifications(block, &STOPWATCH).await.unwrap();

    // Second offchain trigger shares the same VidGenerator
    let store2 = Arc::new(MockStore::new(BTreeMap::new()));
    let mut cache2 = EntityCache::new(store2, vid_gen.cheap_clone());
    let band2_data = entity! { SCHEMA => id: "band2", name: "Second Band" };
    let band2_key = make_band_key("band2");
    cache2
        .set(band2_key.clone(), band2_data, None)
        .await
        .unwrap();
    let result2 = cache2.as_modifications(block, &STOPWATCH).await.unwrap();

    let vid1 = match &result1.modifications[0] {
        EntityModification::Insert { data, .. } => data.vid(),
        _ => panic!("expected Insert"),
    };
    let vid2 = match &result2.modifications[0] {
        EntityModification::Insert { data, .. } => data.vid(),
        _ => panic!("expected Insert"),
    };

    // With a shared VidGenerator, VIDs are different
    assert_ne!(
        vid1, vid2,
        "VIDs should NOT collide when sharing a VidGenerator"
    );
    let expected_vid1 = ((block as i64) << 32) + 100;
    let expected_vid2 = ((block as i64) << 32) + 101;
    assert_eq!(vid1, expected_vid1, "first trigger starts at vid_seq 100");
    assert_eq!(
        vid2, expected_vid2,
        "second trigger continues at vid_seq 101"
    );
}

// Simulate the ipfs.map() pattern: multiple EntityCache instances each create
// an entity using a shared SeqGenerator. VIDs must be unique and sequential.
#[graph::test]
async fn ipfs_map_pattern_vid_uniqueness() {
    let block: i32 = 42;
    let vid_gen = SeqGenerator::new(block);

    let mut all_vids = Vec::new();
    for i in 0..5u32 {
        let store = Arc::new(MockStore::new(BTreeMap::new()));
        let mut cache = EntityCache::new(store, vid_gen.cheap_clone());
        let data = entity! { SCHEMA => id: format!("band{i}"), name: format!("Band {i}") };
        let key = make_band_key(&format!("band{i}"));
        cache.set(key, data, None).await.unwrap();
        let result = cache.as_modifications(block, &STOPWATCH).await.unwrap();
        let vid = match &result.modifications[0] {
            EntityModification::Insert { data, .. } => data.vid(),
            _ => panic!("expected Insert"),
        };
        all_vids.push(vid);
    }

    for i in 1..all_vids.len() {
        assert_eq!(
            all_vids[i],
            all_vids[i - 1] + 1,
            "VIDs should be sequential"
        );
    }
}

const ACCOUNT_GQL: &str = "
    type Account @entity {
        id: ID!
        name: String!
        email: String!
        age: Int!
        wallets: [Wallet!]! @derivedFrom(field: \"account\")
    }

    interface Purse {
        id: ID!
        balance: Int!
    }

    type Wallet implements Purse @entity {
        id: ID!
        balance: Int!
        account: Account!
    }
";

const ACCOUNT: &str = "Account";
const WALLET: &str = "Wallet";
const PURSE: &str = "Purse";

lazy_static! {
    static ref LOAD_RELATED_ID_STRING: String = String::from("loadrelatedsubgraph");
    static ref LOAD_RELATED_ID: DeploymentHash =
        DeploymentHash::new(LOAD_RELATED_ID_STRING.as_str()).unwrap();
    static ref LOAD_RELATED_SUBGRAPH: InputSchema =
        InputSchema::parse_latest(ACCOUNT_GQL, LOAD_RELATED_ID.clone())
            .expect("Failed to parse user schema");
    static ref TEST_BLOCK_1_PTR: BlockPtr = (
        B256::from(hex!(
            "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13"
        )),
        1u64
    )
        .into();
    static ref WALLET_TYPE: EntityType = LOAD_RELATED_SUBGRAPH.entity_type(WALLET).unwrap();
    static ref ACCOUNT_TYPE: EntityType = LOAD_RELATED_SUBGRAPH.entity_type(ACCOUNT).unwrap();
    static ref PURSE_TYPE: EntityType = LOAD_RELATED_SUBGRAPH.entity_type(PURSE).unwrap();
}

fn run_store_test<R, F>(test: F)
where
    F: FnOnce(
            EntityCache,
            Arc<DieselSubgraphStore>,
            DeploymentLocator,
            Arc<dyn WritableStore>,
        ) -> R
        + Send
        + 'static,
    R: std::future::Future<Output = ()> + Send + 'static,
{
    run_test_sequentially(|store| async move {
        let subgraph_store = store.subgraph_store();
        // Reset state before starting
        remove_subgraphs().await;

        // Seed database with test data
        let deployment = insert_test_data(subgraph_store.clone()).await;
        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("we can get a writable store");

        // we send the information to the database
        writable.flush().await.unwrap();

        let read_store = Arc::new(writable.clone());

        let cache = EntityCache::new(read_store, SeqGenerator::new(0));
        // Run test and wait for the background writer to finish its work so
        // it won't conflict with the next test
        test(cache, subgraph_store.clone(), deployment, writable.clone()).await;
        writable.flush().await.unwrap();
    });
}

async fn insert_test_data(store: Arc<DieselSubgraphStore>) -> DeploymentLocator {
    let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
        id: LOAD_RELATED_ID.clone(),
        spec_version: Version::new(1, 3, 0),
        features: Default::default(),
        description: None,
        repository: None,
        schema: LOAD_RELATED_SUBGRAPH.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
        indexer_hints: None,
    };

    // Create SubgraphDeploymentEntity
    let deployment = DeploymentCreate::new(String::new(), &manifest, None);
    let name = SubgraphName::new("test/store").unwrap();
    let node_id = NodeId::new("test").unwrap();
    let deployment = store
        .create_subgraph_deployment(
            name,
            &LOAD_RELATED_SUBGRAPH,
            deployment,
            node_id,
            NETWORK_NAME.to_string(),
            SubgraphVersionSwitchingMode::Instant,
        )
        .await
        .unwrap();

    // 1 account 3 wallets
    let test_entity_1 = create_account_entity("1", "Johnton", "tonofjohn@email.com", 67_i32, 1);
    let id_one = WALLET_TYPE.parse_id("1").unwrap();
    let wallet_entity_1 = create_wallet_operation("1", &id_one, 67_i32, 1);
    let wallet_entity_2 = create_wallet_operation("2", &id_one, 92_i32, 2);
    let wallet_entity_3 = create_wallet_operation("3", &id_one, 192_i32, 3);
    // 1 account 1 wallet
    let test_entity_2 = create_account_entity("2", "Cindini", "dinici@email.com", 42_i32, 2);
    let id_two = WALLET_TYPE.parse_id("2").unwrap();
    let wallet_entity_4 = create_wallet_operation("4", &id_two, 32_i32, 4);
    // 1 account 0 wallets
    let test_entity_3 = create_account_entity("3", "Shaqueeena", "queensha@email.com", 28_i32, 3);
    transact_entity_operations(
        &store,
        &deployment,
        GENESIS_PTR.clone(),
        vec![
            test_entity_1,
            test_entity_2,
            test_entity_3,
            wallet_entity_1,
            wallet_entity_2,
            wallet_entity_3,
            wallet_entity_4,
        ],
    )
    .await
    .unwrap();
    deployment
}

fn create_account_entity(id: &str, name: &str, email: &str, age: i32, vid: i64) -> EntityOperation {
    let test_entity =
        entity! { LOAD_RELATED_SUBGRAPH => id: id, name: name, email: email, age: age, vid: vid};

    EntityOperation::Set {
        key: ACCOUNT_TYPE.parse_key(id).unwrap(),
        data: test_entity,
    }
}

fn create_wallet_entity(id: &str, account_id: &Id, balance: i32, vid: i64) -> Entity {
    let account_id = Value::from(account_id.clone());
    entity! { LOAD_RELATED_SUBGRAPH => id: id, account: account_id, balance: balance, vid: vid}
}

fn create_wallet_entity_no_vid(id: &str, account_id: &Id, balance: i32) -> Entity {
    let account_id = Value::from(account_id.clone());
    entity! { LOAD_RELATED_SUBGRAPH => id: id, account: account_id, balance: balance}
}

fn create_wallet_operation(id: &str, account_id: &Id, balance: i32, vid: i64) -> EntityOperation {
    let test_wallet = create_wallet_entity(id, account_id, balance, vid);
    EntityOperation::Set {
        key: WALLET_TYPE.parse_key(id).unwrap(),
        data: test_wallet,
    }
}

#[test]
fn check_for_account_with_multiple_wallets() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("1").unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let wallet_1 = create_wallet_entity("1", &account_id, 67_i32, 1);
        let wallet_2 = create_wallet_entity("2", &account_id, 92_i32, 2);
        let wallet_3 = create_wallet_entity("3", &account_id, 192_i32, 3);
        let expeted_vec = vec![wallet_1, wallet_2, wallet_3];

        assert_eq!(result, expeted_vec);
    });
}

#[test]
fn check_for_account_with_single_wallet() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("2").unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let wallet_1 = create_wallet_entity("4", &account_id, 32_i32, 4);
        let expeted_vec = vec![wallet_1];

        assert_eq!(result, expeted_vec);
    });
}

#[test]
fn check_for_account_with_no_wallet() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("3").unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id,
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let expeted_vec = vec![];

        assert_eq!(result, expeted_vec);
    });
}

#[test]
fn check_for_account_that_doesnt_exist() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("4").unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id,
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let expeted_vec = vec![];

        assert_eq!(result, expeted_vec);
    });
}

#[test]
fn check_for_non_existent_field() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("1").unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "friends".into(),
            entity_id: account_id,
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap_err();
        let expected = format!(
            "Entity {}[{}]: unknown field `{}`",
            request.entity_type, request.entity_id, request.entity_field,
        );

        assert_eq!(format!("{}", result), expected);
    });
}

#[test]
fn check_for_insert_async_store() {
    run_store_test(|mut cache, store, deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("2").unwrap();
        // insert a new wallet
        let wallet_entity_5 = create_wallet_operation("5", &account_id, 79_i32, 12);
        let wallet_entity_6 = create_wallet_operation("6", &account_id, 200_i32, 13);

        transact_entity_operations(
            &store,
            &deployment,
            TEST_BLOCK_1_PTR.clone(),
            vec![wallet_entity_5, wallet_entity_6],
        )
        .await
        .unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let wallet_1 = create_wallet_entity("4", &account_id, 32_i32, 4);
        let wallet_2 = create_wallet_entity("5", &account_id, 79_i32, 12);
        let wallet_3 = create_wallet_entity("6", &account_id, 200_i32, 13);
        let expeted_vec = vec![wallet_1, wallet_2, wallet_3];

        assert_eq!(result, expeted_vec);
    });
}
#[test]
fn check_for_insert_async_not_related() {
    run_store_test(|mut cache, store, deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("2").unwrap();
        // insert a new wallet
        let wallet_entity_5 = create_wallet_operation("5", &account_id, 79_i32, 5);
        let wallet_entity_6 = create_wallet_operation("6", &account_id, 200_i32, 6);

        transact_entity_operations(
            &store,
            &deployment,
            TEST_BLOCK_1_PTR.clone(),
            vec![wallet_entity_5, wallet_entity_6],
        )
        .await
        .unwrap();
        let account_id = ACCOUNT_TYPE.parse_id("1").unwrap();
        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let wallet_1 = create_wallet_entity("1", &account_id, 67_i32, 1);
        let wallet_2 = create_wallet_entity("2", &account_id, 92_i32, 2);
        let wallet_3 = create_wallet_entity("3", &account_id, 192_i32, 3);
        let expeted_vec = vec![wallet_1, wallet_2, wallet_3];

        assert_eq!(result, expeted_vec);
    });
}

#[test]
fn check_for_update_async_related() {
    run_store_test(|mut cache, store, deployment, writable| async move {
        let entity_key = WALLET_TYPE.parse_key("1").unwrap();
        let account_id = entity_key.entity_id.clone();
        let wallet_entity_update = create_wallet_operation("1", &account_id, 79_i32, 11);

        let new_data = match wallet_entity_update {
            EntityOperation::Set { ref data, .. } => data.clone(),
            _ => unreachable!(),
        };
        assert_ne!(writable.get(&entity_key).await.unwrap().unwrap(), new_data);
        // insert a new wallet
        transact_entity_operations(
            &store,
            &deployment,
            TEST_BLOCK_1_PTR.clone(),
            vec![wallet_entity_update],
        )
        .await
        .unwrap();

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let wallet_2 = create_wallet_entity("2", &account_id, 92_i32, 2);
        let wallet_3 = create_wallet_entity("3", &account_id, 192_i32, 3);
        let expeted_vec = vec![new_data, wallet_2, wallet_3];

        assert_eq!(result, expeted_vec);
    });
}

#[test]
fn check_for_delete_async_related() {
    run_store_test(|mut cache, store, deployment, _writable| async move {
        let account_id = ACCOUNT_TYPE.parse_id("1").unwrap();
        let del_key = WALLET_TYPE.parse_key("1").unwrap();
        // delete wallet
        transact_entity_operations(
            &store,
            &deployment,
            TEST_BLOCK_1_PTR.clone(),
            vec![EntityOperation::Remove { key: del_key }],
        )
        .await
        .unwrap();

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: account_id.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();
        let wallet_2 = create_wallet_entity("2", &account_id, 92_i32, 2);
        let wallet_3 = create_wallet_entity("3", &account_id, 192_i32, 3);
        let expeted_vec = vec![wallet_2, wallet_3];

        assert_eq!(result, expeted_vec);
    });
}
#[test]
fn scoped_get() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        // Key for an existing entity that is in the store
        let account1 = ACCOUNT_TYPE.parse_id("1").unwrap();
        let key1 = WALLET_TYPE.parse_key("1").unwrap();
        let wallet1 = create_wallet_entity_no_vid("1", &account1, 67);

        // Create a new entity that is not in the store
        let account5 = ACCOUNT_TYPE.parse_id("5").unwrap();
        let mut wallet5 = create_wallet_entity_no_vid("5", &account5, 100);
        let key5 = WALLET_TYPE.parse_key("5").unwrap();
        cache
            .set(key5.clone(), wallet5.clone(), None)
            .await
            .unwrap();

        wallet5.set_vid(100).unwrap();
        // For the new entity, we can retrieve it with either scope
        let act5 = cache.get(&key5, GetScope::InBlock).await.unwrap();
        assert_eq!(Some(&wallet5), act5.as_ref().map(|e| e.as_ref()));
        let act5 = cache.get(&key5, GetScope::Store).await.unwrap();
        assert_eq!(Some(&wallet5), act5.as_ref().map(|e| e.as_ref()));

        let mut wallet1a = wallet1.clone();
        wallet1a.set_vid(1).unwrap();
        // For an entity in the store, we can not get it `InBlock` but with
        // `Store`
        let act1 = cache.get(&key1, GetScope::InBlock).await.unwrap();
        assert_eq!(None, act1);
        let act1 = cache.get(&key1, GetScope::Store).await.unwrap();
        assert_eq!(Some(&wallet1a), act1.as_ref().map(|e| e.as_ref()));

        // Even after reading from the store, the entity is not visible with
        // `InBlock`
        let act1 = cache.get(&key1, GetScope::InBlock).await.unwrap();
        assert_eq!(None, act1);
        // But if it gets updated, it becomes visible with either scope
        let mut wallet1 = wallet1;
        wallet1.set("balance", 70).unwrap();
        cache
            .set(key1.clone(), wallet1.clone(), None)
            .await
            .unwrap();
        wallet1a = wallet1;
        wallet1a.set_vid(101).unwrap();
        let act1 = cache.get(&key1, GetScope::InBlock).await.unwrap();
        assert_eq!(Some(&wallet1a), act1.as_ref().map(|e| e.as_ref()));
        let act1 = cache.get(&key1, GetScope::Store).await.unwrap();
        assert_eq!(Some(&wallet1a), act1.as_ref().map(|e| e.as_ref()));
    })
}

/// Entities should never contain a `__typename` or `g$parent_id` field, if
/// they do, that can cause PoI divergences, because entities will differ
/// depending on whether they had to be loaded from the database or stuck
/// around in the cache where they won't have these attributes
#[test]
fn no_internal_keys() {
    run_store_test(|mut cache, _, _, writable| async move {
        #[track_caller]
        fn check(key: &EntityKey, entity: &Entity) {
            // Validate checks that all attributes are actually declared in
            // the schema
            entity.validate(key).expect("the entity is valid");
        }
        let key = WALLET_TYPE.parse_key("1").unwrap();

        let wallet = writable.get(&key).await.unwrap().unwrap();
        check(&key, &wallet);

        let wallet = cache.get(&key, GetScope::Store).await.unwrap().unwrap();
        check(&key, &wallet);
    });
}

#[test]
fn no_interface_mods() {
    run_store_test(|mut cache, _, _, _| async move {
        let key = PURSE_TYPE.parse_key("1").unwrap();

        // This should probably be an error, but changing that would not be
        // backwards compatible
        assert_eq!(None, cache.get(&key, GetScope::InBlock).await.unwrap());

        assert!(matches!(
            cache.get(&key, GetScope::Store).await,
            Err(StoreError::UnknownTable(_))
        ));

        let entity = entity! { LOAD_RELATED_SUBGRAPH => id: "1", balance: 100 };

        cache.set(key, entity, None).await.unwrap_err();
    })
}

// ---------------------------------------------------------------------------
// Tests for `EntityCache::load_related` under same-block changes to
// derived field membership. Each test exercises a write that should
// move an entity into or out of an `account.wallets` derived
// collection (reassignment, removal, or fresh creation), asserting
// that the returned collection reflects the entity's final state.
//
// Fixture (from `insert_test_data`):
//   - account "1" owns wallets "1", "2", "3"
//   - account "2" owns wallet "4"
//   - account "3" owns no wallets
//
// Tests that need writes in `EntityCache::handler_updates` (rather than
// `updates`) drive a handler lifecycle through `BlockState::enter_handler`
// / `exit_handler`. `EntityCache` has two in-block write layers: writes
// outside a handler go to `self.updates`; writes inside an active
// handler go to `self.handler_updates`. `exit_handler` merges
// `handler_updates` into `updates`. Driving this lifecycle requires
// `BlockState` because the `enter`/`exit` methods on `EntityCache` are
// crate-private.
// ---------------------------------------------------------------------------

/// Wallet "1" is reassigned from account "1" to account "3" within a
/// single handler. Account "1"'s derived `wallets` collection must
/// reflect the final state and exclude wallet "1".
#[test]
fn load_related_excludes_wallet_that_leaves_collection() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let acc_1 = ACCOUNT_TYPE.parse_id("1").unwrap();
        let acc_3 = ACCOUNT_TYPE.parse_id("3").unwrap();
        let wallet_1_key = WALLET_TYPE.parse_key("1").unwrap();

        // Precondition: fixture placed wallet "1" under account "1".
        // The `set` below is a reassignment of an existing row.
        let pre = cache
            .get(&wallet_1_key, GetScope::Store)
            .await
            .unwrap()
            .expect("fixture should have inserted wallet 1");
        assert_eq!(pre.get("account").unwrap(), &Value::from(acc_1.clone()));

        let reassigned = create_wallet_entity_no_vid("1", &acc_3, 67_i32);
        cache.set(wallet_1_key, reassigned, None).await.unwrap();

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: acc_1.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();

        let result_ids: Vec<_> = result
            .iter()
            .map(|e| e.get("id").unwrap().clone())
            .collect();

        assert_eq!(
            result.len(),
            2,
            "account 1 should own 2 wallets after reassignment, got {:?}",
            result_ids,
        );
        for w in &result {
            assert_eq!(
                w.get("account").unwrap(),
                &Value::from(acc_1.clone()),
                "every wallet in account 1's collection must have account=1, got {:?}",
                w,
            );
        }
    });
}

/// Wallet "4" is moved from account "2" to account "3" in handler 1,
/// then back to account "2" in handler 2. Account "3"'s derived
/// `wallets` collection must reflect the final state and exclude
/// wallet "4".
#[test]
fn load_related_excludes_wallet_that_leaves_collection_across_handlers() {
    run_store_test(|_cache, _store, _deployment, writable| async move {
        let acc_2 = ACCOUNT_TYPE.parse_id("2").unwrap();
        let acc_3 = ACCOUNT_TYPE.parse_id("3").unwrap();
        let wallet_4_key = WALLET_TYPE.parse_key("4").unwrap();

        let mut state = BlockState::new(
            writable.clone(),
            EntityLfuCache::new(),
            SeqGenerator::new(10),
        );

        // Precondition: fixture placed wallet "4" under account "2".
        let pre = writable
            .get(&wallet_4_key)
            .await
            .unwrap()
            .expect("fixture should have inserted wallet 4");
        assert_eq!(pre.get("account").unwrap(), &Value::from(acc_2.clone()));

        // Handler 1: move wallet "4" to account "3". On exit, this
        // write is promoted from handler_updates into updates.
        state.enter_handler();
        state
            .entity_cache
            .set(
                wallet_4_key.clone(),
                create_wallet_entity_no_vid("4", &acc_3, 32_i32),
                None,
            )
            .await
            .unwrap();
        state.exit_handler();

        // Handler 2: move wallet "4" back to account "2". This write
        // is in handler_updates; the prior handler's write is in updates.
        state.enter_handler();
        state
            .entity_cache
            .set(
                wallet_4_key,
                create_wallet_entity_no_vid("4", &acc_2, 32_i32),
                None,
            )
            .await
            .unwrap();

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: acc_3.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = state.entity_cache.load_related(&request).await.unwrap();

        assert!(
            result.is_empty(),
            "account 3 should own no wallets (wallet 4's final account is 2), got {:?}",
            result,
        );
    });
}

/// Wallet "4" is touched in handler 1 without changing its `account`,
/// then reassigned from account "2" to account "3" in handler 2.
/// Account "3"'s derived `wallets` collection must reflect the final
/// state and include wallet "4".
#[test]
fn load_related_includes_wallet_that_joins_collection_across_handlers() {
    run_store_test(|_cache, _store, _deployment, writable| async move {
        let acc_2 = ACCOUNT_TYPE.parse_id("2").unwrap();
        let acc_3 = ACCOUNT_TYPE.parse_id("3").unwrap();
        let wallet_4_key = WALLET_TYPE.parse_key("4").unwrap();

        let mut state = BlockState::new(
            writable.clone(),
            EntityLfuCache::new(),
            SeqGenerator::new(10),
        );

        // Precondition: fixture placed wallet "4" under account "2".
        let pre = writable
            .get(&wallet_4_key)
            .await
            .unwrap()
            .expect("fixture should have inserted wallet 4");
        assert_eq!(pre.get("account").unwrap(), &Value::from(acc_2.clone()));

        // Handler 1: touch wallet "4" (bump balance) but keep its account.
        // On exit, this write is promoted into updates.
        state.enter_handler();
        state
            .entity_cache
            .set(
                wallet_4_key.clone(),
                create_wallet_entity_no_vid("4", &acc_2, 999_i32),
                None,
            )
            .await
            .unwrap();
        state.exit_handler();

        // Handler 2: reassign wallet "4" to account "3". This write is
        // in handler_updates; handler 1's write remains in updates.
        state.enter_handler();
        state
            .entity_cache
            .set(
                wallet_4_key.clone(),
                create_wallet_entity_no_vid("4", &acc_3, 999_i32),
                None,
            )
            .await
            .unwrap();

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: acc_3.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = state.entity_cache.load_related(&request).await.unwrap();

        assert_eq!(
            result.len(),
            1,
            "account 3 should own wallet 4 after reassignment, got {:?}",
            result,
        );
        assert_eq!(result[0].get("id").unwrap(), &Value::from("4"),);
        assert_eq!(
            result[0].get("account").unwrap(),
            &Value::from(acc_3.clone()),
            "wallet 4 must carry handler 2's post-merge account=3",
        );
    });
}

/// Wallet "1" is removed via a write that lands directly in
/// `EntityCache::updates` (no active handler). Account "1"'s derived
/// `wallets` collection must reflect the final state and exclude
/// wallet "1".
#[test]
fn load_related_excludes_wallet_removed_via_updates() {
    run_store_test(|mut cache, _store, _deployment, _writable| async move {
        let acc_1 = ACCOUNT_TYPE.parse_id("1").unwrap();
        let wallet_1_key = WALLET_TYPE.parse_key("1").unwrap();

        // Precondition: fixture placed wallet "1" under account "1".
        let pre = cache
            .get(&wallet_1_key, GetScope::Store)
            .await
            .unwrap()
            .expect("fixture should have inserted wallet 1");
        assert_eq!(pre.get("account").unwrap(), &Value::from(acc_1.clone()));

        cache.remove(wallet_1_key);

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: acc_1.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = cache.load_related(&request).await.unwrap();

        let result_ids: Vec<_> = result
            .iter()
            .map(|e| e.get("id").unwrap().clone())
            .collect();

        assert_eq!(
            result.len(),
            2,
            "account 1 should own 2 wallets after wallet 1 is removed, got {:?}",
            result_ids,
        );
        assert!(
            !result_ids.contains(&Value::from("1")),
            "removed wallet 1 must not appear, got {:?}",
            result_ids,
        );
    });
}

/// Wallet "1" is removed inside an active handler so the Remove op
/// sits in `EntityCache::handler_updates` at query time. Account "1"'s
/// derived `wallets` collection must reflect the final state and
/// exclude wallet "1".
#[test]
fn load_related_excludes_wallet_removed_via_handler_updates() {
    run_store_test(|_cache, _store, _deployment, writable| async move {
        let acc_1 = ACCOUNT_TYPE.parse_id("1").unwrap();
        let wallet_1_key = WALLET_TYPE.parse_key("1").unwrap();

        let mut state = BlockState::new(
            writable.clone(),
            EntityLfuCache::new(),
            SeqGenerator::new(10),
        );

        // Precondition: fixture placed wallet "1" under account "1".
        let pre = writable
            .get(&wallet_1_key)
            .await
            .unwrap()
            .expect("fixture should have inserted wallet 1");
        assert_eq!(pre.get("account").unwrap(), &Value::from(acc_1.clone()));

        // Remove wallet "1" inside an active handler. The Remove op
        // sits in handler_updates and is queried before exit_handler.
        state.enter_handler();
        state.entity_cache.remove(wallet_1_key);

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: acc_1.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = state.entity_cache.load_related(&request).await.unwrap();

        let result_ids: Vec<_> = result
            .iter()
            .map(|e| e.get("id").unwrap().clone())
            .collect();

        assert_eq!(
            result.len(),
            2,
            "account 1 should own 2 wallets after wallet 1 is removed, got {:?}",
            result_ids,
        );
        assert!(
            !result_ids.contains(&Value::from("1")),
            "removed wallet 1 must not appear, got {:?}",
            result_ids,
        );
    });
}

/// A wallet is created inside a handler with no prior store baseline.
/// `load_related` called from the same handler must observe the new
/// wallet in its parent account's derived collection.
#[test]
fn load_related_includes_wallet_created_in_handler() {
    run_store_test(|_cache, _store, _deployment, writable| async move {
        let acc_3 = ACCOUNT_TYPE.parse_id("3").unwrap();
        let new_wallet_key = WALLET_TYPE.parse_key("99").unwrap();

        let mut state = BlockState::new(
            writable.clone(),
            EntityLfuCache::new(),
            SeqGenerator::new(10),
        );

        // Precondition: account "3" starts with no wallets and
        // wallet "99" does not exist in the store.
        assert!(
            writable.get(&new_wallet_key).await.unwrap().is_none(),
            "wallet 99 should not exist in the store baseline",
        );

        // Create a fresh wallet under account "3" inside a handler.
        state.enter_handler();
        state
            .entity_cache
            .set(
                new_wallet_key,
                create_wallet_entity_no_vid("99", &acc_3, 500_i32),
                None,
            )
            .await
            .unwrap();

        let request = LoadRelatedRequest {
            entity_type: ACCOUNT_TYPE.clone(),
            entity_field: "wallets".into(),
            entity_id: acc_3.clone(),
            causality_region: CausalityRegion::ONCHAIN,
        };
        let result = state.entity_cache.load_related(&request).await.unwrap();

        assert_eq!(
            result.len(),
            1,
            "account 3 should own the newly created wallet 99, got {:?}",
            result,
        );
        assert_eq!(result[0].get("id").unwrap(), &Value::from("99"));
        assert_eq!(
            result[0].get("account").unwrap(),
            &Value::from(acc_3.clone()),
        );
    });
}

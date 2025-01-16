use graph::blockchain::block_stream::{EntitySourceOperation, FirehoseCursor};
use graph::data::subgraph::schema::DeploymentCreate;
use graph::data::value::Word;
use graph::data_source::CausalityRegion;
use graph::schema::{EntityKey, EntityType, InputSchema};
use lazy_static::lazy_static;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::ops::Range;
use test_store::*;

use graph::components::store::{
    DeploymentLocator, DerivedEntityQuery, SourceableStore, WritableStore,
};
use graph::data::subgraph::*;
use graph::semver::Version;
use graph::{entity, prelude::*};
use graph_store_postgres::layout_for_tests::writable;
use graph_store_postgres::{Store as DieselStore, SubgraphStore as DieselSubgraphStore};
use web3::types::H256;

const SCHEMA_GQL: &str = "
    type Counter @entity {
        id: ID!,
        count: Int!,
    }
    type Counter2 @entity(immutable: true) {
        id: ID!,
        count: Int!,
    }
";

const COUNTER: &str = "Counter";
const COUNTER2: &str = "Counter2";

lazy_static! {
    static ref TEST_SUBGRAPH_ID_STRING: String = String::from("writableSubgraph");
    static ref TEST_SUBGRAPH_ID: DeploymentHash =
        DeploymentHash::new(TEST_SUBGRAPH_ID_STRING.as_str()).unwrap();
    static ref TEST_SUBGRAPH_SCHEMA: InputSchema =
        InputSchema::parse_latest(SCHEMA_GQL, TEST_SUBGRAPH_ID.clone())
            .expect("Failed to parse user schema");
    static ref COUNTER_TYPE: EntityType = TEST_SUBGRAPH_SCHEMA.entity_type(COUNTER).unwrap();
    static ref COUNTER2_TYPE: EntityType = TEST_SUBGRAPH_SCHEMA.entity_type(COUNTER2).unwrap();
}

/// Inserts test data into the store.
///
/// Create a new empty subgraph with schema `SCHEMA_GQL`
async fn insert_test_data(store: Arc<DieselSubgraphStore>) -> DeploymentLocator {
    let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
        id: TEST_SUBGRAPH_ID.clone(),
        spec_version: Version::new(1, 0, 0),
        features: Default::default(),
        description: None,
        repository: None,
        schema: TEST_SUBGRAPH_SCHEMA.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
        indexer_hints: None,
    };

    // Create SubgraphDeploymentEntity
    let deployment = DeploymentCreate::new(String::new(), &manifest, None);
    let name = SubgraphName::new("test/writable").unwrap();
    let node_id = NodeId::new("test").unwrap();

    store
        .create_subgraph_deployment(
            name,
            &TEST_SUBGRAPH_SCHEMA,
            deployment,
            node_id,
            NETWORK_NAME.to_string(),
            SubgraphVersionSwitchingMode::Instant,
        )
        .unwrap()
}

/// Removes test data from the database behind the store.
fn remove_test_data(store: Arc<DieselSubgraphStore>) {
    store
        .delete_all_entities_for_test_use_only()
        .expect("deleting test entities succeeds");
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(
            Arc<DieselStore>,
            Arc<dyn WritableStore>,
            Arc<dyn SourceableStore>,
            DeploymentLocator,
        ) -> R
        + Send
        + 'static,
    R: std::future::Future<Output = ()> + Send + 'static,
{
    run_test_sequentially(|store| async move {
        let subgraph_store = store.subgraph_store();
        // Reset state before starting
        remove_test_data(subgraph_store.clone());

        // Seed database with test data
        let deployment = insert_test_data(subgraph_store.clone()).await;
        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("we can get a writable store");
        let sourceable = store
            .subgraph_store()
            .sourceable(deployment.id)
            .await
            .expect("we can get a writable store");

        // Run test and wait for the background writer to finish its work so
        // it won't conflict with the next test
        test(store, writable, sourceable, deployment).await;
    });
}

fn block_pointer(number: u8) -> BlockPtr {
    let hash = H256::from([number; 32]);
    BlockPtr::from((hash, number as BlockNumber))
}

fn count_key(id: &str) -> EntityKey {
    COUNTER_TYPE.parse_key(id).unwrap()
}

async fn insert_count(
    store: &Arc<DieselSubgraphStore>,
    deployment: &DeploymentLocator,
    block: u8,
    count: u8,
    immutable_only: bool,
) {
    let count_key_local = |counter_type: &EntityType, id: &str| counter_type.parse_key(id).unwrap();
    let data = entity! { TEST_SUBGRAPH_SCHEMA =>
        id: "1",
        count: count as i32,
        vid: block as i64,
    };
    let entity_op = if block != 3 && block != 5 && block != 7 {
        EntityOperation::Set {
            key: count_key_local(&COUNTER_TYPE, &data.get("id").unwrap().to_string()),
            data,
        }
    } else {
        EntityOperation::Remove {
            key: count_key_local(&COUNTER_TYPE, &data.get("id").unwrap().to_string()),
        }
    };
    let mut ops = if immutable_only {
        vec![]
    } else {
        vec![entity_op]
    };
    if block < 6 {
        let data = entity! { TEST_SUBGRAPH_SCHEMA =>
            id: &block.to_string(),
            count :count as i32,
            vid: block as i64,
        };
        let entity_op = EntityOperation::Set {
            key: count_key_local(&COUNTER2_TYPE, &data.get("id").unwrap().to_string()),
            data,
        };
        ops.push(entity_op);
    }
    transact_entity_operations(store, deployment, block_pointer(block), ops)
        .await
        .unwrap();
}

async fn pause_writer(deployment: &DeploymentLocator) {
    flush(deployment).await.unwrap();
    writable::allow_steps(deployment, 0).await;
}

/// Test that looking up entities when several changes to the same entity
/// are queued works. When `batch` is true, the changes all reside in one
/// batch. If it is false, each change is in its own batch.
///
/// `read_count` lets us look up entities in different ways to exercise
/// different methods in `WritableStore`
fn get_with_pending<F>(batch: bool, read_count: F)
where
    F: Send + Fn(&dyn WritableStore) -> i32 + Sync + 'static,
{
    run_test(move |store, writable, _, deployment| async move {
        let subgraph_store = store.subgraph_store();

        let read_count = || read_count(writable.as_ref());

        if !batch {
            writable.deployment_synced().unwrap();
        }

        for count in 1..4 {
            insert_count(&subgraph_store, &deployment, count, count, false).await;
        }

        // Test reading back with pending writes to the same entity
        pause_writer(&deployment).await;
        for count in 4..7 {
            insert_count(&subgraph_store, &deployment, count, count, false).await;
        }
        assert_eq!(6, read_count());

        writable.flush().await.unwrap();
        assert_eq!(6, read_count());

        // Test reading back with pending writes and a pending revert
        for count in 7..10 {
            insert_count(&subgraph_store, &deployment, count, count, false).await;
        }
        writable
            .revert_block_operations(block_pointer(2), FirehoseCursor::None)
            .await
            .unwrap();

        assert_eq!(2, read_count());

        writable.flush().await.unwrap();
        assert_eq!(2, read_count());
    })
}

/// Get the count using `WritableStore::get_many`
fn count_get_many(writable: &dyn WritableStore) -> i32 {
    let key = count_key("1");
    let keys = BTreeSet::from_iter(vec![key.clone()]);
    let counter = writable.get_many(keys).unwrap().get(&key).unwrap().clone();
    counter.get("count").unwrap().as_int().unwrap()
}

/// Get the count using `WritableStore::get`
fn count_get(writable: &dyn WritableStore) -> i32 {
    let counter = writable.get(&count_key("1")).unwrap().unwrap();
    counter.get("count").unwrap().as_int().unwrap()
}

fn count_get_derived(writable: &dyn WritableStore) -> i32 {
    let key = count_key("1");
    let query = DerivedEntityQuery {
        entity_type: key.entity_type.clone(),
        entity_field: Word::from("id"),
        value: key.entity_id.clone(),
        causality_region: CausalityRegion::ONCHAIN,
    };
    let map = writable.get_derived(&query).unwrap();
    let counter = map.get(&key).unwrap();
    counter.get("count").unwrap().as_int().unwrap()
}

#[test]
fn get_batch() {
    get_with_pending(true, count_get);
}

#[test]
fn get_nobatch() {
    get_with_pending(false, count_get);
}

#[test]
fn get_many_batch() {
    get_with_pending(true, count_get_many);
}

#[test]
fn get_many_nobatch() {
    get_with_pending(false, count_get_many);
}

#[test]
fn get_derived_batch() {
    get_with_pending(true, count_get_derived);
}

#[test]
fn get_derived_nobatch() {
    get_with_pending(false, count_get_derived);
}

#[test]
fn restart() {
    run_test(|store, writable, _, deployment| async move {
        let subgraph_store = store.subgraph_store();
        let schema = subgraph_store.input_schema(&deployment.hash).unwrap();

        // Cause an error by leaving out the non-nullable `count` attribute
        let entity_ops = vec![EntityOperation::Set {
            key: count_key("1"),
            data: entity! { schema => id: "1", vid: 0i64},
        }];
        transact_entity_operations(
            &subgraph_store,
            &deployment,
            block_pointer(1),
            entity_ops.clone(),
        )
        .await
        .unwrap();
        // flush checks for errors and therefore fails
        writable
            .flush()
            .await
            .expect_err("writing with missing non-nullable field should fail");

        // We now have a poisoned store. Restarting it gives us a new store
        // that works again
        let writable = writable.restart().await.unwrap().unwrap();
        writable.flush().await.unwrap();

        // Retry our write with correct data
        let entity_ops = vec![EntityOperation::Set {
            key: count_key("1"),
            data: entity! { schema => id: "1", count: 1, vid: 0i64},
        }];
        // `SubgraphStore` caches the correct writable so that this call
        // uses the restarted writable, and is equivalent to using
        // `writable` directly
        transact_entity_operations(
            &subgraph_store,
            &deployment,
            block_pointer(1),
            entity_ops.clone(),
        )
        .await
        .unwrap();
        // Look, no errors
        writable.flush().await.unwrap();
    })
}

#[test]
fn read_range_test() {
    run_test(|store, writable, sourceable, deployment| async move {
        let result_entities = vec![
            r#"(1, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter), entity: Entity { count: Int(2), id: String("1"), vid: Int8(1) }, vid: 1 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(2), id: String("1"), vid: Int8(1) }, vid: 1 }])"#,
            r#"(2, [EntitySourceOperation { entity_op: Modify, entity_type: EntityType(Counter), entity: Entity { count: Int(4), id: String("1"), vid: Int8(2) }, vid: 2 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(4), id: String("2"), vid: Int8(2) }, vid: 2 }])"#,
            r#"(3, [EntitySourceOperation { entity_op: Delete, entity_type: EntityType(Counter), entity: Entity { count: Int(4), id: String("1"), vid: Int8(2) }, vid: 2 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(6), id: String("3"), vid: Int8(3) }, vid: 3 }])"#,
            r#"(4, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter), entity: Entity { count: Int(8), id: String("1"), vid: Int8(4) }, vid: 4 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(8), id: String("4"), vid: Int8(4) }, vid: 4 }])"#,
            r#"(5, [EntitySourceOperation { entity_op: Delete, entity_type: EntityType(Counter), entity: Entity { count: Int(8), id: String("1"), vid: Int8(4) }, vid: 4 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(10), id: String("5"), vid: Int8(5) }, vid: 5 }])"#,
            r#"(6, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter), entity: Entity { count: Int(12), id: String("1"), vid: Int8(6) }, vid: 6 }])"#,
            r#"(7, [EntitySourceOperation { entity_op: Delete, entity_type: EntityType(Counter), entity: Entity { count: Int(12), id: String("1"), vid: Int8(6) }, vid: 6 }])"#,
        ];
        let subgraph_store = store.subgraph_store();
        writable.deployment_synced().unwrap();

        for count in 1..=5 {
            insert_count(&subgraph_store, &deployment, count, 2 * count, false).await;
        }
        writable.flush().await.unwrap();
        writable.deployment_synced().unwrap();

        let br: Range<BlockNumber> = 0..18;
        let entity_types = vec![COUNTER_TYPE.clone(), COUNTER2_TYPE.clone()];
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types.clone(), CausalityRegion::ONCHAIN, br.clone())
            .unwrap();
        assert_eq!(e.len(), 5);
        for en in &e {
            let index = *en.0 - 1;
            let a = result_entities[index as usize];
            assert_eq!(a, format!("{:?}", en));
        }
        for count in 6..=7 {
            insert_count(&subgraph_store, &deployment, count, 2 * count, false).await;
        }
        writable.flush().await.unwrap();
        writable.deployment_synced().unwrap();
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types, CausalityRegion::ONCHAIN, br)
            .unwrap();
        assert_eq!(e.len(), 7);
        for en in &e {
            let index = *en.0 - 1;
            let a = result_entities[index as usize];
            assert_eq!(a, format!("{:?}", en));
        }
    })
}

#[test]
fn read_immutable_only_range_test() {
    run_test(|store, writable, sourceable, deployment| async move {
        let subgraph_store = store.subgraph_store();
        writable.deployment_synced().unwrap();

        for count in 1..=4 {
            insert_count(&subgraph_store, &deployment, count, 2 * count, true).await;
        }
        writable.flush().await.unwrap();
        writable.deployment_synced().unwrap();
        let br: Range<BlockNumber> = 0..18;
        let entity_types = vec![COUNTER2_TYPE.clone()];
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types.clone(), CausalityRegion::ONCHAIN, br.clone())
            .unwrap();
        assert_eq!(e.len(), 4);
    })
}

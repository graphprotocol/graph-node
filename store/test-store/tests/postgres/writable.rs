use graph::blockchain::block_stream::{EntitySourceOperation, FirehoseCursor};
use graph::data::subgraph::schema::DeploymentCreate;
use graph::data::value::Word;
use graph::data_source::CausalityRegion;
use graph::prelude::alloy::primitives::B256;
use graph::schema::{EntityKey, EntityType, InputSchema};
use lazy_static::lazy_static;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
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

const SCHEMA_GQL: &str = "
    type Counter @entity {
        id: ID!,
        count: Int!,
    }
    type Counter2 @entity(immutable: true) {
        id: ID!,
        count: Int!,
    }
    type BytesId @entity {
        id: Bytes!,
        value: String!
    }
    type Int8Id @entity {
        id: Int8!,
        value: String!
    }
    type StringId @entity {
        id: String!,
        value: String!
    }
    type PoolCreated @entity(immutable: true) {
        id: Bytes!,
        token0: Bytes!,
        token1: Bytes!,
        fee: Int!,
        tickSpacing: Int!,
        pool: Bytes!,
        blockNumber: BigInt!,
        blockTimestamp: BigInt!,
        transactionHash: Bytes!,
        transactionFrom: Bytes!,
        transactionGasPrice: BigInt!,
        logIndex: BigInt!
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
        spec_version: Version::new(1, 3, 0),
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
        .await
        .unwrap()
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
    R: Future<Output = ()> + Send + 'static,
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
    let hash = B256::from([number; 32]);
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
fn get_with_pending<R, F>(batch: bool, read_count: F)
where
    F: Send + Fn(Arc<dyn WritableStore>) -> R + Sync + 'static,
    R: Future<Output = i32> + Send + 'static,
{
    run_test(move |store, writable, _, deployment| async move {
        let subgraph_store = store.subgraph_store();

        let read_count = || read_count(writable.cheap_clone());

        if !batch {
            writable.deployment_synced(block_pointer(0)).await.unwrap();
        }

        for count in 1..4 {
            insert_count(&subgraph_store, &deployment, count, count, false).await;
        }

        // Test reading back with pending writes to the same entity
        pause_writer(&deployment).await;
        for count in 4..7 {
            insert_count(&subgraph_store, &deployment, count, count, false).await;
        }
        assert_eq!(6, read_count().await);

        writable.flush().await.unwrap();
        assert_eq!(6, read_count().await);

        // Test reading back with pending writes and a pending revert
        for count in 7..10 {
            insert_count(&subgraph_store, &deployment, count, count, false).await;
        }
        writable
            .revert_block_operations(block_pointer(2), FirehoseCursor::None)
            .await
            .unwrap();

        assert_eq!(2, read_count().await);

        writable.flush().await.unwrap();
        assert_eq!(2, read_count().await);
    })
}

/// Get the count using `WritableStore::get_many`
async fn count_get_many(writable: Arc<dyn WritableStore>) -> i32 {
    let key = count_key("1");
    let keys = BTreeSet::from_iter(vec![key.clone()]);
    let counter = writable
        .get_many(keys)
        .await
        .unwrap()
        .get(&key)
        .unwrap()
        .clone();
    counter.get("count").unwrap().as_int().unwrap()
}

/// Get the count using `WritableStore::get`
async fn count_get(writable: Arc<dyn WritableStore>) -> i32 {
    let counter = writable.get(&count_key("1")).await.unwrap().unwrap();
    counter.get("count").unwrap().as_int().unwrap()
}

async fn count_get_derived(writable: Arc<dyn WritableStore>) -> i32 {
    let key = count_key("1");
    let query = DerivedEntityQuery {
        entity_type: key.entity_type.clone(),
        entity_field: Word::from("id"),
        value: key.entity_id.clone(),
        causality_region: CausalityRegion::ONCHAIN,
    };
    let map = writable.get_derived(&query).await.unwrap();
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
        let schema = subgraph_store.input_schema(&deployment.hash).await.unwrap();

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
        let result_entities = [
            r#"(1, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter), entity: Entity { count: Int(2), id: String("1"), vid: Int8(1) }, vid: 1 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(2), id: String("1"), vid: Int8(1) }, vid: 1 }])"#,
            r#"(2, [EntitySourceOperation { entity_op: Modify, entity_type: EntityType(Counter), entity: Entity { count: Int(4), id: String("1"), vid: Int8(2) }, vid: 2 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(4), id: String("2"), vid: Int8(2) }, vid: 2 }])"#,
            r#"(3, [EntitySourceOperation { entity_op: Delete, entity_type: EntityType(Counter), entity: Entity { count: Int(4), id: String("1"), vid: Int8(2) }, vid: 2 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(6), id: String("3"), vid: Int8(3) }, vid: 3 }])"#,
            r#"(4, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter), entity: Entity { count: Int(8), id: String("1"), vid: Int8(4) }, vid: 4 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(8), id: String("4"), vid: Int8(4) }, vid: 4 }])"#,
            r#"(5, [EntitySourceOperation { entity_op: Delete, entity_type: EntityType(Counter), entity: Entity { count: Int(8), id: String("1"), vid: Int8(4) }, vid: 4 }, EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter2), entity: Entity { count: Int(10), id: String("5"), vid: Int8(5) }, vid: 5 }])"#,
            r#"(6, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(Counter), entity: Entity { count: Int(12), id: String("1"), vid: Int8(6) }, vid: 6 }])"#,
            r#"(7, [EntitySourceOperation { entity_op: Delete, entity_type: EntityType(Counter), entity: Entity { count: Int(12), id: String("1"), vid: Int8(6) }, vid: 6 }])"#,
        ];
        let subgraph_store = store.subgraph_store();
        writable.deployment_synced(block_pointer(0)).await.unwrap();

        for count in 1..=5 {
            insert_count(&subgraph_store, &deployment, count, 2 * count, false).await;
        }
        writable.flush().await.unwrap();
        writable.deployment_synced(block_pointer(0)).await.unwrap();

        let br: Range<BlockNumber> = 0..18;
        let entity_types = vec![COUNTER_TYPE.clone(), COUNTER2_TYPE.clone()];
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types.clone(), CausalityRegion::ONCHAIN, br.clone())
            .await
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
        writable.deployment_synced(block_pointer(0)).await.unwrap();
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types, CausalityRegion::ONCHAIN, br)
            .await
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
        writable.deployment_synced(block_pointer(0)).await.unwrap();

        for count in 1..=4 {
            insert_count(&subgraph_store, &deployment, count, 2 * count, true).await;
        }
        writable.flush().await.unwrap();
        writable.deployment_synced(block_pointer(0)).await.unwrap();
        let br: Range<BlockNumber> = 0..18;
        let entity_types = vec![COUNTER2_TYPE.clone()];
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types.clone(), CausalityRegion::ONCHAIN, br.clone())
            .await
            .unwrap();
        assert_eq!(e.len(), 4);
    })
}

#[test]
fn read_range_pool_created_test() {
    run_test(|store, writable, sourceable, deployment| async move {
        let result_entities = ["(1, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(PoolCreated), entity: Entity { blockNumber: BigInt(12369621), blockTimestamp: BigInt(1620243254), fee: Int(500), id: Bytes(0xff80818283848586), logIndex: BigInt(0), pool: Bytes(0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8), tickSpacing: Int(10), token0: Bytes(0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48), token1: Bytes(0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2), transactionFrom: Bytes(0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48), transactionGasPrice: BigInt(100000000000), transactionHash: Bytes(0x12340000000000000000000000000000000000000000000000000000000000000000000000000000), vid: Int8(1) }, vid: 1 }])",
            "(2, [EntitySourceOperation { entity_op: Create, entity_type: EntityType(PoolCreated), entity: Entity { blockNumber: BigInt(12369622), blockTimestamp: BigInt(1620243255), fee: Int(3000), id: Bytes(0xff90919293949596), logIndex: BigInt(1), pool: Bytes(0x4585fe77225b41b697c938b018e2ac67ac5a20c0), tickSpacing: Int(60), token0: Bytes(0x2260fac5e5542a773aa44fbcfedf7c193bc2c599), token1: Bytes(0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2), transactionFrom: Bytes(0x2260fac5e5542a773aa44fbcfedf7c193bc2c599), transactionGasPrice: BigInt(100000000000), transactionHash: Bytes(0x12340000000000000000000000000000000000000000000000000000000000000000000000000001), vid: Int8(2) }, vid: 2 }])"];

        // Rest of the test remains the same
        let subgraph_store = store.subgraph_store();
        writable.deployment_synced(block_pointer(0)).await.unwrap();

        let pool_created_type = TEST_SUBGRAPH_SCHEMA.entity_type("PoolCreated").unwrap();
        let entity_types = vec![pool_created_type.clone()];

        let mut last_op: Option<EntityOperation> = None;
        for count in (1..=2).map(|x| x as i64) {
            let id = if count == 1 {
                "0xff80818283848586"
            } else {
                "0xff90919293949596"
            };

            let data = entity! { TEST_SUBGRAPH_SCHEMA =>
                id: id,
                token0: if count == 1 { "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" } else { "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599" },
                token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                fee: if count == 1 { 500 } else { 3000 },
                tickSpacing: if count == 1 { 10 } else { 60 },
                pool: if count == 1 { "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" } else { "0x4585fe77225b41b697c938b018e2ac67ac5a20c0" },
                blockNumber: 12369621 + count - 1,
                blockTimestamp: 1620243254 + count - 1,
                transactionHash: format!("0x1234{:0>76}", if count == 1 { "0" } else { "1" }),
                transactionFrom: if count == 1 { "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" } else { "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599" },
                transactionGasPrice: 100000000000i64,
                logIndex: count - 1,
                vid: count
            };

            let key = pool_created_type.parse_key(id).unwrap();
            let op = EntityOperation::Set {
                key: key.clone(),
                data,
            };

            last_op = Some(op.clone());
            transact_entity_operations(
                &subgraph_store,
                &deployment,
                block_pointer(count as u8),
                vec![op],
            )
            .await
            .unwrap();
        }
        writable.flush().await.unwrap();
        writable.deployment_synced(block_pointer(0)).await.unwrap();

        let br: Range<BlockNumber> = 0..18;
        let e: BTreeMap<i32, Vec<EntitySourceOperation>> = sourceable
            .get_range(entity_types.clone(), CausalityRegion::ONCHAIN, br.clone())
            .await
            .unwrap();
        assert_eq!(e.len(), 2);
        for en in &e {
            let index = *en.0 - 1;
            let a = result_entities[index as usize];
            assert_eq!(a, format!("{:?}", en));
        }

        // Make sure we get a constraint violation
        let op = last_op.take().unwrap();

        transact_entity_operations(&subgraph_store, &deployment, block_pointer(3), vec![op])
            .await
            .unwrap();
        let res = writable.flush().await;
        let exp = "duplicate key value violates unique constraint \"pool_created_pkey\": Key (vid)=(2) already exists.";
        match res {
            Ok(_) => panic!("Expected error, but got success"),
            Err(StoreError::ConstraintViolation(msg)) => {
                assert_eq!(msg, exp);
            }
            Err(e) => panic!("Expected constraint violation, but got {:?}", e),
        }
    })
}

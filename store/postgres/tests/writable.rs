use graph::data::subgraph::schema::DeploymentCreate;
use lazy_static::lazy_static;
use std::marker::PhantomData;
use test_store::*;

use graph::components::store::{DeploymentLocator, WritableStore};
use graph::components::store::{EntityKey, EntityType};
use graph::data::subgraph::*;
use graph::prelude::*;
use graph::semver::Version;
use graph_store_postgres::layout_for_tests::writable;
use graph_store_postgres::{Store as DieselStore, SubgraphStore as DieselSubgraphStore};
use web3::types::H256;

const SCHEMA_GQL: &str = "
    type Counter @entity {
        id: ID!,
        count: Int,
    }
";

const COUNTER: &str = "Counter";

lazy_static! {
    static ref TEST_SUBGRAPH_ID_STRING: String = String::from("writableSubgraph");
    static ref TEST_SUBGRAPH_ID: DeploymentHash =
        DeploymentHash::new(TEST_SUBGRAPH_ID_STRING.as_str()).unwrap();
    static ref TEST_SUBGRAPH_SCHEMA: Schema =
        Schema::parse(SCHEMA_GQL, TEST_SUBGRAPH_ID.clone()).expect("Failed to parse user schema");
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
    };

    // Create SubgraphDeploymentEntity
    let deployment = DeploymentCreate::new(&manifest, None);
    let name = SubgraphName::new("test/writable").unwrap();
    let node_id = NodeId::new("test").unwrap();
    let deployment = store
        .create_subgraph_deployment(
            name,
            &TEST_SUBGRAPH_SCHEMA,
            deployment,
            node_id,
            NETWORK_NAME.to_string(),
            SubgraphVersionSwitchingMode::Instant,
        )
        .unwrap();
    deployment
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
    F: FnOnce(Arc<DieselStore>, Arc<dyn WritableStore>, DeploymentLocator) -> R + Send + 'static,
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
            .writable(LOGGER.clone(), deployment.id)
            .await
            .expect("we can get a writable store");

        // Run test and wait for the background writer to finish its work so
        // it won't conflict with the next test
        test(store, writable.clone(), deployment).await;
        writable.flush().await.unwrap();
    });
}

fn block_pointer(number: u8) -> BlockPtr {
    let hash = H256::from([number; 32]);
    BlockPtr::from((hash, number as BlockNumber))
}

fn count_key(deployment: &DeploymentLocator, id: &str) -> EntityKey {
    EntityKey {
        subgraph_id: deployment.hash.clone(),
        entity_type: EntityType::from(COUNTER),
        entity_id: id.to_owned(),
    }
}

async fn insert_count(store: &Arc<DieselSubgraphStore>, deployment: &DeploymentLocator, count: u8) {
    let data = entity! {
        id: "1",
        count: count as i32
    };
    let entity_op = EntityOperation::Set {
        key: count_key(deployment, &data.get("id").unwrap().to_string()),
        data,
    };
    transact_entity_operations(store, deployment, block_pointer(count), vec![entity_op])
        .await
        .unwrap();
}

async fn pause_writer(deployment: &DeploymentLocator) {
    flush(&deployment).await.unwrap();
    writable::allow_steps(0).await;
}

async fn resume_writer(deployment: &DeploymentLocator, steps: usize) {
    writable::allow_steps(steps).await;
    flush(&deployment).await.unwrap();
}

#[test]
fn tracker() {
    run_test(|store, writable, deployment| async move {
        let subgraph_store = store.subgraph_store();

        let read_count = || {
            let counter = writable.get(&count_key(&deployment, "1")).unwrap().unwrap();
            counter.get("count").unwrap().as_int().unwrap()
        };
        for count in 1..4 {
            insert_count(&subgraph_store, &deployment, count).await;
        }
        pause_writer(&deployment).await;

        // Test reading back with a pending write
        insert_count(&subgraph_store, &deployment, 4).await;
        assert_eq!(4, read_count());
        resume_writer(&deployment, 1).await;
        assert_eq!(4, read_count());

        // Test reading back with a pending revert
        writable
            .revert_block_operations(block_pointer(2), None)
            .await
            .unwrap();

        assert_eq!(2, read_count());

        resume_writer(&deployment, 1).await;
        assert_eq!(2, read_count());
    })
}

extern crate diesel;
extern crate ethereum_types;
extern crate futures;
#[macro_use]
extern crate lazy_static;
extern crate graph;
extern crate graph_store_postgres;
extern crate web3;

use diesel::pg::PgConnection;
use diesel::*;
use std::fmt::Debug;
use std::panic;
use std::sync::Mutex;
use web3::types::*;

use graph::components::store::{StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange};
use graph::prelude::*;
use graph_store_postgres::{db_schema, Store as DieselStore, StoreConfig};

fn test_subgraph_id() -> SubgraphId {
    SubgraphId("test_subgraph".to_owned())
}

fn block100_hash() -> H256 {
    "cabee56587df7541e577e845a3615e1a8304ffe7b8130869d3d1412fc941ae51"
        .parse()
        .unwrap()
}
fn block101_hash() -> H256 {
    "38396ae55061d3ec68aa968ea64a13e4404da01d7cc7df506fa6d46fdd1a24e4"
        .parse()
        .unwrap()
}
fn block102_hash() -> H256 {
    "e8593c0cf6c13225957cb4c858e45149c4397d2bb9e492dac8ecc60d0874e3d2"
        .parse()
        .unwrap()
}

fn block100() -> Block<Transaction> {
    Block {
        hash: Some(block100_hash()),
        parent_hash: H256::zero(),
        uncles_hash: H256::zero(),
        author: H160::zero(),
        state_root: H256::zero(),
        transactions_root: H256::zero(),
        receipts_root: H256::zero(),
        number: Some(100.into()),
        gas_used: U256::zero(),
        gas_limit: U256::zero(),
        extra_data: Bytes(vec![]),
        logs_bloom: H2048::zero(),
        timestamp: U256::zero(),
        difficulty: U256::zero(),
        total_difficulty: U256::zero(),
        seal_fields: vec![],
        uncles: vec![],
        transactions: vec![],
        size: None,
    }
}

fn block101() -> Block<Transaction> {
    Block {
        hash: Some(block101_hash()),
        parent_hash: block100_hash(),
        uncles_hash: H256::zero(),
        author: H160::zero(),
        state_root: H256::zero(),
        transactions_root: H256::zero(),
        receipts_root: H256::zero(),
        number: Some(101.into()),
        gas_used: U256::zero(),
        gas_limit: U256::zero(),
        extra_data: Bytes(vec![]),
        logs_bloom: H2048::zero(),
        timestamp: U256::zero(),
        difficulty: U256::zero(),
        total_difficulty: U256::zero(),
        seal_fields: vec![],
        uncles: vec![],
        transactions: vec![],
        size: None,
    }
}

fn block102() -> Block<Transaction> {
    Block {
        hash: Some(block102_hash()),
        parent_hash: block101_hash(),
        uncles_hash: H256::zero(),
        author: H160::zero(),
        state_root: H256::zero(),
        transactions_root: H256::zero(),
        receipts_root: H256::zero(),
        number: Some(102.into()),
        gas_used: U256::zero(),
        gas_limit: U256::zero(),
        extra_data: Bytes(vec![]),
        logs_bloom: H2048::zero(),
        timestamp: U256::zero(),
        difficulty: U256::zero(),
        total_difficulty: U256::zero(),
        seal_fields: vec![],
        uncles: vec![],
        transactions: vec![],
        size: None,
    }
}

/// Helper function to ensure and obtain the Postgres URL to use for testing.
fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

/// Helper function to create a Store for a test instance
fn create_diesel_store() -> DieselStore {
    DieselStore::new(
        StoreConfig {
            url: postgres_test_url(),
            network_name: "_testsuite".to_owned(),
        },
        &Logger::root(slog::Discard, o!()),
    ).unwrap()
}

lazy_static! {
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F) -> ()
where
    F: FnOnce() -> R + Send + 'static,
    R: IntoFuture + Send + 'static,
    R::Item: Send,
    R::Error: Send + Debug,
    R::Future: Send,
{
    // Lock regardless of poisoning.
    let _test_lock = match TEST_MUTEX.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    runtime
        .block_on(future::lazy(|| {
            remove_test_data();
            future::ok::<_, ()>(())
        }))
        .expect("Failed to remove test data");

    runtime
        .block_on(future::lazy(|| {
            insert_test_data();
            future::ok::<_, ()>(())
        }))
        .expect("Failed to insert test data");

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        runtime.block_on(future::lazy(|| test()))
    }));

    result.expect("Failed to run test").expect("Test failed");
}

/// Creates a test entity.
fn create_test_entity(
    id: String,
    entity: String,
    name: String,
    email: String,
    age: i32,
    weight: f32,
    coffee: bool,
) -> (StoreKey, Entity) {
    let test_key = StoreKey {
        subgraph: String::from("test_subgraph"),
        entity: entity,
        id: id,
    };
    let mut test_entity = Entity::new();
    test_entity.insert(String::from("name"), Value::String(name));
    test_entity.insert(String::from("email"), Value::String(email));
    test_entity.insert(String::from("age"), Value::Int(age));
    test_entity.insert(String::from("weight"), Value::Float(weight));
    test_entity.insert(String::from("coffee"), Value::Bool(coffee));
    (test_key, test_entity)
}

/// Inserts test data into the store.
fn insert_test_data() {
    let store = create_diesel_store();

    store
        .upsert_blocks(stream::iter_ok(vec![block100(), block101(), block102()]))
        .wait()
        .expect("could not insert blocks into store");
    assert_eq!(
        store
            .attempt_head_update(2)
            .expect("could not update head ptr"),
        vec![]
    );

    store
        .add_subgraph_if_missing(test_subgraph_id())
        .expect("Failed to register test subgraph in store");

    store
        .set_block_ptr_with_no_changes(
            test_subgraph_id(),
            store.block_ptr(test_subgraph_id()).unwrap(),
            EthereumBlockPointer::to_parent(&block100()),
        )
        .unwrap();

    let mut tx = store
        .begin_transaction(test_subgraph_id(), block100())
        .unwrap();
    let test_entity_1 = create_test_entity(
        String::from("1"),
        String::from("user"),
        String::from("Johnton"),
        String::from("tonofjohn@email.com"),
        67 as i32,
        184.4 as f32,
        false,
    );
    tx.set(test_entity_1.0, test_entity_1.1).unwrap();
    tx.commit().unwrap();

    let mut tx = store
        .begin_transaction(test_subgraph_id(), block101())
        .unwrap();
    let test_entity_2 = create_test_entity(
        String::from("2"),
        String::from("user"),
        String::from("Cindini"),
        String::from("dinici@email.com"),
        43 as i32,
        159.1 as f32,
        true,
    );
    tx.set(test_entity_2.0, test_entity_2.1).unwrap();

    let test_entity_3 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena"),
        String::from("queensha@email.com"),
        28 as i32,
        111.7 as f32,
        false,
    );
    tx.set(test_entity_3.0, test_entity_3.1).unwrap();
    tx.commit().unwrap();

    let mut tx = store
        .begin_transaction(test_subgraph_id(), block102())
        .unwrap();
    let test_entity_3_2 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena"),
        String::from("teeko@email.com"),
        28 as i32,
        111.7 as f32,
        false,
    );
    tx.set(test_entity_3_2.0, test_entity_3_2.1).unwrap();
    tx.commit().unwrap();
}

/// Removes test data from the database behind the store.
fn remove_test_data() {
    use db_schema::entities::dsl::*;
    use db_schema::ethereum_blocks::dsl::*;
    use db_schema::ethereum_networks::dsl::*;
    use db_schema::subgraphs::dsl::*;
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    delete(entities)
        .execute(&conn)
        .expect("Failed to remove test entity data");
    delete(ethereum_blocks)
        .execute(&conn)
        .expect("Failed to remove test block data");
    delete(ethereum_networks)
        .execute(&conn)
        .expect("Failed to remove test networks table");
    delete(subgraphs)
        .execute(&conn)
        .expect("Failed to remove test subgraph data");
}

#[test]
fn delete_entity() {
    run_test(|| -> Result<(), ()> {
        use db_schema::entities::dsl::*;
        let store = create_diesel_store();
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        let test_key = StoreKey {
            subgraph: test_subgraph_id().0,
            entity: String::from("user"),
            id: String::from("3"),
        };
        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();
        tx.delete(test_key).unwrap();
        tx.commit().unwrap();

        //Get all ids in table
        let all_ids = entities
            .select(id)
            .load::<String>(&*store.conn.lock().unwrap())
            .unwrap();

        // Check that that the deleted entity id is not present
        assert!(!all_ids.contains(&String::from("3")));

        Ok(())
    })
}

#[test]
fn get_entity() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();

        let key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("1"),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let result = store.get(key, block_ptr).unwrap();

        let mut expected_entity = Entity::new();
        expected_entity.insert(String::from("name"), Value::String(String::from("Johnton")));
        expected_entity.insert(
            String::from("email"),
            Value::String(String::from("tonofjohn@email.com")),
        );
        expected_entity.insert(String::from("age"), Value::Int(67 as i32));
        expected_entity.insert(String::from("weight"), Value::Float(184.4 as f32));
        expected_entity.insert(String::from("coffee"), Value::Bool(false));

        // Check that the expected entity was returned
        assert_eq!(result, expected_entity);

        Ok(())
    })
}

#[test]
fn insert_entity() {
    run_test(|| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        let store = create_diesel_store();
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        let test_entity_1 = create_test_entity(
            String::from("7"),
            String::from("user"),
            String::from("Wanjon"),
            String::from("wanawana@email.com"),
            76 as i32,
            111.7 as f32,
            true,
        );
        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();
        tx.set(test_entity_1.0, test_entity_1.1).unwrap();
        tx.commit().unwrap();

        // Check that new record is in the store
        let all_ids = entities
            .select(id)
            .load::<String>(&*store.conn.lock().unwrap())
            .unwrap();
        assert!(all_ids.iter().any(|x| x == &String::from("7")));

        Ok(())
    })
}

#[test]
fn update_existing() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();

        let entity_key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("1"),
        };

        let test_entity_1 = create_test_entity(
            String::from("1"),
            String::from("user"),
            String::from("Wanjon"),
            String::from("wanawana@email.com"),
            76 as i32,
            111.7 as f32,
            true,
        );

        store.revert_block(test_subgraph_id(), block102()).unwrap();

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(
            store.get(entity_key.clone(), block101().into()).unwrap(),
            test_entity_1.1
        );

        // Set test entity; as the entity already exists an update should be performed
        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();
        tx.set(test_entity_1.0, test_entity_1.1.clone()).unwrap();
        tx.commit().unwrap();

        // Verify that the entity in the store has changed to what we have set
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        assert_eq!(store.get(entity_key, block_ptr).unwrap(), test_entity_1.1);

        Ok(())
    })
}

#[test]
fn partially_update_existing() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        let entity_key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("1"),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store.get(entity_key.clone(), block101().into()).unwrap();
        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(original_entity, partial_entity);

        // Set test entity; as the entity already exists an update should be performed
        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();
        tx.set(entity_key.clone(), partial_entity.clone()).unwrap();
        tx.commit().unwrap();

        // Obtain the updated entity from the store
        let updated_entity = store.get(entity_key, block102().into()).unwrap();

        // Verify that the values of all attributes we have set were either unset
        // (in the case of Value::Null) or updated to the new values
        assert_eq!(updated_entity.get("id"), partial_entity.get("id"));
        assert_eq!(updated_entity.get("user"), partial_entity.get("user"));
        assert_eq!(updated_entity.get("email"), None);

        // Verify that all attributes we have not set have remained at their old values
        assert_eq!(updated_entity.get("age"), original_entity.get("age"));
        assert_eq!(updated_entity.get("weight"), original_entity.get("weight"));
        assert_eq!(updated_entity.get("coffee"), original_entity.get("coffee"));

        Ok(())
    })
}

#[test]
fn find_string_contains() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Contains(
                String::from("name"),
                Value::String(String::from("%ind%")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Make sure the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
fn find_string_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Make sure the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
fn find_string_not_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_greater_than() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_less_than() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        //There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_less_than_order_by_asc() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let result = store
            .find(this_query, block_ptr)
            .expect("Failed to fetch entities from the store");

        // Check that the number and order of users is correct
        assert_eq!(2, result.len());
        let names: Vec<&Value> = result
            .iter()
            .map(|entity| {
                entity
                    .get(&String::from("name"))
                    .expect("Entity without \"name\" attribute returned")
            })
            .collect();
        assert_eq!(
            names,
            vec![
                &Value::String(String::from("Cindini")),
                &Value::String(String::from("Johnton")),
            ]
        );

        Ok(())
    })
}

#[test]
fn find_string_less_than_order_by_desc() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let result = store
            .find(this_query, block_ptr)
            .expect("Failed to fetch entities from the store");

        // Check that the number and order of users is correct
        assert_eq!(2, result.len());
        let names: Vec<&Value> = result
            .iter()
            .map(|entity| {
                entity
                    .get(&String::from("name"))
                    .expect("Entity without \"name\" attribute returned")
            })
            .collect();
        assert_eq!(
            names,
            vec![
                &Value::String(String::from("Johnton")),
                &Value::String(String::from("Cindini")),
            ]
        );

        Ok(())
    })
}

#[test]
fn find_string_less_than_range() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("ZZZ")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_multiple_and() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![
                StoreFilter::LessThan(String::from("name"), Value::String(String::from("Cz"))),
                StoreFilter::Equal(String::from("name"), Value::String(String::from("Cindini"))),
            ])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_ends_with() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::EndsWith(
                String::from("name"),
                Value::String(String::from("ini")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_not_ends_with() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotEndsWith(
                String::from("name"),
                Value::String(String::from("ini")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("name"),
                vec![Value::String(String::from("Johnton"))],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_string_not_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("name"),
                vec![Value::String(String::from("Shaqueeena"))],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));

        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 user returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("weight"),
                Value::Float(184.4 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_not_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("weight"),
                Value::Float(184.4 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_greater_than() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_less_than() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini";
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_less_than_order_by_desc() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_less_than_range() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(161 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");
        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("weight"),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_float_not_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("weight"),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 users returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 users returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_not_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_greater_than() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_greater_or_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterOrEqual(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_less_than() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(50 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        //There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_less_or_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessOrEqual(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini";
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_less_than_order_by_desc() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(50 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_less_than_range() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("age"),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_int_not_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("age"),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 users returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_bool_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("coffee"),
                Value::Bool(true),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_bool_not_equal() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("coffee"),
                Value::Bool(true),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find query failed");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Johnton"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_bool_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("coffee"),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn find_bool_not_in() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("coffee"),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let block_ptr = store
            .block_ptr(SubgraphId("test_subgraph".to_owned()))
            .unwrap();
        let returned_entities = store
            .find(this_query, block_ptr)
            .expect("store.find operation failed");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Shaqueeena"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 2 users returned in results
        assert_eq!(2, returned_entities.len());

        Ok(())
    })
}

#[test]
fn revert_block() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Shaqueeena")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        // Revert all events from block 102
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        let returned_entities = store
            .find(this_query.clone(), block101().into())
            .expect("store.find operation failed");

        // Check if the first user in the result vector has email "queensha@email.com"
        let returned_name = returned_entities[0].get(&String::from("email"));
        let test_value = Value::String(String::from("queensha@email.com"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn revert_block_with_delete() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        let this_query = StoreQuery {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        // Delete an entity in block 102
        let del_key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("2"),
        };

        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();
        tx.delete(del_key.clone()).unwrap();
        tx.commit().unwrap();

        // Revert all events associated with block 102
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        let returned_entities = store
            .find(this_query.clone(), block102().into())
            .expect("store.find operation failed");

        // Check if "dinici@email.com" is in result set
        let returned_name = returned_entities[0].get(&String::from("email"));
        let test_value = Value::String(String::from("dinici@email.com"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 entity returned in results
        assert_eq!(1, returned_entities.len());

        Ok(())
    })
}

#[test]
fn revert_block_with_partial_update() {
    run_test(|| -> Result<(), ()> {
        let store = create_diesel_store();
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        let entity_key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("1"),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store.get(entity_key.clone(), block101().into()).unwrap();

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(original_entity, partial_entity);

        // Set test entity; as the entity already exists an update should be performed
        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();
        tx.set(entity_key.clone(), partial_entity.clone()).unwrap();
        tx.commit().unwrap();

        // Check that update happened
        let updated_entity = store.get(entity_key.clone(), block102().into()).unwrap();
        assert_eq!(partial_entity, updated_entity);
        assert_ne!(original_entity, updated_entity);

        // Perform revert operation, reversing the partial update
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        // Obtain the reverted entity from the store
        let reverted_entity = store.get(entity_key.clone(), block101().into()).unwrap();

        // Verify that the entity has been returned to its original state
        assert_eq!(reverted_entity, original_entity);

        Ok(())
    })
}

#[test]
fn entity_changes_are_fired_and_forwarded_to_subscriptions() {
    run_test(|| {
        let store = Arc::new(create_diesel_store());
        store.revert_block(test_subgraph_id(), block102()).unwrap();

        // Create a store subscription
        let subscription = store.subscribe(vec![(test_subgraph_id().0, String::from("User"))]);

        let mut tx = store
            .begin_transaction(test_subgraph_id(), block102())
            .unwrap();

        // Add two entities to the store
        let added_entities = vec![
            (
                String::from("1"),
                Entity::from(vec![
                    ("id", Value::from("1")),
                    ("name", Value::from("Johnny Boy")),
                ]),
            ),
            (
                String::from("2"),
                Entity::from(vec![
                    ("id", Value::from("2")),
                    ("name", Value::from("Tessa")),
                ]),
            ),
        ];
        for (id, entity) in added_entities.iter() {
            tx.set(
                StoreKey {
                    subgraph: String::from("subgraph-id"),
                    entity: String::from("User"),
                    id: id.clone(),
                },
                entity.clone(),
            ).expect("failed to add entity to the store");
        }

        // Update an entity in the store
        let updated_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny")),
        ]);
        tx.set(
            StoreKey {
                subgraph: String::from("subgraph-id"),
                entity: String::from("User"),
                id: String::from("1"),
            },
            updated_entity.clone(),
        ).expect("failed to update entity in the store");

        // Delete an entity in the store
        tx.delete(StoreKey {
            subgraph: String::from("subgraph-id"),
            entity: String::from("User"),
            id: String::from("2"),
        }).expect("failed to delete entity from the store");

        tx.commit().unwrap();

        // We're expecting four events to be written to the subscription stream
        let store = store.clone();
        subscription
            .take(4)
            .collect()
            .and_then(move |changes| {
                // Keep the store around until we're done reading from it; otherwise
                // it would be dropped too early and its entity change listener would
                // be terminated as well
                let _store = store;

                assert_eq!(
                    changes,
                    vec![
                        EntityChange {
                            subgraph: String::from("subgraph-id"),
                            entity: String::from("User"),
                            id: added_entities[0].clone().0,
                            operation: EntityChangeOperation::Added,
                        },
                        EntityChange {
                            subgraph: String::from("subgraph-id"),
                            entity: String::from("User"),
                            id: added_entities[1].clone().0,
                            operation: EntityChangeOperation::Added,
                        },
                        EntityChange {
                            subgraph: String::from("subgraph-id"),
                            entity: String::from("User"),
                            id: String::from("1"),
                            operation: EntityChangeOperation::Updated,
                        },
                        EntityChange {
                            subgraph: String::from("subgraph-id"),
                            entity: String::from("User"),
                            id: added_entities[1].clone().0,
                            operation: EntityChangeOperation::Removed,
                        },
                    ]
                );

                Ok(())
            })
            .and_then(|_| Ok(()))
    })
}

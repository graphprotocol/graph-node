extern crate diesel;
extern crate futures;
#[macro_use]
extern crate lazy_static;
extern crate graph;
extern crate graph_store_postgres;
extern crate hex;

use diesel::pg::PgConnection;
use diesel::*;
use std::fmt::Debug;
use std::panic;
use std::str::FromStr;
use std::sync::Mutex;

use graph::components::store::{StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange};
use graph::data::store::scalar;
use graph::prelude::*;
use graph::web3::types::H256;
use graph_store_postgres::{db_schema, Store as DieselStore, StoreConfig};

/// Helper function to ensure and obtain the Postgres URL to use for testing.
fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

lazy_static! {
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    static ref TEST_SUBGRAPH_ID: SubgraphId = "test_subgraph".to_owned();
    static ref TEST_BLOCK_0_PTR: EthereumBlockPointer = (
        H256::from("0xbd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"),
        0u64
    )
        .into();
    static ref TEST_BLOCK_1_PTR: EthereumBlockPointer = (
        H256::from("0x8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13"),
        1u64
    )
        .into();
    static ref TEST_BLOCK_2_PTR: EthereumBlockPointer = (
        H256::from("0xb98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1"),
        2u64
    )
        .into();
    static ref TEST_BLOCK_3_PTR: EthereumBlockPointer = (
        H256::from("0x977c084229c72a0fa377cae304eda9099b6a2cb5d83b25cdf0f0969b69874255"),
        3u64
    )
        .into();
    static ref TEST_BLOCK_3A_PTR: EthereumBlockPointer = (
        H256::from("0xd163aec0592c7cb00c2700ab65dcaac93289f5d250b3b889b39198b07e1fbe4a"),
        3u64
    )
        .into();
    static ref TEST_BLOCK_4_PTR: EthereumBlockPointer = (
        H256::from("0x007a03cdf635ebb66f5e79ae66cc90ca23d98031665649db056ff9c6aac2d74d"),
        4u64
    )
        .into();
    static ref TEST_BLOCK_4A_PTR: EthereumBlockPointer = (
        H256::from("0x8fab27e9e9285b0a39110f4d9877f05d0f43d2effa157e55f4dcc49c3cf8cbd7"),
        4u64
    )
        .into();
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(Arc<DieselStore>) -> R + Send + 'static,
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
        .block_on(future::lazy(move || {
            // Set up Store
            let logger = Logger::root(slog::Discard, o!());
            let url = postgres_test_url();
            let net_identifiers = EthereumNetworkIdentifier {
                net_version: "graph test suite".to_owned(),
                genesis_block_hash: TEST_BLOCK_0_PTR.hash,
            };
            let network_name = "fake_network".to_owned();
            let store = Arc::new(DieselStore::new(
                StoreConfig { url, network_name },
                &logger,
                net_identifiers,
            ));

            // Reset state before starting
            remove_test_data();

            // Seed database with test data
            insert_test_data(store.clone());

            // Run test
            test(store.clone())
        })).expect("Failed to run Store test");
}

/// Inserts test data into the store.
///
/// Inserts data in test blocks 1, 2, and 3, leaving test blocks 3A, 4, and 4A for the tests to
/// use.
fn insert_test_data(store: Arc<DieselStore>) {
    store
        .add_subgraph_if_missing(TEST_SUBGRAPH_ID.clone(), *TEST_BLOCK_0_PTR)
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
    store
        .transact_block_operations(
            TEST_SUBGRAPH_ID.clone(),
            *TEST_BLOCK_0_PTR,
            *TEST_BLOCK_1_PTR,
            vec![test_entity_1],
        ).unwrap();

    let test_entity_2 = create_test_entity(
        String::from("2"),
        String::from("user"),
        String::from("Cindini"),
        String::from("dinici@email.com"),
        43 as i32,
        159.1 as f32,
        true,
    );
    store
        .transact_block_operations(
            TEST_SUBGRAPH_ID.clone(),
            *TEST_BLOCK_1_PTR,
            *TEST_BLOCK_2_PTR,
            vec![test_entity_2],
        ).unwrap();

    let test_entity_3_1 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena"),
        String::from("queensha@email.com"),
        28 as i32,
        111.7 as f32,
        false,
    );

    let test_entity_3_2 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena"),
        String::from("teeko@email.com"),
        28 as i32,
        111.7 as f32,
        false,
    );
    store
        .transact_block_operations(
            TEST_SUBGRAPH_ID.clone(),
            *TEST_BLOCK_2_PTR,
            *TEST_BLOCK_3_PTR,
            vec![test_entity_3_1, test_entity_3_2],
        ).unwrap();
}

/// Creates a test entity.
fn create_test_entity(
    id: String,
    entity_type: String,
    name: String,
    email: String,
    age: i32,
    weight: f32,
    coffee: bool,
) -> EntityOperation {
    let mut test_entity = Entity::new();

    let bin_name = scalar::Bytes::from_str(&hex::encode(&name)).unwrap();
    test_entity.insert(String::from("name"), Value::String(name));
    test_entity.insert(String::from("bin_name"), Value::Bytes(bin_name));
    test_entity.insert(String::from("email"), Value::String(email));
    test_entity.insert(String::from("age"), Value::Int(age));
    test_entity.insert(String::from("weight"), Value::Float(weight));
    test_entity.insert(String::from("coffee"), Value::Bool(coffee));

    EntityOperation::Set {
        subgraph_id: TEST_SUBGRAPH_ID.clone(),
        entity_type,
        entity_id: id,
        data: test_entity,
    }
}

/// Removes test data from the database behind the store.
fn remove_test_data() {
    use db_schema::entities::dsl::*;
    use db_schema::subgraphs::dsl::*;

    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    delete(entities)
        .execute(&conn)
        .expect("Failed to remove entity test data");
    delete(subgraphs)
        .execute(&conn)
        .expect("Failed to remove subgraph test data");
}

#[test]
fn delete_entity() {
    run_test(|store| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![EntityOperation::Remove {
                    subgraph_id: TEST_SUBGRAPH_ID.clone(),
                    entity_type: "user".to_owned(),
                    entity_id: "3".to_owned(),
                }],
            ).unwrap();

        // Get all ids in table
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
    run_test(|store| -> Result<(), ()> {
        let key = StoreKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: String::from("user"),
            entity_id: String::from("1"),
        };
        let result = store.get(key).unwrap();

        let mut expected_entity = Entity::new();

        let name = "Johnton".to_owned();
        expected_entity.insert(
            String::from("bin_name"),
            Value::Bytes(name.as_bytes().into()),
        );
        expected_entity.insert(String::from("name"), Value::String(name));
        expected_entity.insert(
            String::from("email"),
            Value::String(String::from("tonofjohn@email.com")),
        );
        expected_entity.insert(String::from("age"), Value::Int(67 as i32));
        expected_entity.insert(String::from("weight"), Value::Float(184.4 as f32));
        expected_entity.insert(String::from("coffee"), Value::Bool(false));

        // Check that the expected entity was returned
        assert_eq!(result, Some(expected_entity));

        Ok(())
    })
}

#[test]
fn insert_entity() {
    run_test(|store| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        let test_entity = create_test_entity(
            String::from("7"),
            String::from("user"),
            String::from("Wanjon"),
            String::from("wanawana@email.com"),
            76 as i32,
            111.7 as f32,
            true,
        );
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![test_entity],
            ).unwrap();

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
    run_test(|store| -> Result<(), ()> {
        let entity_key = StoreKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: String::from("user"),
            entity_id: String::from("1"),
        };

        let op = create_test_entity(
            String::from("1"),
            String::from("user"),
            String::from("Wanjon"),
            String::from("wanawana@email.com"),
            76 as i32,
            111.7 as f32,
            true,
        );
        let mut new_data = match op {
            EntityOperation::Set { ref data, .. } => data.clone(),
            _ => unreachable!(),
        };

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(store.get(entity_key.clone()).unwrap().unwrap(), new_data);

        // Set test entity; as the entity already exists an update should be performed
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![op],
            ).unwrap();

        // Verify that the entity in the store has changed to what we have set.
        let bin_name = match new_data.get("bin_name") {
            Some(Value::Bytes(bytes)) => bytes.clone(),
            _ => unreachable!(),
        };
        new_data.insert(String::from("bin_name"), Value::Bytes(bin_name));
        assert_eq!(store.get(entity_key).unwrap(), Some(new_data));

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn partially_update_existing() {
    run_test(|store| -> Result<(), ()> {
        let entity_key = StoreKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: String::from("user"),
            entity_id: String::from("1"),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store.get(entity_key.clone()).unwrap();

        // Set test entity; as the entity already exists an update should be performed
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![EntityOperation::Set {}],
            ).unwrap();

        // Obtain the updated entity from the store
        let updated_entity = store.get(entity_key).unwrap();

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
#[cfg(any())]
fn find_string_contains() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Contains(
                String::from("name"),
                Value::String(String::from("%ind%")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

        // Make sure the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn find_string_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

        // Make sure the first user in the result vector is "Cindini"
        let returned_name = returned_entities[0].get(&String::from("name"));
        let test_value = Value::String(String::from("Cindini"));
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn find_string_not_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_greater_than() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_less_than() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_less_than_order_by_asc() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = store
            .find(this_query)
            .expect("Failed to fetch entities from the store");

        // Check that the number and order of users is correct
        assert_eq!(2, result.len());
        let names: Vec<&Value> = result
            .iter()
            .map(|entity| {
                entity
                    .get(&String::from("name"))
                    .expect("Entity without \"name\" attribute returned")
            }).collect();
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
#[cfg(any())]
fn find_string_less_than_order_by_desc() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("Kundi")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = store
            .find(this_query)
            .expect("Failed to fetch entities from the store");

        // Check that the number and order of users is correct
        assert_eq!(2, result.len());
        let names: Vec<&Value> = result
            .iter()
            .map(|entity| {
                entity
                    .get(&String::from("name"))
                    .expect("Entity without \"name\" attribute returned")
            }).collect();
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
#[cfg(any())]
fn find_string_less_than_range() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("name"),
                Value::String(String::from("ZZZ")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_multiple_and() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![
                StoreFilter::LessThan(String::from("name"), Value::String(String::from("Cz"))),
                StoreFilter::Equal(String::from("name"), Value::String(String::from("Cindini"))),
            ])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_ends_with() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::EndsWith(
                String::from("name"),
                Value::String(String::from("ini")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_not_ends_with() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotEndsWith(
                String::from("name"),
                Value::String(String::from("ini")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("name"),
                vec![Value::String(String::from("Johnton"))],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_string_not_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("name"),
                vec![Value::String(String::from("Shaqueeena"))],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("weight"),
                Value::Float(184.4 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_not_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("weight"),
                Value::Float(184.4 as f32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_greater_than() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_less_than() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: Some((String::From("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_less_than_order_by_desc() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_less_than_range() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(161 as f32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");
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
#[cfg(any())]
fn find_float_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("weight"),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_float_not_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("weight"),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_not_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_greater_than() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_greater_or_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterOrEqual(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_less_than() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(50 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_less_or_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessOrEqual(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_less_than_order_by_desc() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(50 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_less_than_range() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("age"),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_int_not_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("age"),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_bool_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("coffee"),
                Value::Bool(true),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_bool_not_equal() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("coffee"),
                Value::Bool(true),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find query failed");

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
#[cfg(any())]
fn find_bool_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("coffee"),
                vec![Value::Bool(true)],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn find_bool_not_in() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("coffee"),
                vec![Value::Bool(true)],
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 5, skip: 0 }),
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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
#[cfg(any())]
fn revert_block() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Shaqueeena")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let block_hash = "znuyjijnezBiGFuZAW9Q";
        let event_source =
            EventSource::EthereumBlock(H256::from_slice(&block_hash.as_bytes())).to_string();

        // Revert all events associated with event_source, "znuyjijnezBiGFuZAW9Q"
        store.revert_events(event_source, this_query.subgraph.clone());

        let returned_entities = store
            .find(this_query.clone())
            .expect("store.find operation failed");

        // Check if the first user in the result vector has email "queensha@email.com"
        let returned_name = returned_entities[0].get(&String::from("email"));
        let test_value = Value::String(String::from("queensha@email.com"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        // Perform revert operation again to confirm idempotent nature of revert_events()
        let returned_entities = store.find(this_query).expect("store.find operation failed");
        let returned_name = returned_entities[0].get(&String::from("email"));
        let test_value = Value::String(String::from("queensha@email.com"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn revert_block_with_delete() {
    run_test(|store| -> Result<(), ()> {
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: Some((String::from("name"), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        // Delete an entity using a randomly created event source
        let del_key = StoreKey {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            id: String::from("2"),
        };

        let block_hash = "test_block_to_revert";
        let event_source = EventSource::EthereumBlock(H256::from_slice(&block_hash.as_bytes()));
        let revert_event_source = event_source.to_string();
        store
            .delete(del_key.clone(), event_source)
            .expect("Store.delete operation failed");

        // Revert all events associated with our random event_source
        store.revert_events(revert_event_source, this_query.subgraph.clone());

        let returned_entities = store
            .find(this_query.clone())
            .expect("store.find operation failed");

        // Check if "dinici@email.com" is in result set
        let returned_name = returned_entities[0].get(&String::from("email"));
        let test_value = Value::String(String::from("dinici@email.com"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be 1 entity returned in results
        assert_eq!(1, returned_entities.len());

        // Perform revert operation again to confirm idempotent nature of revert_events()
        // Delete an entity using a randomly created event source
        let block_hash = "test_block_to_revert";
        let event_source = EventSource::EthereumBlock(H256::from_slice(&block_hash.as_bytes()));
        let revert_event_source = event_source.to_string();
        store
            .delete(del_key.clone(), event_source)
            .expect("Store.delete operation failed");
        store.revert_events(revert_event_source, this_query.subgraph.clone());
        let returned_entities = store
            .find(this_query.clone())
            .expect("store.find operation failed");
        let returned_name = returned_entities[0].get(&String::from("email"));
        let test_value = Value::String(String::from("dinici@email.com"));
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn revert_block_with_partial_update() {
    run_test(|store| -> Result<(), ()> {
        let entity_key = StoreKey {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            id: String::from("1"),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store.get(entity_key.clone()).unwrap();
        let event_source = EventSource::EthereumBlock(H256::random());
        let revert_event_source = event_source.to_string();

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(original_entity, partial_entity);

        // Set test entity; as the entity already exists an update should be performed
        store
            .set(entity_key.clone(), partial_entity, event_source)
            .expect("Failed to update entity that already exists");

        // Perform revert operation, reversing the partial update
        store.revert_events(revert_event_source.clone(), entity_key.subgraph.clone());

        // Obtain the reverted entity from the store
        let reverted_entity = store.get(entity_key.clone()).unwrap();

        // Verify that the entity has been returned to its original state
        assert_eq!(reverted_entity, original_entity);

        // Perform revert operation again and verify the same results to confirm the
        // idempotent nature of the revert_events function
        store.revert_events(revert_event_source, entity_key.subgraph.clone());
        let reverted_entity = store.get(entity_key).unwrap();
        assert_eq!(reverted_entity, original_entity);

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn entity_changes_are_fired_and_forwarded_to_subscriptions() {
    run_test(|store| {
        // Create a store subscription
        let subscription =
            store.subscribe(vec![(String::from("subgraph-id"), String::from("User"))]);

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
            store
                .set(
                    StoreKey {
                        subgraph: String::from("subgraph-id"),
                        entity: String::from("User"),
                        id: id.clone(),
                    },
                    entity.clone(),
                    EventSource::EthereumBlock(H256::random()),
                ).expect("failed to add entity to the store");
        }

        // Update an entity in the store
        let updated_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny")),
        ]);
        store
            .set(
                StoreKey {
                    subgraph: String::from("subgraph-id"),
                    entity: String::from("User"),
                    id: String::from("1"),
                },
                updated_entity.clone(),
                EventSource::EthereumBlock(H256::random()),
            ).expect("failed to update entity in the store");

        // Delete an entity in the store
        store
            .delete(
                StoreKey {
                    subgraph: String::from("subgraph-id"),
                    entity: String::from("User"),
                    id: String::from("2"),
                },
                EventSource::EthereumBlock(H256::random()),
            ).expect("failed to delete entity from the store");

        // We're expecting four events to be written to the subscription stream
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
            }).and_then(|_| Ok(()))
    })
}

#[cfg(any())]
#[test]
fn find_bytes_equal() {
    run_test(|| -> Result<(), ()> {
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let store = DieselStore::new(StoreConfig { url }, &logger);
        let this_query = StoreQuery {
            subgraph: TEST_SUBGRAPH_ID.clone(),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("hex_name"),
                Value::Bytes(scalar::Bytes::from_str(&hex::encode("Johnton")).unwrap()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let returned_entities = store.find(this_query).expect("store.find operation failed");

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

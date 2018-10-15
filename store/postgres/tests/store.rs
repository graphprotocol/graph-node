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
use std::mem;
use std::panic;
use std::str::FromStr;
use std::sync::Mutex;

use graph::components::store::{
    EventSource, StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange,
};
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
}

/// Test harness for running database integration tests.
#[cfg(any())]
fn run_test<R, F>(test: F) -> ()
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

    // Set up Store
    let logger = Logger::root(slog::Discard, o!());
    let url = postgres_test_url();
    let net_identifiers = EthereumNetworkIdentifier {
        net_version: "graph test suite".to_owned(),
        genesis_block_hash: H256::random(),
    };
    let network_name = "fake_network".to_owned();
    let store = Arc::new(DieselStore::new(
        StoreConfig { url, network_name },
        &logger,
        net_identifiers,
    ));

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    runtime
        .block_on(future::lazy(move || {
            insert_test_data(store.clone());
            future::ok::<_, ()>(())
        })).expect("Failed to insert test data");

    let result = panic::catch_unwind(panic::AssertUnwindSafe(move || {
        runtime.block_on(future::lazy(move || test(store.clone())))
    }));

    mem::drop(store);

    runtime
        .block_on(future::lazy(|| {
            remove_test_data();
            future::ok::<_, ()>(())
        })).expect("Failed to remove test data");

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
    block_hash: String,
) -> (StoreKey, Entity, EventSource) {
    let test_key = StoreKey {
        subgraph: String::from("test_subgraph"),
        entity: entity,
        id: id,
    };
    let mut test_entity = Entity::new();
    let hex_name = scalar::Bytes::from_str(&hex::encode(&name)).unwrap();
    test_entity.insert(String::from("name"), Value::String(name));
    test_entity.insert(String::from("hex_name"), Value::Bytes(hex_name));
    test_entity.insert(String::from("email"), Value::String(email));
    test_entity.insert(String::from("age"), Value::Int(age));
    test_entity.insert(String::from("weight"), Value::Float(weight));
    test_entity.insert(String::from("coffee"), Value::Bool(coffee));
    (
        test_key,
        test_entity,
        EventSource::EthereumBlock(H256::from_slice(&block_hash.as_bytes())),
    )
}

/// Inserts test data into the store.
#[cfg(any())]
fn insert_test_data(store: Arc<DieselStore>) {
    let test_entity_1 = create_test_entity(
        String::from("1"),
        String::from("user"),
        String::from("Johnton"),
        String::from("tonofjohn@email.com"),
        67 as i32,
        184.4 as f32,
        false,
        String::from("1cYsEjD7LKVExSj0aFA8"),
    );
    store
        .set(test_entity_1.0, test_entity_1.1, test_entity_1.2)
        .expect("Failed to insert test entity into the store");

    let test_entity_2 = create_test_entity(
        String::from("2"),
        String::from("user"),
        String::from("Cindini"),
        String::from("dinici@email.com"),
        43 as i32,
        159.1 as f32,
        true,
        String::from("b7kJ8ghP6PSITWx4lUZB"),
    );
    store
        .set(test_entity_2.0, test_entity_2.1, test_entity_2.2)
        .expect("Failed to insert test entity into the store");

    let test_entity_3 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena"),
        String::from("queensha@email.com"),
        28 as i32,
        111.7 as f32,
        false,
        String::from("TA7xjCbrczBiGFuZAW9Q"),
    );
    store
        .set(test_entity_3.0, test_entity_3.1, test_entity_3.2)
        .expect("Failed to insert test entity into the store");

    let test_entity_3_2 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena"),
        String::from("teeko@email.com"),
        28 as i32,
        111.7 as f32,
        false,
        String::from("znuyjijnezBiGFuZAW9Q"),
    );

    store
        .set(test_entity_3_2.0, test_entity_3_2.1, test_entity_3_2.2)
        .expect("Failed to insert test entity into the store");
}

/// Removes test data from the database behind the store.
fn remove_test_data() {
    use db_schema::entities::dsl::*;
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    delete(entities)
        .execute(&conn)
        .expect("Failed to remove test data");
}

#[test]
#[cfg(any())]
fn delete_entity() {
    run_test(|store| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        let test_key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("3"),
        };
        let source = EventSource::EthereumBlock(H256::random());
        store.delete(test_key, source).unwrap();

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
#[cfg(any())]
fn get_entity() {
    run_test(|store| -> Result<(), ()> {
        let key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("1"),
        };
        let result = store.get(key).unwrap();

        let mut expected_entity = Entity::new();

        let name = "Johnton".to_owned();
        let hex_name = format!("0x{}", hex::encode(&name));
        expected_entity.insert(String::from("hex_name"), Value::String(hex_name));
        expected_entity.insert(String::from("name"), Value::String(name));
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
#[cfg(any())]
fn insert_entity() {
    run_test(|store| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        let test_entity_1 = create_test_entity(
            String::from("7"),
            String::from("user"),
            String::from("Wanjon"),
            String::from("wanawana@email.com"),
            76 as i32,
            111.7 as f32,
            true,
            String::from("MSjZmOE7UqBOzzYibsw9"),
        );
        store
            .set(test_entity_1.0, test_entity_1.1, test_entity_1.2)
            .expect("Failed to set entity in the store");

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
#[cfg(any())]
fn update_existing() {
    run_test(|store| -> Result<(), ()> {
        let entity_key = StoreKey {
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            id: String::from("1"),
        };

        let mut test_entity_1 = create_test_entity(
            String::from("1"),
            String::from("user"),
            String::from("Wanjon"),
            String::from("wanawana@email.com"),
            76 as i32,
            111.7 as f32,
            true,
            String::from("6SFIlpqNoDy6FfJQryNM"),
        );

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(store.get(entity_key.clone()).unwrap(), test_entity_1.1);

        // Set test entity; as the entity already exists an update should be performed
        store
            .set(test_entity_1.0, test_entity_1.1.clone(), test_entity_1.2)
            .expect("Failed to update entity that already exists");

        // Verify that the entity in the store has changed to what we have set.
        // The `hex_name` will be returned as a `Value::String`.
        let hex_name = match test_entity_1.1.get("hex_name") {
            Some(Value::Bytes(bytes)) => bytes.clone(),
            _ => unreachable!(),
        };
        test_entity_1.1.insert(
            String::from("hex_name"),
            Value::String(hex_name.to_string()),
        );
        assert_eq!(store.get(entity_key).unwrap(), test_entity_1.1);

        Ok(())
    })
}

#[test]
#[cfg(any())]
fn partially_update_existing() {
    run_test(|store| -> Result<(), ()> {
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

        let original_entity = store.get(entity_key.clone()).unwrap();
        let event_source = EventSource::EthereumBlock(H256::random());
        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(original_entity, partial_entity);

        // Set test entity; as the entity already exists an update should be performed
        store
            .set(entity_key.clone(), partial_entity.clone(), event_source)
            .expect("Failed to update entity that already exists");

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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(160 as f32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("weight"),
                Value::Float(161 as f32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("weight"),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("weight"),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterOrEqual(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(50 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessOrEqual(
                String::from("age"),
                Value::Int(43 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(50 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                String::from("age"),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("age"),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("age"),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("coffee"),
                Value::Bool(true),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                String::from("coffee"),
                Value::Bool(true),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::In(
                String::from("coffee"),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::NotIn(
                String::from("coffee"),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Shaqueeena")),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
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
            subgraph: String::from("test_subgraph"),
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                String::from("name"),
                Value::String(String::from("Cindini")),
            )])),
            order_by: Some(("name".to_string(), ValueType::String)),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        // Delete an entity using a randomly created event source
        let del_key = StoreKey {
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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
            subgraph: String::from("test_subgraph"),
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

extern crate diesel;
extern crate futures;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate thegraph;
extern crate thegraph_store_postgres_diesel;
extern crate tokio_core;

use diesel::pg::PgConnection;
use diesel::*;
use futures::prelude::*;
use futures::sync::mpsc::Sender;
use futures::sync::oneshot;
use std::ops::Deref;
use std::panic;
use tokio_core::reactor::Core;

use thegraph::components::store::{StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange};
use thegraph::prelude::*;
use thegraph_store_postgres_diesel::{db_schema, Store as DieselStore, StoreConfig};

/// Helper function to ensure and obtain the Postgres URL to use for testing.
fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

/// Test harness for running database integration tests.
fn run_test<T>(test: T) -> ()
where
    T: FnOnce() -> () + panic::UnwindSafe,
{
    insert_test_data();
    let result = panic::catch_unwind(|| test());
    remove_test_data();
    result.expect("Failed to run test");
}

fn store_get(
    runtime: &mut Core,
    request_sink: Sender<StoreRequest>,
    key: StoreKey,
) -> StoreGetResponse {
    let (sender, receiver) = oneshot::channel();
    request_sink
        .send(StoreRequest::Get(key, sender))
        .wait()
        .unwrap();
    runtime.run(receiver).unwrap()
}

fn store_set(
    runtime: &mut Core,
    request_sink: Sender<StoreRequest>,
    key: StoreKey,
    entity: Entity,
) -> StoreSetResponse {
    let (sender, receiver) = oneshot::channel();
    request_sink
        .send(StoreRequest::Set(key, entity, sender))
        .wait()
        .unwrap();
    runtime.run(receiver).unwrap()
}

fn store_delete(
    runtime: &mut Core,
    request_sink: Sender<StoreRequest>,
    key: StoreKey,
) -> StoreDeleteResponse {
    let (sender, receiver) = oneshot::channel();
    request_sink
        .send(StoreRequest::Delete(key, sender))
        .wait()
        .unwrap();
    runtime.run(receiver).unwrap()
}

fn store_find(
    runtime: &mut Core,
    request_sink: Sender<StoreRequest>,
    query: StoreQuery,
) -> StoreFindResponse {
    let (sender, receiver) = oneshot::channel();
    request_sink
        .send(StoreRequest::Find(query, sender))
        .wait()
        .unwrap();
    runtime.run(receiver).unwrap()
}

fn fresh_store() -> (Core, DieselStore, Sender<StoreRequest>) {
    let core = Core::new().unwrap();
    let logger = slog::Logger::root(slog::Discard, o!());
    let url = postgres_test_url();
    let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
    let store_requests = store.request_sink();
    return (core, store, store_requests);
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
        entity: entity,
        id: id,
    };
    let mut test_entity = Entity::new();
    test_entity.insert("name".to_string(), Value::String(name));
    test_entity.insert("email".to_string(), Value::String(email));
    test_entity.insert("age".to_string(), Value::Int(age));
    test_entity.insert("weight".to_string(), Value::Float(weight));
    test_entity.insert("coffee".to_string(), Value::Bool(coffee));
    (test_key, test_entity)
}

/// Inserts test data into the store.
fn insert_test_data() {
    let (mut core, _store, store_requests) = fresh_store();

    let test_entity_1 = create_test_entity(
        String::from("1"),
        String::from("user"),
        String::from("Johnton".to_string()),
        String::from("tonofjohn@email.com".to_string()),
        67 as i32,
        184.4 as f32,
        false,
    );
    store_set(
        &mut core,
        store_requests.clone(),
        test_entity_1.0,
        test_entity_1.1,
    ).expect("Failed to insert test entity into the store");

    let test_entity_2 = create_test_entity(
        String::from("2"),
        String::from("user"),
        String::from("Cindini".to_string()),
        String::from("dinici@email.com".to_string()),
        43 as i32,
        159.1 as f32,
        true,
    );
    store_set(
        &mut core,
        store_requests.clone(),
        test_entity_2.0,
        test_entity_2.1,
    ).expect("Failed to insert test entity into the store");

    let test_entity_3 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena".to_string()),
        String::from("queensha@email.com".to_string()),
        28 as i32,
        111.7 as f32,
        false,
    );
    store_set(
        &mut core,
        store_requests.clone(),
        test_entity_3.0,
        test_entity_3.1,
    ).expect("Failed to insert test entity into the store");
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
fn delete_entity() {
    run_test(|| {
        use db_schema::entities::dsl::*;

        let (mut core, store, store_requests) = fresh_store();

        let key = StoreKey {
            entity: String::from("user"),
            id: String::from("3"),
        };
        store_delete(&mut core, store_requests, key).expect("Failed to delete entity");

        //Get all ids in table
        let conn = store.conn.lock().unwrap();
        let all_ids = entities.select(id).load::<String>(conn.deref()).unwrap();

        // Check that that the deleted entity id is not present
        assert!(!all_ids.contains(&"3".to_string()));
    })
}

#[test]
fn get_entity() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let key = StoreKey {
            entity: String::from("user"),
            id: String::from("1"),
        };
        let result = store_get(&mut core, store_requests, key).expect("Failed to get entity");

        let mut expected_entity = Entity::new();
        expected_entity.insert("name".to_string(), Value::String("Johnton".to_string()));
        expected_entity.insert(
            "email".to_string(),
            Value::String("tonofjohn@email.com".to_string()),
        );
        expected_entity.insert("age".to_string(), Value::Int(67 as i32));
        expected_entity.insert("weight".to_string(), Value::Float(184.4 as f32));
        expected_entity.insert("coffee".to_string(), Value::Bool(false));

        // Check that the expected entity was returned
        assert_eq!(result, expected_entity);
    })
}

#[test]
fn insert_entity() {
    run_test(|| {
        use db_schema::entities::dsl::*;

        let (mut core, store, store_requests) = fresh_store();

        let test_entity_1 = create_test_entity(
            String::from("7"),
            String::from("user"),
            String::from("Wanjon".to_string()),
            String::from("wanawana@email.com".to_string()),
            76 as i32,
            111.7 as f32,
            true,
        );
        store_set(&mut core, store_requests, test_entity_1.0, test_entity_1.1)
            .expect("Failed to set entity in the store");

        // Check that new record is in the store
        let conn = store.conn.lock().unwrap();
        let all_ids = entities.select(id).load::<String>(conn.deref()).unwrap();
        assert!(all_ids.iter().any(|x| x == &"7".to_string()));
    })
}

#[test]
fn update_existing() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let entity_key = StoreKey {
            entity: String::from("user"),
            id: String::from("1"),
        };

        let test_entity_1 = create_test_entity(
            String::from("1"),
            String::from("user"),
            String::from("Wanjon".to_string()),
            String::from("wanawana@email.com".to_string()),
            76 as i32,
            111.7 as f32,
            true,
        );

        // Get the entity before updating it
        let existing = store_get(&mut core, store_requests.clone(), entity_key.clone())
            .expect("Failed to get existing entity");

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(existing, test_entity_1.1);

        // Set test entity; as the entity already exists an update should be performed
        store_set(
            &mut core,
            store_requests.clone(),
            test_entity_1.0.clone(),
            test_entity_1.1.clone(),
        ).expect("Failed to update entity");

        // Fetch the updated entity
        let after = store_get(&mut core, store_requests.clone(), entity_key)
            .expect("Failed to update entity that already exists");

        // Verify that the entity in the store has changed to what we have set
        assert_eq!(after, test_entity_1.1);
    })
}

#[test]
fn find_string_contains() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Contains(
                "name".to_string(),
                Value::String("%ind%".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Make sure the first user in the result vector is "Cindini"
        let returned_entity = &result[0];
        let returned_name = returned_entity.get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());
    })
}

#[test]
fn find_string_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "name".to_string(),
                Value::String("Cindini".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Make sure the first user in the result vector is "Cindini"
        let returned_entity = &result[0];
        let returned_name = returned_entity.get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());
    })
}

#[test]
fn find_string_not_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "name".to_string(),
                Value::String("Cindini".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_string_greater_than() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_string_less_than() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        //There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_string_less_than_order_by_desc() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_string_less_than_range() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("ZZZ".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_string_multiple_and() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![
                StoreFilter::LessThan("name".to_string(), Value::String("Cz".to_string())),
                StoreFilter::Equal("name".to_string(), Value::String("Cindini".to_string())),
            ])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_float_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "weight".to_string(),
                Value::Float(184.4 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_float_not_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "weight".to_string(),
                Value::Float(184.4 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_float_greater_than() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                "weight".to_string(),
                Value::Float(160 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_float_less_than() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "weight".to_string(),
                Value::Float(160 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini";
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_float_less_than_order_by_desc() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "weight".to_string(),
                Value::Float(160 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_float_less_than_range() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "weight".to_string(),
                Value::Float(161 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_int_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "age".to_string(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one users returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_int_not_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "age".to_string(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_int_greater_than() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                "age".to_string(),
                Value::Int(43 as i32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_int_greater_or_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterOrEqual(
                "age".to_string(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_int_less_than() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "age".to_string(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_int_less_or_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThanOrEqual(
                "age".to_string(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini";
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_int_less_than_order_by_desc() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "age".to_string(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

#[test]
fn find_int_less_than_range() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "age".to_string(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_bool_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "coffee".to_string(),
                Value::Bool(true),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Cindini"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, result.len());
    })
}

#[test]
fn find_bool_not_equal() {
    run_test(|| {
        let (mut core, _store, store_requests) = fresh_store();

        let query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "coffee".to_string(),
                Value::Bool(true),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };

        let result = store_find(&mut core, store_requests, query).expect("Failed to find entities");

        // Check if the first user in the result vector is "Johnton"
        let returned_name = result[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, result.len());
    })
}

extern crate diesel;
extern crate serde_json;
extern crate graph;
extern crate graph_store_postgres;
extern crate tokio_core;
#[macro_use]
extern crate slog;

use diesel::pg::PgConnection;
use diesel::*;
use slog::Logger;
use std::panic;
use tokio_core::reactor::Core;

use graph::components::store::{
    EventSource, StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange,
};
use graph::prelude::*;
use graph_store_postgres::{db_schema, Store as DieselStore, StoreConfig};

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
        entity: entity,
        id: id,
    };
    let mut test_entity = Entity::new();
    test_entity.insert("name".to_string(), Value::String(name));
    test_entity.insert("email".to_string(), Value::String(email));
    test_entity.insert("age".to_string(), Value::Int(age));
    test_entity.insert("weight".to_string(), Value::Float(weight));
    test_entity.insert("coffee".to_string(), Value::Bool(coffee));
    (
        test_key,
        test_entity,
        EventSource::LocalProcess(String::from(block_hash)),
    )
}

/// Inserts test data into the store.
fn insert_test_data() {
    let core = Core::new().unwrap();
    let logger = Logger::root(slog::Discard, o!());
    let url = postgres_test_url();
    let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

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
        String::from("Shaqueeena".to_string()),
        String::from("queensha@email.com".to_string()),
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
        String::from("Shaqueeena".to_string()),
        String::from("teeko@email.com".to_string()),
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
fn delete_entity() {
    run_test(|| {
        use db_schema::entities::dsl::*;
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

        let test_key = StoreKey {
            entity: String::from("user"),
            id: String::from("3"),
        };
        let source = EventSource::LocalProcess(String::from("delete_operation"));
        store.delete(test_key, source).unwrap();

        //Get all ids in table
        let all_ids = entities.select(id).load::<String>(&store.conn).unwrap();
        // Check that that the deleted entity id is not present
        assert!(!all_ids.contains(&"3".to_string()));
    })
}

#[test]
fn get_entity() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

        let key = StoreKey {
            entity: String::from("user"),
            id: String::from("1"),
        };
        let result = store.get(key).unwrap();

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

        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

        let test_entity_1 = create_test_entity(
            String::from("7"),
            String::from("user"),
            String::from("Wanjon".to_string()),
            String::from("wanawana@email.com".to_string()),
            76 as i32,
            111.7 as f32,
            true,
            String::from("MSjZmOE7UqBOzzYibsw9"),
        );
        store
            .set(test_entity_1.0, test_entity_1.1, test_entity_1.2)
            .expect("Failed to set entity in the store");

        // Check that new record is in the store
        let all_ids = entities.select(id).load::<String>(&store.conn).unwrap();
        assert!(all_ids.iter().any(|x| x == &"7".to_string()));
    })
}

#[test]
fn update_existing() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

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
            String::from("6SFIlpqNoDy6FfJQryNM"),
        );

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(store.get(entity_key.clone()).unwrap(), test_entity_1.1);

        // Set test entity; as the entity already exists an update should be performed
        store
            .set(test_entity_1.0, test_entity_1.1.clone(), test_entity_1.2)
            .expect("Failed to update entity that already exists");

        // Verify that the entity in the store has changed to what we have set
        assert_eq!(store.get(entity_key).unwrap(), test_entity_1.1);
    })
}

#[test]
fn partially_update_existing() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

        let entity_key = StoreKey {
            entity: String::from("user"),
            id: String::from("1"),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store.get(entity_key.clone()).unwrap();

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(original_entity, partial_entity);

        // Set test entity; as the entity already exists an update should be performed
        store
            .set(entity_key.clone(), partial_entity.clone())
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
    })
}

#[test]
fn find_string_contains() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Contains(
                "name".to_string(),
                Value::String("%ind%".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Make sure the first user in the result vector is "Cindini"
        let returned_entity = &result.unwrap()[0];
        let returned_name = returned_entity.get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());
    })
}

#[test]
fn find_string_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "name".to_string(),
                Value::String("Cindini".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Make sure the first user in the result vector is "Cindini"
        let returned_entity = &result.unwrap()[0];
        let returned_name = returned_entity.get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());
    })
}

#[test]
fn find_string_not_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "name".to_string(),
                Value::String("Cindini".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_string_greater_than() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_string_less_than() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"; fail if it is
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(&test_value, returned_name.unwrap());

        //There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_string_less_than_order_by_asc() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = new_store
            .find(this_query)
            .expect("Failed to fetch entities from the store");

        // Check that the number and order of users is correct
        assert_eq!(2, result.len());
        let names: Vec<&Value> = result
            .iter()
            .map(|entity| {
                entity
                    .get(&"name".to_string())
                    .expect("Entity without \"name\" attribute returned")
            })
            .collect();
        assert_eq!(
            names,
            vec![
                &Value::String("Cindini".to_string()),
                &Value::String("Johnton".to_string()),
            ]
        );
    })
}

#[test]
fn find_string_less_than_order_by_desc() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("Kundi".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store
            .find(this_query)
            .expect("Failed to fetch entities from the store");

        // Check that the number and order of users is correct
        assert_eq!(2, result.len());
        let names: Vec<&Value> = result
            .iter()
            .map(|entity| {
                entity
                    .get(&"name".to_string())
                    .expect("Entity without \"name\" attribute returned")
            })
            .collect();
        assert_eq!(
            names,
            vec![
                &Value::String("Johnton".to_string()),
                &Value::String("Cindini".to_string()),
            ]
        );
    })
}

#[test]
fn find_string_less_than_range() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "name".to_string(),
                Value::String("ZZZ".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Johnton"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_string_multiple_and() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![
                StoreFilter::LessThan("name".to_string(), Value::String("Cz".to_string())),
                StoreFilter::Equal("name".to_string(), Value::String("Cindini".to_string())),
            ])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_float_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "weight".to_string(),
                Value::Float(184.4 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Johnton"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_float_not_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "weight".to_string(),
                Value::Float(184.4 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_float_greater_than() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                "weight".to_string(),
                Value::Float(160 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Johnton"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_float_less_than() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "weight".to_string(),
                Value::Float(160 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini";
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        //There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_float_less_than_order_by_desc() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "weight".to_string(),
                Value::Float(160 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_float_less_than_range() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "weight".to_string(),
                Value::Float(161 as f32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());
        // Check if the first user in the result vector is "Cindini"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_int_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "age".to_string(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one users returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_int_not_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "age".to_string(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_int_greater_than() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterThan(
                "age".to_string(),
                Value::Int(43 as i32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Johnton"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_int_greater_or_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::GreaterOrEqual(
                "age".to_string(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_int_less_than() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "age".to_string(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        //There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_int_less_or_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessOrEqual(
                "age".to_string(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini";
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        //There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_int_less_than_order_by_desc() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "age".to_string(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Shaqueeena"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Shaqueeena".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn find_int_less_than_range() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::LessThan(
                "age".to_string(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: Some(StoreRange { first: 1, skip: 1 }),
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Johnton"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_bool_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "coffee".to_string(),
                Value::Bool(true),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Cindini"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn find_bool_not_equal() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = Logger::root(slog::Discard, o!());
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Not(
                "coffee".to_string(),
                Value::Bool(true),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Ascending),
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector is "Johnton"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = Value::String("Johnton".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be two users returned in results
        assert_eq!(2, returned_entities.len());
    })
}

#[test]
fn revert_block() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = postgres_test_url();
        let new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "name".to_string(),
                Value::String("Shaqueeena".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        // Revert all events associated with event_source, "znuyjijnezBiGFuZAW9Q"
        new_store.revert_events("znuyjijnezBiGFuZAW9Q".to_string());

        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if the first user in the result vector has email "queensha@email.com"
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"email".to_string());
        println!("{:?}", &returned_name);
        let test_value = Value::String("queensha@email.com".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one user returned in results
        assert_eq!(1, returned_entities.len());
    })
}

#[test]
fn revert_block_with_delete() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = postgres_test_url();
        let mut new_store = DieselStore::new(StoreConfig { url }, &logger, core.handle());
        let this_query = StoreQuery {
            entity: String::from("user"),
            filter: Some(StoreFilter::And(vec![StoreFilter::Equal(
                "name".to_string(),
                Value::String("Cindini".to_string()),
            )])),
            order_by: Some(String::from("name")),
            order_direction: Some(StoreOrder::Descending),
            range: None,
        };

        //Delete an entity using custom event source "delete_operation"
        let del_key = StoreKey {
            entity: "user".to_string(),
            id: "2".to_string(),
        };
        let source = EventSource::LocalProcess(String::from("delete_operation"));
        new_store.delete(del_key, source).unwrap();

        // Revert all events associated with our custom event_source, "delete_operation"
        new_store.revert_events("delete_operation".to_string());

        let result = new_store.find(this_query);
        assert!(result.is_ok());

        // Check if "cindini@email.com" is in result set
        let returned_entities = result.unwrap();
        let returned_name = returned_entities[0].get(&"email".to_string());
        println!("NAME {:?}", &returned_name);
        let test_value = Value::String("dinici@email.com".to_string());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        // There should be one entity returned in results
        assert_eq!(1, returned_entities.len());
    })
}

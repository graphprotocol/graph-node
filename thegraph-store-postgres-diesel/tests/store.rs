extern crate diesel;
extern crate serde_json;
extern crate thegraph;
extern crate thegraph_store_postgres_diesel;
extern crate tokio_core;

use diesel::pg::PgConnection;
use diesel::*;
use std::panic;
use thegraph::components::store::{StoreFilter, StoreKey, StoreQuery};
use thegraph::prelude::*;
use thegraph::util::log::logger;
use thegraph_store_postgres_diesel::{db_schema, Store as DieselStore, StoreConfig};
use tokio_core::reactor::Core;

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
) -> (StoreKey, Entity) {
    let test_key = StoreKey {
        entity: entity,
        id: id,
    };
    let mut test_entity = Entity::new();
    test_entity.insert("name".to_string(), Value::String(name));
    test_entity.insert("email".to_string(), Value::String(email));
    (test_key, test_entity)
}

/// Inserts test data into the store.
fn insert_test_data() {
    let core = Core::new().unwrap();
    let logger = logger();
    let url = postgres_test_url();
    let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

    let test_entity_1 = create_test_entity(
        String::from("1"),
        String::from("user"),
        String::from("Johnton".to_string()),
        String::from("tonofjohn@email.com".to_string()),
    );
    store
        .set(test_entity_1.0, test_entity_1.1)
        .expect("Failed to insert test entity into the store");

    let test_entity_2 = create_test_entity(
        String::from("2"),
        String::from("user"),
        String::from("Cindini".to_string()),
        String::from("dinici@email.com".to_string()),
    );
    store
        .set(test_entity_2.0, test_entity_2.1)
        .expect("Failed to insert test entity into the store");

    let test_entity_3 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena".to_string()),
        String::from("queensha@email.com".to_string()),
    );
    store
        .set(test_entity_3.0, test_entity_3.1)
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
        let logger = logger();
        let url = postgres_test_url();
        let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

        let test_key = StoreKey {
            entity: String::from("user"),
            id: String::from("3"),
        };
        store.delete(test_key).unwrap();

        let all_ids = entities.select(id).load::<String>(&store.conn).unwrap();
        assert!(!all_ids.contains(&"3".to_string()));
    })
}

#[test]
fn get_entity() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
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

        // For now just making sure there are sane results
        assert_eq!(result, expected_entity);
    })
}

#[test]
fn insert_new_entity() {
    run_test(|| {
        use db_schema::entities::dsl::*;

        let core = Core::new().unwrap();
        let logger = logger();
        let url = postgres_test_url();
        let mut store = DieselStore::new(StoreConfig { url }, &logger, core.handle());

        let test_entity_1 = create_test_entity(
            String::from("7"),
            String::from("user"),
            String::from("Wanjon".to_string()),
            String::from("wanawana@email.com".to_string()),
        );
        store
            .set(test_entity_1.0, test_entity_1.1)
            .expect("Failed to set entity in the store");

        // Check that new record is in the store
        let all_ids = entities.select(id).load::<String>(&store.conn).unwrap();
        assert!(all_ids.iter().any(|x| x == &"7".to_string()));
    })
}

#[test]
fn update_existing_entity() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
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
        );

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(store.get(entity_key.clone()).unwrap(), test_entity_1.1);

        // Set test entity; as the entity already exists an update should be performed
        store
            .set(test_entity_1.0, test_entity_1.1.clone())
            .expect("Failed to update entity that already exists");

        // Verify that the entity in the store has changed to what we have set
        assert_eq!(store.get(entity_key).unwrap(), test_entity_1.1);
    })
}

#[test]
fn find_entities_contain_string() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let new_store = dieselstore::Store::new(
            StoreConfig {
                url: url.to_string(),
            },
            &logger,
            core.handle(),
        );
        let this_query = StoreQuery {
            entity: String::from("user"),
            filters: vec![
                StoreFilter::Contains(
                    "name".to_string(),
                    thegraph::prelude::Value::String("%ind%".to_string()),
                ),
            ],
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        //Make sure the first user in the result vector is "Cindini"
        let returned_entity = &result.unwrap()[0];
        let returned_name = returned_entity.get(&"name".to_string());
        let test_value = &thegraph::prelude::Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(test_value, returned_name.unwrap());
    })
}

#[test]
fn find_entities_equal_string() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let new_store = dieselstore::Store::new(
            StoreConfig {
                url: url.to_string(),
            },
            &logger,
            core.handle(),
        );
        let this_query = StoreQuery {
            entity: String::from("user"),
            filters: vec![
                StoreFilter::Equal(
                    "name".to_string(),
                    thegraph::prelude::Value::String("Cindini".to_string()),
                ),
            ],
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        //Make sure the first user in the result vector is "Cindini"
        let returned_entity = &result.unwrap()[0];
        let returned_name = returned_entity.get(&"name".to_string());
        let test_value = &thegraph::prelude::Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_eq!(test_value, returned_name.unwrap());
    })
}

#[test]
fn find_entities_not_equal_string() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let new_store = dieselstore::Store::new(
            StoreConfig {
                url: url.to_string(),
            },
            &logger,
            core.handle(),
        );
        let this_query = StoreQuery {
            entity: String::from("user"),
            filters: vec![
                StoreFilter::Not(
                    "name".to_string(),
                    thegraph::prelude::Value::String("Cindini".to_string()),
                ),
            ],
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        //Check if the first user in the result vector is "Cindini"
        //If it is fail
        let returned_entities = &result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = &thegraph::prelude::Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(test_value, returned_name.unwrap());

        //There should be 3 users returned in results
        assert_eq!(&(2 as usize), &returned_entities.len());
    })
}

#[test]
fn find_entities_greater_than_string() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let new_store = dieselstore::Store::new(
            StoreConfig {
                url: url.to_string(),
            },
            &logger,
            core.handle(),
        );
        let this_query = StoreQuery {
            entity: String::from("user"),
            filters: vec![
                StoreFilter::GreaterThan(
                    "name".to_string(),
                    thegraph::prelude::Value::String("Kundi".to_string()),
                ),
            ],
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        //Check if the first user in the result vector is "Cindini"
        //If it is fail
        let returned_entities = &result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = &thegraph::prelude::Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(test_value, returned_name.unwrap());

        //There should be 3 users returned in results
        assert_eq!(&(1 as usize), &returned_entities.len());
    })
}

#[test]
fn find_entities_less_than_string() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let new_store = dieselstore::Store::new(
            StoreConfig {
                url: url.to_string(),
            },
            &logger,
            core.handle(),
        );
        let this_query = StoreQuery {
            entity: String::from("user"),
            filters: vec![
                StoreFilter::LessThan(
                    "name".to_string(),
                    thegraph::prelude::Value::String("Kundi".to_string()),
                ),
            ],
            order_by: None,
            order_direction: None,
            range: None,
        };
        let result = new_store.find(this_query);
        assert!(result.is_ok());

        //Check if the first user in the result vector is "Cindini"
        //If it is fail
        let returned_entities = &result.unwrap();
        let returned_name = returned_entities[0].get(&"name".to_string());
        let test_value = &thegraph::prelude::Value::String("Cindini".to_string());
        assert!(returned_name.is_some());
        assert_ne!(test_value, returned_name.unwrap());

        //There should be 3 users returned in results
        assert_eq!(&(2 as usize), &returned_entities.len());
    })
}

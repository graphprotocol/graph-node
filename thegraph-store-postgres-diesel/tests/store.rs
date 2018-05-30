extern crate diesel;
extern crate serde_json;
extern crate thegraph_store_postgres_diesel;
extern crate thegraph;
extern crate tokio_core;
#[macro_use]
extern crate slog;

use diesel::pg::PgConnection;
use diesel::*;
use std::panic;
use thegraph::prelude::Store;
use thegraph::util::log::logger;
use thegraph::components::store::StoreKey;
use thegraph::data::store::Entity;
use thegraph_store_postgres_diesel::{store as dieselstore, ourschema, StoreConfig};
use tokio_core::reactor::Core;

/// Test harness for running database integration tests.
fn run_test<T>(test: T) -> ()
    where T: FnOnce() -> () + panic::UnwindSafe
{
    insert_test_data();

    let result = panic::catch_unwind(|| {
        test()
    });

    remove_test_data();

    assert!(result.is_ok())
}

fn insert_test_data() {
    let core = Core::new().unwrap();
    let logger = logger();
    let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
    let mut new_store = dieselstore::Store::new(StoreConfig { url: url.to_string() }, &logger, core.handle());

    let test_entity_1 = create_test_entity(
        String::from("1"),
        String::from("user"),
        String::from("Johnton".to_string()),
        String::from("tonofjohn@email.com".to_string())
    );
    assert!(new_store.set(test_entity_1.0, test_entity_1.1).is_ok());

    let test_entity_2 = create_test_entity(
        String::from("2"),
        String::from("user"),
        String::from("Cindini".to_string()),
        String::from("dinici@email.com".to_string())
    );
    assert!(new_store.set(test_entity_2.0, test_entity_2.1).is_ok());

    let test_entity_3 = create_test_entity(
        String::from("3"),
        String::from("user"),
        String::from("Shaqueeena".to_string()),
        String::from("queensha@email.com".to_string())
    );
    assert!(new_store.set(test_entity_3.0, test_entity_3.1).is_ok());
}

fn remove_test_data() {
    use ourschema::entities::dsl::*;
    let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
    let conn =
        PgConnection::establish(&url).expect("Failed to connect to Postgres");
    let _results = delete(entities)
        .execute(&conn).is_ok();
}

fn create_test_entity(id: String, entity: String, name: String, email: String) -> (StoreKey, Entity) {
    let test_key = StoreKey {
        entity: entity,
        id: id
    };
    let mut test_entity = Entity::new();
    test_entity.insert("name".to_string(), thegraph::prelude::Value::String(name));
    test_entity.insert("email".to_string(), thegraph::prelude::Value::String(email));
    (test_key, test_entity)
}

#[test]
fn delete_entity() {
    run_test(|| {
        use ourschema::entities::dsl::*;
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let mut new_store = dieselstore::Store::new(StoreConfig { url: url.to_string() }, &logger, core.handle());

        let test_key = StoreKey {
            entity: String::from("user"),
            id: String::from("3")
        };
        let _num_rows_deleted = new_store.delete(test_key).unwrap();

        let all_ids = entities.select(id).load::<i32>(&new_store.conn).unwrap();
        assert!(!all_ids.contains(&(3 as i32)));
    })
}

#[test]
fn get_entity() {
    run_test(|| {
        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let new_store = dieselstore::Store::new(StoreConfig { url: url.to_string() }, &logger, core.handle());

        let key = StoreKey {
            entity: String::from("user"),
            id: String::from("1")
        };
        let result = new_store.get(key).unwrap();

        // Would like to implement Entity comparison traits here in order to compare against compare_map.
        let mut compare_map = Entity::new();
        compare_map.insert("name".to_string(), thegraph::prelude::Value::String("Johnton".to_string()));
        compare_map.insert("email".to_string(), thegraph::prelude::Value::String("tonofjohn@email.com".to_string()));

        //For now just making sure there are sane results
        assert!(result.contains_key("name"));
    })
}

#[test]
fn add_new_entity() {
    run_test(|| {
        use ourschema::entities::dsl::*;

        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let mut new_store = dieselstore::Store::new(StoreConfig { url: url.to_string() }, &logger, core.handle());

        let test_entity_1 = create_test_entity(
            String::from("7"), String::from("user"),
            String::from("Wanjon".to_string()),
            String::from("wanawana@email.com".to_string())
        );
        assert!(new_store.set(test_entity_1.0, test_entity_1.1).is_ok());

        //Check that new record is in the Store
        let all_ids = entities.select(id).load::<i32>(&new_store.conn).unwrap();
        assert!(all_ids.iter().any(|x| x == &(7i32)));
    })
}

#[test]
fn update_existing_entity() {
    run_test(|| {
        // use ourschema::entities::dsl::*;

        let core = Core::new().unwrap();
        let logger = logger();
        let url = "postgres://testuser:testpassword@192.168.99.100:31599/tests";
        let mut new_store = dieselstore::Store::new(StoreConfig { url: url.to_string() }, &logger, core.handle());

        let test_entity_1 = create_test_entity(
            String::from("1"), String::from("user"),
            String::from("Wanjon".to_string()),
            String::from("wanawana@email.com".to_string())
        );
        //Set test entity, since entity already exists an update should be performed
        assert!(new_store.set(test_entity_1.0, test_entity_1.1).is_ok());

        //Create fetch key, get entity that should have changed
        let test_key = StoreKey {
            entity: String::from("user"),
            id: String::from("1")
        };
        let result: Entity = new_store.get(test_key).unwrap();

        //Assert that the name attribute has been successfully changed to "Wanjon"
        assert_eq!(&thegraph::prelude::Value::String("Wanjon".to_string()), result.get("name").unwrap());
    })
}

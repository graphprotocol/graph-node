//! Test mapping of GraphQL schema to a relational schema
use diesel::connection::SimpleConnection as _;
use diesel::pg::PgConnection;
use graph::prelude::{
    o, slog, tokio, web3::types::H256, DeploymentHash, Entity, EntityCollection, EntityFilter,
    EntityKey, EntityOrder, EntityQuery, EntityRange, Logger, Schema, StopwatchMetrics, Value,
    ValueType, BLOCK_NUMBER_MAX,
};
use graph_mock::MockMetricsRegistry;
use graph_store_postgres::layout_for_tests::set_account_like;
use graph_store_postgres::layout_for_tests::LayoutCache;
use graph_store_postgres::layout_for_tests::SqlName;
use hex_literal::hex;
use lazy_static::lazy_static;
use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use graph::{
    components::store::{AttributeNames, EntityType},
    data::store::scalar::{BigDecimal, BigInt, Bytes},
};
use graph_store_postgres::{
    layout_for_tests::make_dummy_site,
    layout_for_tests::{Layout, Namespace, STRING_PREFIX_SIZE},
};

use test_store::*;

const THINGS_GQL: &str = r#"
    type _Schema_ @fulltext(
        name: "userSearch"
        language: en
        algorithm: rank
        include: [
            {
                entity: "User",
                fields: [
                    { name: "name"},
                    { name: "email"},
                ]
            }
        ]
    ) @fulltext(
        name: "nullableStringsSearch"
        language: en
        algorithm: rank
        include: [
            {
                entity: "NullableStrings",
                fields: [
                    { name: "name"},
                    { name: "description"},
                    { name: "test"},
                ]
            }
        ]
    )

    type Thing @entity {
        id: ID!
        bigThing: Thing!
    }

    enum Color { yellow, red, BLUE }

    type Scalar @entity {
        id: ID,
        bool: Boolean,
        int: Int,
        bigDecimal: BigDecimal,
        bigDecimalArray: [BigDecimal!]!
        string: String,
        strings: [String!],
        bytes: Bytes,
        byteArray: [Bytes!],
        bigInt: BigInt,
        bigIntArray: [BigInt!]!
        color: Color,
    }

    interface Pet {
        id: ID!,
        name: String!
    }

    type Cat implements Pet @entity {
        id: ID!,
        name: String!
    }

    type Dog implements Pet @entity {
        id: ID!,
        name: String!
    }

    type Ferret implements Pet @entity {
        id: ID!,
        name: String!
    }

    type User @entity {
        id: ID!,
        name: String!,
        bin_name: Bytes!,
        email: String!,
        age: Int!,
        seconds_age: BigInt!,
        weight: BigDecimal!,
        coffee: Boolean!,
        favorite_color: Color,
        drinks: [String!]
    }

    type NullableStrings @entity {
        id: ID!,
        name: String,
        description: String,
        test: String
    }
"#;

lazy_static! {
    static ref THINGS_SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("things").unwrap();
    static ref NAMESPACE: Namespace = Namespace::new("sgd0815".to_string()).unwrap();
    static ref LARGE_INT: BigInt = BigInt::from(std::i64::MAX).pow(17);
    static ref LARGE_DECIMAL: BigDecimal =
        BigDecimal::from(1) / BigDecimal::new(LARGE_INT.clone(), 1);
    static ref BYTES_VALUE: H256 = H256::from(hex!(
        "e8b3b02b936c4a4a331ac691ac9a86e197fb7731f14e3108602c87d4dac55160"
    ));
    static ref BYTES_VALUE2: H256 = H256::from(hex!(
        "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1"
    ));
    static ref BYTES_VALUE3: H256 = H256::from(hex!(
        "977c084229c72a0fa377cae304eda9099b6a2cb5d83b25cdf0f0969b69874255"
    ));
    static ref SCALAR_ENTITY: Entity = {
        let mut entity = Entity::new();
        let strings = Value::from(
            vec!["left", "right", "middle"]
                .into_iter()
                .map(|s| Value::from(s))
                .collect::<Vec<_>>(),
        );
        let byte_array = Value::from(
            vec![*BYTES_VALUE, *BYTES_VALUE2, *BYTES_VALUE3]
                .into_iter()
                .map(|s| Value::from(s))
                .collect::<Vec<_>>(),
        );
        entity.set("id", "one");
        entity.set("bool", true);
        entity.set("int", std::i32::MAX);
        let decimal = (*LARGE_DECIMAL).clone();
        entity.set("bigDecimal", decimal.clone());
        entity.set(
            "bigDecimalArray",
            vec![decimal.clone(), (decimal + 1.into()).clone()],
        );
        entity.set("string", "scalar");
        entity.set("strings", strings);
        entity.set("bytes", *BYTES_VALUE);
        entity.set("byteArray", byte_array);
        let big_int = (*LARGE_INT).clone();
        entity.set("bigInt", big_int.clone());
        entity.set(
            "bigIntArray",
            vec![big_int.clone(), (big_int + 1.into()).clone()],
        );
        entity.set("color", "yellow");
        entity.set("__typename", "Scalar");
        entity
    };
    static ref EMPTY_NULLABLESTRINGS_ENTITY: Entity = {
        let mut entity = Entity::new();
        entity.set("id", "one");
        entity.set("__typename", "NullableStrings");
        entity
    };
    static ref SCALAR: EntityType = EntityType::from("Scalar");
    static ref NO_ENTITY: EntityType = EntityType::from("NoEntity");
    static ref NULLABLE_STRINGS: EntityType = EntityType::from("NullableStrings");
    static ref MOCK_STOPWATCH: StopwatchMetrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        THINGS_SUBGRAPH_ID.clone(),
        Arc::new(MockMetricsRegistry::new()),
    );
}

/// Removes test data from the database behind the store.
fn remove_test_data(conn: &PgConnection) {
    let query = format!("drop schema if exists {} cascade", NAMESPACE.as_str());
    conn.batch_execute(&query)
        .expect("Failed to drop test schema");
}

fn insert_entity(
    conn: &PgConnection,
    layout: &Layout,
    entity_type: &str,
    mut entities: Vec<Entity>,
) {
    let entities_with_keys_owned = entities
        .drain(..)
        .map(|entity| {
            let key = EntityKey::data(
                THINGS_SUBGRAPH_ID.clone(),
                entity_type.to_owned(),
                entity.id().unwrap(),
            );
            (key, entity)
        })
        .collect::<Vec<(EntityKey, Entity)>>();
    let mut entities_with_keys: Vec<_> = entities_with_keys_owned
        .iter()
        .map(|(key, entity)| (key, Cow::from(entity)))
        .collect();
    let entity_type = EntityType::from(entity_type);
    let errmsg = format!(
        "Failed to insert entities {}[{:?}]",
        entity_type, entities_with_keys
    );
    let inserted = layout
        .insert(
            &conn,
            &entity_type,
            &mut entities_with_keys,
            0,
            &MOCK_STOPWATCH,
        )
        .expect(&errmsg);
    assert_eq!(inserted, entities_with_keys_owned.len());
}

fn update_entity(
    conn: &PgConnection,
    layout: &Layout,
    entity_type: &str,
    mut entities: Vec<Entity>,
) {
    let entities_with_keys_owned: Vec<(EntityKey, Entity)> = entities
        .drain(..)
        .map(|entity| {
            let key = EntityKey::data(
                THINGS_SUBGRAPH_ID.clone(),
                entity_type.to_owned(),
                entity.id().unwrap(),
            );
            (key, entity)
        })
        .collect();
    let mut entities_with_keys: Vec<_> = entities_with_keys_owned
        .iter()
        .map(|(key, entity)| (key, Cow::from(entity)))
        .collect();

    let entity_type = EntityType::from(entity_type);
    let errmsg = format!(
        "Failed to insert entities {}[{:?}]",
        entity_type, entities_with_keys
    );

    let updated = layout
        .update(
            &conn,
            &entity_type,
            &mut entities_with_keys,
            0,
            &MOCK_STOPWATCH,
        )
        .expect(&errmsg);
    assert_eq!(updated, entities_with_keys_owned.len());
}

fn insert_user_entity(
    conn: &PgConnection,
    layout: &Layout,
    id: &str,
    entity_type: &str,
    name: &str,
    email: &str,
    age: i32,
    weight: f64,
    coffee: bool,
    favorite_color: Option<&str>,
    drinks: Option<Vec<&str>>,
) {
    let mut user = Entity::new();

    user.insert("id".to_owned(), Value::String(id.to_owned()));
    user.insert("name".to_owned(), Value::String(name.to_owned()));
    let bin_name = Bytes::from_str(&hex::encode(name)).unwrap();
    user.insert("bin_name".to_owned(), Value::Bytes(bin_name));
    user.insert("email".to_owned(), Value::String(email.to_owned()));
    user.insert("age".to_owned(), Value::Int(age));
    user.insert(
        "seconds_age".to_owned(),
        Value::BigInt(BigInt::from(age) * 31557600.into()),
    );
    user.insert("weight".to_owned(), Value::BigDecimal(weight.into()));
    user.insert("coffee".to_owned(), Value::Bool(coffee));
    user.insert(
        "favorite_color".to_owned(),
        favorite_color
            .map(|s| Value::String(s.to_owned()))
            .unwrap_or(Value::Null),
    );
    if let Some(drinks) = drinks {
        user.insert("drinks".to_owned(), drinks.into());
    }

    insert_entity(conn, layout, entity_type, vec![user]);
}

fn insert_users(conn: &PgConnection, layout: &Layout) {
    insert_user_entity(
        conn,
        layout,
        "1",
        "User",
        "Johnton",
        "tonofjohn@email.com",
        67 as i32,
        184.4,
        false,
        Some("yellow"),
        None,
    );
    insert_user_entity(
        conn,
        layout,
        "2",
        "User",
        "Cindini",
        "dinici@email.com",
        43 as i32,
        159.1,
        true,
        Some("red"),
        Some(vec!["beer", "wine"]),
    );
    insert_user_entity(
        conn,
        layout,
        "3",
        "User",
        "Shaqueeena",
        "teeko@email.com",
        28 as i32,
        111.7,
        false,
        None,
        Some(vec!["coffee", "tea"]),
    );
}

fn update_user_entity(
    conn: &PgConnection,
    layout: &Layout,
    id: &str,
    entity_type: &str,
    name: &str,
    email: &str,
    age: i32,
    weight: f64,
    coffee: bool,
    favorite_color: Option<&str>,
    drinks: Option<Vec<&str>>,
) {
    let mut user = Entity::new();

    user.insert("id".to_owned(), Value::String(id.to_owned()));
    user.insert("name".to_owned(), Value::String(name.to_owned()));
    let bin_name = Bytes::from_str(&hex::encode(name)).unwrap();
    user.insert("bin_name".to_owned(), Value::Bytes(bin_name));
    user.insert("email".to_owned(), Value::String(email.to_owned()));
    user.insert("age".to_owned(), Value::Int(age));
    user.insert(
        "seconds_age".to_owned(),
        Value::BigInt(BigInt::from(age) * 31557600.into()),
    );
    user.insert("weight".to_owned(), Value::BigDecimal(weight.into()));
    user.insert("coffee".to_owned(), Value::Bool(coffee));
    user.insert(
        "favorite_color".to_owned(),
        favorite_color
            .map(|s| Value::String(s.to_owned()))
            .unwrap_or(Value::Null),
    );
    if let Some(drinks) = drinks {
        user.insert("drinks".to_owned(), drinks.into());
    }

    update_entity(conn, layout, entity_type, vec![user]);
}

fn insert_pet(conn: &PgConnection, layout: &Layout, entity_type: &str, id: &str, name: &str) {
    let mut pet = Entity::new();
    pet.set("id", id);
    pet.set("name", name);
    insert_entity(conn, layout, entity_type, vec![pet]);
}

fn insert_pets(conn: &PgConnection, layout: &Layout) {
    insert_pet(conn, layout, "Dog", "pluto", "Pluto");
    insert_pet(conn, layout, "Cat", "garfield", "Garfield");
}

fn insert_test_data(conn: &PgConnection) -> Layout {
    let schema = Schema::parse(THINGS_GQL, THINGS_SUBGRAPH_ID.clone()).unwrap();
    let site = make_dummy_site(
        THINGS_SUBGRAPH_ID.clone(),
        NAMESPACE.clone(),
        NETWORK_NAME.to_string(),
    );
    let query = format!("create schema {}", NAMESPACE.as_str());
    conn.batch_execute(&*query).unwrap();

    Layout::create_relational_schema(&conn, Arc::new(site), &schema)
        .expect("Failed to create relational schema")
}

fn scrub(entity: &Entity) -> Entity {
    let mut scrubbed = Entity::new();
    // merge_remove_null_fields has the side-effect of removing any attribute
    // that is Value::Null
    scrubbed.merge_remove_null_fields(entity.clone());
    scrubbed
}

macro_rules! assert_entity_eq {
    ($left:expr, $right:expr) => {{
        let (left, right) = (&($left), &($right));
        let mut pass = true;

        for (key, left_value) in left.clone().sorted() {
            match right.get(&key) {
                None => {
                    pass = false;
                    println!("key '{}' missing from right", key);
                }
                Some(right_value) => {
                    if left_value != *right_value {
                        pass = false;
                        println!(
                            "values for '{}' differ:\n     left: {:?}\n    right: {:?}",
                            key, left_value, right_value
                        );
                    }
                }
            }
        }
        for (key, _) in right.clone().sorted() {
            if left.get(&key).is_none() {
                pass = false;
                println!("key '{}' missing from left", key);
            }
        }
        assert!(pass, "left and right entities are different");
    }};
}

/// Test harness for running database integration tests.
fn run_test<F>(test: F)
where
    F: FnOnce(&PgConnection, &Layout) -> (),
{
    run_test_with_conn(|conn| {
        // Reset state before starting
        remove_test_data(conn);

        // Seed database with test data
        let layout = insert_test_data(conn);

        // Run test
        test(conn, &layout);
    });
}

#[test]
fn find() {
    run_test(|conn, layout| {
        insert_entity(&conn, &layout, "Scalar", vec![SCALAR_ENTITY.clone()]);

        // Happy path: find existing entity
        let entity = layout
            .find(conn, &*SCALAR, "one", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[one]")
            .unwrap();
        assert_entity_eq!(scrub(&*SCALAR_ENTITY), entity);

        // Find non-existing entity
        let entity = layout
            .find(conn, &*SCALAR, "noone", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[noone]");
        assert!(entity.is_none());

        // Find for non-existing entity type
        let err = layout.find(conn, &*NO_ENTITY, "one", BLOCK_NUMBER_MAX);
        match err {
            Err(e) => assert_eq!("unknown table 'NoEntity'", e.to_string()),
            _ => {
                println!("{:?}", err);
                assert!(false)
            }
        }
    });
}

#[test]
fn insert_null_fulltext_fields() {
    run_test(|conn, layout| {
        insert_entity(
            &conn,
            &layout,
            "NullableStrings",
            vec![EMPTY_NULLABLESTRINGS_ENTITY.clone()],
        );

        // Find entity with null string values
        let entity = layout
            .find(conn, &*NULLABLE_STRINGS, "one", BLOCK_NUMBER_MAX)
            .expect("Failed to read NullableStrings[one]")
            .unwrap();
        assert_entity_eq!(scrub(&*EMPTY_NULLABLESTRINGS_ENTITY), entity);
    });
}

#[test]
fn update() {
    run_test(|conn, layout| {
        insert_entity(&conn, &layout, "Scalar", vec![SCALAR_ENTITY.clone()]);

        // Update with overwrite
        let mut entity = SCALAR_ENTITY.clone();
        entity.set("string", "updated");
        entity.remove("strings");
        entity.set("bool", Value::Null);
        let key = EntityKey::data(
            THINGS_SUBGRAPH_ID.clone(),
            "Scalar".to_owned(),
            entity.id().unwrap().clone(),
        );

        let entity_type = EntityType::from("Scalar");
        let mut entities = vec![(&key, Cow::from(&entity))];
        layout
            .update(&conn, &entity_type, &mut entities, 0, &MOCK_STOPWATCH)
            .expect("Failed to update");

        let actual = layout
            .find(conn, &*SCALAR, "one", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[one]")
            .unwrap();
        assert_entity_eq!(scrub(&entity), actual);
    });
}

#[test]
fn update_many() {
    run_test(|conn, layout| {
        let mut one = SCALAR_ENTITY.clone();
        let mut two = SCALAR_ENTITY.clone();
        two.set("id", "two");
        let mut three = SCALAR_ENTITY.clone();
        three.set("id", "three");
        insert_entity(
            &conn,
            &layout,
            "Scalar",
            vec![one.clone(), two.clone(), three.clone()],
        );

        // confidence test: there should be 3 scalar entities in store right now
        assert_eq!(3, count_scalar_entities(conn, layout));

        // update with overwrite
        one.set("string", "updated");
        one.remove("strings");

        two.set("string", "updated too");
        two.set("bool", false);

        three.set("string", "updated in a different way");
        three.remove("strings");
        three.set("color", "red");

        // generate keys
        let entity_type = EntityType::from("Scalar");
        let keys: Vec<EntityKey> = ["one", "two", "three"]
            .iter()
            .map(|id| {
                EntityKey::data(
                    THINGS_SUBGRAPH_ID.clone(),
                    "Scalar".to_owned(),
                    String::from(*id),
                )
            })
            .collect();

        let entities_vec = vec![one, two, three];
        let mut entities: Vec<(&EntityKey, Cow<'_, Entity>)> = keys
            .iter()
            .zip(entities_vec.iter().map(|e| Cow::Borrowed(e)))
            .collect();

        layout
            .update(&conn, &entity_type, &mut entities, 0, &MOCK_STOPWATCH)
            .expect("Failed to update");

        // check updates took effect
        let updated: Vec<Entity> = ["one", "two", "three"]
            .iter()
            .map(|id| {
                layout
                    .find(conn, &*SCALAR, id, BLOCK_NUMBER_MAX)
                    .expect(&format!("Failed to read Scalar[{}]", id))
                    .unwrap()
            })
            .collect();
        let new_one = &updated[0];
        let new_two = &updated[1];
        let new_three = &updated[2];

        // check they have the same id
        assert_eq!(new_one.get("id"), Some(&Value::String("one".to_string())));
        assert_eq!(new_two.get("id"), Some(&Value::String("two".to_string())));
        assert_eq!(
            new_three.get("id"),
            Some(&Value::String("three".to_string()))
        );

        // check their fields got updated as expected
        assert_eq!(
            new_one.get("string"),
            Some(&Value::String("updated".to_string()))
        );
        assert_eq!(new_one.get("strings"), None);
        assert_eq!(
            new_two.get("string"),
            Some(&Value::String("updated too".to_string()))
        );
        assert_eq!(new_two.get("bool"), Some(&Value::Bool(false)));
        assert_eq!(
            new_three.get("string"),
            Some(&Value::String("updated in a different way".to_string()))
        );
        assert_eq!(
            new_three.get("color"),
            Some(&Value::String("red".to_string()))
        );
    });
}

/// Test that we properly handle BigDecimal values with a negative scale.
#[test]
fn serialize_bigdecimal() {
    run_test(|conn, layout| {
        insert_entity(&conn, &layout, "Scalar", vec![SCALAR_ENTITY.clone()]);

        // Update with overwrite
        let mut entity = SCALAR_ENTITY.clone();

        for d in &["50", "50.00", "5000", "0.5000", "0.050", "0.5", "0.05"] {
            let d = BigDecimal::from_str(d).unwrap();
            entity.set("bigDecimal", d);

            let key = EntityKey::data(
                THINGS_SUBGRAPH_ID.clone(),
                "Scalar".to_owned(),
                entity.id().unwrap().clone(),
            );
            let entity_type = EntityType::from("Scalar");
            let mut entities = vec![(&key, Cow::Borrowed(&entity))];
            layout
                .update(
                    &conn,
                    &entity_type,
                    entities.as_mut_slice(),
                    0,
                    &MOCK_STOPWATCH,
                )
                .expect("Failed to update");

            let actual = layout
                .find(conn, &*SCALAR, "one", BLOCK_NUMBER_MAX)
                .expect("Failed to read Scalar[one]")
                .unwrap();
            assert_entity_eq!(entity, actual);
        }
    });
}

fn count_scalar_entities(conn: &PgConnection, layout: &Layout) -> usize {
    let filter = EntityFilter::Or(vec![
        EntityFilter::Equal("bool".into(), true.into()),
        EntityFilter::Equal("bool".into(), false.into()),
    ]);
    let collection = EntityCollection::All(vec![(SCALAR.to_owned(), AttributeNames::All)]);
    layout
        .query::<Entity>(
            &*LOGGER,
            &conn,
            collection,
            Some(filter),
            EntityOrder::Default,
            EntityRange {
                first: None,
                skip: 0,
            },
            BLOCK_NUMBER_MAX,
            None,
        )
        .expect("Count query failed")
        .len()
}

#[test]
fn delete() {
    run_test(|conn, layout| {
        insert_entity(&conn, &layout, "Scalar", vec![SCALAR_ENTITY.clone()]);
        let mut two = SCALAR_ENTITY.clone();
        two.set("id", "two");
        insert_entity(&conn, &layout, "Scalar", vec![two]);

        // Delete where nothing is getting deleted
        let key = EntityKey::data(
            THINGS_SUBGRAPH_ID.clone(),
            "Scalar".to_owned(),
            "no such entity".to_owned(),
        );
        let entity_type = EntityType::from("Scalar");
        let mut entity_keys = vec![key.entity_id.as_str()];
        let count = layout
            .delete(
                &conn,
                &entity_type.clone(),
                &entity_keys,
                1,
                &MOCK_STOPWATCH,
            )
            .expect("Failed to delete");
        assert_eq!(0, count);
        assert_eq!(2, count_scalar_entities(conn, layout));

        // Delete entity two
        entity_keys
            .get_mut(0)
            .map(|key| *key = "two")
            .expect("Failed to update key");

        let count = layout
            .delete(&conn, &entity_type, &entity_keys, 1, &MOCK_STOPWATCH)
            .expect("Failed to delete");
        assert_eq!(1, count);
        assert_eq!(1, count_scalar_entities(conn, layout));
    });
}

#[test]
fn insert_many_and_delete_many() {
    run_test(|conn, layout| {
        let one = SCALAR_ENTITY.clone();
        let mut two = SCALAR_ENTITY.clone();
        two.set("id", "two");
        let mut three = SCALAR_ENTITY.clone();
        three.set("id", "three");
        insert_entity(&conn, &layout, "Scalar", vec![one, two, three]);

        // confidence test: there should be 3 scalar entities in store right now
        assert_eq!(3, count_scalar_entities(conn, layout));

        // Delete entities with ids equal to "two" and "three"
        let entity_type = EntityType::from("Scalar");
        let entity_keys = vec!["two", "three"];
        let num_removed = layout
            .delete(&conn, &entity_type, &entity_keys, 1, &MOCK_STOPWATCH)
            .expect("Failed to delete");
        assert_eq!(2, num_removed);
        assert_eq!(1, count_scalar_entities(conn, layout));
    });
}

#[tokio::test]
async fn layout_cache() {
    run_test_with_conn(|conn| {
        let id = DeploymentHash::new("primaryLayoutCache").unwrap();
        let _loc = create_test_subgraph(&id, THINGS_GQL);
        let site = Arc::new(primary_mirror().find_active_site(&id).unwrap().unwrap());
        let table_name = SqlName::verbatim("scalar".to_string());

        let cache = LayoutCache::new(Duration::from_millis(10));

        // Without an entry, account_like is false
        let layout = cache
            .get(&*LOGGER, &conn, site.clone())
            .expect("we can get the layout");
        let table = layout.table(&table_name).unwrap();
        assert_eq!(false, table.is_account_like);

        set_account_like(conn, site.as_ref(), &table_name, true)
            .expect("we can set 'scalar' to account-like");
        sleep(Duration::from_millis(50));

        // Flip account_like to true
        let layout = cache
            .get(&*LOGGER, &conn, site.clone())
            .expect("we can get the layout");
        let table = layout.table(&table_name).unwrap();
        assert_eq!(true, table.is_account_like);

        // Set it back to false
        set_account_like(conn, site.as_ref(), &table_name, false)
            .expect("we can set 'scalar' to account-like");
        sleep(Duration::from_millis(50));

        let layout = cache
            .get(&*LOGGER, &conn, site.clone())
            .expect("we can get the layout");
        let table = layout.table(&table_name).unwrap();
        assert_eq!(false, table.is_account_like);
    })
}

#[test]
fn conflicting_entity() {
    run_test(|conn, layout| {
        let id = "fred";
        let cat = EntityType::from("Cat");
        let dog = EntityType::from("Dog");
        let ferret = EntityType::from("Ferret");

        let mut fred = Entity::new();
        fred.set("id", id);
        fred.set("name", id);
        insert_entity(&conn, &layout, "Cat", vec![fred]);

        // If we wanted to create Fred the dog, which is forbidden, we'd run this:
        let conflict = layout
            .conflicting_entity(&conn, &id.to_owned(), vec![cat.clone(), ferret.clone()])
            .unwrap();
        assert_eq!(Some("Cat".to_owned()), conflict);

        // If we wanted to manipulate Fred the cat, which is ok, we'd run:
        let conflict = layout
            .conflicting_entity(&conn, &id.to_owned(), vec![dog.clone(), ferret.clone()])
            .unwrap();
        assert_eq!(None, conflict);

        // Chairs are not pets
        let chair = EntityType::from("Chair");
        let result = layout.conflicting_entity(
            &conn,
            &id.to_owned(),
            vec![dog.clone(), ferret.clone(), chair.clone()],
        );
        assert!(result.is_err());
        assert_eq!("unknown table 'Chair'", result.err().unwrap().to_string());
    })
}

struct QueryChecker<'a> {
    conn: &'a PgConnection,
    layout: &'a Layout,
}

impl<'a> QueryChecker<'a> {
    fn new(conn: &'a PgConnection, layout: &'a Layout) -> Self {
        insert_users(conn, layout);
        update_user_entity(
            conn,
            layout,
            "1",
            "User",
            "Jono",
            "achangedemail@email.com",
            67 as i32,
            184.4,
            false,
            Some("yellow"),
            None,
        );
        insert_pets(conn, layout);

        Self { conn, layout }
    }

    fn check(self, expected_entity_ids: Vec<&'static str>, query: EntityQuery) -> Self {
        let unordered = matches!(query.order, EntityOrder::Unordered);
        let entities = self
            .layout
            .query::<Entity>(
                &*LOGGER,
                self.conn,
                query.collection,
                query.filter,
                query.order,
                query.range,
                BLOCK_NUMBER_MAX,
                None,
            )
            .expect("layout.query failed to execute query");

        let mut entity_ids: Vec<_> = entities
            .into_iter()
            .map(|entity| match entity.get("id") {
                Some(Value::String(id)) => id.to_owned(),
                Some(_) => panic!("layout.query returned entity with non-string ID attribute"),
                None => panic!("layout.query returned entity with no ID attribute"),
            })
            .collect();

        let mut expected_entity_ids: Vec<String> =
            expected_entity_ids.into_iter().map(str::to_owned).collect();

        if unordered {
            entity_ids.sort();
            expected_entity_ids.sort();
        }

        assert_eq!(entity_ids, expected_entity_ids);
        self
    }
}

fn query(entity_types: Vec<&str>) -> EntityQuery {
    EntityQuery::new(
        THINGS_SUBGRAPH_ID.clone(),
        BLOCK_NUMBER_MAX,
        EntityCollection::All(
            entity_types
                .into_iter()
                .map(|entity_type| (EntityType::from(entity_type), AttributeNames::All))
                .collect(),
        ),
    )
}

fn user_query() -> EntityQuery {
    query(vec!["User"])
}

trait EasyOrder {
    fn asc(self, attr: &str) -> Self;
    fn desc(self, attr: &str) -> Self;
    fn unordered(self) -> Self;
}

impl EasyOrder for EntityQuery {
    fn asc(self, attr: &str) -> Self {
        // The ValueType doesn't matter since relational layouts ignore it
        self.order(EntityOrder::Ascending(attr.to_owned(), ValueType::String))
    }

    fn desc(self, attr: &str) -> Self {
        // The ValueType doesn't matter since relational layouts ignore it
        self.order(EntityOrder::Descending(attr.to_owned(), ValueType::String))
    }

    fn unordered(self) -> Self {
        self.order(EntityOrder::Unordered)
    }
}

#[test]
fn check_find() {
    run_test(move |conn, layout| {
        // find with interfaces
        let checker = QueryChecker::new(conn, layout)
            .check(vec!["garfield", "pluto"], query(vec!["Cat", "Dog"]))
            .check(
                vec!["pluto", "garfield"],
                query(vec!["Cat", "Dog"]).desc("name"),
            )
            .check(
                vec!["garfield"],
                query(vec!["Cat", "Dog"])
                    .filter(EntityFilter::StartsWith("name".into(), Value::from("Gar")))
                    .desc("name"),
            )
            .check(
                vec!["pluto", "garfield"],
                query(vec!["Cat", "Dog"]).desc("id"),
            )
            .check(
                vec!["garfield", "pluto"],
                query(vec!["Cat", "Dog"]).asc("id"),
            )
            .check(
                vec!["garfield", "pluto"],
                query(vec!["Cat", "Dog"]).unordered(),
            );

        // fulltext
        let checker = checker
            .check(
                vec!["3"],
                user_query().filter(EntityFilter::Equal("userSearch".into(), "Shaq:*".into())),
            )
            .check(
                vec!["1"],
                user_query().filter(EntityFilter::Equal(
                    "userSearch".into(),
                    "Jono & achangedemail@email.com".into(),
                )),
            );

        // list contains
        fn drinks_query(v: Vec<&str>) -> EntityQuery {
            let drinks: Option<Value> = Some(v.into());
            user_query().filter(EntityFilter::Contains("drinks".into(), drinks.into()))
        }

        let checker = checker
            .check(vec!["2"], drinks_query(vec!["beer"]))
            // Reverse of how we stored it
            .check(vec!["3"], drinks_query(vec!["tea", "coffee"]))
            .check(vec![], drinks_query(vec!["beer", "tea"]))
            .check(vec![], drinks_query(vec!["beer", "water"]))
            .check(vec![], drinks_query(vec!["beer", "wine", "water"]));

        // list not contains
        let checker = checker
            // User 3 do not have "beer" on its drinks list.
            .check(
                vec!["3"],
                user_query().filter(EntityFilter::NotContains(
                    "drinks".into(),
                    vec!["beer"].into(),
                )),
            )
            // Users 2 do not have "tea" on its drinks list.
            .check(
                vec!["2"],
                user_query().filter(EntityFilter::NotContains(
                    "drinks".into(),
                    vec!["tea"].into(),
                )),
            );

        // string attributes
        let checker = checker
            .check(
                vec!["2"],
                user_query().filter(EntityFilter::Contains("name".into(), "ind".into())),
            )
            .check(
                vec!["2"],
                user_query().filter(EntityFilter::Equal("name".to_owned(), "Cindini".into())),
            )
            // Test that we can order by id
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::Equal("name".to_owned(), "Cindini".into()))
                    .desc("id"),
            )
            .check(
                vec!["1", "3"],
                user_query()
                    .filter(EntityFilter::Not("name".to_owned(), "Cindini".into()))
                    .asc("name"),
            )
            .check(
                vec!["3"],
                user_query().filter(EntityFilter::GreaterThan("name".to_owned(), "Kundi".into())),
            )
            .check(
                vec!["2", "1"],
                user_query()
                    .filter(EntityFilter::LessThan("name".to_owned(), "Kundi".into()))
                    .asc("name"),
            )
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::LessThan("name".to_owned(), "Kundi".into()))
                    .desc("name"),
            )
            .check(
                vec!["1"],
                user_query()
                    .filter(EntityFilter::LessThan("name".to_owned(), "ZZZ".into()))
                    .desc("name")
                    .first(1)
                    .skip(1),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::And(vec![
                        EntityFilter::LessThan("name".to_owned(), "Cz".into()),
                        EntityFilter::Equal("name".to_owned(), "Cindini".into()),
                    ]))
                    .desc("name"),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::EndsWith("name".to_owned(), "ini".into()))
                    .desc("name"),
            )
            .check(
                vec!["3", "1"],
                user_query()
                    .filter(EntityFilter::NotEndsWith("name".to_owned(), "ini".into()))
                    .desc("name"),
            )
            .check(
                vec!["1"],
                user_query()
                    .filter(EntityFilter::In(
                        "name".to_owned(),
                        vec!["Jono".into(), "Nobody".into(), "Still nobody".into()],
                    ))
                    .desc("name"),
            )
            .check(
                vec![],
                user_query().filter(EntityFilter::In("name".to_owned(), vec![])),
            )
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "name".to_owned(),
                        vec!["Shaqueeena".into()],
                    ))
                    .desc("name"),
            );
        // float attributes
        let checker = checker
            .check(
                vec!["1"],
                user_query().filter(EntityFilter::Equal(
                    "weight".to_owned(),
                    Value::BigDecimal(184.4.into()),
                )),
            )
            .check(
                vec!["3", "2"],
                user_query()
                    .filter(EntityFilter::Not(
                        "weight".to_owned(),
                        Value::BigDecimal(184.4.into()),
                    ))
                    .desc("name"),
            )
            .check(
                vec!["1"],
                user_query().filter(EntityFilter::GreaterThan(
                    "weight".to_owned(),
                    Value::BigDecimal(160.0.into()),
                )),
            )
            .check(
                vec!["2", "3"],
                user_query()
                    .filter(EntityFilter::LessThan(
                        "weight".to_owned(),
                        Value::BigDecimal(160.0.into()),
                    ))
                    .asc("name"),
            )
            .check(
                vec!["3", "2"],
                user_query()
                    .filter(EntityFilter::LessThan(
                        "weight".to_owned(),
                        Value::BigDecimal(160.0.into()),
                    ))
                    .desc("name"),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::LessThan(
                        "weight".to_owned(),
                        Value::BigDecimal(161.0.into()),
                    ))
                    .desc("name")
                    .first(1)
                    .skip(1),
            )
            .check(
                vec!["3", "1"],
                user_query()
                    .filter(EntityFilter::In(
                        "weight".to_owned(),
                        vec![
                            Value::BigDecimal(184.4.into()),
                            Value::BigDecimal(111.7.into()),
                        ],
                    ))
                    .desc("name")
                    .first(5),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "weight".to_owned(),
                        vec![
                            Value::BigDecimal(184.4.into()),
                            Value::BigDecimal(111.7.into()),
                        ],
                    ))
                    .desc("name")
                    .first(5),
            );

        // int attributes
        let checker = checker
            .check(
                vec!["1"],
                user_query()
                    .filter(EntityFilter::Equal("age".to_owned(), Value::Int(67 as i32)))
                    .desc("name"),
            )
            .check(
                vec!["3", "2"],
                user_query()
                    .filter(EntityFilter::Not("age".to_owned(), Value::Int(67 as i32)))
                    .desc("name"),
            )
            .check(
                vec!["1"],
                user_query().filter(EntityFilter::GreaterThan(
                    "age".to_owned(),
                    Value::Int(43 as i32),
                )),
            )
            .check(
                vec!["2", "1"],
                user_query()
                    .filter(EntityFilter::GreaterOrEqual(
                        "age".to_owned(),
                        Value::Int(43 as i32),
                    ))
                    .asc("name"),
            )
            .check(
                vec!["2", "3"],
                user_query()
                    .filter(EntityFilter::LessThan(
                        "age".to_owned(),
                        Value::Int(50 as i32),
                    ))
                    .asc("name"),
            )
            .check(
                vec!["2", "3"],
                user_query()
                    .filter(EntityFilter::LessOrEqual(
                        "age".to_owned(),
                        Value::Int(43 as i32),
                    ))
                    .asc("name"),
            )
            .check(
                vec!["3", "2"],
                user_query()
                    .filter(EntityFilter::LessThan(
                        "age".to_owned(),
                        Value::Int(50 as i32),
                    ))
                    .desc("name"),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::LessThan(
                        "age".to_owned(),
                        Value::Int(67 as i32),
                    ))
                    .desc("name")
                    .first(1)
                    .skip(1),
            )
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::In(
                        "age".to_owned(),
                        vec![Value::Int(67 as i32), Value::Int(43 as i32)],
                    ))
                    .desc("name")
                    .first(5),
            )
            .check(
                vec!["3"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "age".to_owned(),
                        vec![Value::Int(67 as i32), Value::Int(43 as i32)],
                    ))
                    .desc("name")
                    .first(5),
            );

        // bool attributes
        let checker = checker
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::Equal("coffee".to_owned(), Value::Bool(true)))
                    .desc("name"),
            )
            .check(
                vec!["1", "3"],
                user_query()
                    .filter(EntityFilter::Not("coffee".to_owned(), Value::Bool(true)))
                    .asc("name"),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::In(
                        "coffee".to_owned(),
                        vec![Value::Bool(true)],
                    ))
                    .desc("name")
                    .first(5),
            )
            .check(
                vec!["3", "1"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "coffee".to_owned(),
                        vec![Value::Bool(true)],
                    ))
                    .desc("name")
                    .first(5),
            );
        // misc tests
        let checker = checker
            .check(
                vec!["1"],
                user_query()
                    .filter(EntityFilter::Equal(
                        "bin_name".to_owned(),
                        Value::Bytes("Jono".as_bytes().into()),
                    ))
                    .desc("name"),
            )
            .check(
                vec!["3"],
                user_query()
                    .filter(EntityFilter::Equal(
                        "favorite_color".to_owned(),
                        Value::Null,
                    ))
                    .desc("name"),
            )
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::Not("favorite_color".to_owned(), Value::Null))
                    .desc("name"),
            )
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "favorite_color".to_owned(),
                        vec![Value::Null],
                    ))
                    .desc("name"),
            )
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "favorite_color".to_owned(),
                        vec!["red".into(), Value::Null],
                    ))
                    .desc("name"),
            )
            .check(vec!["3", "2", "1"], user_query().asc("weight"))
            .check(vec!["1", "2", "3"], user_query().desc("weight"))
            .check(vec!["1", "2", "3"], user_query().unordered())
            .check(vec!["1", "2", "3"], user_query().asc("id"))
            .check(vec!["3", "2", "1"], user_query().desc("id"))
            .check(vec!["1", "2", "3"], user_query().unordered())
            .check(vec!["3", "2", "1"], user_query().asc("age"))
            .check(vec!["1", "2", "3"], user_query().desc("age"))
            .check(vec!["2", "1", "3"], user_query().asc("name"))
            .check(vec!["3", "1", "2"], user_query().desc("name"))
            .check(
                vec!["1", "2"],
                user_query()
                    .filter(EntityFilter::And(vec![EntityFilter::Or(vec![
                        EntityFilter::Equal("id".to_owned(), Value::from("1")),
                        EntityFilter::Equal("id".to_owned(), Value::from("2")),
                    ])]))
                    .asc("id"),
            );

        // enum attributes
        let checker = checker
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::Equal(
                        "favorite_color".to_owned(),
                        "red".into(),
                    ))
                    .desc("name"),
            )
            .check(
                vec!["1"],
                user_query()
                    .filter(EntityFilter::Not("favorite_color".to_owned(), "red".into()))
                    .asc("name"),
            )
            .check(
                vec!["2"],
                user_query()
                    .filter(EntityFilter::In(
                        "favorite_color".to_owned(),
                        vec!["red".into()],
                    ))
                    .desc("name")
                    .first(5),
            )
            .check(
                vec!["1"],
                user_query()
                    .filter(EntityFilter::NotIn(
                        "favorite_color".to_owned(),
                        vec!["red".into()],
                    ))
                    .desc("name")
                    .first(5),
            );

        // empty and / or

        // It's somewhat arbitrary that we define empty 'or' and 'and' to
        // be 'true' and 'false'; it's mostly this way since that's what the
        // JSONB storage filters do

        checker
            // An empty 'or' is 'false'
            .check(
                vec![],
                user_query().filter(EntityFilter::And(vec![EntityFilter::Or(vec![])])),
            )
            // An empty 'and' is 'true'
            .check(
                vec!["1", "2", "3"],
                user_query().filter(EntityFilter::Or(vec![EntityFilter::And(vec![])])),
            );
    })
}

// We call our test strings aN so that
//   aN = "a" * (STRING_PREFIX_SIZE - 2 + N)
// chosen so that they straddle the boundary between strings that fit into
// the index, and strings that have only a prefix in the index
// Return (a1, a2, a2b, a3)
// Note that that is the order for these ids, though the
// underlying strings are in the order a1 < a2 < a3 < a2b
fn ferrets() -> (String, String, String, String) {
    (
        "a".repeat(STRING_PREFIX_SIZE - 1),
        "a".repeat(STRING_PREFIX_SIZE),
        format!("{}b", "a".repeat(STRING_PREFIX_SIZE)),
        "a".repeat(STRING_PREFIX_SIZE + 1),
    )
}

fn text_find(expected_entity_ids: Vec<&str>, filter: EntityFilter) {
    let expected_entity_ids: Vec<String> =
        expected_entity_ids.into_iter().map(str::to_owned).collect();

    run_test(move |conn, layout| {
        let (a1, a2, a2b, a3) = ferrets();
        insert_pet(conn, layout, "Ferret", "a1", &a1);
        insert_pet(conn, layout, "Ferret", "a2", &a2);
        insert_pet(conn, layout, "Ferret", "a2b", &a2b);
        insert_pet(conn, layout, "Ferret", "a3", &a3);

        let query = query(vec!["Ferret"]).filter(filter).asc("id");

        let entities = layout
            .query::<Entity>(
                &*LOGGER,
                conn,
                query.collection,
                query.filter,
                query.order,
                query.range,
                BLOCK_NUMBER_MAX,
                None,
            )
            .expect("layout.query failed to execute query");

        let entity_ids: Vec<_> = entities
            .into_iter()
            .map(|entity| match entity.get("id") {
                Some(Value::String(id)) => id.to_owned(),
                Some(_) => panic!("layout.query returned entity with non-string ID attribute"),
                None => panic!("layout.query returned entity with no ID attribute"),
            })
            .collect();

        assert_eq!(expected_entity_ids, entity_ids);
    })
}

#[test]
fn text_equal() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(name: String) -> EntityFilter {
        EntityFilter::Equal("name".to_owned(), name.into())
    }
    text_find(vec!["a1"], filter(a1));
    text_find(vec!["a2"], filter(a2));
    text_find(vec!["a2b"], filter(a2b));
    text_find(vec!["a3"], filter(a3));
}

#[test]
fn text_not_equal() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(name: String) -> EntityFilter {
        EntityFilter::Not("name".to_owned(), name.into())
    }
    text_find(vec!["a2", "a2b", "a3"], filter(a1));
    text_find(vec!["a1", "a2b", "a3"], filter(a2));
    text_find(vec!["a1", "a2", "a3"], filter(a2b));
    text_find(vec!["a1", "a2", "a2b"], filter(a3));
}

#[test]
fn text_less_than() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(name: String) -> EntityFilter {
        EntityFilter::LessThan("name".to_owned(), name.into())
    }
    text_find(vec![], filter(a1));
    text_find(vec!["a1"], filter(a2));
    text_find(vec!["a1", "a2", "a3"], filter(a2b));
    text_find(vec!["a1", "a2"], filter(a3));
}

#[test]
fn text_less_or_equal() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(name: String) -> EntityFilter {
        EntityFilter::LessOrEqual("name".to_owned(), name.into())
    }
    text_find(vec!["a1"], filter(a1));
    text_find(vec!["a1", "a2"], filter(a2));
    text_find(vec!["a1", "a2", "a2b", "a3"], filter(a2b));
    text_find(vec!["a1", "a2", "a3"], filter(a3));
}

#[test]
fn text_greater_than() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(name: String) -> EntityFilter {
        EntityFilter::GreaterThan("name".to_owned(), name.into())
    }
    text_find(vec!["a2", "a2b", "a3"], filter(a1));
    text_find(vec!["a2b", "a3"], filter(a2));
    text_find(vec![], filter(a2b));
    text_find(vec!["a2b"], filter(a3));
}

#[test]
fn text_greater_or_equal() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(name: String) -> EntityFilter {
        EntityFilter::GreaterOrEqual("name".to_owned(), name.into())
    }
    text_find(vec!["a1", "a2", "a2b", "a3"], filter(a1));
    text_find(vec!["a2", "a2b", "a3"], filter(a2));
    text_find(vec!["a2b"], filter(a2b));
    text_find(vec!["a2b", "a3"], filter(a3));
}

#[test]
fn text_in() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(names: Vec<&str>) -> EntityFilter {
        EntityFilter::In(
            "name".to_owned(),
            names
                .into_iter()
                .map(|name| Value::from(name.to_owned()))
                .collect(),
        )
    }

    text_find(vec!["a1"], filter(vec![&a1]));
    text_find(vec!["a2"], filter(vec![&a2]));
    text_find(vec!["a2b"], filter(vec![&a2b]));
    text_find(vec!["a3"], filter(vec![&a3]));
    text_find(vec!["a1", "a2"], filter(vec![&a1, &a2]));
    text_find(vec!["a1", "a3"], filter(vec![&a1, &a3]));
}

#[test]
fn text_not_in() {
    let (a1, a2, a2b, a3) = ferrets();
    fn filter(names: Vec<&str>) -> EntityFilter {
        EntityFilter::NotIn(
            "name".to_owned(),
            names
                .into_iter()
                .map(|name| Value::from(name.to_owned()))
                .collect(),
        )
    }

    text_find(vec!["a2", "a2b", "a3"], filter(vec![&a1]));
    text_find(vec!["a1", "a2b", "a3"], filter(vec![&a2]));
    text_find(vec!["a1", "a2", "a3"], filter(vec![&a2b]));
    text_find(vec!["a1", "a2", "a2b"], filter(vec![&a3]));
    text_find(vec!["a2b", "a3"], filter(vec![&a1, &a2]));
    text_find(vec!["a2", "a2b"], filter(vec![&a1, &a3]));
}

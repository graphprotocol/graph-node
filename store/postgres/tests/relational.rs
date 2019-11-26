//! Test mapping of GraphQL schema to a relational schema
use diesel::connection::SimpleConnection as _;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use futures::future::{self, IntoFuture};
use hex_literal::hex;
use lazy_static::lazy_static;
use std::fmt::Debug;
use std::str::FromStr;

use graph::data::store::scalar::{BigDecimal, BigInt, Bytes};
use graph::prelude::{
    bigdecimal::One, web3::types::H256, Entity, EntityFilter, EntityKey, EntityOrder, EntityQuery,
    EntityRange, Schema, SubgraphDeploymentId, Value, ValueType,
};
use graph_store_postgres::layout_for_tests::{Layout, BLOCK_NUMBER_MAX, STRING_PREFIX_SIZE};

use test_store::*;

const THINGS_GQL: &str = "
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
        string: String,
        strings: [String!],
        bytes: Bytes,
        byteArray: [Bytes!],
        bigInt: BigInt,
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
";

const SCHEMA_NAME: &str = "layout";

lazy_static! {
    static ref THINGS_SUBGRAPH_ID: SubgraphDeploymentId =
        SubgraphDeploymentId::new("things").unwrap();
    static ref LARGE_INT: BigInt = BigInt::from(std::i64::MAX).pow(17);
    static ref LARGE_DECIMAL: BigDecimal =
        BigDecimal::one() / LARGE_INT.clone().to_big_decimal(BigInt::from(1));
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
        entity.set("bigDecimal", (*LARGE_DECIMAL).clone());
        entity.set("string", "scalar");
        entity.set("strings", strings);
        entity.set("bytes", (*BYTES_VALUE).clone());
        entity.set("byteArray", byte_array);
        entity.set("bigInt", (*LARGE_INT).clone());
        entity.set("color", "yellow");
        entity.set("__typename", "Scalar");
        entity
    };
}

/// Removes test data from the database behind the store.
fn remove_test_data(conn: &PgConnection) {
    let query = format!("drop schema if exists {} cascade", SCHEMA_NAME);
    conn.batch_execute(&query)
        .expect("Failed to drop test schema");
}

fn insert_entity(conn: &PgConnection, layout: &Layout, entity_type: &str, entity: Entity) {
    let key = EntityKey {
        subgraph_id: THINGS_SUBGRAPH_ID.clone(),
        entity_type: entity_type.to_owned(),
        entity_id: entity.id().unwrap(),
    };
    let errmsg = format!("Failed to insert entity {}[{}]", entity_type, key.entity_id);
    layout.insert(&conn, &key, &entity, 0).expect(&errmsg);
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
        user.insert(
            "drinks".to_owned(),
            drinks
                .into_iter()
                .map(|drink| drink.into())
                .collect::<Vec<Value>>()
                .into(),
        );
    }

    insert_entity(conn, layout, entity_type, user);
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

fn insert_pet(conn: &PgConnection, layout: &Layout, entity_type: &str, id: &str, name: &str) {
    let mut pet = Entity::new();
    pet.set("id", id);
    pet.set("name", name);
    insert_entity(conn, layout, entity_type, pet);
}

fn insert_pets(conn: &PgConnection, layout: &Layout) {
    insert_pet(conn, layout, "Dog", "pluto", "Pluto");
    insert_pet(conn, layout, "Cat", "garfield", "Garfield");
}

fn insert_test_data(conn: &PgConnection) -> Layout {
    let schema = Schema::parse(THINGS_GQL, THINGS_SUBGRAPH_ID.clone()).unwrap();

    let query = format!("create schema {}", SCHEMA_NAME);
    conn.batch_execute(&*query).unwrap();

    let layout = Layout::create_relational_schema(
        &conn,
        SCHEMA_NAME,
        THINGS_SUBGRAPH_ID.clone(),
        &schema.document,
    )
    .expect("Failed to create relational schema");

    layout
}

fn scrub(entity: &Entity) -> Entity {
    let mut scrubbed = Entity::new();
    // merge has the sideffect of removing any attribute
    // that is Value::Null
    scrubbed.merge(entity.clone());
    scrubbed
}

macro_rules! assert_entity_eq {
    ($left:expr, $right:expr) => {{
        let (left, right) = (&($left), &($right));
        let mut pass = true;

        for (key, left_value) in left.iter() {
            match right.get(key) {
                None => {
                    pass = false;
                    println!("key '{}' missing from right", key);
                }
                Some(right_value) => {
                    if left_value != right_value {
                        pass = false;
                        println!(
                            "values for '{}' differ:\n     left: {:?}\n    right: {:?}",
                            key, left_value, right_value
                        );
                    }
                }
            }
        }
        for key in right.keys() {
            if left.get(key).is_none() {
                pass = false;
                println!("key '{}' missing from left", key);
            }
        }
        assert!(pass, "left and right entities are different");
    }};
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(&PgConnection, &Layout) -> R + Send + 'static,
    R: IntoFuture<Item = ()> + Send + 'static,
    R::Error: Send + Debug,
    R::Future: Send,
{
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");

    // Lock regardless of poisoning. This also forces sequential test execution.
    let mut runtime = match STORE_RUNTIME.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    runtime
        .block_on(future::lazy(move || {
            // Reset state before starting
            remove_test_data(&conn);

            // Seed database with test data
            let layout = insert_test_data(&conn);

            // Run test
            test(&conn, &layout)
        }))
        .expect("Failed to run ChainHead test");
}

#[test]
fn find() {
    run_test(|conn, layout| -> Result<(), ()> {
        insert_entity(&conn, &layout, "Scalar", SCALAR_ENTITY.clone());

        // Happy path: find existing entity
        let entity = layout
            .find(conn, "Scalar", "one", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[one]")
            .unwrap();
        assert_entity_eq!(scrub(&*SCALAR_ENTITY), entity);

        // Find non-existing entity
        let entity = layout
            .find(conn, "Scalar", "noone", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[noone]");
        assert!(entity.is_none());

        // Find for non-existing entity type
        let err = layout.find(conn, "NoEntity", "one", BLOCK_NUMBER_MAX);
        match err {
            Err(e) => assert_eq!("unknown table 'NoEntity'", e.to_string()),
            _ => {
                println!("{:?}", err);
                assert!(false)
            }
        }
        Ok(())
    });
}

#[test]
fn update() {
    run_test(|conn, layout| -> Result<(), ()> {
        insert_entity(&conn, &layout, "Scalar", SCALAR_ENTITY.clone());

        // Update with overwrite
        let mut entity = SCALAR_ENTITY.clone();
        entity.set("string", "updated");
        entity.remove("strings");
        entity.set("bool", Value::Null);
        let key = EntityKey {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_type: "Scalar".to_owned(),
            entity_id: entity.id().unwrap().clone(),
        };
        layout
            .update(&conn, &key, &entity, 1)
            .expect("Failed to update");

        // The missing 'strings' will show up as Value::Null in the
        // loaded entity
        entity.set("strings", Value::Null);

        let actual = layout
            .find(conn, "Scalar", "one", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[one]")
            .unwrap();
        assert_entity_eq!(scrub(&entity), actual);
        Ok(())
    });
}

/// Test that we properly handle BigDecimal values with a negative scale.
#[test]
fn serialize_bigdecimal() {
    run_test(|conn, layout| -> Result<(), ()> {
        insert_entity(&conn, &layout, "Scalar", SCALAR_ENTITY.clone());

        // Update with overwrite
        let mut entity = SCALAR_ENTITY.clone();

        let d = BigDecimal::from_str("5000").unwrap();
        let d = d.with_scale(-2);
        entity.set("bigDecimal", d);

        let key = EntityKey {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_type: "Scalar".to_owned(),
            entity_id: entity.id().unwrap().clone(),
        };
        layout
            .update(&conn, &key, &entity, 1)
            .expect("Failed to update");

        let actual = layout
            .find(conn, "Scalar", "one", BLOCK_NUMBER_MAX)
            .expect("Failed to read Scalar[one]")
            .unwrap();
        assert_entity_eq!(&entity, actual);
        Ok(())
    });
}

fn count_scalar_entities(conn: &PgConnection, layout: &Layout) -> usize {
    let filter = EntityFilter::Or(vec![
        EntityFilter::Equal("bool".into(), true.into()),
        EntityFilter::Equal("bool".into(), false.into()),
    ]);
    layout
        .query(
            &conn,
            vec!["Scalar".to_owned()],
            Some(filter),
            None,
            None,
            0,
            BLOCK_NUMBER_MAX,
        )
        .expect("Count query failed")
        .len()
}

#[test]
fn delete() {
    run_test(|conn, layout| -> Result<(), ()> {
        insert_entity(&conn, &layout, "Scalar", SCALAR_ENTITY.clone());
        let mut two = SCALAR_ENTITY.clone();
        two.set("id", "two");
        insert_entity(&conn, &layout, "Scalar", two);

        // Delete where nothing is getting deleted
        let mut key = EntityKey {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_type: "Scalar".to_owned(),
            entity_id: "no such entity".to_owned(),
        };
        let count = layout.delete(&conn, &key, 1).expect("Failed to delete");
        assert_eq!(0, count);
        assert_eq!(2, count_scalar_entities(conn, layout));

        // Delete entity two
        key.entity_id = "two".to_owned();
        let count = layout.delete(&conn, &key, 1).expect("Failed to delete");
        assert_eq!(1, count);
        assert_eq!(1, count_scalar_entities(conn, layout));
        Ok(())
    });
}

#[test]
fn conflicting_entity() {
    run_test(|conn, layout| -> Result<(), ()> {
        let id = "fred";
        let cat = "Cat".to_owned();
        let dog = "Dog".to_owned();
        let ferret = "Ferret".to_owned();

        let mut fred = Entity::new();
        fred.set("id", id);
        fred.set("name", id);
        insert_entity(&conn, &layout, "Cat", fred);

        // If we wanted to create Fred the dog, which is forbidden, we'd run this:
        let conflict = layout
            .conflicting_entity(&conn, &id.to_owned(), vec![&cat, &ferret])
            .unwrap();
        assert_eq!(Some("Cat".to_owned()), conflict);

        // If we wanted to manipulate Fred the cat, which is ok, we'd run:
        let conflict = layout
            .conflicting_entity(&conn, &id.to_owned(), vec![&dog, &ferret])
            .unwrap();
        assert_eq!(None, conflict);

        // Chairs are not pets
        let chair = "Chair".to_owned();
        let result = layout.conflicting_entity(&conn, &id.to_owned(), vec![&dog, &ferret, &chair]);
        assert!(result.is_err());
        assert_eq!("unknown table 'Chair'", result.err().unwrap().to_string());
        Ok(())
    })
}

fn test_find(expected_entity_ids: Vec<&str>, query: EntityQuery) {
    let expected_entity_ids: Vec<String> =
        expected_entity_ids.into_iter().map(str::to_owned).collect();

    run_test(move |conn, layout| -> Result<(), ()> {
        insert_users(conn, layout);
        insert_pets(conn, layout);

        let order = match query.order_by {
            Some((attribute, value_type)) => {
                let direction = query
                    .order_direction
                    .map(|direction| match direction {
                        EntityOrder::Ascending => "ASC",
                        EntityOrder::Descending => "DESC",
                    })
                    .unwrap_or("ASC");
                Some((attribute, value_type, direction))
            }
            None => None,
        };

        let entities = layout
            .query(
                conn,
                query.entity_types,
                query.filter,
                order,
                query.range.first,
                query.range.skip,
                BLOCK_NUMBER_MAX,
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

        assert_eq!(entity_ids, expected_entity_ids);

        Ok(())
    })
}

#[test]
fn find_interface() {
    test_find(
        vec!["garfield", "pluto"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["Cat".to_owned(), "Dog".to_owned()],
            filter: None,
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    );

    test_find(
        vec!["pluto", "garfield"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["Cat".to_owned(), "Dog".to_owned()],
            filter: None,
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );

    test_find(
        vec!["garfield"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["Cat".to_owned(), "Dog".to_owned()],
            filter: Some(EntityFilter::StartsWith("name".into(), Value::from("Gar"))),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );

    // Test that we can order by id
    test_find(
        vec!["pluto", "garfield"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["Cat".to_owned(), "Dog".to_owned()],
            filter: None,
            order_by: Some(("id".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );

    test_find(
        vec!["garfield", "pluto"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["Cat".to_owned(), "Dog".to_owned()],
            filter: None,
            order_by: Some(("id".to_owned(), ValueType::String)),
            order_direction: None,
            range: EntityRange::first(100),
        },
    );
}

#[test]
fn find_string_contains() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Contains(
                "name".into(),
                "ind".into(),
            )])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_list_contains() {
    fn query(v: Vec<&str>) -> EntityQuery {
        let drinks: Option<Value> = Some(
            v.into_iter()
                .map(|drink| drink.into())
                .collect::<Vec<Value>>()
                .into(),
        );
        let filter = Some(EntityFilter::Contains("drinks".into(), drinks.into()));
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter,
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        }
    }

    test_find(vec!["2"], query(vec!["beer"]));
    // Reverse of how we stored it
    test_find(vec!["3"], query(vec!["tea", "coffee"]));
    test_find(vec![], query(vec!["beer", "tea"]));
    test_find(vec![], query(vec!["beer", "water"]));
    test_find(vec![], query(vec!["beer", "wine", "water"]));
}

#[test]
fn find_string_equal() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "name".to_owned(),
                "Cindini".into(),
            )])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    );

    // Test that we can order by id
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "name".to_owned(),
                "Cindini".into(),
            )])),
            order_by: Some(("id".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_not_equal() {
    test_find(
        vec!["1", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "name".to_owned(),
                "Cindini".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_greater_than() {
    test_find(
        vec!["3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterThan(
                "name".to_owned(),
                "Kundi".into(),
            )])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_less_than_order_by_asc() {
    test_find(
        vec!["2", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "name".to_owned(),
                "Kundi".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_less_than_order_by_desc() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "name".to_owned(),
                "Kundi".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_less_than_range() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "name".to_owned(),
                "ZZZ".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange {
                first: Some(1),
                skip: 1,
            },
        },
    )
}

#[test]
fn find_string_multiple_and() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![
                EntityFilter::LessThan("name".to_owned(), "Cz".into()),
                EntityFilter::Equal("name".to_owned(), "Cindini".into()),
            ])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_ends_with() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::EndsWith(
                "name".to_owned(),
                "ini".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_not_ends_with() {
    test_find(
        vec!["3", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::NotEndsWith(
                "name".to_owned(),
                "ini".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_in() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "name".to_owned(),
                vec!["Johnton".into(), "Nobody".into(), "Still nobody".into()],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_string_not_in() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "name".to_owned(),
                vec!["Shaqueeena".into()],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_float_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "weight".to_owned(),
                Value::BigDecimal(184.4.into()),
            )])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_float_not_equal() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "weight".to_owned(),
                Value::BigDecimal(184.4.into()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_float_greater_than() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterThan(
                "weight".to_owned(),
                Value::BigDecimal(160.0.into()),
            )])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_float_less_than() {
    test_find(
        vec!["2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "weight".to_owned(),
                Value::BigDecimal(160.0.into()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_float_less_than_order_by_desc() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "weight".to_owned(),
                Value::BigDecimal(160.0.into()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_float_less_than_range() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "weight".to_owned(),
                Value::BigDecimal(161.0.into()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange {
                first: Some(1),
                skip: 1,
            },
        },
    )
}

#[test]
fn find_float_in() {
    test_find(
        vec!["3", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "weight".to_owned(),
                vec![
                    Value::BigDecimal(184.4.into()),
                    Value::BigDecimal(111.7.into()),
                ],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_float_not_in() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "weight".to_owned(),
                vec![
                    Value::BigDecimal(184.4.into()),
                    Value::BigDecimal(111.7.into()),
                ],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_int_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "age".to_owned(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_not_equal() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "age".to_owned(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_greater_than() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterThan(
                "age".to_owned(),
                Value::Int(43 as i32),
            )])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_greater_or_equal() {
    test_find(
        vec!["2", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterOrEqual(
                "age".to_owned(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_less_than() {
    test_find(
        vec!["2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "age".to_owned(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_less_or_equal() {
    test_find(
        vec!["2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessOrEqual(
                "age".to_owned(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_less_than_order_by_desc() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "age".to_owned(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_int_less_than_range() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "age".to_owned(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange {
                first: Some(1),
                skip: 1,
            },
        },
    )
}

#[test]
fn find_int_in() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "age".to_owned(),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_int_not_in() {
    test_find(
        vec!["3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "age".to_owned(),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_bool_equal() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "coffee".to_owned(),
                Value::Bool(true),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_bool_not_equal() {
    test_find(
        vec!["1", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "coffee".to_owned(),
                Value::Bool(true),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_bool_in() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "coffee".to_owned(),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_bool_not_in() {
    test_find(
        vec!["3", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "coffee".to_owned(),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_bytes_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "bin_name".to_owned(),
                Value::Bytes("Johnton".as_bytes().into()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_null_equal() {
    test_find(
        vec!["3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::Equal(
                "favorite_color".to_owned(),
                Value::Null,
            )),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_null_not_equal() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::Not("favorite_color".to_owned(), Value::Null)),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_null_not_in() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::NotIn(
                "favorite_color".to_owned(),
                vec![Value::Null],
            )),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );

    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::NotIn(
                "favorite_color".to_owned(),
                vec!["red".into(), Value::Null],
            )),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );
}

#[test]
fn find_order_by_float() {
    test_find(
        vec!["3", "2", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("weight".to_owned(), ValueType::BigDecimal)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    );
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("weight".to_owned(), ValueType::BigDecimal)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );
}

#[test]
fn find_order_by_id() {
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("id".to_owned(), ValueType::ID)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    );
    test_find(
        vec!["3", "2", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("id".to_owned(), ValueType::ID)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );
}

#[test]
fn find_order_by_int() {
    test_find(
        vec!["3", "2", "1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("age".to_owned(), ValueType::Int)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    );
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("age".to_owned(), ValueType::Int)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );
}

#[test]
fn find_order_by_string() {
    test_find(
        vec!["2", "1", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    );
    test_find(
        vec!["3", "1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: None,
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    );
}

#[test]
fn find_where_nested_and_or() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Or(vec![
                EntityFilter::Equal("id".to_owned(), Value::from("1")),
                EntityFilter::Equal("id".to_owned(), Value::from("2")),
            ])])),
            order_by: Some(("id".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_enum_equal() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "favorite_color".to_owned(),
                "red".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_enum_not_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "favorite_color".to_owned(),
                "red".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        },
    )
}

#[test]
fn find_enum_in() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "favorite_color".to_owned(),
                vec!["red".into()],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
}

#[test]
fn find_enum_not_in() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "favorite_color".to_owned(),
                vec!["red".into()],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: EntityRange::first(5),
        },
    )
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

    run_test(move |conn, layout| -> Result<(), ()> {
        let (a1, a2, a2b, a3) = ferrets();
        insert_pet(conn, layout, "Ferret", "a1", &a1);
        insert_pet(conn, layout, "Ferret", "a2", &a2);
        insert_pet(conn, layout, "Ferret", "a2b", &a2b);
        insert_pet(conn, layout, "Ferret", "a3", &a3);

        let query = EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["Ferret".to_owned()],
            filter: Some(filter),
            order_by: Some(("id".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: EntityRange::first(100),
        };

        let order = match query.order_by {
            Some((attribute, value_type)) => {
                let direction = query
                    .order_direction
                    .map(|direction| match direction {
                        EntityOrder::Ascending => "ASC",
                        EntityOrder::Descending => "DESC",
                    })
                    .unwrap_or("ASC");
                Some((attribute, value_type, direction))
            }
            None => None,
        };

        let entities = layout
            .query(
                conn,
                query.entity_types,
                query.filter,
                order,
                query.range.first,
                query.range.skip,
                BLOCK_NUMBER_MAX,
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

        Ok(())
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

#[test]
fn find_empty_and_or() {
    // It's somewhat arbitrary that we define empty 'or' and 'and' to
    // be 'true' and 'false'; it's mostly this way since that's what the
    // JSONB storage filters do

    // An empty 'or' is 'false'
    test_find(
        vec![],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::And(vec![EntityFilter::Or(vec![])])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    );

    // An empty 'and' is 'true'
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_types: vec!["User".to_owned()],
            filter: Some(EntityFilter::Or(vec![EntityFilter::And(vec![])])),
            order_by: None,
            order_direction: None,
            range: EntityRange::first(100),
        },
    )
}

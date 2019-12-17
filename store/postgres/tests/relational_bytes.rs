//! Test relational schemas that use `Bytes` to store ids
use diesel::connection::SimpleConnection as _;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use futures::future::{self, IntoFuture};
use hex_literal::hex;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::fmt::Debug;

use graph::data::store::scalar::{BigDecimal, BigInt};
use graph::prelude::{
    bigdecimal::One, web3::types::H256, Entity, EntityKey, Schema, SubgraphDeploymentId, Value,
    BLOCK_NUMBER_MAX,
};
use graph_store_postgres::layout_for_tests::{IdType, Layout};

use test_store::*;

const THINGS_GQL: &str = "
    type Thing @entity {
        id: ID!
        name: String!
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
    static ref BEEF_ENTITY: Entity = {
        let mut entity = Entity::new();
        entity.set("id", "deadbeef");
        entity.set("name", "Beef");
        entity.set("__typename", "Thing");
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

fn insert_thing(conn: &PgConnection, layout: &Layout, id: &str, name: &str) {
    let mut thing = Entity::new();

    thing.insert("id".to_owned(), Value::String(id.to_owned()));
    thing.insert("name".to_owned(), Value::String(name.to_owned()));

    insert_entity(conn, layout, "Thing", thing);
}

fn create_schema(conn: &PgConnection) -> Layout {
    let schema = Schema::parse(THINGS_GQL, THINGS_SUBGRAPH_ID.clone()).unwrap();

    let query = format!("create schema {}", SCHEMA_NAME);
    conn.batch_execute(&*query).unwrap();

    let layout = Layout::create_relational_schema(
        &conn,
        SCHEMA_NAME,
        IdType::Bytes,
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
            let layout = create_schema(&conn);

            // Run test
            test(&conn, &layout)
        }))
        .expect("Failed to run ChainHead test");
}

#[test]
fn bad_id() {
    run_test(|conn, layout| -> Result<(), ()> {
        // We test that we get errors for various strings that are not
        // valid 'Bytes' strings; we use `find` to force the conversion
        // from String -> Bytes internally
        let res = layout.find(conn, "Thing", "bad", BLOCK_NUMBER_MAX);
        assert!(res.is_err());
        assert_eq!(
            "store error: Odd number of digits",
            res.err().unwrap().to_string()
        );

        // We do not allow the `\x` prefix that Postgres uses
        let res = layout.find(conn, "Thing", "\\xbadd", BLOCK_NUMBER_MAX);
        assert!(res.is_err());
        assert_eq!(
            "store error: Invalid character \'\\\' at position 0",
            res.err().unwrap().to_string()
        );

        // Having the '0x' prefix is ok
        let res = layout.find(conn, "Thing", "0xbadd", BLOCK_NUMBER_MAX);
        assert!(res.is_ok());

        // Using non-hex characters is also bad
        let res = layout.find(conn, "Thing", "nope", BLOCK_NUMBER_MAX);
        assert!(res.is_err());
        assert_eq!(
            "store error: Invalid character \'n\' at position 0",
            res.err().unwrap().to_string()
        );

        Ok(())
    });
}

#[test]
fn find() {
    run_test(|conn, layout| -> Result<(), ()> {
        const ID: &str = "deadbeef";
        const NAME: &str = "Beef";
        insert_thing(&conn, &layout, ID, NAME);

        // Happy path: find existing entity
        let entity = layout
            .find(conn, "Thing", ID, BLOCK_NUMBER_MAX)
            .expect("Failed to read Thing[deadbeef]")
            .unwrap();
        assert_entity_eq!(scrub(&*BEEF_ENTITY), entity);

        // Find non-existing entity
        let entity = layout
            .find(conn, "Thing", "badd", BLOCK_NUMBER_MAX)
            .expect("Failed to read Thing[badd]");
        assert!(entity.is_none());
        Ok(())
    });
}

#[test]
fn find_many() {
    run_test(|conn, layout| -> Result<(), ()> {
        const ID: &str = "deadbeef";
        const NAME: &str = "Beef";
        const ID2: &str = "deadbeef02";
        const NAME2: &str = "Moo";
        insert_thing(&conn, &layout, ID, NAME);
        insert_thing(&conn, &layout, ID2, NAME2);

        let mut id_map: BTreeMap<&str, Vec<&str>> = BTreeMap::default();
        id_map.insert("Thing", vec![ID, ID2, "badd"]);

        let entities = layout
            .find_many(conn, id_map, BLOCK_NUMBER_MAX)
            .expect("Failed to read many things");
        assert_eq!(1, entities.len());

        let ids = entities
            .get("Thing")
            .expect("We got some things")
            .iter()
            .map(|thing| thing.id().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(2, ids.len());
        assert!(ids.contains(&ID.to_owned()), "Missing ID");
        assert!(ids.contains(&ID2.to_owned()), "Missing ID2");
        Ok(())
    });
}

#[test]
fn update() {
    run_test(|conn, layout| -> Result<(), ()> {
        insert_entity(&conn, &layout, "Thing", BEEF_ENTITY.clone());

        // Update the entity
        let mut entity = BEEF_ENTITY.clone();
        entity.set("name", "Moo");
        let key = EntityKey {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_type: "Thing".to_owned(),
            entity_id: entity.id().unwrap().clone(),
        };
        layout
            .update(&conn, &key, &entity, 1)
            .expect("Failed to update");

        let actual = layout
            .find(conn, "Thing", &entity.id().unwrap(), BLOCK_NUMBER_MAX)
            .expect("Failed to read Thing[deadbeef]")
            .unwrap();
        assert_entity_eq!(scrub(&entity), actual);
        Ok(())
    });
}

#[test]
fn delete() {
    run_test(|conn, layout| -> Result<(), ()> {
        const TWO_ID: &str = "deadbeef02";

        insert_entity(&conn, &layout, "Thing", BEEF_ENTITY.clone());
        let mut two = BEEF_ENTITY.clone();
        two.set("id", TWO_ID);
        insert_entity(&conn, &layout, "Thing", two);

        // Delete where nothing is getting deleted
        let mut key = EntityKey {
            subgraph_id: THINGS_SUBGRAPH_ID.clone(),
            entity_type: "Thing".to_owned(),
            entity_id: "ffff".to_owned(),
        };
        let count = layout.delete(&conn, &key, 1).expect("Failed to delete");
        assert_eq!(0, count);

        // Delete entity two
        key.entity_id = TWO_ID.to_owned();
        let count = layout.delete(&conn, &key, 1).expect("Failed to delete");
        assert_eq!(1, count);
        Ok(())
    });
}

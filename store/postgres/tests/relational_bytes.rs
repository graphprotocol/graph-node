//! Test relational schemas that use `Bytes` to store ids
use diesel::connection::SimpleConnection as _;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use futures::future::IntoFuture;
use hex_literal::hex;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::fmt::Debug;

use graph::data::store::scalar::{BigDecimal, BigInt};
use graph::prelude::{
    web3::types::H256, ChildMultiplicity, Entity, EntityCollection, EntityKey, EntityLink,
    EntityOrder, EntityRange, EntityWindow, Future01CompatExt, ParentLink, Schema,
    SubgraphDeploymentId, Value, WindowAttribute, BLOCK_NUMBER_MAX,
};
use graph_store_postgres::layout_for_tests::Layout;

use test_store::*;

const THINGS_GQL: &str = "
    type Thing @entity {
        id: Bytes!
        name: String!
        # We use these only in the EntityQuery tests
        parent: Thing
        children: [Thing!]
    }
";

const SCHEMA_NAME: &str = "layout";

macro_rules! entity {
    ($($name:ident: $value:expr,)*) => {
        {
            let mut result = ::graph::prelude::Entity::new();
            $(
                result.insert(stringify!($name).to_string(), Value::from($value));
            )*
            result
        }
    };
    ($($name:ident: $value:expr),*) => {
        entity! {$($name: $value,)*}
    };
}

lazy_static! {
    static ref THINGS_SUBGRAPH_ID: SubgraphDeploymentId =
        SubgraphDeploymentId::new("things").unwrap();
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
    static ref BEEF_ENTITY: Entity = entity! {
        id: "deadbeef",
        name: "Beef",
        __typename: "Thing"
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
    layout.insert(&conn, &key, entity, 0).expect(&errmsg);
}

fn insert_thing(conn: &PgConnection, layout: &Layout, id: &str, name: &str) {
    insert_entity(
        conn,
        layout,
        "Thing",
        entity! {
            id: id,
            name: name
        },
    );
}

fn create_schema(conn: &PgConnection) -> Layout {
    let schema = Schema::parse(THINGS_GQL, THINGS_SUBGRAPH_ID.clone()).unwrap();

    let query = format!("create schema {}", SCHEMA_NAME);
    conn.batch_execute(&*query).unwrap();

    Layout::create_relational_schema(&conn, &schema, SCHEMA_NAME.to_owned())
        .expect("Failed to create relational schema")
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
    // We don't need a full STORE, but we need to initialize it because
    // we depend on stored procedures that schema initialization loads
    let _store = STORE.clone();

    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");

    // Lock regardless of poisoning. This also forces sequential test execution.
    let mut runtime = match STORE_RUNTIME.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    runtime
        .block_on(async {
            // Reset state before starting
            remove_test_data(&conn);

            // Seed database with test data
            let layout = create_schema(&conn);

            // Run test
            test(&conn, &layout).into_future().compat().await
        })
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
            "store error: Invalid character \'\\\\\' at position 0",
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
            .update(&conn, &key, entity.clone(), 1)
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

//
// Test Layout::query to check that query generation is syntactically sound
//
const ROOT: &str = "dead00";
const CHILD1: &str = "babe01";
const CHILD2: &str = "babe02";
const GRANDCHILD1: &str = "fafa01";
const GRANDCHILD2: &str = "fafa02";

/// Create a set of test data that forms a tree through the `parent` and `children` attributes.
/// The tree has this form:
///
///   root
///     +- child1
///          +- grandchild1
///     +- child2
///          +- grandchild2
///
fn make_thing_tree(conn: &PgConnection, layout: &Layout) -> (Entity, Entity, Entity) {
    let root = entity! {
        id: ROOT,
        name: "root",
        children: vec!["babe01", "babe02"]
    };
    let child1 = entity! {
        id: CHILD1,
        name: "child1",
        parent: "dead00",
        children: vec![GRANDCHILD1]
    };
    let child2 = entity! {
        id: CHILD2,
        name: "child2",
        parent: "dead00",
        children: vec![GRANDCHILD1]
    };
    let grand_child1 = entity! {
        id: GRANDCHILD1,
        name: "grandchild1",
        parent: CHILD1
    };
    let grand_child2 = entity! {
        id: GRANDCHILD2,
        name: "grandchild2",
        parent: CHILD2
    };

    insert_entity(conn, layout, "Thing", root.clone());
    insert_entity(conn, layout, "Thing", child1.clone());
    insert_entity(conn, layout, "Thing", child2.clone());
    insert_entity(conn, layout, "Thing", grand_child1.clone());
    insert_entity(conn, layout, "Thing", grand_child2.clone());
    (root, child1, child2)
}

#[test]
fn query() {
    fn fetch(conn: &PgConnection, layout: &Layout, coll: EntityCollection) -> Vec<String> {
        layout
            .query::<Entity>(
                &*LOGGER,
                conn,
                coll,
                None,
                EntityOrder::Default,
                EntityRange::first(10),
                BLOCK_NUMBER_MAX,
                None,
            )
            .expect("the query succeeds")
            .into_iter()
            .map(|e| e.id().expect("entities have an id"))
            .collect::<Vec<_>>()
    }

    run_test(|conn, layout| -> Result<(), ()> {
        // This test exercises the different types of queries we generate;
        // the type of query is based on knowledge of what the test data
        // looks like, not on just an inference from the GraphQL model.
        // Especially the multiplicity for type A and B queries is determined
        // by knowing whether there are one or many entities per parent
        // in the test data
        make_thing_tree(conn, layout);

        // See https://graphprotocol.github.io/rfcs/engineering-plans/0001-graphql-query-prefetching.html#handling-parentchild-relationships
        // for a discussion of the various types of relationships and queries

        // EntityCollection::All
        let coll = EntityCollection::All(vec!["Thing".to_owned()]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![CHILD1, CHILD2, ROOT, GRANDCHILD1, GRANDCHILD2], things);

        // EntityCollection::Window, type A, many
        //   things(where: { children_contains: [CHILD1] }) { id }
        let coll = EntityCollection::Window(vec![EntityWindow {
            child_type: "Thing".to_owned(),
            ids: vec![CHILD1.to_owned()],
            link: EntityLink::Direct(
                WindowAttribute::List("children".to_string()),
                ChildMultiplicity::Many,
            ),
        }]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![ROOT], things);

        // EntityCollection::Window, type A, single
        //   things(where: { children_contains: [GRANDCHILD1, GRANDCHILD2] }) { id }
        let coll = EntityCollection::Window(vec![EntityWindow {
            child_type: "Thing".to_owned(),
            ids: vec![GRANDCHILD1.to_owned(), GRANDCHILD2.to_owned()],
            link: EntityLink::Direct(
                WindowAttribute::List("children".to_string()),
                ChildMultiplicity::Single,
            ),
        }]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![CHILD1, CHILD2], things);

        // EntityCollection::Window, type B, many
        //   things(where: { parent: [ROOT] }) { id }
        let coll = EntityCollection::Window(vec![EntityWindow {
            child_type: "Thing".to_owned(),
            ids: vec![ROOT.to_owned()],
            link: EntityLink::Direct(
                WindowAttribute::Scalar("parent".to_string()),
                ChildMultiplicity::Many,
            ),
        }]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![CHILD1, CHILD2], things);

        // EntityCollection::Window, type B, single
        //   things(where: { parent: [CHILD1, CHILD2] }) { id }
        let coll = EntityCollection::Window(vec![EntityWindow {
            child_type: "Thing".to_owned(),
            ids: vec![CHILD1.to_owned(), CHILD2.to_owned()],
            link: EntityLink::Direct(
                WindowAttribute::Scalar("parent".to_string()),
                ChildMultiplicity::Single,
            ),
        }]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![GRANDCHILD1, GRANDCHILD2], things);

        // EntityCollection::Window, type C
        //   things { children { id } }
        // This is the inner 'children' query
        let coll = EntityCollection::Window(vec![EntityWindow {
            child_type: "Thing".to_owned(),
            ids: vec![ROOT.to_owned()],
            link: EntityLink::Parent(ParentLink::List(vec![vec![
                CHILD1.to_owned(),
                CHILD2.to_owned(),
            ]])),
        }]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![CHILD1, CHILD2], things);

        // EntityCollection::Window, type D
        //   things { parent { id } }
        // This is the inner 'parent' query
        let coll = EntityCollection::Window(vec![EntityWindow {
            child_type: "Thing".to_owned(),
            ids: vec![CHILD1.to_owned(), CHILD2.to_owned()],
            link: EntityLink::Parent(ParentLink::Scalar(vec![ROOT.to_owned(), ROOT.to_owned()])),
        }]);
        let things = fetch(conn, layout, coll);
        assert_eq!(vec![ROOT, ROOT], things);

        Ok(())
    });
}

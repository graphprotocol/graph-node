//! Test mapping of GraphQL schema to a relational schema
use diesel::connection::SimpleConnection as _;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use futures::future::{self, IntoFuture};
use hex_literal::hex;
use lazy_static::lazy_static;
use std::fmt::Debug;

use graph::data::store::scalar::{BigDecimal, BigInt};
use graph::prelude::{
    bigdecimal::One, web3::types::H256, Entity, EntityKey, Schema, SubgraphDeploymentId, Value,
};
use graph_store_postgres::mapping_for_tests::Mapping;

use test_store::*;

const THINGS_GQL: &str = "
    type Thing @entity {
        id: ID!
        bigThing: Thing!
    }

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
    }";

const SCHEMA_NAME: &str = "mapping";

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

fn insert_test_data(conn: &PgConnection) -> Mapping {
    let schema = Schema::parse(THINGS_GQL, THINGS_SUBGRAPH_ID.clone()).unwrap();

    let query = format!("create schema {}", SCHEMA_NAME);
    conn.batch_execute(&*query).unwrap();

    let mapping = Mapping::create_relational_schema(
        &conn,
        SCHEMA_NAME,
        &*THINGS_SUBGRAPH_ID,
        &schema.document,
    )
    .expect("Failed to create relational schema");

    let key = EntityKey {
        subgraph_id: THINGS_SUBGRAPH_ID.clone(),
        entity_type: "Scalar".to_owned(),
        entity_id: "one".to_owned(),
    };

    mapping
        .insert(&conn, &key, SCALAR_ENTITY.clone())
        .expect("Failed to insert test row");

    mapping
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(&PgConnection, &Mapping) -> R + Send + 'static,
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
            let mapping = insert_test_data(&conn);

            // Run test
            test(&conn, &mapping)
        }))
        .expect("Failed to run ChainHead test");
}

#[test]
fn test_find() {
    run_test(|conn, mapping| -> Result<(), ()> {
        // Happy path: find existing entity
        let entity = mapping
            .find(conn, "Scalar", "one")
            .expect("Failed to read Scalar[one]")
            .unwrap();
        assert_eq!(&*SCALAR_ENTITY, &entity);

        // Find non-existing entity
        let entity = mapping
            .find(conn, "Scalar", "noone")
            .expect("Failed to read Scalar[noone]");
        assert!(entity.is_none());

        // Find for non-existing entity type
        let err = mapping.find(conn, "NoEntity", "one");
        match err {
            Err(e) => assert_eq!("store error: unknown table 'NoEntity'", e.to_string()),
            _ => {
                println!("{:?}", err);
                assert!(false)
            }
        }
        Ok(())
    });
}

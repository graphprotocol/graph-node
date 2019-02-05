// Tests for graphql interfaces.

extern crate diesel;
extern crate graph;
extern crate graph_core;
#[macro_use]
extern crate lazy_static;
extern crate graph_graphql;
extern crate graph_store_postgres;
extern crate graphql_parser;
extern crate hex;

use tokio::runtime::Runtime;

use graph::prelude::{Store as StoreTrait, *};
use graph::web3::types::H256;
use graph_graphql::prelude::{api_schema, execute_query, QueryExecutionOptions, StoreResolver};
use graph_store_postgres::{Store, StoreConfig};

/// Helper function to ensure and obtain the Postgres URL to use for testing.
fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

lazy_static! {
    // Create Store instance once for use with each of the tests
    static ref STORE: Arc<Store> = {
            let mut runtime = Runtime::new().unwrap();
            let store = runtime.block_on(future::lazy(|| -> Result<_, ()> {
                let logger = Logger::root(slog::Discard, o!());
                let postgres_url = postgres_test_url();
                let net_identifiers = EthereumNetworkIdentifier {
                    net_version: "graph test suite".to_owned(),
                    genesis_block_hash: H256::from("0xbd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"),
                };
                let network_name = "fake_network".to_owned();

                Ok(Arc::new(Store::new(
                    StoreConfig {
                        postgres_url,
                        network_name,
                    },
                    &logger,
                    net_identifiers,
                )))
                })).unwrap();
            store
    };
}

fn insert_and_query(
    subgraph_id: &str,
    schema: &str,
    entities: Vec<Entity>,
    query: &str,
) -> QueryResult {
    let subgraph_id = SubgraphDeploymentId::new(subgraph_id).unwrap();
    let mut schema = Schema::parse(schema, subgraph_id.clone()).unwrap();
    schema.document = api_schema(&schema.document).unwrap();

    let insert_ops = entities.into_iter().map(|data| EntityOperation::Set {
        key: EntityKey {
            subgraph_id: subgraph_id.clone(),
            entity_type: "Animal".to_owned(),
            entity_id: "1".to_owned(),
        },
        data,
    });
    STORE
        .apply_entity_operations(insert_ops.collect(), EventSource::None)
        .unwrap();

    let logger = Logger::root(slog::Discard, o!());
    let resolver = StoreResolver::new(&logger, STORE.clone());

    let options = QueryExecutionOptions { logger, resolver };
    let document = graphql_parser::parse_query(query).unwrap();
    let query = Query {
        schema,
        document,
        variables: None,
    };
    execute_query(&query, options)
}

/*
#[test]
fn one_interface_zero_entities() {
    let subgraph_id = "oneInterfaceOneEntity";
    let schema = "interface Legged { legs: Int }
                  type Animal implements HasLegs @entity { id: ID!, legs: Int }";

    let query = "query { animals { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![entity], query);
    assert!(res.data.is_none() && res.errors.is_empty());
}*/

#[test]
fn one_interface_one_entity() {
    let subgraph_id = "oneInterfaceOneEntity";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let entity = Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]);

    let query = "query { leggeds { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![entity], query);
    dbg!(&res);
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"animals\": List([Object({\"legs\": Int(Number(3))})])})"
    )
}

/*
#[test]
fn one_interface_multiple_entities() {
    let subgraph_id = "oneInterfaceOneEntity";
    let schema = "interface Legged { legs: Int }
                  type Animal implements HasLegs @entity { id: ID!, legs: Int }
                  type Furniture implements HasLegs @entity { id: ID!, legs: Int }
                  ";

    let entity = Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]);

    let query = "query { animals { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![entity], query);
    assert!(res.errors.is_empty());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"animals\": List([Object({\"legs\": Int(Number(3))})])})"
    )
}
*/

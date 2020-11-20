#[macro_use]
extern crate pretty_assertions;

use graphql_parser::{query as q, Pos};
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::{Duration, Instant};

use graph::prelude::{
    async_trait, futures03::stream::StreamExt, futures03::FutureExt, futures03::TryFutureExt, o,
    slog, tokio, ApiSchema, DeploymentState, Entity, EntityKey, EntityOperation,
    EthereumBlockPointer, FutureExtension, GraphQlRunner as _, Logger, Query, QueryError,
    QueryExecutionError, QueryLoadManager, QueryResult, QueryVariables, Schema, Store,
    SubgraphDeploymentEntity, SubgraphDeploymentId, SubgraphManifest, SubgraphVersionSwitchingMode,
    Subscription, SubscriptionError, Value, BLOCK_NUMBER_MAX,
};
use graph::{
    data::graphql::{object, object_value},
    data::query::CacheStatus,
    prelude::{NodeId, SubgraphName},
};
use graph_graphql::prelude::*;
use test_store::{
    execute_subgraph_query_with_complexity, execute_subgraph_query_with_deadline,
    run_test_sequentially, transact_entity_operations, BLOCK_ONE, GENESIS_PTR, LOAD_MANAGER,
    LOGGER, STORE,
};

fn setup() -> SubgraphDeploymentId {
    use test_store::block_store::{self, BLOCK_ONE, BLOCK_TWO, GENESIS_BLOCK};

    let id = SubgraphDeploymentId::new("graphqlTestsQuery").unwrap();

    let chain = vec![&*GENESIS_BLOCK, &*BLOCK_ONE, &*BLOCK_TWO];
    block_store::remove();
    block_store::insert(chain, "fake_network");
    test_store::remove_subgraphs();
    insert_test_entities(STORE.as_ref(), id.clone());

    id
}

fn test_schema(id: SubgraphDeploymentId) -> Schema {
    Schema::parse(
        "
            type Musician @entity {
                id: ID!
                name: String!
                mainBand: Band
                bands: [Band!]!
                writtenSongs: [Song]! @derivedFrom(field: \"writtenBy\")
            }

            type Band @entity {
                id: ID!
                name: String!
                members: [Musician!]! @derivedFrom(field: \"bands\")
                originalSongs: [Song!]!
            }

            type Song @entity {
                id: ID!
                title: String!
                writtenBy: Musician!
                band: Band @derivedFrom(field: \"originalSongs\")
            }

            type SongStat @entity {
                id: ID!
                song: Song @derivedFrom(field: \"id\")
                played: Int!
            }
            ",
        id,
    )
    .expect("Test schema invalid")
}

fn api_test_schema(id: &SubgraphDeploymentId) -> ApiSchema {
    let mut schema = test_schema(id.clone());
    schema.document = api_schema(&schema.document).expect("Failed to derive API schema");
    schema.add_subgraph_id_directives(id.clone());
    ApiSchema::from_api_schema(schema).unwrap()
}

fn insert_test_entities(store: &impl Store, id: SubgraphDeploymentId) {
    let schema = test_schema(id.clone());

    // First insert the manifest.
    let manifest = SubgraphManifest {
        id: id.clone(),
        location: String::new(),
        spec_version: "1".to_owned(),
        description: None,
        repository: None,
        schema: schema.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
    };

    let deployment = SubgraphDeploymentEntity::new(&manifest, false, None);
    let name = SubgraphName::new("test/query").unwrap();
    let node_id = NodeId::new("test").unwrap();
    store
        .create_subgraph_deployment(
            name,
            &schema,
            deployment,
            node_id,
            SubgraphVersionSwitchingMode::Instant,
        )
        .unwrap();

    let entities0 = vec![
        Entity::from(vec![
            ("__typename", Value::from("Musician")),
            ("id", Value::from("m1")),
            ("name", Value::from("John")),
            ("mainBand", Value::from("b1")),
            (
                "bands",
                Value::List(vec![Value::from("b1"), Value::from("b2")]),
            ),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Musician")),
            ("id", Value::from("m2")),
            ("name", Value::from("Lisa")),
            ("mainBand", Value::from("b1")),
            ("bands", Value::List(vec![Value::from("b1")])),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Band")),
            ("id", Value::from("b1")),
            ("name", Value::from("The Musicians")),
            (
                "originalSongs",
                Value::List(vec![Value::from("s1"), Value::from("s2")]),
            ),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Band")),
            ("id", Value::from("b2")),
            ("name", Value::from("The Amateurs")),
            (
                "originalSongs",
                Value::List(vec![
                    Value::from("s1"),
                    Value::from("s3"),
                    Value::from("s4"),
                ]),
            ),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Song")),
            ("id", Value::from("s1")),
            ("title", Value::from("Cheesy Tune")),
            ("writtenBy", Value::from("m1")),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Song")),
            ("id", Value::from("s2")),
            ("title", Value::from("Rock Tune")),
            ("writtenBy", Value::from("m2")),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Song")),
            ("id", Value::from("s3")),
            ("title", Value::from("Pop Tune")),
            ("writtenBy", Value::from("m1")),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Song")),
            ("id", Value::from("s4")),
            ("title", Value::from("Folk Tune")),
            ("writtenBy", Value::from("m3")),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("SongStat")),
            ("id", Value::from("s1")),
            ("played", Value::from(10)),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("SongStat")),
            ("id", Value::from("s2")),
            ("played", Value::from(15)),
        ]),
    ];

    let entities1 = vec![
        Entity::from(vec![
            ("__typename", Value::from("Musician")),
            ("id", Value::from("m3")),
            ("name", Value::from("Tom")),
            ("mainBand", Value::from("b2")),
            (
                "bands",
                Value::List(vec![Value::from("b1"), Value::from("b2")]),
            ),
        ]),
        Entity::from(vec![
            ("__typename", Value::from("Musician")),
            ("id", Value::from("m4")),
            ("name", Value::from("Valerie")),
            ("bands", Value::List(vec![])),
            ("writtenSongs", Value::List(vec![Value::from("s2")])),
        ]),
    ];

    fn insert_at(entities: Vec<Entity>, id: SubgraphDeploymentId, block_ptr: EthereumBlockPointer) {
        let insert_ops = entities.into_iter().map(|data| EntityOperation::Set {
            key: EntityKey {
                subgraph_id: id.clone(),
                entity_type: data["__typename"].clone().as_string().unwrap(),
                entity_id: data["id"].clone().as_string().unwrap(),
            },
            data,
        });

        transact_entity_operations(
            &STORE,
            id.clone(),
            block_ptr,
            insert_ops.collect::<Vec<_>>(),
        )
        .unwrap();
    }

    insert_at(entities0, id.clone(), GENESIS_PTR.clone());
    insert_at(entities1, id.clone(), BLOCK_ONE.clone());
}

async fn execute_query_document(id: &SubgraphDeploymentId, query: q::Document) -> QueryResult {
    execute_query_document_with_variables(id, query, None).await
}

async fn execute_query_document_with_variables(
    id: &SubgraphDeploymentId,
    query: q::Document,
    variables: Option<QueryVariables>,
) -> QueryResult {
    let runner = Arc::new(GraphQlRunner::new(
        &*LOGGER,
        STORE.clone(),
        LOAD_MANAGER.clone(),
    ));
    let query = Query::new(Arc::new(api_test_schema(id)), query, variables, None);
    let state = DeploymentState {
        id: query.schema.id().clone(),
        reorg_count: 0,
        max_reorg_depth: 0,
        latest_ethereum_block_number: BLOCK_NUMBER_MAX,
    };

    runner
        .run_query_with_complexity(query, state, None, None, None, None, false)
        .await
        .as_ref()
        .clone()
}

async fn execute_query_document_with_state(
    id: &SubgraphDeploymentId,
    query: q::Document,
    state: DeploymentState,
) -> QueryResult {
    let runner = Arc::new(GraphQlRunner::new(
        &*LOGGER,
        STORE.clone(),
        LOAD_MANAGER.clone(),
    ));
    let query = Query::new(Arc::new(api_test_schema(id)), query, None, None);

    graph::prelude::futures03::executor::block_on(
        runner.run_query_with_complexity(query, state, None, None, None, None, false),
    )
    .as_ref()
    .clone()
}

struct MockQueryLoadManager(Arc<tokio::sync::Semaphore>);

#[async_trait]
impl QueryLoadManager for MockQueryLoadManager {
    async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.0.clone().acquire_owned().await
    }

    fn record_work(&self, _shape_hash: u64, _duration: Duration, _cache_status: CacheStatus) {}
}

fn mock_query_load_manager() -> Arc<MockQueryLoadManager> {
    Arc::new(MockQueryLoadManager(Arc::new(tokio::sync::Semaphore::new(
        10,
    ))))
}

/// Extract the data from a `QueryResult`, and panic if it has errors
macro_rules! extract_data {
    ($result: expr) => {
        match $result.to_result() {
            Err(errors) => panic!(format!("Unexpected errors return for query: {:#?}", errors)),
            Ok(data) => data,
        }
    };
}

#[test]
fn can_query_one_to_one_relationship() {
    run_test_sequentially(setup, |_, id| async move {
        let result = execute_query_document(
            &id,
            graphql_parser::parse_query(
                "
            query {
                musicians(first: 100, orderBy: id) {
                    name
                    mainBand {
                        name
                    }
                }
                songStats(first: 100, orderBy: id) {
                    id
                    song {
                      id
                      title
                    }
                    played
                }
            }
            ",
            )
            .expect("Invalid test query"),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![
                (
                    "musicians",
                    q::Value::List(vec![
                        object_value(vec![
                            ("name", q::Value::String(String::from("John"))),
                            (
                                "mainBand",
                                object_value(vec![(
                                    "name",
                                    q::Value::String(String::from("The Musicians")),
                                )]),
                            ),
                        ]),
                        object_value(vec![
                            ("name", q::Value::String(String::from("Lisa"))),
                            (
                                "mainBand",
                                object_value(vec![(
                                    "name",
                                    q::Value::String(String::from("The Musicians")),
                                )]),
                            ),
                        ]),
                        object_value(vec![
                            ("name", q::Value::String(String::from("Tom"))),
                            (
                                "mainBand",
                                object_value(vec![(
                                    "name",
                                    q::Value::String(String::from("The Amateurs")),
                                )]),
                            ),
                        ]),
                        object_value(vec![
                            ("name", q::Value::String(String::from("Valerie"))),
                            ("mainBand", q::Value::Null),
                        ]),
                    ])
                ),
                (
                    "songStats",
                    q::Value::List(vec![
                        object_value(vec![
                            ("id", q::Value::String(String::from("s1"))),
                            ("played", q::Value::Int(q::Number::from(10))),
                            (
                                "song",
                                object_value(vec![
                                    ("id", q::Value::String(String::from("s1"))),
                                    ("title", q::Value::String(String::from("Cheesy Tune")))
                                ])
                            ),
                        ]),
                        object_value(vec![
                            ("id", q::Value::String(String::from("s2"))),
                            ("played", q::Value::Int(q::Number::from(15))),
                            (
                                "song",
                                object_value(vec![
                                    ("id", q::Value::String(String::from("s2"))),
                                    ("title", q::Value::String(String::from("Rock Tune")))
                                ])
                            ),
                        ])
                    ])
                )
            ]))
        )
    })
}

#[test]
fn can_query_one_to_many_relationships_in_both_directions() {
    run_test_sequentially(setup, |_, id| async move {
        let result = execute_query_document(
            &id,
            graphql_parser::parse_query(
                "
        query {
            musicians(first: 100, orderBy: id) {
                name
                writtenSongs(first: 100, orderBy: id) {
                    title
                    writtenBy { name }
                }
            }
        }
        ",
            )
            .expect("Invalid test query"),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![
                        ("name", q::Value::String(String::from("John"))),
                        (
                            "writtenSongs",
                            q::Value::List(vec![
                                object_value(vec![
                                    ("title", q::Value::String(String::from("Cheesy Tune"))),
                                    (
                                        "writtenBy",
                                        object_value(vec![(
                                            "name",
                                            q::Value::String(String::from("John")),
                                        )]),
                                    ),
                                ]),
                                object_value(vec![
                                    ("title", q::Value::String(String::from("Pop Tune"))),
                                    (
                                        "writtenBy",
                                        object_value(vec![(
                                            "name",
                                            q::Value::String(String::from("John")),
                                        )]),
                                    ),
                                ]),
                            ]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Lisa"))),
                        (
                            "writtenSongs",
                            q::Value::List(vec![object_value(vec![
                                ("title", q::Value::String(String::from("Rock Tune"))),
                                (
                                    "writtenBy",
                                    object_value(vec![(
                                        "name",
                                        q::Value::String(String::from("Lisa")),
                                    )]),
                                ),
                            ])]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Tom"))),
                        (
                            "writtenSongs",
                            q::Value::List(vec![object_value(vec![
                                ("title", q::Value::String(String::from("Folk Tune"))),
                                (
                                    "writtenBy",
                                    object_value(vec![(
                                        "name",
                                        q::Value::String(String::from("Tom"))
                                    )]),
                                ),
                            ])]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Valerie"))),
                        ("writtenSongs", q::Value::List(vec![])),
                    ]),
                ]),
            )])),
        )
    })
}

#[test]
fn can_query_many_to_many_relationship() {
    run_test_sequentially(setup, |_, id| async move {
        let result = execute_query_document(
            &id,
            graphql_parser::parse_query(
                "
            query {
                musicians(first: 100, orderBy: id) {
                    name
                    bands(first: 100, orderBy: id) {
                        name
                        members(first: 100, orderBy: id) {
                            name
                        }
                    }
                }
            }
            ",
            )
            .expect("Invalid test query"),
        )
        .await;

        let the_musicians = object_value(vec![
            ("name", q::Value::String(String::from("The Musicians"))),
            (
                "members",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                ]),
            ),
        ]);

        let the_amateurs = object_value(vec![
            ("name", q::Value::String(String::from("The Amateurs"))),
            (
                "members",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                ]),
            ),
        ]);

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![
                        ("name", q::Value::String(String::from("John"))),
                        (
                            "bands",
                            q::Value::List(vec![the_musicians.clone(), the_amateurs.clone()]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Lisa"))),
                        ("bands", q::Value::List(vec![the_musicians.clone()])),
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Tom"))),
                        (
                            "bands",
                            q::Value::List(vec![the_musicians.clone(), the_amateurs.clone()]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Valerie"))),
                        ("bands", q::Value::List(vec![])),
                    ]),
                ])
            )]))
        );
    })
}

#[test]
fn query_variables_are_used() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians($where: Musician_filter!) {
          musicians(first: 100, where: $where) {
            name
          }
        }
    ",
        )
        .expect("invalid test query");

        let result = execute_query_document_with_variables(
            &id,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(
                    String::from("where"),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                )]
                .into_iter(),
            ))),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![object_value(vec![(
                    "name",
                    q::Value::String(String::from("Tom"))
                )])],)
            )]))
        );
    })
}

#[test]
fn skip_directive_works_with_query_variables() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians($skip: Boolean!) {
          musicians(first: 100, orderBy: id) {
            id @skip(if: $skip)
            name
          }
        }
    ",
        )
        .expect("invalid test query");

        // Set variable $skip to true
        let result = execute_query_document_with_variables(
            &id,
            query.clone(),
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("skip"), q::Value::Boolean(true))].into_iter(),
            ))),
        )
        .await;

        // Assert that only names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", q::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );

        // Set variable $skip to false
        let result = execute_query_document_with_variables(
            &id,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("skip"), q::Value::Boolean(false))].into_iter(),
            ))),
        )
        .await;

        // Assert that IDs and names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![
                        ("id", q::Value::String(String::from("m1"))),
                        ("name", q::Value::String(String::from("John")))
                    ]),
                    object_value(vec![
                        ("id", q::Value::String(String::from("m2"))),
                        ("name", q::Value::String(String::from("Lisa")))
                    ]),
                    object_value(vec![
                        ("id", q::Value::String(String::from("m3"))),
                        ("name", q::Value::String(String::from("Tom")))
                    ]),
                    object_value(vec![
                        ("id", q::Value::String(String::from("m4"))),
                        ("name", q::Value::String(String::from("Valerie")))
                    ]),
                ],)
            )]))
        );
    })
}

#[test]
fn include_directive_works_with_query_variables() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians($include: Boolean!) {
          musicians(first: 100, orderBy: id) {
            id @include(if: $include)
            name
          }
        }
    ",
        )
        .expect("invalid test query");

        // Set variable $include to true
        let result = execute_query_document_with_variables(
            &id,
            query.clone(),
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("include"), q::Value::Boolean(true))].into_iter(),
            ))),
        )
        .await;

        // Assert that IDs and names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![
                        ("id", q::Value::String(String::from("m1"))),
                        ("name", q::Value::String(String::from("John")))
                    ]),
                    object_value(vec![
                        ("id", q::Value::String(String::from("m2"))),
                        ("name", q::Value::String(String::from("Lisa")))
                    ]),
                    object_value(vec![
                        ("id", q::Value::String(String::from("m3"))),
                        ("name", q::Value::String(String::from("Tom")))
                    ]),
                    object_value(vec![
                        ("id", q::Value::String(String::from("m4"))),
                        ("name", q::Value::String(String::from("Valerie")))
                    ]),
                ],)
            )]))
        );

        // Set variable $include to false
        let result = execute_query_document_with_variables(
            &id,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("include"), q::Value::Boolean(false))].into_iter(),
            ))),
        )
        .await;

        // Assert that only names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", q::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );
    })
}

#[test]
fn query_complexity() {
    run_test_sequentially(setup, |_, id| async move {
        let query = Query::new(
            Arc::new(api_test_schema(&id)),
            graphql_parser::parse_query(
                "query {
                musicians(orderBy: id) {
                    name
                    bands(first: 100, orderBy: id) {
                        name
                        members(first: 100, orderBy: id) {
                            name
                        }
                    }
                }
            }",
            )
            .unwrap(),
            None,
            None,
        );
        let max_complexity = Some(1_010_100);

        // This query is exactly at the maximum complexity.
        let result = graph::spawn_blocking_allow_panic(move || {
            execute_subgraph_query_with_complexity(query, max_complexity)
        })
        .await
        .unwrap();
        assert!(!result.has_errors());

        let query = Query::new(
            Arc::new(api_test_schema(&id)),
            graphql_parser::parse_query(
                "query {
                musicians(orderBy: id) {
                    name
                    bands(first: 100, orderBy: id) {
                        name
                        members(first: 100, orderBy: id) {
                            name
                        }
                    }
                }
                __schema {
                    types {
                        name
                    }
                }
            }",
            )
            .unwrap(),
            None,
            None,
        );

        // The extra introspection causes the complexity to go over.
        let result = graph::spawn_blocking_allow_panic(move || {
            execute_subgraph_query_with_complexity(query, max_complexity)
        })
        .await
        .unwrap();
        match result.to_result().unwrap_err()[0] {
            QueryError::ExecutionError(QueryExecutionError::TooComplex(1_010_200, _)) => (),
            _ => panic!("did not catch complexity"),
        };
    })
}

#[test]
fn query_complexity_subscriptions() {
    run_test_sequentially(setup, |_, id| async move {
        let logger = Logger::root(slog::Discard, o!());
        let store = STORE.clone().query_store(true);
        let store_resolver = StoreResolver::for_subscription(&logger, id.clone(), store);

        let query = Query::new(
            Arc::new(api_test_schema(&id)),
            graphql_parser::parse_query(
                "subscription {
                musicians(orderBy: id) {
                    name
                    bands(first: 100, orderBy: id) {
                        name
                        members(first: 100, orderBy: id) {
                            name
                        }
                    }
                }
            }",
            )
            .unwrap(),
            None,
            None,
        );
        let max_complexity = Some(1_010_100);
        let options = SubscriptionExecutionOptions {
            logger: logger.clone(),
            resolver: store_resolver,
            timeout: None,
            max_complexity,
            max_depth: 100,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,
            load_manager: mock_query_load_manager(),
        };

        // This query is exactly at the maximum complexity.
        // FIXME: Not collecting the stream because that will hang the test.
        let _ignore_stream = execute_subscription(Subscription { query }, options).unwrap();

        let query = Query::new(
            Arc::new(api_test_schema(&id)),
            graphql_parser::parse_query(
                "subscription {
                musicians(orderBy: id) {
                    name
                    bands(first: 100, orderBy: id) {
                        name
                        members(first: 100, orderBy: id) {
                            name
                        }
                    }
                }
                __schema {
                    types {
                        name
                    }
                }
            }",
            )
            .unwrap(),
            None,
            None,
        );

        let store = STORE.clone().query_store(true);
        let store_resolver = StoreResolver::for_subscription(&logger, id.clone(), store);

        let options = SubscriptionExecutionOptions {
            logger,
            resolver: store_resolver,
            timeout: None,
            max_complexity,
            max_depth: 100,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,
            load_manager: mock_query_load_manager(),
        };

        // The extra introspection causes the complexity to go over.
        let result = execute_subscription(Subscription { query }, options);
        match result {
            Err(SubscriptionError::GraphQLError(e)) => match e[0] {
                QueryExecutionError::TooComplex(1_010_200, _) => (), // Expected
                _ => panic!("did not catch complexity"),
            },
            _ => panic!("did not catch complexity"),
        }
    })
}

#[test]
fn instant_timeout() {
    run_test_sequentially(setup, |_, id| async move {
        let query = Query::new(
            Arc::new(api_test_schema(&id)),
            graphql_parser::parse_query("query { musicians(first: 100) { name } }").unwrap(),
            None,
            None,
        );

        match graph::spawn_blocking_allow_panic(move || {
            execute_subgraph_query_with_deadline(query, Some(Instant::now()))
        })
        .await
        .unwrap()
        .to_result()
        .unwrap_err()[0]
        {
            QueryError::ExecutionError(QueryExecutionError::Timeout) => (), // Expected
            _ => panic!("did not time out"),
        };
    })
}

#[test]
fn variable_defaults() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians($orderDir: OrderDirection = desc) {
          bands(first: 2, orderBy: id, orderDirection: $orderDir) {
            id
          }
        }
    ",
        )
        .expect("invalid test query");

        // Assert that missing variables are defaulted.
        let result = execute_query_document_with_variables(
            &id,
            query.clone(),
            Some(QueryVariables::default()),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "bands",
                q::Value::List(vec![
                    object_value(vec![("id", q::Value::String(String::from("b2")))]),
                    object_value(vec![("id", q::Value::String(String::from("b1")))])
                ],)
            )]))
        );

        // Assert that null variables are not defaulted.
        let result = execute_query_document_with_variables(
            &id,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("orderDir"), q::Value::Null)].into_iter(),
            ))),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "bands",
                q::Value::List(vec![
                    object_value(vec![("id", q::Value::String(String::from("b1")))]),
                    object_value(vec![("id", q::Value::String(String::from("b2")))])
                ],)
            )]))
        );
    })
}

#[test]
fn skip_is_nullable() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians {
          musicians(orderBy: id, skip: null) {
            name
          }
        }
    ",
        )
        .expect("invalid test query");

        let result = execute_query_document_with_variables(&id, query, None).await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", q::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );
    })
}

#[test]
fn first_is_nullable() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians {
          musicians(first: null, orderBy: id) {
            name
          }
        }
    ",
        )
        .expect("invalid test query");

        let result = execute_query_document_with_variables(&id, query, None).await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", q::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", q::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );
    })
}

#[test]
fn nested_variable() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        query musicians($name: String) {
          musicians(first: 100, where: { name: $name }) {
            name
          }
        }
    ",
        )
        .expect("invalid test query");

        let result = execute_query_document_with_variables(
            &id,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("name"), q::Value::String("Lisa".to_string()))].into_iter(),
            ))),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![object_value(vec![(
                    "name",
                    q::Value::String(String::from("Lisa"))
                )])])
            )]))
        );
    })
}

#[test]
fn ambiguous_derived_from_result() {
    run_test_sequentially(setup, |_, id| async move {
        let query = graphql_parser::parse_query(
            "
        {
          songs(first: 100, orderBy: id) {
            id
            band {
              id
            }
          }
        }
        ",
        )
        .expect("invalid test query");

        let result = execute_query_document_with_variables(&id, query, None).await;

        match &result.to_result().unwrap_err()[0] {
            QueryError::ExecutionError(QueryExecutionError::AmbiguousDerivedFromResult(
                pos,
                derived_from_field,
                target_type,
                target_field,
            )) => {
                assert_eq!(
                    pos,
                    &Pos {
                        line: 5,
                        column: 13
                    }
                );
                assert_eq!(derived_from_field.as_str(), "band");
                assert_eq!(target_type.as_str(), "Band");
                assert_eq!(target_field.as_str(), "originalSongs");
            }
            e => panic!(format!(
                "expected AmbiguousDerivedFromResult error, got {}",
                e
            )),
        }
    })
}

#[test]
fn can_filter_by_relationship_fields() {
    run_test_sequentially(setup, |_, id| async move {
        let result = execute_query_document(
            &id,
            graphql_parser::parse_query(
                "
        query {
            musicians(orderBy: id, where: { mainBand: \"b2\" }) {
                id name
                mainBand { id }
            }
            bands(orderBy: id, where: { originalSongs: [\"s1\", \"s3\", \"s4\"] }) {
                id name
                originalSongs { id }
            }
        }
        ",
            )
            .expect("invalid test query"),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![
                (
                    "musicians",
                    q::Value::List(vec![object_value(vec![
                        ("id", q::Value::String(String::from("m3"))),
                        ("name", q::Value::String(String::from("Tom"))),
                        (
                            "mainBand",
                            object_value(vec![("id", q::Value::String(String::from("b2")))])
                        )
                    ])])
                ),
                (
                    "bands",
                    q::Value::List(vec![object_value(vec![
                        ("id", q::Value::String(String::from("b2"))),
                        ("name", q::Value::String(String::from("The Amateurs"))),
                        (
                            "originalSongs",
                            q::Value::List(vec![
                                object_value(vec![("id", q::Value::String(String::from("s1")))]),
                                object_value(vec![("id", q::Value::String(String::from("s3")))]),
                                object_value(vec![("id", q::Value::String(String::from("s4")))]),
                            ])
                        )
                    ])])
                )
            ]))
        );
    })
}

#[test]
fn cannot_filter_by_derved_relationship_fields() {
    run_test_sequentially(setup, |_, id| async move {
        let result = execute_query_document(
            &id,
            graphql_parser::parse_query(
                "
        query {
            musicians(orderBy: id, where: { writtenSongs: [\"s1\"] }) {
                id name
                mainBand { id }
            }
        }
        ",
            )
            .expect("invalid test query"),
        )
        .await;

        match &result.to_result().unwrap_err()[0] {
            QueryError::ExecutionError(QueryExecutionError::InvalidArgumentError(_, s, v)) => {
                assert_eq!(s, "where");
                assert_eq!(
                    v,
                    &object_value(vec![(
                        "writtenSongs",
                        q::Value::List(vec![q::Value::String(String::from("s1"))])
                    )]),
                );
            }
            e => panic!(format!("expected ResolveEntitiesError, got {}", e)),
        };
    })
}

#[test]
fn subscription_gets_result_even_without_events() {
    run_test_sequentially(setup, |_, id| async move {
        let logger = Logger::root(slog::Discard, o!());
        let store = STORE.clone().query_store(true);
        let store_resolver = StoreResolver::for_subscription(&logger, id.clone(), store);

        let query = Query::new(
            Arc::new(api_test_schema(&id)),
            graphql_parser::parse_query(
                "subscription {
              musicians(orderBy: id, first: 2) {
                name
              }
            }",
            )
            .unwrap(),
            None,
            None,
        );

        let options = SubscriptionExecutionOptions {
            logger: logger.clone(),
            resolver: store_resolver,
            timeout: None,
            max_complexity: None,
            max_depth: 100,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,
            load_manager: mock_query_load_manager(),
        };

        // Execute the subscription and expect at least one result to be
        // available in the result stream
        let stream = execute_subscription(Subscription { query }, options).unwrap();
        let results: Vec<_> = stream
            .take(1)
            .collect()
            .map(Result::<_, ()>::Ok)
            .compat()
            .timeout(Duration::from_secs(3))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            extract_data!(result.as_ref().clone()),
            Some(object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![("name", q::Value::String(String::from("John")))]),
                    object_value(vec![("name", q::Value::String(String::from("Lisa")))])
                ])
            )])),
        );
    })
}

#[test]
fn can_use_nested_filter() {
    run_test_sequentially(setup, |_, id| async move {
        let result = execute_query_document(
            &id,
            graphql_parser::parse_query(
                "
        query {
            musicians(orderBy: id) {
                name
                bands(where: { originalSongs: [\"s1\", \"s3\", \"s4\"] }) { id }
            }
        }
        ",
            )
            .expect("invalid test query"),
        )
        .await;

        assert_eq!(
            extract_data!(result).unwrap(),
            object_value(vec![(
                "musicians",
                q::Value::List(vec![
                    object_value(vec![
                        ("name", q::Value::String(String::from("John"))),
                        (
                            "bands",
                            q::Value::List(vec![object_value(vec![(
                                "id",
                                q::Value::String(String::from("b2"))
                            )])])
                        )
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Lisa"))),
                        ("bands", q::Value::List(vec![]))
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Tom"))),
                        (
                            "bands",
                            q::Value::List(vec![object_value(vec![
                                (("id", q::Value::String(String::from("b2"))))
                            ])])
                        )
                    ]),
                    object_value(vec![
                        ("name", q::Value::String(String::from("Valerie"))),
                        ("bands", q::Value::List(vec![]))
                    ])
                ])
            )])
        );
    })
}

async fn check_musicians_at(
    id: &SubgraphDeploymentId,
    query: &str,
    block_var: Option<(&str, q::Value)>,
    expected: Result<Vec<&str>, &str>,
    qid: &str,
) {
    let query = graphql_parser::parse_query(query).expect("invalid test query");
    let vars = block_var.map(|(name, value)| {
        let mut map = HashMap::new();
        map.insert(name.to_owned(), value);
        QueryVariables::new(map)
    });

    let result = execute_query_document_with_variables(id, query, vars).await;

    match expected {
        Ok(ids) => {
            let ids: Vec<_> = ids
                .into_iter()
                .map(|id| object_value(vec![("id", q::Value::String(String::from(id)))]))
                .collect();
            let expected = Some(object_value(vec![("musicians", q::Value::List(ids))]));
            let data = match result.to_result() {
                Err(errors) => panic!("unexpected error: {:?} ({})\n", errors, qid),
                Ok(data) => data,
            };
            assert_eq!(data, expected, "failed query: ({})", qid);
        }
        Err(msg) => {
            let errors = match result.to_result() {
                Err(errors) => errors,
                Ok(_) => panic!(
                    "expected error `{}` but got successful result ({})",
                    msg, qid
                ),
            };
            let actual = errors
                .first()
                .expect("we expect one error message")
                .to_string();

            assert!(
                actual.contains(msg),
                "expected error message `{}` but got {:?} ({})",
                msg,
                errors,
                qid
            );
        }
    }
}

#[test]
fn query_at_block() {
    run_test_sequentially(setup, |_, id| async move {
        use test_store::block_store::{
            FakeBlock, BLOCK_ONE, BLOCK_THREE, BLOCK_TWO, GENESIS_BLOCK,
        };

        async fn musicians_at(
            id: &SubgraphDeploymentId,
            block: &str,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query = format!("query {{ musicians(block: {{ {} }}) {{ id }} }}", block);
            check_musicians_at(id, &query, None, expected, qid).await;
        }

        fn hash(block: &FakeBlock) -> String {
            format!("hash : \"0x{}\"", block.hash)
        }

        const BLOCK_NOT_INDEXED: &str = "subgraph graphqlTestsQuery has only indexed \
         up to block number 1 and data for block number 7000 is therefore not yet available";
        const BLOCK_HASH_NOT_FOUND: &str = "no block with that hash found";

        musicians_at(&id, "number: 7000", Err(BLOCK_NOT_INDEXED), "n7000").await;
        musicians_at(&id, "number: 0", Ok(vec!["m1", "m2"]), "n0").await;
        musicians_at(&id, "number: 1", Ok(vec!["m1", "m2", "m3", "m4"]), "n1").await;

        musicians_at(&id, &hash(&*GENESIS_BLOCK), Ok(vec!["m1", "m2"]), "h0").await;
        musicians_at(
            &id,
            &hash(&*BLOCK_ONE),
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "h1",
        )
        .await;
        musicians_at(
            &id,
            &hash(&*BLOCK_TWO),
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "h2",
        )
        .await;
        musicians_at(&id, &hash(&*BLOCK_THREE), Err(BLOCK_HASH_NOT_FOUND), "h3").await;
    })
}

#[test]
fn query_at_block_with_vars() {
    run_test_sequentially(setup, |_, id| async move {
        use test_store::block_store::{
            FakeBlock, BLOCK_ONE, BLOCK_THREE, BLOCK_TWO, GENESIS_BLOCK,
        };

        async fn musicians_at_nr(
            id: &SubgraphDeploymentId,
            block: i32,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query = "query by_nr($block: Int!) { musicians(block: { number: $block }) { id } }";
            let number = q::Value::Int(q::Number::from(block));
            let var = Some(("block", number.clone()));

            check_musicians_at(id, query, var, expected.clone(), qid).await;

            let query = "query by_nr($block: Block_height!) { musicians(block: $block) { id } }";
            let mut map = BTreeMap::new();
            map.insert("number".to_owned(), number);
            let block = q::Value::Object(map);
            let var = Some(("block", block));

            check_musicians_at(id, query, var, expected, qid).await;
        }

        async fn musicians_at_hash(
            id: &SubgraphDeploymentId,
            block: &FakeBlock,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query =
                "query by_hash($block: String!) { musicians(block: { hash: $block }) { id } }";
            let var = Some(("block", q::Value::String(block.hash.to_owned())));

            check_musicians_at(id, query, var, expected, qid).await;
        }

        const BLOCK_NOT_INDEXED: &str = "subgraph graphqlTestsQuery has only indexed \
         up to block number 1 and data for block number 7000 is therefore not yet available";
        const BLOCK_HASH_NOT_FOUND: &str = "no block with that hash found";

        musicians_at_nr(&id, 7000, Err(BLOCK_NOT_INDEXED), "n7000").await;
        musicians_at_nr(&id, 0, Ok(vec!["m1", "m2"]), "n0").await;
        musicians_at_nr(&id, 1, Ok(vec!["m1", "m2", "m3", "m4"]), "n1").await;

        musicians_at_hash(&id, &GENESIS_BLOCK, Ok(vec!["m1", "m2"]), "h0").await;
        musicians_at_hash(&id, &BLOCK_ONE, Ok(vec!["m1", "m2", "m3", "m4"]), "h1").await;
        musicians_at_hash(&id, &BLOCK_TWO, Ok(vec!["m1", "m2", "m3", "m4"]), "h2").await;
        musicians_at_hash(&id, &BLOCK_THREE, Err(BLOCK_HASH_NOT_FOUND), "h3").await;
    })
}

#[test]
fn query_detects_reorg() {
    run_test_sequentially(setup, |_, id| async move {
        let query = "query { musician(id: \"m1\") { id } }";
        let query = graphql_parser::parse_query(query).expect("invalid test query");
        let state = STORE
            .deployment_state_from_id(id.clone())
            .expect("failed to get state");

        // When there is no revert, queries work fine
        let result = execute_query_document_with_state(&id, query.clone(), state.clone()).await;

        assert_eq!(
            extract_data!(result),
            Some(object!(musician: object!(id: "m1")))
        );

        // Revert one block
        STORE
            .revert_block_operations(id.clone(), BLOCK_ONE.clone(), GENESIS_PTR.clone())
            .unwrap();
        // A query is still fine since we implicitly query at block 0; we were
        // at block 1 when we got `state`, and reorged once by one block, which
        // can not affect block 0, and it's therefore ok to query at block 0
        // even with a concurrent reorg
        let result = execute_query_document_with_state(&id, query.clone(), state.clone()).await;
        assert_eq!(
            extract_data!(result),
            Some(object!(musician: object!(id: "m1")))
        );

        // We move the subgraph head forward, which will execute the query at block 1
        // But the state we have is also for block 1, but with a smaller reorg count
        // and we therefore report an error
        transact_entity_operations(&*STORE, id.clone(), BLOCK_ONE.clone(), vec![]).unwrap();
        let result = execute_query_document_with_state(&id, query.clone(), state).await;
        match result.to_result().unwrap_err()[0] {
            QueryError::ExecutionError(QueryExecutionError::DeploymentReverted) => { /* expected */
            }
            _ => panic!("unexpected error from block reorg"),
        }
    })
}

#[test]
fn can_query_meta() {
    run_test_sequentially(setup, |_, id| async move {
        // metadata for the latest block (block 1)
        let query = "query { _meta { deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query).expect("invalid test query");

        let result = execute_query_document(&id, query).await;
        let exp = object! {
            _meta: object! {
                block: object! {
                    hash: "0x8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
                    number: 1
                },
                deployment: "graphqlTestsQuery"
            },
        };
        assert_eq!(extract_data!(result), Some(exp));

        // metadata for block 0 by number
        let query = "query { _meta(block: { number: 0 }) { deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query).expect("invalid test query");

        let result = execute_query_document(&id, query).await;
        let exp = object! {
            _meta: object! {
                block: object! {
                    hash: q::Value::Null,
                    number: 0
                },
                deployment: "graphqlTestsQuery"
            },
        };
        assert_eq!(extract_data!(result), Some(exp));

        // metadata for block 0 by hash
        let query = "query { _meta(block: { hash: \"bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f\" }) { \
                                        deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query).expect("invalid test query");

        let result = execute_query_document(&id, query).await;
        let exp = object! {
            _meta: object! {
                block: object! {
                    hash: "0xbd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
                    number: 0
                },
                deployment: "graphqlTestsQuery"
            },
        };
        assert_eq!(extract_data!(result), Some(exp));

        // metadata for block 2, which is beyond what the subgraph has indexed
        let query = "query { _meta(block: { number: 2 }) { deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query).expect("invalid test query");

        let result = execute_query_document(&id, query).await;
        assert!(result.has_errors());
    })
}

#[macro_use]
extern crate pretty_assertions;

use graph::data::subgraph::schema::DeploymentCreate;
use graph::data::value::Object;
use graphql_parser::Pos;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
};

use graph::{
    components::store::DeploymentLocator,
    data::graphql::{object, object_value},
    data::subgraph::schema::SubgraphError,
    data::{
        query::{QueryResults, QueryTarget},
        subgraph::SubgraphFeature,
    },
    prelude::{
        futures03::stream::StreamExt, o, q, r, serde_json, slog, BlockPtr, DeploymentHash, Entity,
        EntityKey, EntityOperation, FutureExtension, GraphQlRunner as _, Logger, NodeId, Query,
        QueryError, QueryExecutionError, QueryResult, QueryStoreManager, QueryVariables, Schema,
        SubgraphManifest, SubgraphName, SubgraphStore, SubgraphVersionSwitchingMode, Subscription,
        SubscriptionError, Value,
    },
    semver::Version,
};
use graph_graphql::{prelude::*, subscription::execute_subscription};
use test_store::{
    deployment_state, execute_subgraph_query_with_complexity, execute_subgraph_query_with_deadline,
    result_size_metrics, revert_block, run_test_sequentially, transact_errors, Store, BLOCK_ONE,
    GENESIS_PTR, LOAD_MANAGER, LOGGER, METRICS_REGISTRY, STORE, SUBSCRIPTION_MANAGER,
};

const NETWORK_NAME: &str = "fake_network";

async fn setup(store: &Store) -> DeploymentLocator {
    setup_with_features(store, "graphqlTestsQuery", BTreeSet::new()).await
}

async fn setup_with_features(
    store: &Store,
    id: &str,
    features: BTreeSet<SubgraphFeature>,
) -> DeploymentLocator {
    use test_store::block_store::{self, BLOCK_ONE, BLOCK_TWO, GENESIS_BLOCK};

    let id = DeploymentHash::new(id).unwrap();

    let chain = vec![&*GENESIS_BLOCK, &*BLOCK_ONE, &*BLOCK_TWO];
    block_store::set_chain(chain, NETWORK_NAME);
    test_store::remove_subgraphs();

    let schema = test_schema(id.clone());
    let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
        id: id.clone(),
        spec_version: Version::new(1, 0, 0),
        features,
        description: None,
        repository: None,
        schema: schema.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
    };

    insert_test_entities(store.subgraph_store().as_ref(), manifest).await
}

fn test_schema(id: DeploymentHash) -> Schema {
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

async fn insert_test_entities(
    store: &impl SubgraphStore,
    manifest: SubgraphManifest<graph_chain_ethereum::Chain>,
) -> DeploymentLocator {
    let deployment = DeploymentCreate::new(&manifest, None);
    let name = SubgraphName::new("test/query").unwrap();
    let node_id = NodeId::new("test").unwrap();
    let deployment = store
        .create_subgraph_deployment(
            name,
            &manifest.schema,
            deployment,
            node_id,
            NETWORK_NAME.to_string(),
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

    async fn insert_at(entities: Vec<Entity>, deployment: &DeploymentLocator, block_ptr: BlockPtr) {
        let insert_ops = entities.into_iter().map(|data| EntityOperation::Set {
            key: EntityKey::data(
                deployment.hash.clone(),
                data.get("__typename").unwrap().clone().as_string().unwrap(),
                data.get("id").unwrap().clone().as_string().unwrap(),
            ),
            data,
        });

        test_store::transact_and_wait(
            &STORE.subgraph_store(),
            &deployment,
            block_ptr,
            insert_ops.collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    }

    insert_at(entities0, &deployment, GENESIS_PTR.clone()).await;
    insert_at(entities1, &deployment, BLOCK_ONE.clone()).await;
    deployment
}

async fn execute_query_document(id: &DeploymentHash, query: q::Document) -> QueryResult {
    execute_query_document_with_variables(id, query, None).await
}

async fn execute_query_document_with_variables(
    id: &DeploymentHash,
    query: q::Document,
    variables: Option<QueryVariables>,
) -> QueryResult {
    let runner = Arc::new(GraphQlRunner::new(
        &*LOGGER,
        STORE.clone(),
        SUBSCRIPTION_MANAGER.clone(),
        LOAD_MANAGER.clone(),
        METRICS_REGISTRY.clone(),
    ));
    let target = QueryTarget::Deployment(id.clone());
    let query = Query::new(query, variables);

    runner
        .run_query_with_complexity(query, target, None, None, None, None)
        .await
        .first()
        .unwrap()
        .duplicate()
}

async fn first_result(f: QueryResults) -> QueryResult {
    f.first().unwrap().duplicate()
}

/// Extract the data from a `QueryResult`, and panic if it has errors
macro_rules! extract_data {
    ($result: expr) => {
        match $result.to_result() {
            Err(errors) => panic!("Unexpected errors return for query: {:#?}", errors),
            Ok(data) => data,
        }
    };
}

#[test]
fn can_query_one_to_one_relationship() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
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
            .expect("Invalid test query")
            .into_static(),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![
                (
                    "musicians",
                    r::Value::List(vec![
                        object_value(vec![
                            ("name", r::Value::String(String::from("John"))),
                            (
                                "mainBand",
                                object_value(vec![(
                                    "name",
                                    r::Value::String(String::from("The Musicians")),
                                )]),
                            ),
                        ]),
                        object_value(vec![
                            ("name", r::Value::String(String::from("Lisa"))),
                            (
                                "mainBand",
                                object_value(vec![(
                                    "name",
                                    r::Value::String(String::from("The Musicians")),
                                )]),
                            ),
                        ]),
                        object_value(vec![
                            ("name", r::Value::String(String::from("Tom"))),
                            (
                                "mainBand",
                                object_value(vec![(
                                    "name",
                                    r::Value::String(String::from("The Amateurs")),
                                )]),
                            ),
                        ]),
                        object_value(vec![
                            ("name", r::Value::String(String::from("Valerie"))),
                            ("mainBand", r::Value::Null),
                        ]),
                    ])
                ),
                (
                    "songStats",
                    r::Value::List(vec![
                        object_value(vec![
                            ("id", r::Value::String(String::from("s1"))),
                            (
                                "song",
                                object_value(vec![
                                    ("id", r::Value::String(String::from("s1"))),
                                    ("title", r::Value::String(String::from("Cheesy Tune")))
                                ])
                            ),
                            ("played", r::Value::Int(10)),
                        ]),
                        object_value(vec![
                            ("id", r::Value::String(String::from("s2"))),
                            (
                                "song",
                                object_value(vec![
                                    ("id", r::Value::String(String::from("s2"))),
                                    ("title", r::Value::String(String::from("Rock Tune")))
                                ])
                            ),
                            ("played", r::Value::Int(15)),
                        ])
                    ])
                )
            ]))
        )
    })
}

#[test]
fn can_query_one_to_many_relationships_in_both_directions() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
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
            .expect("Invalid test query")
            .into_static(),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![
                        ("name", r::Value::String(String::from("John"))),
                        (
                            "writtenSongs",
                            r::Value::List(vec![
                                object_value(vec![
                                    ("title", r::Value::String(String::from("Cheesy Tune"))),
                                    (
                                        "writtenBy",
                                        object_value(vec![(
                                            "name",
                                            r::Value::String(String::from("John")),
                                        )]),
                                    ),
                                ]),
                                object_value(vec![
                                    ("title", r::Value::String(String::from("Pop Tune"))),
                                    (
                                        "writtenBy",
                                        object_value(vec![(
                                            "name",
                                            r::Value::String(String::from("John")),
                                        )]),
                                    ),
                                ]),
                            ]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Lisa"))),
                        (
                            "writtenSongs",
                            r::Value::List(vec![object_value(vec![
                                ("title", r::Value::String(String::from("Rock Tune"))),
                                (
                                    "writtenBy",
                                    object_value(vec![(
                                        "name",
                                        r::Value::String(String::from("Lisa")),
                                    )]),
                                ),
                            ])]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Tom"))),
                        (
                            "writtenSongs",
                            r::Value::List(vec![object_value(vec![
                                ("title", r::Value::String(String::from("Folk Tune"))),
                                (
                                    "writtenBy",
                                    object_value(vec![(
                                        "name",
                                        r::Value::String(String::from("Tom"))
                                    )]),
                                ),
                            ])]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Valerie"))),
                        ("writtenSongs", r::Value::List(vec![])),
                    ]),
                ]),
            )])),
        )
    })
}

#[test]
fn can_query_many_to_many_relationship() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
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
            .expect("Invalid test query")
            .into_static(),
        )
        .await;

        let the_musicians = object_value(vec![
            ("name", r::Value::String(String::from("The Musicians"))),
            (
                "members",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                ]),
            ),
        ]);

        let the_amateurs = object_value(vec![
            ("name", r::Value::String(String::from("The Amateurs"))),
            (
                "members",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                ]),
            ),
        ]);

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![
                        ("name", r::Value::String(String::from("John"))),
                        (
                            "bands",
                            r::Value::List(vec![the_musicians.clone(), the_amateurs.clone()]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Lisa"))),
                        ("bands", r::Value::List(vec![the_musicians.clone()])),
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Tom"))),
                        (
                            "bands",
                            r::Value::List(vec![the_musicians.clone(), the_amateurs.clone()]),
                        ),
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Valerie"))),
                        ("bands", r::Value::List(vec![])),
                    ]),
                ])
            )]))
        );
    })
}

#[test]
fn root_fragments_are_expanded() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = graphql_parser::parse_query(
            r#"
            fragment Musicians on Query {
                musicians(first: 100, where: { name: "Tom" }) {
                    name
                }
            }
            query MyQuery {
                ...Musicians
            }"#,
        )
        .expect("invalid test query")
        .into_static();

        let result = execute_query_document_with_variables(&deployment.hash, query, None).await;
        let exp = object! { musicians: vec![ object! { name: "Tom" }]};
        assert_eq!(extract_data!(result), Some(exp));
    })
}

#[test]
fn query_variables_are_used() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = graphql_parser::parse_query(
            "
        query musicians($where: Musician_filter!) {
          musicians(first: 100, where: $where) {
            name
          }
        }
    ",
        )
        .expect("invalid test query")
        .into_static();

        let result = execute_query_document_with_variables(
            &deployment.hash,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(
                    String::from("where"),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                )]
                .into_iter(),
            ))),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![object_value(vec![(
                    "name",
                    r::Value::String(String::from("Tom"))
                )])],)
            )]))
        );
    })
}

#[test]
fn skip_directive_works_with_query_variables() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
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
        .expect("invalid test query")
        .into_static();

        // Set variable $skip to true
        let result = execute_query_document_with_variables(
            &deployment.hash,
            query.clone(),
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("skip"), r::Value::Boolean(true))].into_iter(),
            ))),
        )
        .await;

        // Assert that only names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", r::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );

        // Set variable $skip to false
        let result = execute_query_document_with_variables(
            &deployment.hash,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("skip"), r::Value::Boolean(false))].into_iter(),
            ))),
        )
        .await;

        // Assert that IDs and names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![
                        ("id", r::Value::String(String::from("m1"))),
                        ("name", r::Value::String(String::from("John")))
                    ]),
                    object_value(vec![
                        ("id", r::Value::String(String::from("m2"))),
                        ("name", r::Value::String(String::from("Lisa")))
                    ]),
                    object_value(vec![
                        ("id", r::Value::String(String::from("m3"))),
                        ("name", r::Value::String(String::from("Tom")))
                    ]),
                    object_value(vec![
                        ("id", r::Value::String(String::from("m4"))),
                        ("name", r::Value::String(String::from("Valerie")))
                    ]),
                ],)
            )]))
        );
    })
}

#[test]
fn include_directive_works_with_query_variables() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
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
        .expect("invalid test query")
        .into_static();

        // Set variable $include to true
        let result = execute_query_document_with_variables(
            &deployment.hash,
            query.clone(),
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("include"), r::Value::Boolean(true))].into_iter(),
            ))),
        )
        .await;

        // Assert that IDs and names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![
                        ("id", r::Value::String(String::from("m1"))),
                        ("name", r::Value::String(String::from("John")))
                    ]),
                    object_value(vec![
                        ("id", r::Value::String(String::from("m2"))),
                        ("name", r::Value::String(String::from("Lisa")))
                    ]),
                    object_value(vec![
                        ("id", r::Value::String(String::from("m3"))),
                        ("name", r::Value::String(String::from("Tom")))
                    ]),
                    object_value(vec![
                        ("id", r::Value::String(String::from("m4"))),
                        ("name", r::Value::String(String::from("Valerie")))
                    ]),
                ],)
            )]))
        );

        // Set variable $include to false
        let result = execute_query_document_with_variables(
            &deployment.hash,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("include"), r::Value::Boolean(false))].into_iter(),
            ))),
        )
        .await;

        // Assert that only names are returned
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", r::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );
    })
}

#[test]
fn query_complexity() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = Query::new(
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
            .unwrap()
            .into_static(),
            None,
        );
        let max_complexity = Some(1_010_100);

        // This query is exactly at the maximum complexity.
        let hash2 = deployment.hash.clone();
        let result = first_result(
            execute_subgraph_query_with_complexity(query, hash2.into(), max_complexity).await,
        )
        .await;
        assert!(!result.has_errors());

        let query = Query::new(
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
            .unwrap()
            .into_static(),
            None,
        );

        // The extra introspection causes the complexity to go over.
        let result = first_result(
            execute_subgraph_query_with_complexity(query, deployment.hash.into(), max_complexity)
                .await,
        )
        .await;
        match result.to_result().unwrap_err()[0] {
            QueryError::ExecutionError(QueryExecutionError::TooComplex(1_010_200, _)) => (),
            _ => panic!("did not catch complexity"),
        };
    })
}

#[test]
fn query_complexity_subscriptions() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let logger = Logger::root(slog::Discard, o!());
        let store = STORE
            .clone()
            .query_store(deployment.hash.clone().into(), true)
            .await
            .unwrap();

        let query = Query::new(
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
            .unwrap()
            .into_static(),
            None,
        );
        let max_complexity = Some(1_010_100);
        let options = SubscriptionExecutionOptions {
            logger: logger.clone(),
            store: store.clone(),
            subscription_manager: SUBSCRIPTION_MANAGER.clone(),
            timeout: None,
            max_complexity,
            max_depth: 100,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,
            result_size: result_size_metrics(),
        };
        let schema = STORE.subgraph_store().api_schema(&deployment.hash).unwrap();

        // This query is exactly at the maximum complexity.
        // FIXME: Not collecting the stream because that will hang the test.
        let _ignore_stream =
            execute_subscription(Subscription { query }, schema.clone(), options).unwrap();

        let query = Query::new(
            graphql_parser::parse_query(
                "subscription {
                musicians(orderBy: id) {
                    name
                    t1: bands(first: 100, orderBy: id) {
                        name
                        members(first: 100, orderBy: id) {
                            name
                        }
                    }
                    t2: bands(first: 200, orderBy: id) {
                      name
                      members(first: 100, orderBy: id) {
                          name
                      }
                  }
                }
            }",
            )
            .unwrap()
            .into_static(),
            None,
        );

        let store = STORE
            .clone()
            .query_store(deployment.hash.into(), true)
            .await
            .unwrap();

        let options = SubscriptionExecutionOptions {
            logger,
            store,
            subscription_manager: SUBSCRIPTION_MANAGER.clone(),
            timeout: None,
            max_complexity,
            max_depth: 100,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,
            result_size: result_size_metrics(),
        };

        let result = execute_subscription(Subscription { query }, schema, options);

        match result {
            Err(SubscriptionError::GraphQLError(e)) => match &e[0] {
                QueryExecutionError::TooComplex(3_030_100, _) => (), // Expected
                e => panic!("did not catch complexity: {:?}", e),
            },
            _ => panic!("did not catch complexity"),
        }
    })
}

#[test]
fn instant_timeout() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = Query::new(
            graphql_parser::parse_query("query { musicians(first: 100) { name } }")
                .unwrap()
                .into_static(),
            None,
        );

        match first_result(
            execute_subgraph_query_with_deadline(
                query,
                deployment.hash.into(),
                Some(Instant::now()),
            )
            .await,
        )
        .await
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
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = graphql_parser::parse_query(
            "
        query musicians($orderDir: OrderDirection = desc) {
          bands(first: 2, orderBy: id, orderDirection: $orderDir) {
            id
          }
        }
    ",
        )
        .expect("invalid test query")
        .into_static();

        // Assert that missing variables are defaulted.
        let result = execute_query_document_with_variables(
            &deployment.hash,
            query.clone(),
            Some(QueryVariables::default()),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "bands",
                r::Value::List(vec![
                    object_value(vec![("id", r::Value::String(String::from("b2")))]),
                    object_value(vec![("id", r::Value::String(String::from("b1")))])
                ],)
            )]))
        );

        // Assert that null variables are not defaulted.
        let result = execute_query_document_with_variables(
            &deployment.hash,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("orderDir"), r::Value::Null)].into_iter(),
            ))),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "bands",
                r::Value::List(vec![
                    object_value(vec![("id", r::Value::String(String::from("b1")))]),
                    object_value(vec![("id", r::Value::String(String::from("b2")))])
                ],)
            )]))
        );
    })
}

#[test]
fn skip_is_nullable() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = graphql_parser::parse_query(
            "
        query musicians {
          musicians(orderBy: id, skip: null) {
            name
          }
        }
    ",
        )
        .expect("invalid test query")
        .into_static();

        let result = execute_query_document_with_variables(&deployment.hash, query, None).await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", r::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );
    })
}

#[test]
fn first_is_nullable() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = graphql_parser::parse_query(
            "
        query musicians {
          musicians(first: null, orderBy: id) {
            name
          }
        }
    ",
        )
        .expect("invalid test query")
        .into_static();

        let result = execute_query_document_with_variables(&deployment.hash, query, None).await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Lisa")))]),
                    object_value(vec![("name", r::Value::String(String::from("Tom")))]),
                    object_value(vec![("name", r::Value::String(String::from("Valerie")))]),
                ],)
            )]))
        );
    })
}

#[test]
fn nested_variable() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = graphql_parser::parse_query(
            "
        query musicians($name: String) {
          musicians(first: 100, where: { name: $name }) {
            name
          }
        }
    ",
        )
        .expect("invalid test query")
        .into_static();

        let result = execute_query_document_with_variables(
            &deployment.hash,
            query,
            Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("name"), r::Value::String("Lisa".to_string()))].into_iter(),
            ))),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![object_value(vec![(
                    "name",
                    r::Value::String(String::from("Lisa"))
                )])])
            )]))
        );
    })
}

#[test]
fn ambiguous_derived_from_result() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
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
        .expect("invalid test query")
        .into_static();

        let result = execute_query_document_with_variables(&deployment.hash, query, None).await;

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
            e => panic!("expected AmbiguousDerivedFromResult error, got {}", e),
        }
    })
}

#[test]
fn can_filter_by_relationship_fields() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
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
            .expect("invalid test query")
            .into_static(),
        )
        .await;

        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![
                (
                    "musicians",
                    r::Value::List(vec![object_value(vec![
                        ("id", r::Value::String(String::from("m3"))),
                        ("name", r::Value::String(String::from("Tom"))),
                        (
                            "mainBand",
                            object_value(vec![("id", r::Value::String(String::from("b2")))])
                        )
                    ])])
                ),
                (
                    "bands",
                    r::Value::List(vec![object_value(vec![
                        ("id", r::Value::String(String::from("b2"))),
                        ("name", r::Value::String(String::from("The Amateurs"))),
                        (
                            "originalSongs",
                            r::Value::List(vec![
                                object_value(vec![("id", r::Value::String(String::from("s1")))]),
                                object_value(vec![("id", r::Value::String(String::from("s3")))]),
                                object_value(vec![("id", r::Value::String(String::from("s4")))]),
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
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
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
            .expect("invalid test query")
            .into_static(),
        )
        .await;

        match &result.to_result().unwrap_err()[0] {
            // With validations
            QueryError::ExecutionError(QueryExecutionError::ValidationError(_, error_message)) => {
                assert_eq!(
                    error_message,
                    "Field \"writtenSongs\" is not defined by type \"Musician_filter\"."
                );
            }
            // Without validations
            QueryError::ExecutionError(QueryExecutionError::InvalidArgumentError(
                _pos,
                error_message,
                _value,
            )) => {
                assert_eq!(error_message, "where");
            }
            e => panic!("expected a runtime/validation error, got {:?}", e),
        };
    })
}

#[test]
fn subscription_gets_result_even_without_events() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let logger = Logger::root(slog::Discard, o!());
        let store = STORE
            .clone()
            .query_store(deployment.hash.clone().into(), true)
            .await
            .unwrap();
        let schema = STORE.subgraph_store().api_schema(&deployment.hash).unwrap();

        let query = Query::new(
            graphql_parser::parse_query(
                "subscription {
              musicians(orderBy: id, first: 2) {
                name
              }
            }",
            )
            .unwrap()
            .into_static(),
            None,
        );

        let options = SubscriptionExecutionOptions {
            logger: logger.clone(),
            store,
            subscription_manager: SUBSCRIPTION_MANAGER.clone(),
            timeout: None,
            max_complexity: None,
            max_depth: 100,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,
            result_size: result_size_metrics(),
        };
        // Execute the subscription and expect at least one result to be
        // available in the result stream
        let stream = execute_subscription(Subscription { query }, schema, options).unwrap();
        let results: Vec<_> = stream
            .take(1)
            .collect()
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let result = Arc::try_unwrap(results.into_iter().next().unwrap()).unwrap();
        assert_eq!(
            extract_data!(result),
            Some(object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![("name", r::Value::String(String::from("John")))]),
                    object_value(vec![("name", r::Value::String(String::from("Lisa")))])
                ])
            )])),
        );
    })
}

#[test]
fn can_use_nested_filter() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
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
            .expect("invalid test query")
            .into_static(),
        )
        .await;

        assert_eq!(
            extract_data!(result).unwrap(),
            object_value(vec![(
                "musicians",
                r::Value::List(vec![
                    object_value(vec![
                        ("name", r::Value::String(String::from("John"))),
                        (
                            "bands",
                            r::Value::List(vec![object_value(vec![(
                                "id",
                                r::Value::String(String::from("b2"))
                            )])])
                        )
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Lisa"))),
                        ("bands", r::Value::List(vec![]))
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Tom"))),
                        (
                            "bands",
                            r::Value::List(vec![object_value(vec![
                                (("id", r::Value::String(String::from("b2"))))
                            ])])
                        )
                    ]),
                    object_value(vec![
                        ("name", r::Value::String(String::from("Valerie"))),
                        ("bands", r::Value::List(vec![]))
                    ])
                ])
            )])
        );
    })
}

// see: graphql-bug-compat
#[test]
fn ignores_invalid_field_arguments() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        // This query has to return all the musicians since `id` is not a
        // valid argument for the `musicians` field and must therefore be
        // ignored
        let result = execute_query_document(
            &deployment.hash,
            graphql_parser::parse_query("query { musicians(id: \"m1\") { id } } ")
                .expect("invalid test query")
                .into_static(),
        )
        .await;

        match &result.to_result() {
            // Without validations
            Ok(Some(r::Value::Object(obj))) => match obj.get("musicians").unwrap() {
                r::Value::List(lst) => {
                    assert_eq!(4, lst.len());
                }
                _ => panic!("expected a list of values"),
            },
            // With validations
            Err(e) => {
                match e.get(0).unwrap() {
                    QueryError::ExecutionError(QueryExecutionError::ValidationError(
                        _pos,
                        message,
                    )) => {
                        assert_eq!(
                            message,
                            "Unknown argument \"id\" on field \"Query.musicians\"."
                        );
                    }
                    r => panic!("unexpexted query error: {:?}", r),
                };
            }
            r => {
                panic!("unexpexted result: {:?}", r);
            }
        }
    })
}

// see: graphql-bug-compat
#[test]
fn leaf_selection_mismatch() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
            // 'name' is a string and doesn't admit a selection
            graphql_parser::parse_query("query { musician(id: \"m1\") { id name { wat }} } ")
                .expect("invalid test query")
                .into_static(),
        )
        .await;

        let exp = object! {
            musician: object! {
                id: "m1",
                name: "John"
            }
        };

        match &result.to_result() {
            // Without validations
            Ok(Some(data)) => {
                assert_eq!(exp, *data);
            }
            // With validations
            Err(e) => {
                match e.get(0).unwrap() {
                    QueryError::ExecutionError(QueryExecutionError::ValidationError(
                        _pos,
                        message,
                    )) => {
                        assert_eq!(message, "Field \"name\" must not have a selection since type \"String!\" has no subfields.");
                    }
                    r => panic!("unexpexted query error: {:?}", r),
                };
                match e.get(1).unwrap() {
                    QueryError::ExecutionError(QueryExecutionError::ValidationError(
                        _pos,
                        message,
                    )) => {
                        assert_eq!(message, "Cannot query field \"wat\" on type \"String\".");
                    }
                    r => panic!("unexpexted query error: {:?}", r),
                }
            }
            r => {
                panic!("unexpexted result: {:?}", r);
            }
        }

        let result = execute_query_document(
            &deployment.hash,
            // 'mainBand' is an object and requires a selection; it is ignored
            graphql_parser::parse_query("query { musician(id: \"m1\") { id name mainBand } } ")
                .expect("invalid test query")
                .into_static(),
        )
        .await;

        match &result.to_result() {
            // Without validations
            Ok(Some(data)) => {
                assert_eq!(exp, *data);
            }
            // With validations
            Err(e) => {
                match e.get(0).unwrap() {
                    QueryError::ExecutionError(QueryExecutionError::ValidationError(
                        _pos,
                        message,
                    )) => {
                        assert_eq!(message, "Field \"mainBand\" of type \"Band\" must have a selection of subfields. Did you mean \"mainBand { ... }\"?");
                    }
                    r => panic!("unexpexted query error: {:?}", r),
                };
            }
            r => {
                panic!("unexpexted result: {:?}", r);
            }
        }
    })
}

// see: graphql-bug-compat
#[test]
fn missing_variable() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
            // '$first' is not defined, use its default from the schema
            graphql_parser::parse_query("query { musicians(first: $first) { id } }")
                .expect("invalid test query")
                .into_static(),
        )
        .await;

        let exp = object! {
            musicians: vec![
                object! { id: "m1" },
                object! { id: "m2" },
                object! { id: "m3" },
                object! { id: "m4" },
            ]
        };

        match &result.to_result() {
            // We silently set `$first` to 100 and `$skip` to 0, and therefore
            Ok(Some(data)) => {
                assert_eq!(exp, *data);
            }
            // With GraphQL validations active, this query fails
            Err(e) => match e.get(0).unwrap() {
                QueryError::ExecutionError(QueryExecutionError::ValidationError(_pos, message)) => {
                    assert_eq!(message, "Variable \"$first\" is not defined.");
                }
                r => panic!("unexpexted query error: {:?}", r),
            },
            r => {
                panic!("unexpexted result: {:?}", r);
            }
        }

        let result = execute_query_document(
            &deployment.hash,
            // '$where' is not defined but nullable, ignore the argument
            graphql_parser::parse_query("query { musicians(where: $where) { id } }")
                .expect("invalid test query")
                .into_static(),
        )
        .await;

        match &result.to_result() {
            // '$where' is not defined but nullable, ignore the argument
            Ok(Some(data)) => {
                assert_eq!(exp, *data);
            }
            // With GraphQL validations active, this query fails
            Err(e) => match e.get(0).unwrap() {
                QueryError::ExecutionError(QueryExecutionError::ValidationError(_pos, message)) => {
                    assert_eq!(message, "Variable \"$where\" is not defined.");
                }
                r => panic!("unexpexted query error: {:?}", r),
            },
            r => {
                panic!("unexpexted result: {:?}", r);
            }
        }
    })
}

// see: graphql-bug-compat
// Test that queries with nonmergeable fields do not cause a panic. Can be
// deleted once queries are validated
#[test]
fn invalid_field_merge() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(
            &deployment.hash,
            graphql_parser::parse_query("query { musicians { t: id t: mainBand { id } } }")
                .expect("invalid test query")
                .into_static(),
        )
        .await;
        assert!(result.has_errors());
    })
}

async fn check_musicians_at(
    id: &DeploymentHash,
    query: &str,
    block_var: Option<(&str, r::Value)>,
    expected: Result<Vec<&str>, &str>,
    qid: &str,
) {
    let query = graphql_parser::parse_query(query)
        .expect("invalid test query")
        .into_static();
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
                .map(|id| object_value(vec![("id", r::Value::String(String::from(id)))]))
                .collect();
            let expected = Some(object_value(vec![("musicians", r::Value::List(ids))]));
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
    run_test_sequentially(|store| async move {
        use test_store::block_store::{
            FakeBlock, BLOCK_ONE, BLOCK_THREE, BLOCK_TWO, GENESIS_BLOCK,
        };

        async fn musicians_at(
            deployment: &DeploymentLocator,
            block: &str,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query = format!("query {{ musicians(block: {{ {} }}) {{ id }} }}", block);
            check_musicians_at(&deployment.hash, &query, None, expected, qid).await;
        }

        fn hash(block: &FakeBlock) -> String {
            format!("hash : \"0x{}\"", block.hash)
        }

        const BLOCK_NOT_INDEXED: &str = "subgraph graphqlTestsQuery has only indexed \
         up to block number 1 and data for block number 7000 is therefore not yet available";
        const BLOCK_HASH_NOT_FOUND: &str = "no block with that hash found";

        let deployment = setup(store.as_ref()).await;
        musicians_at(&deployment, "number: 7000", Err(BLOCK_NOT_INDEXED), "n7000").await;
        musicians_at(&deployment, "number: 0", Ok(vec!["m1", "m2"]), "n0").await;
        musicians_at(
            &deployment,
            "number: 1",
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "n1",
        )
        .await;

        musicians_at(
            &deployment,
            &hash(&*GENESIS_BLOCK),
            Ok(vec!["m1", "m2"]),
            "h0",
        )
        .await;
        musicians_at(
            &deployment,
            &hash(&*BLOCK_ONE),
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "h1",
        )
        .await;
        musicians_at(
            &deployment,
            &hash(&*BLOCK_TWO),
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "h2",
        )
        .await;
        musicians_at(
            &deployment,
            &hash(&*BLOCK_THREE),
            Err(BLOCK_HASH_NOT_FOUND),
            "h3",
        )
        .await;
    })
}

#[test]
fn query_at_block_with_vars() {
    run_test_sequentially(|store| async move {
        use test_store::block_store::{
            FakeBlock, BLOCK_ONE, BLOCK_THREE, BLOCK_TWO, GENESIS_BLOCK,
        };

        async fn musicians_at_nr(
            deployment: &DeploymentLocator,
            block: i32,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query = "query by_nr($block: Int!) { musicians(block: { number: $block }) { id } }";
            let number = r::Value::Int(block.into());
            let var = Some(("block", number.clone()));

            check_musicians_at(&deployment.hash, query, var, expected.clone(), qid).await;

            let query = "query by_nr($block: Block_height!) { musicians(block: $block) { id } }";
            let mut map = Object::new();
            map.insert("number".to_owned(), number);
            let block = r::Value::Object(map);
            let var = Some(("block", block));

            check_musicians_at(&deployment.hash, query, var, expected, qid).await;
        }

        async fn musicians_at_nr_gte(
            deployment: &DeploymentLocator,
            block: i32,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query =
                "query by_nr($block: Int!) { musicians(block: { number_gte: $block }) { id } }";
            let var = Some(("block", r::Value::Int(block.into())));

            check_musicians_at(&deployment.hash, query, var, expected, qid).await;
        }

        async fn musicians_at_hash(
            deployment: &DeploymentLocator,
            block: &FakeBlock,
            expected: Result<Vec<&str>, &str>,
            qid: &str,
        ) {
            let query =
                "query by_hash($block: Bytes!) { musicians(block: { hash: $block }) { id } }";
            let var = Some(("block", r::Value::String(block.hash.to_owned())));

            check_musicians_at(&deployment.hash, query, var, expected, qid).await;
        }

        const BLOCK_NOT_INDEXED: &str = "subgraph graphqlTestsQuery has only indexed \
         up to block number 1 and data for block number 7000 is therefore not yet available";
        const BLOCK_HASH_NOT_FOUND: &str = "no block with that hash found";

        let deployment = setup(store.as_ref()).await;
        musicians_at_nr(&deployment, 7000, Err(BLOCK_NOT_INDEXED), "n7000").await;
        musicians_at_nr(&deployment, 0, Ok(vec!["m1", "m2"]), "n0").await;
        musicians_at_nr(&deployment, 1, Ok(vec!["m1", "m2", "m3", "m4"]), "n1").await;

        musicians_at_nr_gte(&deployment, 7000, Err(BLOCK_NOT_INDEXED), "ngte7000").await;
        musicians_at_nr_gte(&deployment, 0, Ok(vec!["m1", "m2", "m3", "m4"]), "ngte0").await;
        musicians_at_nr_gte(&deployment, 1, Ok(vec!["m1", "m2", "m3", "m4"]), "ngte1").await;

        musicians_at_hash(&deployment, &GENESIS_BLOCK, Ok(vec!["m1", "m2"]), "h0").await;
        musicians_at_hash(
            &deployment,
            &BLOCK_ONE,
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "h1",
        )
        .await;
        musicians_at_hash(
            &deployment,
            &BLOCK_TWO,
            Ok(vec!["m1", "m2", "m3", "m4"]),
            "h2",
        )
        .await;
        musicians_at_hash(&deployment, &BLOCK_THREE, Err(BLOCK_HASH_NOT_FOUND), "h3").await;
    })
}

#[test]
fn query_detects_reorg() {
    run_test_sequentially(|store| async move {
        let deployment = setup(store.as_ref()).await;
        let query = "query { musician(id: \"m1\") { id } }";
        let query = graphql_parser::parse_query(query)
            .expect("invalid test query")
            .into_static();
        let state = deployment_state(STORE.as_ref(), &deployment.hash).await;

        // Inject a fake initial state; c435c25decbc4ad7bbbadf8e0ced0ff2
        *graph_graphql::test_support::INITIAL_DEPLOYMENT_STATE_FOR_TESTS
            .lock()
            .unwrap() = Some(state);

        // When there is no revert, queries work fine
        let result = execute_query_document(&deployment.hash, query.clone()).await;

        assert_eq!(
            extract_data!(result),
            Some(object!(musician: object!(id: "m1")))
        );

        // Revert one block
        revert_block(&*STORE, &deployment, &*GENESIS_PTR).await;
        // A query is still fine since we implicitly query at block 0; we were
        // at block 1 when we got `state`, and reorged once by one block, which
        // can not affect block 0, and it's therefore ok to query at block 0
        // even with a concurrent reorg
        let result = execute_query_document(&deployment.hash, query.clone()).await;
        assert_eq!(
            extract_data!(result),
            Some(object!(musician: object!(id: "m1")))
        );

        // We move the subgraph head forward, which will execute the query at block 1
        // But the state we have is also for block 1, but with a smaller reorg count
        // and we therefore report an error
        test_store::transact_and_wait(
            &STORE.subgraph_store(),
            &deployment,
            BLOCK_ONE.clone(),
            vec![],
        )
        .await
        .unwrap();

        let result = execute_query_document(&deployment.hash, query.clone()).await;
        match result.to_result().unwrap_err()[0] {
            QueryError::ExecutionError(QueryExecutionError::DeploymentReverted) => { /* expected */
            }
            _ => panic!("unexpected error from block reorg"),
        }

        // Reset the fake initial state; c435c25decbc4ad7bbbadf8e0ced0ff2
        *graph_graphql::test_support::INITIAL_DEPLOYMENT_STATE_FOR_TESTS
            .lock()
            .unwrap() = None;
    })
}

#[test]
fn can_query_meta() {
    run_test_sequentially(|store| async move {
        // metadata for the latest block (block 1)
        let query = "query { _meta { deployment block { hash number __typename } __typename } }";
        let query = graphql_parser::parse_query(query)
            .expect("invalid test query")
            .into_static();

        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(&deployment.hash, query).await;
        let exp = object! {
            _meta: object! {
                deployment: "graphqlTestsQuery",
                block: object! {
                    hash: "0x8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
                    number: 1,
                    __typename: "_Block_"
                },
                __typename: "_Meta_"
            },
        };
        assert_eq!(extract_data!(result), Some(exp));

        // metadata for block 0 by number
        let query = "query { _meta(block: { number: 0 }) { deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query)
            .expect("invalid test query")
            .into_static();

        let result = execute_query_document(&deployment.hash, query).await;
        let exp = object! {
            _meta: object! {
                deployment: "graphqlTestsQuery",
                block: object! {
                    hash: r::Value::Null,
                    number: 0
                },
            },
        };
        assert_eq!(extract_data!(result), Some(exp));

        // metadata for block 0 by hash
        let query = "query { _meta(block: { hash: \"bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f\" }) { \
                                        deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query)
            .expect("invalid test query")
            .into_static();

        let result = execute_query_document(&deployment.hash, query).await;
        let exp = object! {
            _meta: object! {
                deployment: "graphqlTestsQuery",
                block: object! {
                    hash: "0xbd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
                    number: 0
                },
            },
        };
        assert_eq!(extract_data!(result), Some(exp));

        // metadata for block 2, which is beyond what the subgraph has indexed
        let query = "query { _meta(block: { number: 2 }) { deployment block { hash number } } }";
        let query = graphql_parser::parse_query(query)
            .expect("invalid test query")
            .into_static();

        let result = execute_query_document(&deployment.hash, query).await;
        assert!(result.has_errors());
    })
}

#[test]
fn non_fatal_errors() {
    use serde_json::json;
    use test_store::block_store::BLOCK_TWO;

    run_test_sequentially(|store| async move {
        let deployment = setup_with_features(
            store.as_ref(),
            "testNonFatalErrors",
            BTreeSet::from_iter(Some(SubgraphFeature::NonFatalErrors)),
        )
        .await;

        let err = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "cow template handler could not moo event transaction".to_string(),
            block_ptr: Some(BLOCK_TWO.block_ptr()),
            handler: Some("handleMoo".to_string()),
            deterministic: true,
        };

        transact_errors(&*STORE, &deployment, BLOCK_TWO.block_ptr(), vec![err])
            .await
            .unwrap();

        // `subgraphError` is implicitly `deny`, data is omitted.
        let query = "query { musician(id: \"m1\") { id } }";
        let query = graphql_parser::parse_query(query).unwrap().into_static();
        let result = execute_query_document(&deployment.hash, query).await;
        let expected = json!({
            "errors": [
                {
                    "message": "indexing_error"
                }
            ]
        });
        assert_eq!(expected, serde_json::to_value(&result).unwrap());

        // Same result for explicit `deny`.
        let query = "query { musician(id: \"m1\", subgraphError: deny) { id } }";
        let query = graphql_parser::parse_query(query).unwrap().into_static();
        let result = execute_query_document(&deployment.hash, query).await;
        assert_eq!(expected, serde_json::to_value(&result).unwrap());

        // But `_meta` is still returned.
        let query = "query { musician(id: \"m1\") { id }  _meta { hasIndexingErrors } }";
        let query = graphql_parser::parse_query(query).unwrap().into_static();
        let result = execute_query_document(&deployment.hash, query).await;
        let expected = json!({
            "data": {
                "_meta": {
                    "hasIndexingErrors": true
                }
            },
            "errors": [
                {
                    "message": "indexing_error"
                }
            ]
        });
        assert_eq!(expected, serde_json::to_value(&result).unwrap());

        // With `allow`, the error remains but the data is included.
        let query = "query { musician(id: \"m1\", subgraphError: allow) { id } }";
        let query = graphql_parser::parse_query(query).unwrap().into_static();
        let result = execute_query_document(&deployment.hash, query).await;
        let expected = json!({
            "data": {
                "musician": {
                    "id": "m1"
                }
            },
            "errors": [
                {
                    "message": "indexing_error"
                }
            ]
        });
        assert_eq!(expected, serde_json::to_value(&result).unwrap());

        // Test error reverts.
        revert_block(&*STORE, &deployment, &*BLOCK_ONE).await;
        let query = "query { musician(id: \"m1\") { id }  _meta { hasIndexingErrors } }";
        let query = graphql_parser::parse_query(query).unwrap().into_static();
        let result = execute_query_document(&deployment.hash, query).await;
        let expected = json!({
            "data": {
                "musician": {
                    "id": "m1"
                },
                "_meta": {
                    "hasIndexingErrors": false
                }
            }
        });
        assert_eq!(expected, serde_json::to_value(&result).unwrap());
    })
}

#[test]
fn can_query_root_typename() {
    run_test_sequentially(|store| async move {
        let query = "query { __typename }";
        let query = graphql_parser::parse_query(query)
            .expect("invalid test query")
            .into_static();
        let deployment = setup(store.as_ref()).await;
        let result = execute_query_document(&deployment.hash, query).await;
        let exp = object! {
            __typename: "Query"
        };
        assert_eq!(extract_data!(result), Some(exp));
    })
}

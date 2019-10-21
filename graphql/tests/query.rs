#[macro_use]
extern crate pretty_assertions;

use graphql_parser::{query as q, Pos};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use graph::prelude::*;
use graph_graphql::prelude::*;
use test_store::{transact_entity_operations, GENESIS_PTR, STORE};

lazy_static! {
    static ref TEST_SUBGRAPH_ID: SubgraphDeploymentId = {
        // Also populate the store when the ID is first accessed.
        let id = SubgraphDeploymentId::new("graphqlTestsQuery").unwrap();
        insert_test_entities(&**STORE, id.clone());
        id
    };
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

fn api_test_schema() -> Schema {
    let mut schema = test_schema(TEST_SUBGRAPH_ID.clone());
    schema.document = api_schema(&schema.document).expect("Failed to derive API schema");
    schema.add_subgraph_id_directives(TEST_SUBGRAPH_ID.clone());
    schema
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
        templates: vec![],
    };

    let ops = SubgraphDeploymentEntity::new(&manifest, false, false, None, None)
        .create_operations_replace(&id)
        .into_iter()
        .map(|op| op.into())
        .collect();
    store.create_subgraph_deployment(&schema, ops).unwrap();

    let entities = vec![
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
        GENESIS_PTR.clone(),
        insert_ops.collect::<Vec<_>>(),
    )
    .unwrap();
}

fn execute_query_document(query: q::Document) -> QueryResult {
    execute_query_document_with_variables(query, None)
}

fn execute_query_document_with_variables(
    query: q::Document,
    variables: Option<QueryVariables>,
) -> QueryResult {
    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: query,
        variables,
    };

    let logger = Logger::root(slog::Discard, o!());
    let store_resolver = StoreResolver::new(&logger, STORE.clone());

    let options = QueryExecutionOptions {
        logger: logger,
        resolver: store_resolver,
        deadline: None,
        max_complexity: None,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    execute_query(&query, options)
}

#[test]
fn can_query_one_to_one_relationship() {
    let result = execute_query_document(
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
    );

    assert!(
        result.errors.is_none(),
        format!("Unexpected errors return for query: {:#?}", result.errors)
    );

    assert_eq!(
        result.data,
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
            ),
        ]))
    )
}

#[test]
fn can_query_one_to_many_relationships_in_both_directions() {
    let result = execute_query_document(
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
    );

    assert!(
        result.errors.is_none(),
        format!("Unexpected errors return for query: {:#?}", result.errors)
    );

    assert_eq!(
        result.data,
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
                                object_value(vec![("name", q::Value::String(String::from("Tom")))]),
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
}

#[test]
fn can_query_many_to_many_relationship() {
    let result = execute_query_document(
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
    );

    assert!(
        result.errors.is_none(),
        format!("Unexpected errors return for query: {:#?}", result.errors)
    );

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
        result.data,
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
            ]),
        )]))
    );
}

#[test]
fn query_variables_are_used() {
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
        query,
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(
                String::from("where"),
                object_value(vec![("name", q::Value::String(String::from("Tom")))]),
            )]
            .into_iter(),
        ))),
    );

    assert_eq!(
        result.data,
        Some(object_value(vec![(
            "musicians",
            q::Value::List(vec![object_value(vec![(
                "name",
                q::Value::String(String::from("Tom"))
            )])],)
        )]))
    );
}

#[test]
fn skip_directive_works_with_query_variables() {
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
        query.clone(),
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(String::from("skip"), q::Value::Boolean(true))].into_iter(),
        ))),
    );

    // Assert that only names are returned
    assert_eq!(
        result.data,
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
        query,
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(String::from("skip"), q::Value::Boolean(false))].into_iter(),
        ))),
    );

    // Assert that IDs and names are returned
    assert_eq!(
        result.data,
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
}

#[test]
fn include_directive_works_with_query_variables() {
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
        query.clone(),
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(String::from("include"), q::Value::Boolean(true))].into_iter(),
        ))),
    );

    // Assert that IDs and names are returned
    assert_eq!(
        result.data,
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
        query,
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(String::from("include"), q::Value::Boolean(false))].into_iter(),
        ))),
    );

    // Assert that only names are returned
    assert_eq!(
        result.data,
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
}

#[test]
fn query_complexity() {
    let logger = Logger::root(slog::Discard, o!());
    let store_resolver = StoreResolver::new(&logger, STORE.clone());

    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: graphql_parser::parse_query(
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
        variables: None,
    };
    let max_complexity = Some(1_010_100);
    let options = QueryExecutionOptions {
        logger: logger.clone(),
        resolver: store_resolver.clone(),
        deadline: None,
        max_complexity,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    // This query is exactly at the maximum complexity.
    let result = execute_query(&query, options);
    assert!(result.errors.is_none());

    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: graphql_parser::parse_query(
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
        variables: None,
    };

    let options = QueryExecutionOptions {
        logger,
        resolver: store_resolver,
        deadline: None,
        max_complexity,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    // The extra introspection causes the complexity to go over.
    let result = execute_query(&query, options);
    match result.errors.unwrap()[0] {
        QueryError::ExecutionError(QueryExecutionError::TooComplex(1_010_200, _)) => (),
        _ => panic!("did not catch complexity"),
    };
}

#[test]
fn query_complexity_subscriptions() {
    let logger = Logger::root(slog::Discard, o!());
    let store_resolver = StoreResolver::new(&logger, STORE.clone());

    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: graphql_parser::parse_query(
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
        variables: None,
    };
    let max_complexity = Some(1_010_100);
    let options = SubscriptionExecutionOptions {
        logger: logger.clone(),
        resolver: store_resolver.clone(),
        timeout: None,
        max_complexity,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    // This query is exactly at the maximum complexity.
    execute_subscription(&Subscription { query }, options).unwrap();

    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: graphql_parser::parse_query(
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
        variables: None,
    };

    let options = SubscriptionExecutionOptions {
        logger,
        resolver: store_resolver,
        timeout: None,
        max_complexity,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    // The extra introspection causes the complexity to go over.
    let result = execute_subscription(&Subscription { query }, options);
    match result {
        Err(SubscriptionError::GraphQLError(e)) => match e[0] {
            QueryExecutionError::TooComplex(1_010_200, _) => (), // Expected
            _ => panic!("did not catch complexity"),
        },
        _ => panic!("did not catch complexity"),
    }
}

#[test]
fn instant_timeout() {
    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: graphql_parser::parse_query("query { musicians(first: 100) { name } }").unwrap(),
        variables: None,
    };
    let logger = Logger::root(slog::Discard, o!());
    let store_resolver = StoreResolver::new(&logger, STORE.clone());

    let options = QueryExecutionOptions {
        logger: logger,
        resolver: store_resolver,
        deadline: Some(Instant::now()),
        max_complexity: None,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    match execute_query(&query, options).errors.unwrap()[0] {
        QueryError::ExecutionError(QueryExecutionError::Timeout) => (), // Expected
        _ => panic!("did not time out"),
    };
}

#[test]
fn variable_defaults() {
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
    let result =
        execute_query_document_with_variables(query.clone(), Some(QueryVariables::default()));

    assert!(result.errors.is_none());
    assert_eq!(
        result.data,
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
        query,
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(String::from("orderDir"), q::Value::Null)].into_iter(),
        ))),
    );

    assert!(result.errors.is_none());
    assert_eq!(
        result.data,
        Some(object_value(vec![(
            "bands",
            q::Value::List(vec![
                object_value(vec![("id", q::Value::String(String::from("b1")))]),
                object_value(vec![("id", q::Value::String(String::from("b2")))])
            ],)
        )]))
    );
}

#[test]
fn skip_is_nullable() {
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

    let result = execute_query_document_with_variables(query, None);

    assert_eq!(
        result.data,
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
}

#[test]
fn first_is_nullable() {
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

    let result = execute_query_document_with_variables(query, None);

    assert_eq!(
        result.data,
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
}

#[test]
fn nested_variable() {
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
        query,
        Some(QueryVariables::new(HashMap::from_iter(
            vec![(String::from("name"), q::Value::String("Lisa".to_string()))].into_iter(),
        ))),
    );

    assert!(result.errors.is_none());
    assert_eq!(
        result.data,
        Some(object_value(vec![(
            "musicians",
            q::Value::List(vec![object_value(vec![(
                "name",
                q::Value::String(String::from("Lisa"))
            )]),],)
        )]))
    );
}

#[test]
fn ambiguous_derived_from_result() {
    let query = graphql_parser::parse_query(
        "
        {
          songs(first: 100, orderBy: id) {
            id
            band
          }
        }
        ",
    )
    .expect("invalid test query");

    let result = execute_query_document_with_variables(query, None);

    assert!(result.errors.is_some());
    match &result.errors.unwrap()[0] {
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
}

#[test]
fn can_filter_by_relationship_fields() {
    let result = execute_query_document(
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
    );

    assert!(
        result.errors.is_none(),
        format!("Unexpected errors return for query: {:#?}", result.errors)
    );
    assert_eq!(
        result.data,
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
}

#[test]
fn cannot_filter_by_derved_relationship_fields() {
    let result = execute_query_document(
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
    );

    assert!(result.errors.is_some());
    match &result.errors.unwrap()[0] {
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
}

#[test]
fn subscription_gets_result_even_without_events() {
    let logger = Logger::root(slog::Discard, o!());
    let store_resolver = StoreResolver::new(&logger, STORE.clone());

    let query = Query {
        schema: Arc::new(api_test_schema()),
        document: graphql_parser::parse_query(
            "subscription {
              musicians(orderBy: id, first: 2) {
                name
              }
            }",
        )
        .unwrap(),
        variables: None,
    };

    let options = SubscriptionExecutionOptions {
        logger: logger.clone(),
        resolver: store_resolver.clone(),
        timeout: None,
        max_complexity: None,
        max_depth: 100,
        max_first: std::u32::MAX,
    };

    // Execute the subscription and expect at least one result to be
    // available in the result stream
    let stream = execute_subscription(&Subscription { query }, options).unwrap();
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let results = runtime
        .block_on(stream.take(1).collect().timeout(Duration::from_secs(3)))
        .unwrap();

    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert!(result.errors.is_none());
    assert!(result.data.is_some());
    assert_eq!(
        result.data,
        Some(object_value(vec![(
            "musicians",
            q::Value::List(vec![
                object_value(vec![("name", q::Value::String(String::from("John")))]),
                object_value(vec![("name", q::Value::String(String::from("Lisa")))])
            ])
        )])),
    );
}

#[test]
fn can_use_nested_filter() {
    let result = execute_query_document(
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
    );

    assert_eq!(
        result.data.unwrap(),
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
    )
}

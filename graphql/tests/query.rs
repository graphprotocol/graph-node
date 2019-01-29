extern crate failure;
extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate pretty_assertions;
extern crate graph;
extern crate graph_core;
extern crate graph_graphql;

use graph::prelude::*;
use graph_graphql::prelude::*;
use graphql_parser::query as q;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::Instant;

fn test_schema() -> Schema {
    let mut schema = Schema::parse(
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
            }

            type Song @entity {
                id: ID!
                title: String!
                writtenBy: Musician!
            }
            ",
        SubgraphDeploymentId::new("testschema").unwrap(),
    )
    .expect("Test schema invalid");

    schema.document =
        api_schema(&schema.document).expect("Failed to derive API schema from test schema");
    schema
}

#[derive(Clone)]
struct TestStore {
    entities: Vec<Entity>,
}

impl TestStore {
    pub fn new() -> Self {
        TestStore {
            entities: vec![
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
                ]),
                Entity::from(vec![
                    ("__typename", Value::from("Band")),
                    ("id", Value::from("b2")),
                    ("name", Value::from("The Amateurs")),
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
            ],
        }
    }
}

impl Store for TestStore {
    fn block_ptr(&self, _: SubgraphDeploymentId) -> Result<EthereumBlockPointer, Error> {
        unimplemented!()
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn apply_entity_operations(
        &self,
        _: Vec<EntityOperation>,
        _: EventSource,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn build_entity_attribute_indexes(
        &self,
        _: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        unimplemented!()
    }

    fn transact_block_operations(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn revert_block_operations(
        &self,
        _: SubgraphDeploymentId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn subscribe(&self, _: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        unimplemented!()
    }

    fn count_entities(&self, _: SubgraphDeploymentId) -> Result<u64, Error> {
        Ok(1)
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        self.entities
            .iter()
            .find(|entity| {
                entity.get("id") == Some(&Value::String(key.entity_id.clone()))
                    && entity.get("__typename") == Some(&Value::String(key.entity_type.clone()))
            })
            .map_or(
                Err(QueryExecutionError::ResolveEntitiesError(String::from(
                    "Mock get query error",
                ))),
                |entity| Ok(Some(entity.clone())),
            )
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        let entity_name = Value::String(query.entity_types[0].clone());

        let entities = self
            .entities
            .iter()
            .filter(|entity| entity.get("__typename") == Some(&entity_name))
            // We're only supporting the following filters here to to test
            // the filters generated for reference fields and @derivedFrom fields:
            //
            // - And(Contains(...))
            // - And(Equal(...))
            // - And(Or([Equal(...), ...]))
            .filter(|entity| {
                query
                    .filter
                    .as_ref()
                    .and_then(|filter| match filter {
                        EntityFilter::And(filters) => filters.get(0),
                        _ => None,
                    })
                    .map(|filter| match filter {
                        EntityFilter::Equal(k, v) => entity.get(k) == Some(&v),
                        EntityFilter::Contains(k, v) => match entity.get(k) {
                            Some(Value::List(values)) => values.contains(v),
                            _ => false,
                        },
                        EntityFilter::Or(filters) => filters.iter().any(|filter| match filter {
                            EntityFilter::Equal(k, v) => entity.get(k) == Some(&v),
                            _ => unimplemented!(),
                        }),
                        _ => unimplemented!(),
                    })
                    .unwrap_or(true)
            })
            .map(|entity| entity.clone())
            .collect();

        Ok(entities)
    }

    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        Ok(self.find(query)?.pop())
    }
}

fn execute_query_document(query: q::Document) -> QueryResult {
    execute_query_document_with_variables(query, None)
}

fn execute_query_document_with_variables(
    query: q::Document,
    variables: Option<QueryVariables>,
) -> QueryResult {
    let query = Query {
        schema: test_schema(),
        document: query,
        variables,
    };

    let logger = Logger::root(slog::Discard, o!());
    let store = Arc::new(TestStore::new());
    let store_resolver = StoreResolver::new(&logger, store);

    let options = QueryExecutionOptions {
        logger: logger,
        resolver: store_resolver,
        deadline: None,
    };

    execute_query(&query, options)
}

#[test]
fn can_query_one_to_one_relationship() {
    let result = execute_query_document(
        graphql_parser::parse_query(
            "
            query {
                musicians {
                    name
                    mainBand {
                        name
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
            ]),
        )])),
    )
}

#[test]
fn can_query_one_to_many_relationships_in_both_directions() {
    let result = execute_query_document(
        graphql_parser::parse_query(
            "
        query {
            musicians {
                name
                writtenSongs {
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
                musicians {
                    name
                    bands {
                        name
                        members {
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
          musicians(where: $where) {
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
          musicians {
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
          musicians {
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
fn instant_timeout() {
    let query = Query {
        schema: test_schema(),
        document: graphql_parser::parse_query("query { musicians { name } }").unwrap(),
        variables: None,
    };
    let logger = Logger::root(slog::Discard, o!());
    let store = Arc::new(TestStore::new());
    let store_resolver = StoreResolver::new(&logger, store);

    let options = QueryExecutionOptions {
        logger: logger,
        resolver: store_resolver,
        deadline: Some(Instant::now()),
    };

    match execute_query(&query, options).errors.unwrap()[0] {
        QueryError::ExecutionError(QueryExecutionError::Timeout) => (), // Expected
        _ => panic!("did not time out"),
    };
}

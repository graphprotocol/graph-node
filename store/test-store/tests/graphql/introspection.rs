use std::sync::Arc;

use graph::data::graphql::{object, object_value, ObjectOrInterface};
use graph::data::query::Trace;
use graph::prelude::{
    async_trait, o, r, s, slog, tokio, DeploymentHash, Logger, Query, QueryExecutionError,
    QueryResult,
};
use graph::schema::{api_schema, ApiSchema, Schema};

use graph_graphql::prelude::{
    a, execute_query, ExecutionContext, Query as PreparedQuery, QueryExecutionOptions, Resolver,
};
use test_store::graphql_metrics;
use test_store::LOAD_MANAGER;

/// Mock resolver used in tests that don't need a resolver.
#[derive(Clone)]
pub struct MockResolver;

#[async_trait]
impl Resolver for MockResolver {
    const CACHEABLE: bool = false;

    fn prefetch(
        &self,
        _: &ExecutionContext<Self>,
        _: &a::SelectionSet,
    ) -> Result<(Option<r::Value>, Trace), Vec<QueryExecutionError>> {
        Ok((None, Trace::None))
    }

    async fn resolve_objects(
        &self,
        _: Option<r::Value>,
        _field: &a::Field,
        _field_definition: &s::Field,
        _object_type: ObjectOrInterface<'_>,
    ) -> Result<r::Value, QueryExecutionError> {
        Ok(r::Value::Null)
    }

    async fn resolve_object(
        &self,
        __: Option<r::Value>,
        _field: &a::Field,
        _field_definition: &s::Field,
        _object_type: ObjectOrInterface<'_>,
    ) -> Result<r::Value, QueryExecutionError> {
        Ok(r::Value::Null)
    }

    async fn query_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QueryExecutionError> {
        Ok(Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap())
    }
}

/// Creates a basic GraphQL schema that exercies scalars, directives,
/// enums, interfaces, input objects, object types and field arguments.
fn mock_schema() -> Schema {
    Schema::parse(
        "
             scalar ID
             scalar Int
             scalar String
             scalar Boolean

             directive @language(
               language: String = \"English\"
             ) on FIELD_DEFINITION

             enum Role {
               USER
               ADMIN
             }

             interface Node {
               id: ID!
             }

             type User implements Node @entity {
               id: ID!
               name: String! @language(language: \"English\")
               role: Role!
             }

             enum User_orderBy {
               id
               name
             }

             input User_filter {
               name_eq: String = \"default name\",
               name_not: String,
             }

             type Query @entity {
               allUsers(orderBy: User_orderBy, filter: User_filter): [User!]
               anyUserWithAge(age: Int = 99): User
               User: User
             }
             ",
        DeploymentHash::new("mockschema").unwrap(),
    )
    .unwrap()
}

/// Builds the expected result for GraphiQL's introspection query that we are
/// using for testing.
fn expected_mock_schema_introspection() -> r::Value {
    let string_type = object! {
        kind: r::Value::Enum("SCALAR".to_string()),
        name: "String",
        description: r::Value::Null,
        fields: r::Value::Null,
        inputFields: r::Value::Null,
        interfaces: r::Value::Null,
        enumValues: r::Value::Null,
        possibleTypes: r::Value::Null,
    };

    let id_type = object! {
        kind: r::Value::Enum("SCALAR".to_string()),
        name: "ID",
        description: r::Value::Null,
        fields: r::Value::Null,
        inputFields: r::Value::Null,
        interfaces: r::Value::Null,
        enumValues: r::Value::Null,
        possibleTypes: r::Value::Null,
    };

    let int_type = object! {
        kind: r::Value::Enum("SCALAR".to_string()),
        name: "Int",
        description: r::Value::Null,
        fields: r::Value::Null,
        inputFields: r::Value::Null,
        interfaces: r::Value::Null,
        enumValues: r::Value::Null,
        possibleTypes: r::Value::Null,
    };

    let boolean_type = object! {
        kind: r::Value::Enum("SCALAR".to_string()),
        name: "Boolean",
        description: r::Value::Null,
        fields: r::Value::Null,
        inputFields: r::Value::Null,
        interfaces: r::Value::Null,
        enumValues: r::Value::Null,
        possibleTypes: r::Value::Null,
    };

    let role_type = object! {
        kind: r::Value::Enum("ENUM".to_string()),
        name: "Role",
        description: r::Value::Null,
        fields: r::Value::Null,
        inputFields: r::Value::Null,
        interfaces: r::Value::Null,
        enumValues:
            r::Value::List(vec![
                object! {
                    name: "USER",
                    description: r::Value::Null,
                    isDeprecated: r::Value::Boolean(false),
                    deprecationReason: r::Value::Null,
                },
                object! {
                    name: "ADMIN",
                    description: r::Value::Null,
                    isDeprecated: false,
                    deprecationReason: r::Value::Null,
                },
            ]),
        possibleTypes: r::Value::Null,
    };

    let node_type = object_value(vec![
        ("kind", r::Value::Enum("INTERFACE".to_string())),
        ("name", r::Value::String("Node".to_string())),
        ("description", r::Value::Null),
        (
            "fields",
            r::Value::List(vec![object_value(vec![
                ("name", r::Value::String("id".to_string())),
                ("description", r::Value::Null),
                ("args", r::Value::List(vec![])),
                (
                    "type",
                    object_value(vec![
                        ("kind", r::Value::Enum("NON_NULL".to_string())),
                        ("name", r::Value::Null),
                        (
                            "ofType",
                            object_value(vec![
                                ("kind", r::Value::Enum("SCALAR".to_string())),
                                ("name", r::Value::String("ID".to_string())),
                                ("ofType", r::Value::Null),
                            ]),
                        ),
                    ]),
                ),
                ("isDeprecated", r::Value::Boolean(false)),
                ("deprecationReason", r::Value::Null),
            ])]),
        ),
        ("inputFields", r::Value::Null),
        ("interfaces", r::Value::Null),
        ("enumValues", r::Value::Null),
        (
            "possibleTypes",
            r::Value::List(vec![object_value(vec![
                ("kind", r::Value::Enum("OBJECT".to_string())),
                ("name", r::Value::String("User".to_string())),
                ("ofType", r::Value::Null),
            ])]),
        ),
    ]);

    let user_orderby_type = object_value(vec![
        ("kind", r::Value::Enum("ENUM".to_string())),
        ("name", r::Value::String("User_orderBy".to_string())),
        ("description", r::Value::Null),
        ("fields", r::Value::Null),
        ("inputFields", r::Value::Null),
        ("interfaces", r::Value::Null),
        (
            "enumValues",
            r::Value::List(vec![
                object_value(vec![
                    ("name", r::Value::String("id".to_string())),
                    ("description", r::Value::Null),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
                object_value(vec![
                    ("name", r::Value::String("name".to_string())),
                    ("description", r::Value::Null),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
            ]),
        ),
        ("possibleTypes", r::Value::Null),
    ]);

    let user_filter_type = object_value(vec![
        ("kind", r::Value::Enum("INPUT_OBJECT".to_string())),
        ("name", r::Value::String("User_filter".to_string())),
        ("description", r::Value::Null),
        ("fields", r::Value::Null),
        (
            "inputFields",
            r::Value::List(vec![
                object_value(vec![
                    ("name", r::Value::String("name_eq".to_string())),
                    ("description", r::Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("SCALAR".to_string())),
                            ("name", r::Value::String("String".to_string())),
                            ("ofType", r::Value::Null),
                        ]),
                    ),
                    (
                        "defaultValue",
                        r::Value::String("\"default name\"".to_string()),
                    ),
                ]),
                object_value(vec![
                    ("name", r::Value::String("name_not".to_string())),
                    ("description", r::Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("SCALAR".to_string())),
                            ("name", r::Value::String("String".to_string())),
                            ("ofType", r::Value::Null),
                        ]),
                    ),
                    ("defaultValue", r::Value::Null),
                ]),
            ]),
        ),
        ("interfaces", r::Value::Null),
        ("enumValues", r::Value::Null),
        ("possibleTypes", r::Value::Null),
    ]);

    let user_type = object_value(vec![
        ("kind", r::Value::Enum("OBJECT".to_string())),
        ("name", r::Value::String("User".to_string())),
        ("description", r::Value::Null),
        (
            "fields",
            r::Value::List(vec![
                object_value(vec![
                    ("name", r::Value::String("id".to_string())),
                    ("description", r::Value::Null),
                    ("args", r::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("NON_NULL".to_string())),
                            ("name", r::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", r::Value::Enum("SCALAR".to_string())),
                                    ("name", r::Value::String("ID".to_string())),
                                    ("ofType", r::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
                object_value(vec![
                    ("name", r::Value::String("name".to_string())),
                    ("description", r::Value::Null),
                    ("args", r::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("NON_NULL".to_string())),
                            ("name", r::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", r::Value::Enum("SCALAR".to_string())),
                                    ("name", r::Value::String("String".to_string())),
                                    ("ofType", r::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
                object_value(vec![
                    ("name", r::Value::String("role".to_string())),
                    ("description", r::Value::Null),
                    ("args", r::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("NON_NULL".to_string())),
                            ("name", r::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", r::Value::Enum("ENUM".to_string())),
                                    ("name", r::Value::String("Role".to_string())),
                                    ("ofType", r::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
            ]),
        ),
        ("inputFields", r::Value::Null),
        (
            "interfaces",
            r::Value::List(vec![object_value(vec![
                ("kind", r::Value::Enum("INTERFACE".to_string())),
                ("name", r::Value::String("Node".to_string())),
                ("ofType", r::Value::Null),
            ])]),
        ),
        ("enumValues", r::Value::Null),
        ("possibleTypes", r::Value::Null),
    ]);

    let query_type = object_value(vec![
        ("kind", r::Value::Enum("OBJECT".to_string())),
        ("name", r::Value::String("Query".to_string())),
        ("description", r::Value::Null),
        (
            "fields",
            r::Value::List(vec![
                object_value(vec![
                    ("name", r::Value::String("allUsers".to_string())),
                    ("description", r::Value::Null),
                    (
                        "args",
                        r::Value::List(vec![
                            object_value(vec![
                                ("name", r::Value::String("orderBy".to_string())),
                                ("description", r::Value::Null),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", r::Value::Enum("ENUM".to_string())),
                                        ("name", r::Value::String("User_orderBy".to_string())),
                                        ("ofType", r::Value::Null),
                                    ]),
                                ),
                                ("defaultValue", r::Value::Null),
                            ]),
                            object_value(vec![
                                ("name", r::Value::String("filter".to_string())),
                                ("description", r::Value::Null),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", r::Value::Enum("INPUT_OBJECT".to_string())),
                                        ("name", r::Value::String("User_filter".to_string())),
                                        ("ofType", r::Value::Null),
                                    ]),
                                ),
                                ("defaultValue", r::Value::Null),
                            ]),
                        ]),
                    ),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("LIST".to_string())),
                            ("name", r::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", r::Value::Enum("NON_NULL".to_string())),
                                    ("name", r::Value::Null),
                                    (
                                        "ofType",
                                        object_value(vec![
                                            ("kind", r::Value::Enum("OBJECT".to_string())),
                                            ("name", r::Value::String("User".to_string())),
                                            ("ofType", r::Value::Null),
                                        ]),
                                    ),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
                object_value(vec![
                    ("name", r::Value::String("anyUserWithAge".to_string())),
                    ("description", r::Value::Null),
                    (
                        "args",
                        r::Value::List(vec![object_value(vec![
                            ("name", r::Value::String("age".to_string())),
                            ("description", r::Value::Null),
                            (
                                "type",
                                object_value(vec![
                                    ("kind", r::Value::Enum("SCALAR".to_string())),
                                    ("name", r::Value::String("Int".to_string())),
                                    ("ofType", r::Value::Null),
                                ]),
                            ),
                            ("defaultValue", r::Value::String("99".to_string())),
                        ])]),
                    ),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("OBJECT".to_string())),
                            ("name", r::Value::String("User".to_string())),
                            ("ofType", r::Value::Null),
                        ]),
                    ),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
                object_value(vec![
                    ("name", r::Value::String("User".to_string())),
                    ("description", r::Value::Null),
                    ("args", r::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", r::Value::Enum("OBJECT".to_string())),
                            ("name", r::Value::String("User".to_string())),
                            ("ofType", r::Value::Null),
                        ]),
                    ),
                    ("isDeprecated", r::Value::Boolean(false)),
                    ("deprecationReason", r::Value::Null),
                ]),
            ]),
        ),
        ("inputFields", r::Value::Null),
        ("interfaces", r::Value::List(vec![])),
        ("enumValues", r::Value::Null),
        ("possibleTypes", r::Value::Null),
    ]);

    let expected_types = r::Value::List(vec![
        boolean_type,
        id_type,
        int_type,
        node_type,
        query_type,
        role_type,
        string_type,
        user_type,
        user_filter_type,
        user_orderby_type,
    ]);

    let expected_directives = r::Value::List(vec![object_value(vec![
        ("name", r::Value::String("language".to_string())),
        ("description", r::Value::Null),
        (
            "locations",
            r::Value::List(vec![r::Value::Enum(String::from("FIELD_DEFINITION"))]),
        ),
        (
            "args",
            r::Value::List(vec![object_value(vec![
                ("name", r::Value::String("language".to_string())),
                ("description", r::Value::Null),
                (
                    "type",
                    object_value(vec![
                        ("kind", r::Value::Enum("SCALAR".to_string())),
                        ("name", r::Value::String("String".to_string())),
                        ("ofType", r::Value::Null),
                    ]),
                ),
                ("defaultValue", r::Value::String("\"English\"".to_string())),
            ])]),
        ),
    ])]);

    let schema_type = object_value(vec![
        (
            "queryType",
            object_value(vec![("name", r::Value::String("Query".to_string()))]),
        ),
        ("mutationType", r::Value::Null),
        ("subscriptionType", r::Value::Null),
        ("types", expected_types),
        ("directives", expected_directives),
    ]);

    object_value(vec![("__schema", schema_type)])
}

/// Execute an introspection query.
async fn introspection_query(schema: Schema, query: &str) -> QueryResult {
    // Create the query
    let query = Query::new(
        graphql_parser::parse_query(query).unwrap().into_static(),
        None,
        false,
    );

    // Execute it
    let logger = Logger::root(slog::Discard, o!());
    let options = QueryExecutionOptions {
        resolver: MockResolver,
        deadline: None,
        max_first: std::u32::MAX,
        max_skip: std::u32::MAX,
        load_manager: LOAD_MANAGER.clone(),
        trace: false,
    };

    let schema = Arc::new(ApiSchema::from_api_schema(schema).unwrap());
    let result =
        match PreparedQuery::new(&logger, schema, None, query, None, 100, graphql_metrics()) {
            Ok(query) => {
                Ok(Arc::try_unwrap(execute_query(query, None, None, options).await).unwrap())
            }
            Err(e) => Err(e),
        };
    QueryResult::from(result)
}

#[tokio::test]
async fn satisfies_graphiql_introspection_query_without_fragments() {
    let result = introspection_query(
        mock_schema(),
        "
      query IntrospectionQuery {
        __schema {
          queryType { name }
          mutationType { name }
          subscriptionType { name}
          types {
            kind
            name
            description
            fields(includeDeprecated: true) {
              name
              description
              args {
                name
                description
                type {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                            ofType {
                              kind
                              name
                              ofType {
                                kind
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                defaultValue
              }
              type {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                            ofType {
                              kind
                              name
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              isDeprecated
              deprecationReason
            }
            inputFields {
              name
              description
              type {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                            ofType {
                              kind
                              name
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              defaultValue
            }
            interfaces {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            enumValues(includeDeprecated: true) {
              name
              description
              isDeprecated
              deprecationReason
            }
            possibleTypes {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                          }
                        }
                      }
                    }
                  }
                }
              }
           }
          }
          directives {
            name
            description
            locations
            args {
              name
              description
              type {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                            ofType {
                              kind
                              name
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              defaultValue
            }
          }
        }
      }
    ",
    )
    .await;

    let data = result
        .to_result()
        .expect("Introspection query returned no result")
        .unwrap();
    assert_eq!(data, expected_mock_schema_introspection());
}

#[tokio::test]
async fn satisfies_graphiql_introspection_query_with_fragments() {
    let result = introspection_query(
        mock_schema(),
        "
      query IntrospectionQuery {
        __schema {
          queryType { name }
          mutationType { name }
          subscriptionType { name }
          types {
            ...FullType
          }
          directives {
            name
            description
            locations
            args {
              ...InputValue
            }
          }
        }
      }

      fragment FullType on __Type {
        kind
        name
        description
        fields(includeDeprecated: true) {
          name
          description
          args {
            ...InputValue
          }
          type {
            ...TypeRef
          }
          isDeprecated
          deprecationReason
        }
        inputFields {
          ...InputValue
        }
        interfaces {
          ...TypeRef
        }
        enumValues(includeDeprecated: true) {
          name
          description
          isDeprecated
          deprecationReason
        }
        possibleTypes {
          ...TypeRef
        }
      }

      fragment InputValue on __InputValue {
        name
        description
        type { ...TypeRef }
        defaultValue
      }

      fragment TypeRef on __Type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                    }
                  }
                }
              }
            }
          }
        }
      }
    ",
    )
    .await;

    let data = result
        .to_result()
        .expect("Introspection query returned no result")
        .unwrap();
    assert_eq!(data, expected_mock_schema_introspection());
}

const COMPLEX_SCHEMA: &str = "
enum RegEntryStatus {
  regEntry_status_challengePeriod
  regEntry_status_commitPeriod
  regEntry_status_revealPeriod
  regEntry_status_blacklisted
  regEntry_status_whitelisted
}

interface RegEntry {
  regEntry_address: ID
  regEntry_version: Int
  regEntry_status: RegEntryStatus
  regEntry_creator: User
  regEntry_deposit: Int
  regEntry_createdOn: String
  regEntry_challengePeriodEnd: String
  challenge_challenger: User
  challenge_createdOn: String
  challenge_comment: String
  challenge_votingToken: String
  challenge_rewardPool: Int
  challenge_commitPeriodEnd: String
  challenge_revealPeriodEnd: String
  challenge_votesFor: Int
  challenge_votesAgainst: Int
  challenge_votesTotal: Int
  challenge_claimedRewardOn: String
  challenge_vote(vote_voter: ID!): Vote
}

enum VoteOption {
  voteOption_noVote
  voteOption_voteFor
  voteOption_voteAgainst
}

type Vote @entity {
  vote_secretHash: String
  vote_option: VoteOption
  vote_amount: Int
  vote_revealedOn: String
  vote_claimedRewardOn: String
  vote_reward: Int
}

type Meme implements RegEntry @entity {
  regEntry_address: ID
  regEntry_version: Int
  regEntry_status: RegEntryStatus
  regEntry_creator: User
  regEntry_deposit: Int
  regEntry_createdOn: String
  regEntry_challengePeriodEnd: String
  challenge_challenger: User
  challenge_createdOn: String
  challenge_comment: String
  challenge_votingToken: String
  challenge_rewardPool: Int
  challenge_commitPeriodEnd: String
  challenge_revealPeriodEnd: String
  challenge_votesFor: Int
  challenge_votesAgainst: Int
  challenge_votesTotal: Int
  challenge_claimedRewardOn: String
  challenge_vote(vote_voter: ID!): Vote
  # Balance of voting token of a voter. This is client-side only, server doesn't return this
  challenge_availableVoteAmount(voter: ID!): Int
  meme_title: String
  meme_number: Int
  meme_metaHash: String
  meme_imageHash: String
  meme_totalSupply: Int
  meme_totalMinted: Int
  meme_tokenIdStart: Int
  meme_totalTradeVolume: Int
  meme_totalTradeVolumeRank: Int
  meme_ownedMemeTokens(owner: String): [MemeToken]
  meme_tags: [Tag]
}

type Tag  @entity {
  tag_id: ID
  tag_name: String
}

type MemeToken @entity {
  memeToken_tokenId: ID
  memeToken_number: Int
  memeToken_owner: User
  memeToken_meme: Meme
}

enum MemeAuctionStatus {
  memeAuction_status_active
  memeAuction_status_canceled
  memeAuction_status_done
}

type MemeAuction @entity {
  memeAuction_address: ID
  memeAuction_seller: User
  memeAuction_buyer: User
  memeAuction_startPrice: Int
  memeAuction_endPrice: Int
  memeAuction_duration: Int
  memeAuction_startedOn: String
  memeAuction_boughtOn: String
  memeAuction_status: MemeAuctionStatus
  memeAuction_memeToken: MemeToken
}

type ParamChange implements RegEntry @entity {
  regEntry_address: ID
  regEntry_version: Int
  regEntry_status: RegEntryStatus
  regEntry_creator: User
  regEntry_deposit: Int
  regEntry_createdOn: String
  regEntry_challengePeriodEnd: String
  challenge_challenger: User
  challenge_createdOn: String
  challenge_comment: String
  challenge_votingToken: String
  challenge_rewardPool: Int
  challenge_commitPeriodEnd: String
  challenge_revealPeriodEnd: String
  challenge_votesFor: Int
  challenge_votesAgainst: Int
  challenge_votesTotal: Int
  challenge_claimedRewardOn: String
  challenge_vote(vote_voter: ID!): Vote
  # Balance of voting token of a voter. This is client-side only, server doesn't return this
  challenge_availableVoteAmount(voter: ID!): Int
  paramChange_db: String
  paramChange_key: String
  paramChange_value: Int
  paramChange_originalValue: Int
  paramChange_appliedOn: String
}

type User @entity {
  # Ethereum address of an user
  user_address: ID
  # Total number of memes submitted by user
  user_totalCreatedMemes: Int
  # Total number of memes submitted by user, which successfully got into TCR
  user_totalCreatedMemesWhitelisted: Int
  # Largest sale creator has done with his newly minted meme
  user_creatorLargestSale: MemeAuction
  # Position of a creator in leaderboard according to user_totalCreatedMemesWhitelisted
  user_creatorRank: Int
  # Amount of meme tokenIds owned by user
  user_totalCollectedTokenIds: Int
  # Amount of unique memes owned by user
  user_totalCollectedMemes: Int
  # Largest auction user sold, in terms of price
  user_largestSale: MemeAuction
  # Largest auction user bought into, in terms of price
  user_largestBuy: MemeAuction
  # Amount of challenges user created
  user_totalCreatedChallenges: Int
  # Amount of challenges user created and ended up in his favor
  user_totalCreatedChallengesSuccess: Int
  # Total amount of DANK token user received from challenger rewards
  user_challengerTotalEarned: Int
  # Total amount of DANK token user received from challenger rewards
  user_challengerRank: Int
  # Amount of different votes user participated in
  user_totalParticipatedVotes: Int
  # Amount of different votes user voted for winning option
  user_totalParticipatedVotesSuccess: Int
  # Amount of DANK token user received for voting for winning option
  user_voterTotalEarned: Int
  # Position of voter in leaderboard according to user_voterTotalEarned
  user_voterRank: Int
  # Sum of user_challengerTotalEarned and user_voterTotalEarned
  user_curatorTotalEarned: Int
  # Position of curator in leaderboard according to user_curatorTotalEarned
  user_curatorRank: Int
}

type Parameter @entity {
  param_db: ID
  param_key: ID
  param_value: Int
}
";

#[tokio::test]
async fn successfully_runs_introspection_query_against_complex_schema() {
    let mut schema = Schema::parse(
        COMPLEX_SCHEMA,
        DeploymentHash::new("complexschema").unwrap(),
    )
    .unwrap();
    schema.document = api_schema(&schema.document).unwrap();

    let result = introspection_query(
        schema.clone(),
        "
        query IntrospectionQuery {
          __schema {
            queryType { name }
            mutationType { name }
            subscriptionType { name }
            types {
              ...FullType
            }
            directives {
              name
              description
              locations
              args {
                ...InputValue
              }
            }
          }
        }

        fragment FullType on __Type {
          kind
          name
          description
          fields(includeDeprecated: true) {
            name
            description
            args {
              ...InputValue
            }
            type {
              ...TypeRef
            }
            isDeprecated
            deprecationReason
          }
          inputFields {
            ...InputValue
          }
          interfaces {
            ...TypeRef
          }
          enumValues(includeDeprecated: true) {
            name
            description
            isDeprecated
            deprecationReason
          }
          possibleTypes {
            ...TypeRef
          }
        }

        fragment InputValue on __InputValue {
          name
          description
          type { ...TypeRef }
          defaultValue
        }

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
    ",
    )
    .await;

    assert!(!result.has_errors(), "{:#?}", result);
}

#[tokio::test]
async fn introspection_possible_types() {
    let mut schema = Schema::parse(
        COMPLEX_SCHEMA,
        DeploymentHash::new("complexschema").unwrap(),
    )
    .unwrap();
    schema.document = api_schema(&schema.document).unwrap();

    // Test "possibleTypes" introspection in interfaces
    let response = introspection_query(
        schema,
        "query {
          __type(name: \"RegEntry\") {
              name
              possibleTypes {
                name
              }
          }
        }",
    )
    .await
    .to_result()
    .unwrap()
    .unwrap();

    assert_eq!(
        response,
        object_value(vec![(
            "__type",
            object_value(vec![
                ("name", r::Value::String("RegEntry".to_string())),
                (
                    "possibleTypes",
                    r::Value::List(vec![
                        object_value(vec![("name", r::Value::String("Meme".to_owned()))]),
                        object_value(vec![("name", r::Value::String("ParamChange".to_owned()))])
                    ])
                )
            ])
        )])
    )
}

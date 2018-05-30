#[macro_use]
extern crate pretty_assertions;
extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;

extern crate thegraph;
extern crate thegraph_graphql_utils;

use futures::sync::oneshot;
use graphql_parser::query as q;

use thegraph::prelude::{Query, QueryResult, Schema};
use thegraph_graphql_utils::ast::query::object_value;
use thegraph_graphql_utils::{execution, mocks};

/// Creates a basic GraphQL schema that exercies scalars, directives,
/// enums, interfaces, input objects, object types and field arguments.
fn mock_schema() -> Schema {
    Schema {
        id: "mock-schema".to_string(),
        document: graphql_parser::parse_schema(
            "
             scalar String
             scalar ID

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

             type User implements Node {
               id: ID!
               name: String! @language(language: \"English\")
               role: Role!
             }

             enum User_orderBy {
               id
               name
             }

             input User_filter {
               name_eq: String,
               name_not: String,
             }

             type Query {
               allUsers(orderBy: User_orderBy, filter: User_filter): [User!]
               User: User
             }
             ",
        ).unwrap(),
    }
}

/// Builds the expected result for GraphiQL's introspection query that we are
/// using for testing.
fn expected_mock_schema_introspection() -> q::Value {
    let string_type = object_value(vec![
        ("kind", q::Value::Enum("SCALAR".to_string())),
        ("name", q::Value::String("String".to_string())),
        ("description", q::Value::Null),
        ("fields", q::Value::Null),
        ("inputFields", q::Value::Null),
        ("enumValues", q::Value::Null),
        ("interfaces", q::Value::Null),
        ("possibleTypes", q::Value::Null),
    ]);

    let id_type = object_value(vec![
        ("kind", q::Value::Enum("SCALAR".to_string())),
        ("name", q::Value::String("ID".to_string())),
        ("description", q::Value::Null),
        ("fields", q::Value::Null),
        ("inputFields", q::Value::Null),
        ("enumValues", q::Value::Null),
        ("interfaces", q::Value::Null),
        ("possibleTypes", q::Value::Null),
    ]);

    let role_type = object_value(vec![
        ("kind", q::Value::Enum("ENUM".to_string())),
        ("name", q::Value::String("Role".to_string())),
        ("description", q::Value::Null),
        ("fields", q::Value::Null),
        ("inputFields", q::Value::Null),
        (
            "enumValues",
            q::Value::List(vec![
                object_value(vec![
                    ("name", q::Value::String("USER".to_string())),
                    ("description", q::Value::Null),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
                object_value(vec![
                    ("name", q::Value::String("ADMIN".to_string())),
                    ("description", q::Value::Null),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
            ]),
        ),
        ("interfaces", q::Value::Null),
        ("possibleTypes", q::Value::Null),
    ]);

    let node_type = object_value(vec![
        ("kind", q::Value::Enum("INTERFACE".to_string())),
        ("name", q::Value::String("Node".to_string())),
        ("description", q::Value::Null),
        (
            "fields",
            q::Value::List(vec![
                object_value(vec![
                    ("name", q::Value::String("id".to_string())),
                    ("description", q::Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("NON_NULL".to_string())),
                            ("name", q::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", q::Value::Enum("SCALAR".to_string())),
                                    ("name", q::Value::String("ID".to_string())),
                                    ("ofType", q::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("args", q::Value::List(vec![])),
                    ("deprecationReason", q::Value::Null),
                    ("isDeprecated", q::Value::Boolean(false)),
                ]),
            ]),
        ),
        ("inputFields", q::Value::Null),
        ("enumValues", q::Value::Null),
        ("interfaces", q::Value::Null),
        (
            "possibleTypes",
            q::Value::List(vec![
                object_value(vec![
                    ("kind", q::Value::Enum("OBJECT".to_string())),
                    ("name", q::Value::String("User".to_string())),
                    ("ofType", q::Value::Null),
                ]),
            ]),
        ),
    ]);

    let user_orderby_type = object_value(vec![
        ("kind", q::Value::Enum("ENUM".to_string())),
        ("name", q::Value::String("User_orderBy".to_string())),
        ("description", q::Value::Null),
        ("fields", q::Value::Null),
        ("inputFields", q::Value::Null),
        (
            "enumValues",
            q::Value::List(vec![
                object_value(vec![
                    ("name", q::Value::String("id".to_string())),
                    ("description", q::Value::Null),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
                object_value(vec![
                    ("name", q::Value::String("name".to_string())),
                    ("description", q::Value::Null),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
            ]),
        ),
        ("interfaces", q::Value::Null),
        ("possibleTypes", q::Value::Null),
    ]);

    let user_filter_type = object_value(vec![
        ("kind", q::Value::Enum("INPUT_OBJECT".to_string())),
        ("name", q::Value::String("User_filter".to_string())),
        ("description", q::Value::Null),
        ("fields", q::Value::Null),
        (
            "inputFields",
            q::Value::List(vec![
                object_value(vec![
                    ("name", q::Value::String("name_eq".to_string())),
                    ("description", q::Value::Null),
                    ("defaultValue", q::Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("SCALAR".to_string())),
                            ("name", q::Value::String("String".to_string())),
                            ("ofType", q::Value::Null),
                        ]),
                    ),
                ]),
                object_value(vec![
                    ("name", q::Value::String("name_not".to_string())),
                    ("description", q::Value::Null),
                    ("defaultValue", q::Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("SCALAR".to_string())),
                            ("name", q::Value::String("String".to_string())),
                            ("ofType", q::Value::Null),
                        ]),
                    ),
                ]),
            ]),
        ),
        ("enumValues", q::Value::Null),
        ("interfaces", q::Value::Null),
        ("possibleTypes", q::Value::Null),
    ]);

    let user_type = object_value(vec![
        ("kind", q::Value::Enum("OBJECT".to_string())),
        ("name", q::Value::String("User".to_string())),
        ("description", q::Value::Null),
        (
            "fields",
            q::Value::List(vec![
                object_value(vec![
                    ("name", q::Value::String("id".to_string())),
                    ("description", q::Value::Null),
                    ("args", q::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("NON_NULL".to_string())),
                            ("name", q::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", q::Value::Enum("SCALAR".to_string())),
                                    ("name", q::Value::String("ID".to_string())),
                                    ("ofType", q::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
                object_value(vec![
                    ("name", q::Value::String("name".to_string())),
                    ("description", q::Value::Null),
                    ("args", q::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("NON_NULL".to_string())),
                            ("name", q::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", q::Value::Enum("SCALAR".to_string())),
                                    ("name", q::Value::String("String".to_string())),
                                    ("ofType", q::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
                object_value(vec![
                    ("name", q::Value::String("role".to_string())),
                    ("description", q::Value::Null),
                    ("args", q::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("NON_NULL".to_string())),
                            ("name", q::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", q::Value::Enum("ENUM".to_string())),
                                    ("name", q::Value::String("Role".to_string())),
                                    ("ofType", q::Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
            ]),
        ),
        ("inputFields", q::Value::Null),
        ("enumValues", q::Value::Null),
        (
            "interfaces",
            q::Value::List(vec![
                object_value(vec![
                    ("kind", q::Value::Enum("INTERFACE".to_string())),
                    ("name", q::Value::String("Node".to_string())),
                    ("ofType", q::Value::Null),
                ]),
            ]),
        ),
        ("possibleTypes", q::Value::Null),
    ]);

    let query_type = object_value(vec![
        ("kind", q::Value::Enum("OBJECT".to_string())),
        ("name", q::Value::String("Query".to_string())),
        ("description", q::Value::Null),
        (
            "fields",
            q::Value::List(vec![
                object_value(vec![
                    ("name", q::Value::String("allUsers".to_string())),
                    ("description", q::Value::Null),
                    (
                        "args",
                        q::Value::List(vec![
                            object_value(vec![
                                ("defaultValue", q::Value::Null),
                                ("description", q::Value::Null),
                                ("name", q::Value::String("orderBy".to_string())),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", q::Value::Enum("ENUM".to_string())),
                                        ("name", q::Value::String("User_orderBy".to_string())),
                                        ("ofType", q::Value::Null),
                                    ]),
                                ),
                            ]),
                            object_value(vec![
                                ("defaultValue", q::Value::Null),
                                ("description", q::Value::Null),
                                ("name", q::Value::String("filter".to_string())),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", q::Value::Enum("INPUT_OBJECT".to_string())),
                                        ("name", q::Value::String("User_filter".to_string())),
                                        ("ofType", q::Value::Null),
                                    ]),
                                ),
                            ]),
                        ]),
                    ),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("LIST".to_string())),
                            ("name", q::Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", q::Value::Enum("NON_NULL".to_string())),
                                    ("name", q::Value::Null),
                                    (
                                        "ofType",
                                        object_value(vec![
                                            ("kind", q::Value::Enum("OBJECT".to_string())),
                                            ("name", q::Value::String("User".to_string())),
                                            ("ofType", q::Value::Null),
                                        ]),
                                    ),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
                object_value(vec![
                    ("name", q::Value::String("User".to_string())),
                    ("description", q::Value::Null),
                    ("args", q::Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", q::Value::Enum("OBJECT".to_string())),
                            ("name", q::Value::String("User".to_string())),
                            ("ofType", q::Value::Null),
                        ]),
                    ),
                    ("isDeprecated", q::Value::Boolean(false)),
                    ("deprecationReason", q::Value::Null),
                ]),
            ]),
        ),
        ("inputFields", q::Value::Null),
        ("enumValues", q::Value::Null),
        ("interfaces", q::Value::List(vec![])),
        ("possibleTypes", q::Value::Null),
    ]);

    let expected_types = q::Value::List(vec![
        string_type,
        id_type,
        role_type,
        node_type,
        user_type,
        user_orderby_type,
        user_filter_type,
        query_type,
    ]);

    let expected_directives = q::Value::List(vec![
        object_value(vec![
            ("name", q::Value::String("language".to_string())),
            ("description", q::Value::Null),
            (
                "args",
                q::Value::List(vec![
                    object_value(vec![
                        ("name", q::Value::String("language".to_string())),
                        ("description", q::Value::Null),
                        (
                            "defaultValue",
                            q::Value::String("String(\"English\")".to_string()),
                        ),
                        (
                            "type",
                            object_value(vec![
                                ("kind", q::Value::Enum("SCALAR".to_string())),
                                ("name", q::Value::String("String".to_string())),
                                ("ofType", q::Value::Null),
                            ]),
                        ),
                    ]),
                ]),
            ),
        ]),
    ]);

    let schema_type = object_value(vec![
        (
            "queryType",
            object_value(vec![("name", q::Value::String("Query".to_string()))]),
        ),
        ("mutationType", q::Value::Null),
        ("types", expected_types),
        ("directives", expected_directives),
    ]);

    object_value(vec![("__schema", schema_type)])
}

/// Execute an introspection query.
fn introspection_query(query: &str) -> QueryResult {
    // Create the query
    let (sender, _) = oneshot::channel();
    let query = Query {
        schema: mock_schema(),
        document: graphql_parser::parse_query(query).unwrap(),
        variables: None,
        result_sender: sender,
    };

    // Execute it
    execution::execute(
        &query,
        execution::ExecutionOptions {
            logger: slog::Logger::root(slog::Discard, o!()),
            resolver: mocks::MockResolver,
        },
    )
}

#[test]
fn satisfies_graphiql_introspection_query_without_fragments() {
    let result = introspection_query(
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
    );

    let data = result.data.expect("Introspection query returned no result");
    assert_eq!(data, expected_mock_schema_introspection());
}

#[test]
fn satisfies_graphiql_introspection_query_with_fragments() {
    let result = introspection_query(
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
    );

    let data = result.data.expect("Introspection query returned no result");
    assert_eq!(data, expected_mock_schema_introspection());
}

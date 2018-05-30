#[macro_use]
extern crate pretty_assertions;
extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;
extern crate tokio_core;

extern crate thegraph;
extern crate thegraph_core;
extern crate thegraph_mock;

use futures::prelude::*;
use futures::sync::oneshot;
use graphql_parser::query::Value;
use tokio_core::reactor::Core;

use thegraph::prelude::{Query, QueryResult, QueryRunner, Schema};
use thegraph_core::QueryRunner as CoreQueryRunner;
use thegraph_core::object_value;

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

fn expected_mock_schema_introspection() -> Value {
    let string_type = object_value(vec![
        ("kind", Value::Enum("SCALAR".to_string())),
        ("name", Value::String("String".to_string())),
        ("description", Value::Null),
        ("fields", Value::Null),
        ("inputFields", Value::Null),
        ("enumValues", Value::Null),
        ("interfaces", Value::Null),
        ("possibleTypes", Value::Null),
    ]);

    let id_type = object_value(vec![
        ("kind", Value::Enum("SCALAR".to_string())),
        ("name", Value::String("ID".to_string())),
        ("description", Value::Null),
        ("fields", Value::Null),
        ("inputFields", Value::Null),
        ("enumValues", Value::Null),
        ("interfaces", Value::Null),
        ("possibleTypes", Value::Null),
    ]);

    let role_type = object_value(vec![
        ("kind", Value::Enum("ENUM".to_string())),
        ("name", Value::String("Role".to_string())),
        ("description", Value::Null),
        ("fields", Value::Null),
        ("inputFields", Value::Null),
        (
            "enumValues",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("USER".to_string())),
                    ("description", Value::Null),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
                object_value(vec![
                    ("name", Value::String("ADMIN".to_string())),
                    ("description", Value::Null),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
            ]),
        ),
        ("interfaces", Value::Null),
        ("possibleTypes", Value::Null),
    ]);

    let node_type = object_value(vec![
        ("kind", Value::Enum("INTERFACE".to_string())),
        ("name", Value::String("Node".to_string())),
        ("description", Value::Null),
        (
            "fields",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("id".to_string())),
                    ("description", Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("NON_NULL".to_string())),
                            ("name", Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", Value::Enum("SCALAR".to_string())),
                                    ("name", Value::String("ID".to_string())),
                                    ("ofType", Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("args", Value::List(vec![])),
                    ("deprecationReason", Value::Null),
                    ("isDeprecated", Value::Boolean(false)),
                ]),
            ]),
        ),
        ("inputFields", Value::Null),
        ("enumValues", Value::Null),
        ("interfaces", Value::Null),
        (
            "possibleTypes",
            Value::List(vec![
                object_value(vec![
                    ("kind", Value::Enum("OBJECT".to_string())),
                    ("name", Value::String("User".to_string())),
                    ("ofType", Value::Null),
                ]),
            ]),
        ),
    ]);

    let user_orderby_type = object_value(vec![
        ("kind", Value::Enum("ENUM".to_string())),
        ("name", Value::String("User_orderBy".to_string())),
        ("description", Value::Null),
        ("fields", Value::Null),
        ("inputFields", Value::Null),
        (
            "enumValues",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("id".to_string())),
                    ("description", Value::Null),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
                object_value(vec![
                    ("name", Value::String("name".to_string())),
                    ("description", Value::Null),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
            ]),
        ),
        ("interfaces", Value::Null),
        ("possibleTypes", Value::Null),
    ]);

    let user_filter_type = object_value(vec![
        ("kind", Value::Enum("INPUT_OBJECT".to_string())),
        ("name", Value::String("User_filter".to_string())),
        ("description", Value::Null),
        ("fields", Value::Null),
        (
            "inputFields",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("name_eq".to_string())),
                    ("description", Value::Null),
                    ("defaultValue", Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("SCALAR".to_string())),
                            ("name", Value::String("String".to_string())),
                            ("ofType", Value::Null),
                        ]),
                    ),
                ]),
                object_value(vec![
                    ("name", Value::String("name_not".to_string())),
                    ("description", Value::Null),
                    ("defaultValue", Value::Null),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("SCALAR".to_string())),
                            ("name", Value::String("String".to_string())),
                            ("ofType", Value::Null),
                        ]),
                    ),
                ]),
            ]),
        ),
        ("enumValues", Value::Null),
        ("interfaces", Value::Null),
        ("possibleTypes", Value::Null),
    ]);

    let user_type = object_value(vec![
        ("kind", Value::Enum("OBJECT".to_string())),
        ("name", Value::String("User".to_string())),
        ("description", Value::Null),
        (
            "fields",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("id".to_string())),
                    ("description", Value::Null),
                    ("args", Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("NON_NULL".to_string())),
                            ("name", Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", Value::Enum("SCALAR".to_string())),
                                    ("name", Value::String("ID".to_string())),
                                    ("ofType", Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
                object_value(vec![
                    ("name", Value::String("name".to_string())),
                    ("description", Value::Null),
                    ("args", Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("NON_NULL".to_string())),
                            ("name", Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", Value::Enum("SCALAR".to_string())),
                                    ("name", Value::String("String".to_string())),
                                    ("ofType", Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
                object_value(vec![
                    ("name", Value::String("role".to_string())),
                    ("description", Value::Null),
                    ("args", Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("NON_NULL".to_string())),
                            ("name", Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", Value::Enum("ENUM".to_string())),
                                    ("name", Value::String("Role".to_string())),
                                    ("ofType", Value::Null),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
            ]),
        ),
        ("inputFields", Value::Null),
        ("enumValues", Value::Null),
        (
            "interfaces",
            Value::List(vec![
                object_value(vec![
                    ("kind", Value::Enum("INTERFACE".to_string())),
                    ("name", Value::String("Node".to_string())),
                    ("ofType", Value::Null),
                ]),
            ]),
        ),
        ("possibleTypes", Value::Null),
    ]);

    let query_type = object_value(vec![
        ("kind", Value::Enum("OBJECT".to_string())),
        ("name", Value::String("Query".to_string())),
        ("description", Value::Null),
        (
            "fields",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("allUsers".to_string())),
                    ("description", Value::Null),
                    (
                        "args",
                        Value::List(vec![
                            object_value(vec![
                                ("defaultValue", Value::Null),
                                ("description", Value::Null),
                                ("name", Value::String("orderBy".to_string())),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", Value::Enum("ENUM".to_string())),
                                        ("name", Value::String("User_orderBy".to_string())),
                                        ("ofType", Value::Null),
                                    ]),
                                ),
                            ]),
                            object_value(vec![
                                ("defaultValue", Value::Null),
                                ("description", Value::Null),
                                ("name", Value::String("filter".to_string())),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", Value::Enum("INPUT_OBJECT".to_string())),
                                        ("name", Value::String("User_filter".to_string())),
                                        ("ofType", Value::Null),
                                    ]),
                                ),
                            ]),
                        ]),
                    ),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("LIST".to_string())),
                            ("name", Value::Null),
                            (
                                "ofType",
                                object_value(vec![
                                    ("kind", Value::Enum("NON_NULL".to_string())),
                                    ("name", Value::Null),
                                    (
                                        "ofType",
                                        object_value(vec![
                                            ("kind", Value::Enum("OBJECT".to_string())),
                                            ("name", Value::String("User".to_string())),
                                            ("ofType", Value::Null),
                                        ]),
                                    ),
                                ]),
                            ),
                        ]),
                    ),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
                object_value(vec![
                    ("name", Value::String("User".to_string())),
                    ("description", Value::Null),
                    ("args", Value::List(vec![])),
                    (
                        "type",
                        object_value(vec![
                            ("kind", Value::Enum("OBJECT".to_string())),
                            ("name", Value::String("User".to_string())),
                            ("ofType", Value::Null),
                        ]),
                    ),
                    ("isDeprecated", Value::Boolean(false)),
                    ("deprecationReason", Value::Null),
                ]),
            ]),
        ),
        ("inputFields", Value::Null),
        ("enumValues", Value::Null),
        ("interfaces", Value::List(vec![])),
        ("possibleTypes", Value::Null),
    ]);

    let expected_types = Value::List(vec![
        string_type,
        id_type,
        role_type,
        node_type,
        user_type,
        user_orderby_type,
        user_filter_type,
        query_type,
    ]);

    let schema_type = object_value(vec![
        (
            "queryType",
            object_value(vec![("name", Value::String("Query".to_string()))]),
        ),
        ("mutationType", Value::Null),
        ("types", expected_types),
        (
            "directives",
            Value::List(vec![
                object_value(vec![
                    ("name", Value::String("language".to_string())),
                    ("description", Value::Null),
                    (
                        "args",
                        Value::List(vec![
                            object_value(vec![
                                ("name", Value::String("language".to_string())),
                                ("description", Value::Null),
                                (
                                    "defaultValue",
                                    Value::String("String(\"English\")".to_string()),
                                ),
                                (
                                    "type",
                                    object_value(vec![
                                        ("kind", Value::Enum("SCALAR".to_string())),
                                        ("name", Value::String("String".to_string())),
                                        ("ofType", Value::Null),
                                    ]),
                                ),
                            ]),
                        ]),
                    ),
                ]),
            ]),
        ),
    ]);

    object_value(vec![("__schema", schema_type)])
}

fn introspection_query(query: &str) -> QueryResult {
    let mut core = Core::new().unwrap();
    let logger = slog::Logger::root(slog::Discard, o!());

    let store = thegraph_mock::MockStore::new(&logger, core.handle());
    let mut query_runner = CoreQueryRunner::new(&logger, core.handle(), store);
    let query_sink = query_runner.query_sink();

    let (sender, receiver) = oneshot::channel();
    let query = Query {
        schema: mock_schema(),
        document: graphql_parser::parse_query(query).unwrap(),
        variables: None,
        result_sender: sender,
    };

    query_sink.send(query).wait().unwrap();
    core.run(receiver)
        .expect("Failed to run introspection query")
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

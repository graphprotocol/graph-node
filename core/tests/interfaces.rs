// Tests for graphql interfaces.

use pretty_assertions::assert_eq;

use graph::{components::store::EntityType, data::graphql::object};
use graph::{data::query::QueryTarget, prelude::*};
use test_store::*;

// `entities` is `(entity, type)`.
async fn insert_and_query(
    subgraph_id: &str,
    schema: &str,
    entities: Vec<(&str, Entity)>,
    query: &str,
) -> Result<QueryResult, StoreError> {
    let subgraph_id = DeploymentHash::new(subgraph_id).unwrap();
    let deployment = create_test_subgraph(&subgraph_id, schema).await;

    let entities = entities
        .into_iter()
        .map(|(entity_type, data)| (EntityType::new(entity_type.to_owned()), data))
        .collect();

    insert_entities(&deployment, entities).await?;

    let document = graphql_parser::parse_query(query).unwrap().into_static();
    let target = QueryTarget::Deployment(subgraph_id);
    let query = Query::new(document, None);
    Ok(execute_subgraph_query(query, target)
        .await
        .first()
        .unwrap()
        .duplicate())
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

#[tokio::test]
async fn one_interface_zero_entities() {
    let subgraph_id = "oneInterfaceZeroEntities";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let query = "query { leggeds(first: 100) { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: Vec::<r::Value>::new() };
    assert_eq!(data, exp);
}

#[tokio::test]
async fn one_interface_one_entity() {
    let subgraph_id = "oneInterfaceOneEntity";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let entity = (
        "Animal",
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
    );

    // Collection query.
    let query = "query { leggeds(first: 100) { legs } }";
    let res = insert_and_query(subgraph_id, schema, vec![entity], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object!{ legs: 3 }]};
    assert_eq!(data, exp);

    // Query by ID.
    let query = "query { legged(id: \"1\") { legs } }";
    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { legged: object! { legs: 3 }};
    assert_eq!(data, exp);
}

#[tokio::test]
async fn one_interface_one_entity_typename() {
    let subgraph_id = "oneInterfaceOneEntityTypename";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let entity = (
        "Animal",
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
    );

    let query = "query { leggeds(first: 100) { __typename } }";

    let res = insert_and_query(subgraph_id, schema, vec![entity], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object!{ __typename: "Animal" } ]};
    assert_eq!(data, exp);
}

#[tokio::test]
async fn one_interface_multiple_entities() {
    let subgraph_id = "oneInterfaceMultipleEntities";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }
                  type Furniture implements Legged @entity { id: ID!, legs: Int }
                  ";

    let animal = (
        "Animal",
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
    );
    let furniture = (
        "Furniture",
        Entity::from(vec![("id", Value::from("2")), ("legs", Value::from(4))]),
    );

    let query = "query { leggeds(first: 100, orderBy: legs) { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![animal, furniture], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object! { legs: 3 }, object! { legs: 4 }]};
    assert_eq!(data, exp);

    // Test for support issue #32.
    let query = "query { legged(id: \"2\") { legs } }";
    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { legged: object! { legs: 4 }};
    assert_eq!(data, exp);
}

#[tokio::test]
async fn reference_interface() {
    let subgraph_id = "ReferenceInterface";
    let schema = "type Leg @entity { id: ID! }
                  interface Legged { leg: Leg }
                  type Animal implements Legged @entity { id: ID!, leg: Leg }";

    let query = "query { leggeds(first: 100) { leg { id } } }";

    let leg = ("Leg", Entity::from(vec![("id", Value::from("1"))]));
    let animal = (
        "Animal",
        Entity::from(vec![("id", Value::from("1")), ("leg", Value::from("1"))]),
    );

    let res = insert_and_query(subgraph_id, schema, vec![leg, animal], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object!{ leg: object! { id: "1" } }] };
    assert_eq!(data, exp);
}

#[tokio::test]
async fn reference_interface_derived() {
    // Test the different ways in which interface implementations
    // can reference another entity
    let subgraph_id = "ReferenceInterfaceDerived";
    let schema = "
    type Transaction @entity {
        id: ID!,
        buyEvent: BuyEvent!,
        sellEvents: [SellEvent!]!,
        giftEvent: [GiftEvent!]! @derivedFrom(field: \"transaction\"),
    }

    interface Event {
        id: ID!,
        transaction: Transaction!
    }

    type BuyEvent implements Event @entity {
        id: ID!,
        # Derived, but only one buyEvent per Transaction
        transaction: Transaction! @derivedFrom(field: \"buyEvent\")
    }

    type SellEvent implements Event @entity {
        id: ID!
        # Derived, many sellEvents per Transaction
        transaction: Transaction! @derivedFrom(field: \"sellEvents\")
    }

    type GiftEvent implements Event @entity {
        id: ID!,
        # Store the transaction directly
        transaction: Transaction!
    }";

    let query = "query { events { id transaction { id } } }";

    let buy = ("BuyEvent", Entity::from(vec![("id", "buy".into())]));
    let sell1 = ("SellEvent", Entity::from(vec![("id", "sell1".into())]));
    let sell2 = ("SellEvent", Entity::from(vec![("id", "sell2".into())]));
    let gift = (
        "GiftEvent",
        Entity::from(vec![("id", "gift".into()), ("transaction", "txn".into())]),
    );
    let txn = (
        "Transaction",
        Entity::from(vec![
            ("id", "txn".into()),
            ("buyEvent", "buy".into()),
            ("sellEvents", vec!["sell1", "sell2"].into()),
        ]),
    );

    let entities = vec![buy, sell1, sell2, gift, txn];
    let res = insert_and_query(subgraph_id, schema, entities.clone(), query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    let exp = object! {
        events: vec![
            object! { id: "buy", transaction: object! { id: "txn" } },
            object! { id: "gift", transaction: object! { id: "txn" } },
            object! { id: "sell1", transaction: object! { id: "txn" } },
            object! { id: "sell2", transaction: object! { id: "txn" } }
        ]
    };
    assert_eq!(data, exp);
}

#[tokio::test]
async fn follow_interface_reference_invalid() {
    let subgraph_id = "FollowInterfaceReferenceInvalid";
    let schema = "interface Legged { legs: Int! }
                  type Animal implements Legged @entity {
                    id: ID!
                    legs: Int!
                    parent: Legged
                  }";

    let query = "query { legged(id: \"child\") { parent { id } } }";

    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();

    // Depending on whether `ENABLE_GRAPHQL_VALIDATIONS` is set or not, we
    // get different errors
    match &res.to_result().unwrap_err()[0] {
        QueryError::ExecutionError(QueryExecutionError::ValidationError(_, error_message)) => {
            assert_eq!(
                error_message,
                "Cannot query field \"parent\" on type \"Legged\"."
            );
        }
        QueryError::ExecutionError(QueryExecutionError::UnknownField(_, type_name, field_name)) => {
            assert_eq!(type_name, "Legged");
            assert_eq!(field_name, "parent");
        }
        e => panic!("error `{}` is not the expected one", e),
    }
}

#[tokio::test]
async fn follow_interface_reference() {
    let subgraph_id = "FollowInterfaceReference";
    let schema = "interface Legged { id: ID!, legs: Int! }
                  type Animal implements Legged @entity {
                    id: ID!
                    legs: Int!
                    parent: Legged
                  }";

    let query = "query { legged(id: \"child\") { ... on Animal { parent { id } } } }";

    let parent = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("parent")),
            ("legs", Value::from(4)),
            ("parent", Value::Null),
        ]),
    );
    let child = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("child")),
            ("legs", Value::from(3)),
            ("parent", Value::String("parent".into())),
        ]),
    );

    let res = insert_and_query(subgraph_id, schema, vec![parent, child], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    let exp = object! {
        legged: object! { parent: object! { id: "parent" } }
    };
    assert_eq!(data, exp)
}

#[tokio::test]
async fn conflicting_implementors_id() {
    let subgraph_id = "ConflictingImplementorsId";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }
                  type Furniture implements Legged @entity { id: ID!, legs: Int }
                  ";

    let animal = (
        "Animal",
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
    );
    let furniture = (
        "Furniture",
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
    );

    let query = "query { leggeds(first: 100) { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![animal, furniture], query).await;

    let msg = res.unwrap_err().to_string();
    // We don't know in which order the two entities get inserted; the two
    // error messages only differ in who gets inserted first
    const EXPECTED1: &str =
        "tried to set entity of type `Furniture` with ID \"1\" but an entity of type `Animal`, \
         which has an interface in common with `Furniture`, exists with the same ID";
    const EXPECTED2: &str =
        "tried to set entity of type `Animal` with ID \"1\" but an entity of type `Furniture`, \
         which has an interface in common with `Animal`, exists with the same ID";

    assert!(msg == EXPECTED1 || msg == EXPECTED2);
}

#[tokio::test]
async fn derived_interface_relationship() {
    let subgraph_id = "DerivedInterfaceRelationship";
    let schema = "interface ForestDweller { id: ID!, forest: Forest }
                  type Animal implements ForestDweller @entity { id: ID!, forest: Forest }
                  type Forest @entity { id: ID!, dwellers: [ForestDweller]! @derivedFrom(field: \"forest\") }
                  ";

    let forest = ("Forest", Entity::from(vec![("id", Value::from("1"))]));
    let animal = (
        "Animal",
        Entity::from(vec![("id", Value::from("1")), ("forest", Value::from("1"))]),
    );

    let query = "query { forests(first: 100) { dwellers(first: 100) { id } } }";

    let res = insert_and_query(subgraph_id, schema, vec![forest, animal], query)
        .await
        .unwrap();
    let data = extract_data!(res);
    assert_eq!(
        data.unwrap().to_string(),
        "{forests: [{dwellers: [{id: \"1\"}]}]}"
    );
}

#[tokio::test]
async fn two_interfaces() {
    let subgraph_id = "TwoInterfaces";
    let schema = "interface IFoo { foo: String! }
                  interface IBar { bar: Int! }

                  type A implements IFoo @entity { id: ID!, foo: String! }
                  type B implements IBar @entity { id: ID!, bar: Int! }

                  type AB implements IFoo & IBar @entity { id: ID!, foo: String!, bar: Int! }
                  ";

    let a = (
        "A",
        Entity::from(vec![("id", Value::from("1")), ("foo", Value::from("bla"))]),
    );
    let b = (
        "B",
        Entity::from(vec![("id", Value::from("1")), ("bar", Value::from(100))]),
    );
    let ab = (
        "AB",
        Entity::from(vec![
            ("id", Value::from("2")),
            ("foo", Value::from("ble")),
            ("bar", Value::from(200)),
        ]),
    );

    let query = "query {
                    ibars(first: 100, orderBy: bar) { bar }
                    ifoos(first: 100, orderBy: foo) { foo }
                }";
    let res = insert_and_query(subgraph_id, schema, vec![a, b, ab], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! {
        ibars: vec![ object! { bar: 100 }, object! { bar: 200 }],
        ifoos: vec![ object! { foo: "bla" }, object! { foo: "ble" } ]
    };
    assert_eq!(data, exp);
}

#[tokio::test]
async fn interface_non_inline_fragment() {
    let subgraph_id = "interfaceNonInlineFragment";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, name: String, legs: Int }";

    let entity = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("cow")),
            ("legs", Value::from(3)),
        ]),
    );

    // Query only the fragment.
    let query = "query { leggeds { ...frag } } fragment frag on Animal { name }";
    let res = insert_and_query(subgraph_id, schema, vec![entity], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object! { name: "cow" } ]};
    assert_eq!(data, exp);

    // Query the fragment and something else.
    let query = "query { leggeds { legs, ...frag } } fragment frag on Animal { name }";
    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object!{ legs: 3, name: "cow" } ]};
    assert_eq!(data, exp);
}

#[tokio::test]
async fn interface_inline_fragment() {
    let subgraph_id = "interfaceInlineFragment";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, name: String, legs: Int }
                  type Bird implements Legged @entity { id: ID!, airspeed: Int, legs: Int }";

    let animal = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("cow")),
            ("legs", Value::from(4)),
        ]),
    );
    let bird = (
        "Bird",
        Entity::from(vec![
            ("id", Value::from("2")),
            ("airspeed", Value::from(24)),
            ("legs", Value::from(2)),
        ]),
    );

    let query =
        "query { leggeds(orderBy: legs) { ... on Animal { name } ...on Bird { airspeed } } }";
    let res = insert_and_query(subgraph_id, schema, vec![animal, bird], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! { leggeds: vec![ object!{ airspeed: 24 }, object! { name: "cow" }]};
    assert_eq!(data, exp);
}

#[tokio::test]
async fn interface_inline_fragment_with_subquery() {
    let subgraph_id = "InterfaceInlineFragmentWithSubquery";
    let schema = "
        interface Legged { legs: Int }
        type Parent @entity {
          id: ID!
        }
        type Animal implements Legged @entity {
          id: ID!
          name: String
          legs: Int
          parent: Parent
        }
        type Bird implements Legged @entity {
          id: ID!
          airspeed: Int
          legs: Int
          parent: Parent
        }
    ";

    let mama_cow = (
        "Parent",
        Entity::from(vec![("id", Value::from("mama_cow"))]),
    );
    let cow = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("cow")),
            ("legs", Value::from(4)),
            ("parent", Value::from("mama_cow")),
        ]),
    );

    let mama_bird = (
        "Parent",
        Entity::from(vec![("id", Value::from("mama_bird"))]),
    );
    let bird = (
        "Bird",
        Entity::from(vec![
            ("id", Value::from("2")),
            ("airspeed", Value::from(5)),
            ("legs", Value::from(2)),
            ("parent", Value::from("mama_bird")),
        ]),
    );

    let query = "query { leggeds(orderBy: legs) { legs ... on Bird { airspeed parent { id } } } }";
    let res = insert_and_query(
        subgraph_id,
        schema,
        vec![cow, mama_cow, bird, mama_bird],
        query,
    )
    .await
    .unwrap();
    let data = extract_data!(res).unwrap();
    let exp = object! {
        leggeds: vec![ object!{ legs: 2, airspeed: 5, parent: object! { id: "mama_bird" } },
                       object!{ legs: 4 }]
    };
    assert_eq!(data, exp);
}

#[tokio::test]
async fn invalid_fragment() {
    let subgraph_id = "InvalidFragment";
    let schema = "interface Legged { legs: Int! }
                  type Animal implements Legged @entity {
                    id: ID!
                    name: String!
                    legs: Int!
                    parent: Legged
                  }";

    let query = "query { legged(id: \"child\") { ...{ name } } }";

    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();

    match &res.to_result().unwrap_err()[0] {
        QueryError::ExecutionError(QueryExecutionError::ValidationError(_, error_message)) => {
            assert_eq!(
                error_message,
                "Cannot query field \"name\" on type \"Legged\"."
            );
        }
        QueryError::ExecutionError(QueryExecutionError::UnknownField(_, type_name, field_name)) => {
            assert_eq!(type_name, "Legged");
            assert_eq!(field_name, "name");
        }
        e => panic!("error {} is not the expected one", e),
    }
}

#[tokio::test]
async fn alias() {
    let subgraph_id = "Alias";
    let schema = "interface Legged { id: ID!, legs: Int! }
                  type Animal implements Legged @entity {
                    id: ID!
                    legs: Int!
                    parent: Legged
                  }";

    let query = "query {
                    l: legged(id: \"child\") {
                        ... on Animal {
                            p: parent {
                                i: id,
                                t: __typename,
                                __typename
                            }
                        }
                    }
            }";

    let parent = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("parent")),
            ("legs", Value::from(4)),
            ("parent", Value::Null),
        ]),
    );
    let child = (
        "Animal",
        Entity::from(vec![
            ("id", Value::from("child")),
            ("legs", Value::from(3)),
            ("parent", Value::String("parent".into())),
        ]),
    );

    let res = insert_and_query(subgraph_id, schema, vec![parent, child], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            l: object! {
                p: object! {
                    i: "parent",
                    t: "Animal",
                    __typename: "Animal"
                }
            }
        }
    )
}

#[tokio::test]
async fn fragments_dont_panic() {
    let subgraph_id = "FragmentsDontPanic";
    let schema = "
      type Parent @entity {
        id: ID!
        child: Child
      }

      type Child @entity {
        id: ID!
      }
    ";

    let query = "
        query {
            parents {
                ...on Parent {
                    child {
                        id
                    }
                }
                ...Frag
                child {
                    id
                }
            }
        }

        fragment Frag on Parent {
            child {
                id
            }
        }
    ";

    // The panic manifests if two parents exist.
    let parent = (
        "Parent",
        entity!(
            id: "p",
            child: "c",
        ),
    );
    let parent2 = (
        "Parent",
        entity!(
            id: "p2",
            child: Value::Null,
        ),
    );
    let child = (
        "Child",
        entity!(
            id:"c"
        ),
    );

    let res = insert_and_query(subgraph_id, schema, vec![parent, parent2, child], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            parents: vec![
                object! {
                    child: object! {
                        id: "c",
                    }
                },
                object! {
                    child: r::Value::Null
                }
            ]
        }
    )
}

// See issue #1816
#[tokio::test]
async fn fragments_dont_duplicate_data() {
    let subgraph_id = "FragmentsDupe";
    let schema = "
      type Parent @entity {
        id: ID!
        children: [Child!]!
      }

      type Child @entity {
        id: ID!
      }
    ";

    let query = "
        query {
            parents {
                ...Frag
                children {
                    id
                }
            }
        }

        fragment Frag on Parent {
            children {
                id
            }
        }
    ";

    // This bug manifests if two parents exist.
    let parent = (
        "Parent",
        entity!(
            id: "p",
            children: vec!["c"]
        ),
    );
    let parent2 = (
        "Parent",
        entity!(
            id: "b",
            children: Vec::<String>::new()
        ),
    );
    let child = (
        "Child",
        entity!(
            id:"c"
        ),
    );

    let res = insert_and_query(subgraph_id, schema, vec![parent, parent2, child], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            parents: vec![
                object! {
                    children: Vec::<r::Value>::new()
                },
                object! {
                    children: vec![
                        object! {
                            id: "c",
                        }
                    ]
                }
            ]
        }
    )
}

// See also: e0d6da3e-60cf-41a5-b83c-b60a7a766d4a
#[tokio::test]
async fn redundant_fields() {
    let subgraph_id = "RedundantFields";
    let schema = "interface Legged { id: ID!, parent: Legged }
                  type Animal implements Legged @entity {
                    id: ID!
                    parent: Legged
                  }";

    let query = "query {
                    leggeds {
                        parent { id }
                        ...on Animal {
                            parent { id }
                        }
                    }
            }";

    let parent = (
        "Animal",
        entity!(
            id: "parent",
            parent: Value::Null,
        ),
    );
    let child = (
        "Animal",
        entity!(
            id: "child",
            parent: "parent",
        ),
    );

    let res = insert_and_query(subgraph_id, schema, vec![parent, child], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            leggeds: vec![
                object! {
                    parent: object! {
                        id: "parent",
                    },
                },
                object! {
                    parent: r::Value::Null
                }
            ]
        }
    )
}

#[tokio::test]
async fn fragments_merge_selections() {
    let subgraph_id = "FragmentsMergeSelections";
    let schema = "
      type Parent @entity {
        id: ID!
        children: [Child!]!
      }

      type Child @entity {
        id: ID!
        foo: Int!
      }
    ";

    let query = "
        query {
            parents {
                ...Frag
                children {
                    id
                }
            }
        }

        fragment Frag on Parent {
            children {
                foo
            }
        }
    ";

    let parent = (
        "Parent",
        entity!(
            id: "p",
            children: vec!["c"]
        ),
    );
    let child = (
        "Child",
        entity!(
            id: "c",
            foo: 1,
        ),
    );

    let res = insert_and_query(subgraph_id, schema, vec![parent, child], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            parents: vec![
                object! {
                    children: vec![
                        object! {
                            foo: 1,
                            id: "c",
                        }
                    ]
                }
            ]
        }
    )
}

#[tokio::test]
async fn merge_fields_not_in_interface() {
    let subgraph_id = "MergeFieldsNotInInterface";
    let schema = "interface Iface { id: ID! }
                  type Animal implements Iface @entity {
                    id: ID!
                    human: Iface!
                  }
                  type Human implements Iface @entity {
                    id: ID!
                    animal: Iface!
                  }
                  ";

    let query = "query {
                    ifaces {
                        ...on Animal {
                            id
                            friend: human {
                              id
                            }
                        }
                        ...on Human {
                            id
                            friend: animal {
                              id
                            }
                        }
                    }
            }";

    let animal = (
        "Animal",
        entity!(
            id: "cow",
            human: "fred",
        ),
    );
    let human = (
        "Human",
        entity!(
            id: "fred",
            animal: "cow",
        ),
    );

    let res = insert_and_query(subgraph_id, schema, vec![animal, human], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            ifaces: vec![
                object! {
                    id: "cow",
                    friend: object! {
                        id: "fred",
                    },
                },
                object! {
                    id: "fred",
                    friend: object! {
                        id: "cow",
                    },
                },
            ]
        }
    )
}

#[tokio::test]
async fn nested_interface_fragments() {
    let subgraph_id = "NestedInterfaceFragments";
    let schema = "interface I1face { id: ID!, foo1: Foo! }
                  interface I2face { id: ID!, foo2: Foo! }
                  interface I3face { id: ID!, foo3: Foo! }
                  type Foo @entity {
                      id: ID!
                  }
                  type One implements I1face @entity {
                    id: ID!
                    foo1: Foo!
                  }
                  type Two implements I1face & I2face @entity {
                    id: ID!
                    foo1: Foo!
                    foo2: Foo!
                  }
                  type Three implements I1face & I2face & I3face @entity {
                    id: ID!
                    foo1: Foo!
                    foo2: Foo!
                    foo3: Foo!
                  }";

    let query = "query {
                    i1Faces {
                        __typename
                        foo1 {
                            id
                        }
                        ...on I2face {
                            foo2 {
                                id
                            }
                        }
                        ...on I3face {
                            foo3 {
                                id
                            }
                        }
                    }
            }";

    let foo = (
        "Foo",
        entity!(
            id: "foo",
        ),
    );
    let one = (
        "One",
        entity!(
            id: "1",
            foo1: "foo",
        ),
    );
    let two = (
        "Two",
        entity!(
            id: "2",
            foo1: "foo",
            foo2: "foo",
        ),
    );
    let three = (
        "Three",
        entity!(
            id: "3",
            foo1: "foo",
            foo2: "foo",
            foo3: "foo"
        ),
    );

    let res = insert_and_query(subgraph_id, schema, vec![foo, one, two, three], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            i1Faces: vec![
                object! {
                    __typename: "One",
                    foo1: object! {
                        id: "foo",
                    },
                },
                object! {
                    __typename: "Two",
                    foo1: object! {
                        id: "foo",
                    },
                    foo2: object! {
                        id: "foo",
                    },
                },
                object! {
                    __typename: "Three",
                    foo1: object! {
                        id: "foo",
                    },
                    foo2: object! {
                        id: "foo",
                    },
                    foo3: object! {
                        id: "foo",
                    },
                },
            ]
        }
    )
}

#[tokio::test]
async fn nested_interface_fragments_overlapping() {
    let subgraph_id = "NestedInterfaceFragmentsOverlapping";
    let schema = "interface I1face { id: ID!, foo1: Foo! }
                  interface I2face { id: ID!, foo1: Foo! }
                  type Foo @entity {
                      id: ID!
                  }
                  type One implements I1face @entity {
                    id: ID!
                    foo1: Foo!
                  }
                  type Two implements I1face & I2face @entity {
                    id: ID!
                    foo1: Foo!
                  }";

    let query = "query {
                    i1Faces {
                        __typename
                        ...on I2face {
                            foo1 {
                                id
                            }
                        }
                    }
            }";

    let foo = (
        "Foo",
        entity!(
            id: "foo",
        ),
    );
    let one = (
        "One",
        entity!(
            id: "1",
            foo1: "foo",
        ),
    );
    let two = (
        "Two",
        entity!(
            id: "2",
            foo1: "foo",
        ),
    );
    let res = insert_and_query(subgraph_id, schema, vec![foo, one, two], query)
        .await
        .unwrap();

    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            i1Faces: vec![
                object! {
                    __typename: "One"
                },
                object! {
                    __typename: "Two",
                    foo1: object! {
                        id: "foo",
                    },
                },
            ]
        }
    );

    let query = "query {
        i1Faces {
            __typename
            foo1 {
                id
            }
            ...on I2face {
                foo1 {
                    id
                }
            }
        }
    }";

    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
            i1Faces: vec![
                object! {
                    __typename: "One",
                    foo1: object! {
                        id: "foo"
                    }
                },
                object! {
                    __typename: "Two",
                    foo1: object! {
                        id: "foo",
                    },
                },
            ]
        }
    );
}

#[tokio::test]
async fn enums() {
    use r::Value::Enum;
    let subgraph_id = "enums";
    let schema = r#"
       enum Direction {
         NORTH
         EAST
         SOUTH
         WEST
       }

       type Trajectory @entity {
         id: ID!
         direction: Direction!
         meters: Int!
       }"#;

    let entities = vec![
        (
            "Trajectory",
            Entity::from(vec![
                ("id", Value::from("1")),
                ("direction", Value::from("EAST")),
                ("meters", Value::from(10)),
            ]),
        ),
        (
            "Trajectory",
            Entity::from(vec![
                ("id", Value::from("2")),
                ("direction", Value::from("NORTH")),
                ("meters", Value::from(15)),
            ]),
        ),
    ];
    let query = "query { trajectories { id, direction, meters } }";

    let res = insert_and_query(subgraph_id, schema, entities, query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
        trajectories: vec![
            object!{
        id: "1",
        direction: Enum("EAST".to_string()),
        meters: 10,
        },
            object!{
                id: "2",
        direction: Enum("NORTH".to_string()),
        meters: 15,
            },
        ]}
    );
}

#[tokio::test]
async fn enum_list_filters() {
    use r::Value::Enum;
    let subgraph_id = "enum_list_filters";
    let schema = r#"
       enum Direction {
         NORTH
         EAST
         SOUTH
         WEST
       }

       type Trajectory @entity {
         id: ID!
         direction: Direction!
         meters: Int!
       }"#;

    let entities = vec![
        (
            "Trajectory",
            Entity::from(vec![
                ("id", Value::from("1")),
                ("direction", Value::from("EAST")),
                ("meters", Value::from(10)),
            ]),
        ),
        (
            "Trajectory",
            Entity::from(vec![
                ("id", Value::from("2")),
                ("direction", Value::from("NORTH")),
                ("meters", Value::from(15)),
            ]),
        ),
        (
            "Trajectory",
            Entity::from(vec![
                ("id", Value::from("3")),
                ("direction", Value::from("WEST")),
                ("meters", Value::from(20)),
            ]),
        ),
    ];

    let query = "query { trajectories(where: { direction_in: [NORTH, EAST] }) { id, direction } }";
    let res = insert_and_query(subgraph_id, schema, entities, query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
        trajectories: vec![
            object!{
                id: "1",
                direction: Enum("EAST".to_string()),
            },
            object!{
                id: "2",
                direction: Enum("NORTH".to_string()),
            },
        ]}
    );

    let query = "query { trajectories(where: { direction_not_in: [EAST] }) { id, direction } }";
    let res = insert_and_query(subgraph_id, schema, vec![], query)
        .await
        .unwrap();
    let data = extract_data!(res).unwrap();
    assert_eq!(
        data,
        object! {
        trajectories: vec![
            object!{
                id: "2",
                direction: Enum("NORTH".to_string()),
            },
            object!{
                id: "3",
                direction: Enum("WEST".to_string()),
            },
        ]}
    );
}

#[tokio::test]
async fn recursive_fragment() {
    // Depending on whether `ENABLE_GRAPHQL_VALIDATIONS` is set or not, we
    // get different error messages
    const FOO_ERRORS: [&str; 2] = [
        "Cannot spread fragment \"FooFrag\" within itself.",
        "query has fragment cycle including `FooFrag`",
    ];
    const FOO_BAR_ERRORS: [&str; 2] = [
        "Cannot spread fragment \"BarFrag\" within itself via \"FooFrag\".",
        "query has fragment cycle including `BarFrag`",
    ];
    let subgraph_id = "RecursiveFragment";
    let schema = "
        type Foo @entity {
            id: ID!
            foo: Foo!
            bar: Bar!
        }

        type Bar @entity {
            id: ID!
            foo: Foo!
        }
    ";

    let self_recursive = "
        query {
            foos {
              ...FooFrag
            }
        }

        fragment FooFrag on Foo {
          id
          foo {
            ...FooFrag
          }
        }
    ";
    let res = insert_and_query(subgraph_id, schema, vec![], self_recursive)
        .await
        .unwrap();
    let data = res.to_result().unwrap_err()[0].to_string();
    assert!(FOO_ERRORS.contains(&data.as_str()));

    let co_recursive = "
        query {
          foos {
            ...BarFrag
          }
        }

        fragment BarFrag on Bar {
          id
          foo {
             ...FooFrag
          }
        }

        fragment FooFrag on Foo {
          id
          bar {
            ...BarFrag
          }
        }
    ";
    let res = insert_and_query(subgraph_id, schema, vec![], co_recursive)
        .await
        .unwrap();
    let data = res.to_result().unwrap_err()[0].to_string();
    assert!(FOO_BAR_ERRORS.contains(&data.as_str()));
}

// Tests for graphql interfaces.

use graph::prelude::*;
use graph_graphql::prelude::{execute_query, QueryExecutionOptions, StoreResolver};
use test_store::*;

// `entities` is `(entity, type)`.
fn insert_and_query(
    subgraph_id: &str,
    schema: &str,
    entities: Vec<(Entity, &str)>,
    query: &str,
) -> Result<QueryResult, StoreError> {
    let subgraph_id = SubgraphDeploymentId::new(subgraph_id).unwrap();
    let schema = Schema::parse(schema, subgraph_id.clone()).unwrap();

    let manifest = SubgraphManifest {
        id: subgraph_id.clone(),
        location: String::new(),
        spec_version: "1".to_owned(),
        description: None,
        repository: None,
        schema: schema.clone(),
        data_sources: vec![],
    };

    STORE
        .apply_entity_operations(
            SubgraphDeploymentEntity::new(&manifest, false, false, Default::default(), 1)
                .create_operations_replace(&subgraph_id),
            EventSource::None,
        )
        .unwrap();

    let insert_ops = entities
        .into_iter()
        .map(|(data, entity_type)| EntityOperation::Set {
            key: EntityKey {
                subgraph_id: subgraph_id.clone(),
                entity_type: entity_type.to_owned(),
                entity_id: data["id"].clone().as_string().unwrap(),
            },
            data,
        });

    STORE.apply_entity_operations(insert_ops.collect(), EventSource::None)?;

    let logger = Logger::root(slog::Discard, o!());
    let resolver = StoreResolver::new(&logger, STORE.clone());

    let options = QueryExecutionOptions {
        logger,
        resolver,
        deadline: None,
        max_complexity: None,
        max_depth: 100,
    };
    let document = graphql_parser::parse_query(query).unwrap();
    let query = Query {
        schema: STORE.subgraph_schema(&subgraph_id).unwrap(),
        document,
        variables: None,
    };
    Ok(execute_query(&query, options))
}

#[test]
fn one_interface_zero_entities() {
    let subgraph_id = "oneInterfaceZeroEntities";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let query = "query { leggeds(first: 100) { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![], query).unwrap();

    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"leggeds\": List([])})"
    )
}

#[test]
fn one_interface_one_entity() {
    let subgraph_id = "oneInterfaceOneEntity";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let entity = (
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
        "Animal",
    );

    // Collection query.
    let query = "query { leggeds(first: 100) { legs } }";
    let res = insert_and_query(subgraph_id, schema, vec![entity], query).unwrap();
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"leggeds\": List([Object({\"legs\": Int(Number(3))})])})"
    );

    // Query by ID.
    let query = "query { legged(id: \"1\") { legs } }";
    let res = insert_and_query(subgraph_id, schema, vec![], query).unwrap();
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"legged\": Object({\"legs\": Int(Number(3))})})",
    );
}

#[test]
fn one_interface_one_entity_typename() {
    let subgraph_id = "oneInterfaceOneEntityTypename";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }";

    let entity = (
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
        "Animal",
    );

    let query = "query { leggeds(first: 100) { __typename } }";

    let res = insert_and_query(subgraph_id, schema, vec![entity], query).unwrap();
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"leggeds\": List([Object({\"__typename\": String(\"Animal\")})])})"
    )
}

#[test]
fn one_interface_multiple_entities() {
    let subgraph_id = "oneInterfaceMultipleEntities";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }
                  type Furniture implements Legged @entity { id: ID!, legs: Int }
                  ";

    let animal = (
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
        "Animal",
    );
    let furniture = (
        Entity::from(vec![("id", Value::from("2")), ("legs", Value::from(4))]),
        "Furniture",
    );

    let query = "query { leggeds(first: 100, orderBy: legs) { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![animal, furniture], query).unwrap();
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"leggeds\": List([Object({\"legs\": Int(Number(3))}), Object({\"legs\": Int(Number(4))})])})"
    )
}

#[test]
fn reference_interface() {
    let subgraph_id = "ReferenceInterface";
    let schema = "type Leg @entity { id: ID! }
                  interface Legged { leg: Leg }
                  type Animal implements Legged @entity { id: ID!, leg: Leg }";

    let query = "query { leggeds(first: 100) { leg { id } } }";

    let leg = (Entity::from(vec![("id", Value::from("1"))]), "Leg");
    let animal = (
        Entity::from(vec![("id", Value::from("1")), ("leg", Value::from("1"))]),
        "Animal",
    );

    let res = insert_and_query(subgraph_id, schema, vec![leg, animal], query).unwrap();

    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"leggeds\": List([Object({\"leg\": Object({\"id\": String(\"1\")})})])})"
    )
}

#[test]
fn conflicting_implementors_id() {
    let subgraph_id = "ConflictingImplementorsId";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, legs: Int }
                  type Furniture implements Legged @entity { id: ID!, legs: Int }
                  ";

    let animal = (
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
        "Animal",
    );
    let furniture = (
        Entity::from(vec![("id", Value::from("1")), ("legs", Value::from(3))]),
        "Furniture",
    );

    let query = "query { leggeds(first: 100) { legs } }";

    let res = insert_and_query(subgraph_id, schema, vec![animal, furniture], query);
    assert_eq!(
        res.unwrap_err().to_string(),
        "tried to set entity of type `Furniture` with ID \"1\" but an entity of type `Animal`, \
         which has an interface in common with `Furniture`, exists with the same ID"
    );
}

#[test]
fn derived_interface_relationship() {
    let subgraph_id = "DerivedInterfaceRelationship";
    let schema = "interface ForestDweller { id: ID!, forest: Forest }
                  type Animal implements ForestDweller @entity { id: ID!, forest: Forest }
                  type Forest @entity { id: ID!, dwellers: [ForestDweller]! @derivedFrom(field: \"forest\") }
                  ";

    let forest = (Entity::from(vec![("id", Value::from("1"))]), "Forest");
    let animal = (
        Entity::from(vec![("id", Value::from("1")), ("forest", Value::from("1"))]),
        "Animal",
    );

    let query = "query { forests(first: 100) { dwellers(first: 100) { id } } }";

    let res = insert_and_query(subgraph_id, schema, vec![forest, animal], query);
    assert_eq!(
        res.unwrap().data.unwrap().to_string(),
        "{forests: [{dwellers: [{id: \"1\"}]}]}"
    );
}

#[test]
fn two_interfaces() {
    let subgraph_id = "TwoInterfaces";
    let schema = "interface IFoo { foo: String! }
                  interface IBar { bar: Int! }

                  type A implements IFoo @entity { id: ID!, foo: String! }
                  type B implements IBar @entity { id: ID!, bar: Int! }

                  type AB implements IFoo & IBar @entity { id: ID!, foo: String!, bar: Int! }
                  ";

    let a = (
        Entity::from(vec![("id", Value::from("1")), ("foo", Value::from("bla"))]),
        "A",
    );
    let b = (
        Entity::from(vec![("id", Value::from("1")), ("bar", Value::from(100))]),
        "B",
    );
    let ab = (
        Entity::from(vec![
            ("id", Value::from("2")),
            ("foo", Value::from("ble")),
            ("bar", Value::from(200)),
        ]),
        "AB",
    );

    let query = "query {
                    ibars(first: 100, orderBy: bar) { bar }
                    ifoos(first: 100, orderBy: foo) { foo }
                }";
    let res = insert_and_query(subgraph_id, schema, vec![a, b, ab], query).unwrap();
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        "Object({\"ibars\": List([Object({\"bar\": Int(Number(100))}), Object({\"bar\": Int(Number(200))})]), \
                 \"ifoos\": List([Object({\"foo\": String(\"bla\")}), Object({\"foo\": String(\"ble\")})])})"
    );
}

#[test]
fn interface_non_inline_fragment() {
    let subgraph_id = "interfaceNonInlineFragment";
    let schema = "interface Legged { legs: Int }
                  type Animal implements Legged @entity { id: ID!, name: String, legs: Int }";

    let entity = (
        Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("cow")),
            ("legs", Value::from(3)),
        ]),
        "Animal",
    );

    // Query only the fragment.
    let query = "query { leggeds { ...frag } } fragment frag on Animal { name }";
    let res = insert_and_query(subgraph_id, schema, vec![entity], query).unwrap();
    dbg!(&res.errors);
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        r#"Object({"leggeds": List([Object({"name": String("cow")})])})"#
    );

    // Query the fragment and something else.
    let query = "query { leggeds { legs, ...frag } } fragment frag on Animal { name }";
    let res = insert_and_query(subgraph_id, schema, vec![], query).unwrap();
    assert!(res.errors.is_none());
    assert_eq!(
        format!("{:?}", res.data.unwrap()),
        r#"Object({"leggeds": List([Object({"legs": Int(Number(3)), "name": String("cow")})])})"#,
    );
}

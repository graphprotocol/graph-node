use graphql_parser;

use graph::data::graphql::ext::DocumentExt;
use graph::data::graphql::ext::ObjectTypeExt;
use graph::data::schema::{ApiSchema, Schema};
use graph::data::subgraph::DeploymentHash;
use graph::prelude::s::{Document, ObjectType};

use lazy_static::lazy_static;

const INTROSPECTION_SCHEMA: &str = "
scalar Boolean
scalar Float
scalar Int
scalar ID
scalar String

type Query {
  __schema: __Schema!
  __type(name: String!): __Type
}

type __Schema {
  types: [__Type!]!
  queryType: __Type!
  mutationType: __Type
  subscriptionType: __Type
  directives: [__Directive!]!
}

type __Type {
  kind: __TypeKind!
  name: String
  description: String

  # OBJECT and INTERFACE only
  fields(includeDeprecated: Boolean = false): [__Field!]

  # OBJECT only
  interfaces: [__Type!]

  # INTERFACE and UNION only
  possibleTypes: [__Type!]

  # ENUM only
  enumValues(includeDeprecated: Boolean = false): [__EnumValue!]

  # INPUT_OBJECT only
  inputFields: [__InputValue!]

  # NON_NULL and LIST only
  ofType: __Type
}

type __Field {
  name: String!
  description: String
  args: [__InputValue!]!
  type: __Type!
  isDeprecated: Boolean!
  deprecationReason: String
}

type __InputValue {
  name: String!
  description: String
  type: __Type!
  defaultValue: String
}

type __EnumValue {
  name: String!
  description: String
  isDeprecated: Boolean!
  deprecationReason: String
}

enum __TypeKind {
  SCALAR
  OBJECT
  INTERFACE
  UNION
  ENUM
  INPUT_OBJECT
  LIST
  NON_NULL
}

type __Directive {
  name: String!
  description: String
  locations: [__DirectiveLocation!]!
  args: [__InputValue!]!
}

enum __DirectiveLocation {
  QUERY
  MUTATION
  SUBSCRIPTION
  FIELD
  FRAGMENT_DEFINITION
  FRAGMENT_SPREAD
  INLINE_FRAGMENT
  SCHEMA
  SCALAR
  OBJECT
  FIELD_DEFINITION
  ARGUMENT_DEFINITION
  INTERFACE
  UNION
  ENUM
  ENUM_VALUE
  INPUT_OBJECT
  INPUT_FIELD_DEFINITION
}";

lazy_static! {
    pub static ref INTROSPECTION_DOCUMENT: Document =
        graphql_parser::parse_schema(INTROSPECTION_SCHEMA).unwrap();
    pub static ref INTROSPECTION_QUERY_TYPE: &'static ObjectType =
        INTROSPECTION_DOCUMENT.get_root_query_type().unwrap();
}

pub fn introspection_schema(id: DeploymentHash) -> ApiSchema {
    ApiSchema::from_api_schema(Schema::new(id, INTROSPECTION_DOCUMENT.clone())).unwrap()
}

pub fn is_introspection_field(name: &String) -> bool {
    INTROSPECTION_QUERY_TYPE.field(name).is_some()
}

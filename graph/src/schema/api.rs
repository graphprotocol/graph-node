use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use graphql_parser::Pos;
use lazy_static::lazy_static;
use thiserror::Error;

use crate::cheap_clone::CheapClone;
use crate::data::graphql::{ObjectOrInterface, ObjectTypeExt, TypeExt};
use crate::data::store::IdType;
use crate::env::ENV_VARS;
use crate::schema::{ast, META_FIELD_NAME, META_FIELD_TYPE, SCHEMA_TYPE_NAME};

use crate::data::graphql::ext::{
    camel_cased_names, DefinitionExt, DirectiveExt, DocumentExt, ValueExt,
};
use crate::derive::CheapClone;
use crate::prelude::{q, r, s, DeploymentHash};

use super::{Aggregation, Field, InputSchema, Schema, TypeKind};

#[derive(Error, Debug)]
pub enum APISchemaError {
    #[error("type {0} already exists in the input schema")]
    TypeExists(String),
    #[error("Type {0} not found")]
    TypeNotFound(String),
    #[error("Fulltext search is not yet deterministic")]
    FulltextSearchNonDeterministic,
    #[error("Illegal type for `id`: {0}")]
    IllegalIdType(String),
    #[error("Failed to create API schema: {0}")]
    SchemaCreationFailed(String),
}

// The followoing types are defined in meta.graphql
const BLOCK_HEIGHT: &str = "Block_height";
const CHANGE_BLOCK_FILTER_NAME: &str = "BlockChangedFilter";
const ERROR_POLICY_TYPE: &str = "_SubgraphErrorPolicy_";

#[derive(Debug, PartialEq, Eq, Copy, Clone, CheapClone)]
pub enum ErrorPolicy {
    Allow,
    Deny,
}

impl std::str::FromStr for ErrorPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ErrorPolicy, anyhow::Error> {
        match s {
            "allow" => Ok(ErrorPolicy::Allow),
            "deny" => Ok(ErrorPolicy::Deny),
            _ => Err(anyhow::anyhow!("failed to parse `{}` as ErrorPolicy", s)),
        }
    }
}

impl TryFrom<&q::Value> for ErrorPolicy {
    type Error = anyhow::Error;

    /// `value` should be the output of input value coercion.
    fn try_from(value: &q::Value) -> Result<Self, Self::Error> {
        match value {
            q::Value::Enum(s) => ErrorPolicy::from_str(s),
            _ => Err(anyhow::anyhow!("invalid `ErrorPolicy`")),
        }
    }
}

impl TryFrom<&r::Value> for ErrorPolicy {
    type Error = anyhow::Error;

    /// `value` should be the output of input value coercion.
    fn try_from(value: &r::Value) -> Result<Self, Self::Error> {
        match value {
            r::Value::Enum(s) => ErrorPolicy::from_str(s),
            _ => Err(anyhow::anyhow!("invalid `ErrorPolicy`")),
        }
    }
}

/// A GraphQL schema used for responding to queries. These schemas can be
/// generated in one of two ways:
///
/// (1) By calling `api_schema()` on an `InputSchema`. This is the way to
/// generate a query schema for a subgraph.
///
/// (2) By parsing an appropriate GraphQL schema from text and calling
/// `from_graphql_schema`. In that case, it's the caller's responsibility to
/// make sure that the schema has all the types needed for querying, like
/// `Query` and `Subscription`
///
/// Because of the second point, once constructed, it can not be assumed
/// that an `ApiSchema` is based on an `InputSchema` and it can only be used
/// for querying.
#[derive(Debug)]
pub struct ApiSchema {
    schema: Schema,

    // Root types for the api schema.
    pub query_type: Arc<s::ObjectType>,
    pub subscription_type: Option<Arc<s::ObjectType>>,
    object_types: HashMap<String, Arc<s::ObjectType>>,
}

impl ApiSchema {
    /// Set up the `ApiSchema`, mostly by extracting important pieces of
    /// information from it like `query_type` etc.
    ///
    /// In addition, the API schema has an introspection schema mixed into
    /// `api_schema`. In particular, the `Query` type has fields called
    /// `__schema` and `__type`
    pub(in crate::schema) fn from_api_schema(mut schema: Schema) -> Result<Self, anyhow::Error> {
        add_introspection_schema(&mut schema.document);

        let query_type = schema
            .document
            .get_root_query_type()
            .context("no root `Query` in the schema")?
            .clone();
        let subscription_type = schema
            .document
            .get_root_subscription_type()
            .cloned()
            .map(Arc::new);

        let object_types = HashMap::from_iter(
            schema
                .document
                .get_object_type_definitions()
                .into_iter()
                .map(|obj_type| (obj_type.name.clone(), Arc::new(obj_type.clone()))),
        );

        Ok(Self {
            schema,
            query_type: Arc::new(query_type),
            subscription_type,
            object_types,
        })
    }

    /// Create an API Schema that can be used to execute GraphQL queries.
    /// This method is only meant for schemas that are not derived from a
    /// subgraph schema, like the schema for the index-node server. Use
    /// `InputSchema::api_schema` to get an API schema for a subgraph
    pub fn from_graphql_schema(schema: Schema) -> Result<Self, anyhow::Error> {
        Self::from_api_schema(schema)
    }

    pub fn document(&self) -> &s::Document {
        &self.schema.document
    }

    pub fn id(&self) -> &DeploymentHash {
        &self.schema.id
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn types_for_interface(&self) -> &BTreeMap<String, Vec<s::ObjectType>> {
        &self.schema.types_for_interface
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &str) -> Option<&Vec<s::InterfaceType>> {
        self.schema.interfaces_for_type(type_name)
    }

    /// Return an `Arc` around the `ObjectType` from our internal cache
    ///
    /// # Panics
    /// If `obj_type` is not part of this schema, this function panics
    pub fn object_type(&self, obj_type: &s::ObjectType) -> Arc<s::ObjectType> {
        self.object_types
            .get(&obj_type.name)
            .expect("ApiSchema.object_type is only used with existing types")
            .cheap_clone()
    }

    pub fn get_named_type(&self, name: &str) -> Option<&s::TypeDefinition> {
        self.schema.document.get_named_type(name)
    }

    /// Returns true if the given type is an input type.
    ///
    /// Uses the algorithm outlined on
    /// https://facebook.github.io/graphql/draft/#IsInputType().
    pub fn is_input_type(&self, t: &s::Type) -> bool {
        match t {
            s::Type::NamedType(name) => {
                let named_type = self.get_named_type(name);
                named_type.map_or(false, |type_def| match type_def {
                    s::TypeDefinition::Scalar(_)
                    | s::TypeDefinition::Enum(_)
                    | s::TypeDefinition::InputObject(_) => true,
                    _ => false,
                })
            }
            s::Type::ListType(inner) => self.is_input_type(inner),
            s::Type::NonNullType(inner) => self.is_input_type(inner),
        }
    }

    pub fn get_root_query_type_def(&self) -> Option<&s::TypeDefinition> {
        self.schema
            .document
            .definitions
            .iter()
            .find_map(|d| match d {
                s::Definition::TypeDefinition(def @ s::TypeDefinition::Object(_)) => match def {
                    s::TypeDefinition::Object(t) if t.name == "Query" => Some(def),
                    _ => None,
                },
                _ => None,
            })
    }

    pub fn object_or_interface(&self, name: &str) -> Option<ObjectOrInterface<'_>> {
        if name.starts_with("__") {
            INTROSPECTION_SCHEMA.object_or_interface(name)
        } else {
            self.schema.document.object_or_interface(name)
        }
    }

    /// Returns the type definition that a field type corresponds to.
    pub fn get_type_definition_from_field<'a>(
        &'a self,
        field: &s::Field,
    ) -> Option<&'a s::TypeDefinition> {
        self.get_type_definition_from_type(&field.field_type)
    }

    /// Returns the type definition for a type.
    pub fn get_type_definition_from_type<'a>(
        &'a self,
        t: &s::Type,
    ) -> Option<&'a s::TypeDefinition> {
        match t {
            s::Type::NamedType(name) => self.get_named_type(name),
            s::Type::ListType(inner) => self.get_type_definition_from_type(inner),
            s::Type::NonNullType(inner) => self.get_type_definition_from_type(inner),
        }
    }

    #[cfg(debug_assertions)]
    pub fn definitions(&self) -> impl Iterator<Item = &s::Definition> {
        self.schema.document.definitions.iter()
    }
}

lazy_static! {
    static ref INTROSPECTION_SCHEMA: s::Document = {
        let schema = include_str!("introspection.graphql");
        s::parse_schema(schema).expect("the schema `introspection.graphql` is invalid")
    };
    pub static ref INTROSPECTION_QUERY_TYPE: ast::ObjectType = {
        let root_query_type = INTROSPECTION_SCHEMA
            .get_root_query_type()
            .expect("Schema does not have a root query type");
        ast::ObjectType::from(Arc::new(root_query_type.clone()))
    };
}

pub fn is_introspection_field(name: &str) -> bool {
    INTROSPECTION_QUERY_TYPE.field(name).is_some()
}

/// Extend `schema` with the definitions from the introspection schema and
/// modify the root query type to contain the fields from the introspection
/// schema's root query type.
///
/// This results in a schema that combines the original schema with the
/// introspection schema
fn add_introspection_schema(schema: &mut s::Document) {
    fn introspection_fields() -> Vec<s::Field> {
        // Generate fields for the root query fields in an introspection schema,
        // the equivalent of the fields of the `Query` type:
        //
        // type Query {
        //   __schema: __Schema!
        //   __type(name: String!): __Type
        // }

        let type_args = vec![s::InputValue {
            position: Pos::default(),
            description: None,
            name: "name".to_string(),
            value_type: s::Type::NonNullType(Box::new(s::Type::NamedType("String".to_string()))),
            default_value: None,
            directives: vec![],
        }];

        vec![
            s::Field {
                position: Pos::default(),
                description: None,
                name: "__schema".to_string(),
                arguments: vec![],
                field_type: s::Type::NonNullType(Box::new(s::Type::NamedType(
                    "__Schema".to_string(),
                ))),
                directives: vec![],
            },
            s::Field {
                position: Pos::default(),
                description: None,
                name: "__type".to_string(),
                arguments: type_args,
                field_type: s::Type::NamedType("__Type".to_string()),
                directives: vec![],
            },
        ]
    }

    // Add all definitions from the introspection schema to the schema,
    // except for the root query type as that qould clobber the 'real' root
    // query type
    schema.definitions.extend(
        INTROSPECTION_SCHEMA
            .definitions
            .iter()
            .filter(|dfn| !dfn.is_root_query_type())
            .cloned(),
    );

    let query_type = schema
        .definitions
        .iter_mut()
        .filter_map(|d| match d {
            s::Definition::TypeDefinition(s::TypeDefinition::Object(t)) if t.name == "Query" => {
                Some(t)
            }
            _ => None,
        })
        .peekable()
        .next()
        .expect("no root `Query` in the schema");
    query_type.fields.append(&mut introspection_fields());
}

/// Derives a full-fledged GraphQL API schema from an input schema.
///
/// The input schema should only have type/enum/interface/union definitions
/// and must not include a root Query type. This Query type is derived, with
/// all its fields and their input arguments, based on the existing types.
pub(in crate::schema) fn api_schema(
    input_schema: &InputSchema,
) -> Result<s::Document, APISchemaError> {
    // Refactor: Don't clone the schema.
    let mut api = init_api_schema(input_schema)?;
    add_meta_field_type(&mut api.document);
    add_types_for_object_types(&mut api, input_schema)?;
    add_types_for_interface_types(&mut api, input_schema)?;
    add_types_for_aggregation_types(&mut api, input_schema)?;
    add_query_type(&mut api.document, input_schema)?;
    add_subscription_type(&mut api.document, input_schema)?;
    Ok(api.document)
}

/// Initialize the API schema by copying type definitions from the input
/// schema. The copies of the type definitions are modified to allow
/// filtering and ordering of collections of entities.
fn init_api_schema(input_schema: &InputSchema) -> Result<Schema, APISchemaError> {
    /// Add arguments to fields that reference collections of other entities to
    /// allow e.g. filtering and ordering the collections. The `fields` should
    /// be the fields of an object or interface type
    fn add_collection_arguments(fields: &mut [s::Field], input_schema: &InputSchema) {
        for field in fields.iter_mut().filter(|field| field.field_type.is_list()) {
            let field_type = field.field_type.get_base_type();
            // `field_type`` could be an enum or scalar, in which case
            // `type_kind_str` will return `None``
            if let Some(ops) = input_schema
                .kind_of_declared_type(field_type)
                .map(FilterOps::for_kind)
            {
                field.arguments = ops.collection_arguments(field_type);
            }
        }
    }

    fn add_type_def(
        api: &mut s::Document,
        type_def: &s::TypeDefinition,
        input_schema: &InputSchema,
    ) -> Result<(), APISchemaError> {
        match type_def {
            s::TypeDefinition::Object(ot) => {
                if ot.name != SCHEMA_TYPE_NAME {
                    let mut ot = ot.clone();
                    add_collection_arguments(&mut ot.fields, input_schema);
                    let typedef = s::TypeDefinition::Object(ot);
                    let def = s::Definition::TypeDefinition(typedef);
                    api.definitions.push(def);
                }
            }
            s::TypeDefinition::Interface(it) => {
                let mut it = it.clone();
                add_collection_arguments(&mut it.fields, input_schema);
                let typedef = s::TypeDefinition::Interface(it);
                let def = s::Definition::TypeDefinition(typedef);
                api.definitions.push(def);
            }
            s::TypeDefinition::Enum(et) => {
                let typedef = s::TypeDefinition::Enum(et.clone());
                let def = s::Definition::TypeDefinition(typedef);
                api.definitions.push(def);
            }
            s::TypeDefinition::InputObject(_) => {
                // We don't support input object types in subgraph schemas
                // but some subgraphs use that to then pass parameters of
                // that type to queries
                api.definitions
                    .push(s::Definition::TypeDefinition(type_def.clone()));
            }
            s::TypeDefinition::Scalar(_) | s::TypeDefinition::Union(_) => {
                // We don't support these type definitions in subgraph schemas
                // but there are subgraphs out in the wild that contain them. We
                // simply ignore them even though we should produce an error
            }
        }
        Ok(())
    }

    let mut api = s::Document::default();
    for defn in input_schema.schema().document.definitions.iter() {
        match defn {
            s::Definition::SchemaDefinition(_) | s::Definition::TypeExtension(_) => {
                // We don't support these in subgraph schemas but there are
                // subgraphs out in the wild that contain them. We simply
                // ignore them even though we should produce an error
            }
            s::Definition::DirectiveDefinition(_) => {
                // We don't really allow directive definitions in subgraph
                // schemas, but the tests for introspection schemas create
                // an input schema with a directive definition, and it's
                // safer to allow it here rather than fail
                api.definitions.push(defn.clone());
            }
            s::Definition::TypeDefinition(td) => add_type_def(&mut api, td, input_schema)?,
        }
    }

    Schema::new(input_schema.id().clone(), api)
        .map_err(|e| APISchemaError::SchemaCreationFailed(e.to_string()))
}

/// Adds a global `_Meta_` type to the schema. The `_meta` field
/// accepts values of this type
fn add_meta_field_type(api: &mut s::Document) {
    lazy_static! {
        static ref META_FIELD_SCHEMA: s::Document = {
            let schema = include_str!("meta.graphql");
            s::parse_schema(schema).expect("the schema `meta.graphql` is invalid")
        };
    }

    api.definitions
        .extend(META_FIELD_SCHEMA.definitions.iter().cloned());
}

fn add_types_for_object_types(
    api: &mut Schema,
    schema: &InputSchema,
) -> Result<(), APISchemaError> {
    for (name, object_type) in schema.object_types() {
        add_order_by_type(&mut api.document, name, &object_type.fields)?;
        add_filter_type(api, name, &object_type.fields)?;
    }
    Ok(())
}

/// Adds `*_orderBy` and `*_filter` enum types for the given interfaces to the schema.
fn add_types_for_interface_types(
    api: &mut Schema,
    input_schema: &InputSchema,
) -> Result<(), APISchemaError> {
    for (name, interface_type) in input_schema.interface_types() {
        add_order_by_type(&mut api.document, name, &interface_type.fields)?;
        add_filter_type(api, name, &interface_type.fields)?;
    }
    Ok(())
}

fn add_types_for_aggregation_types(
    api: &mut Schema,
    input_schema: &InputSchema,
) -> Result<(), APISchemaError> {
    for (name, agg_type) in input_schema.aggregation_types() {
        add_aggregation_filter_type(api, name, agg_type)?;
    }
    Ok(())
}

/// Adds a `<type_name>_orderBy` enum type for the given fields to the schema.
fn add_order_by_type(
    api: &mut s::Document,
    type_name: &str,
    fields: &[Field],
) -> Result<(), APISchemaError> {
    let type_name = format!("{}_orderBy", type_name);

    match api.get_named_type(&type_name) {
        None => {
            let typedef = s::TypeDefinition::Enum(s::EnumType {
                position: Pos::default(),
                description: None,
                name: type_name,
                directives: vec![],
                values: field_enum_values(api, fields)?,
            });
            let def = s::Definition::TypeDefinition(typedef);
            api.definitions.push(def);
        }
        Some(_) => return Err(APISchemaError::TypeExists(type_name)),
    }
    Ok(())
}

/// Generates enum values for the given set of fields.
fn field_enum_values(
    schema: &s::Document,
    fields: &[Field],
) -> Result<Vec<s::EnumValue>, APISchemaError> {
    let mut enum_values = vec![];
    for field in fields {
        enum_values.push(s::EnumValue {
            position: Pos::default(),
            description: None,
            name: field.name.to_string(),
            directives: vec![],
        });
        enum_values.extend(field_enum_values_from_child_entity(schema, field)?);
    }
    Ok(enum_values)
}

fn enum_value_from_child_entity_field(
    schema: &s::Document,
    parent_field_name: &str,
    field: &s::Field,
) -> Option<s::EnumValue> {
    if ast::is_list_or_non_null_list_field(field) || ast::is_entity_type(schema, &field.field_type)
    {
        // Sorting on lists or entities is not supported.
        None
    } else {
        Some(s::EnumValue {
            position: Pos::default(),
            description: None,
            name: format!("{}__{}", parent_field_name, field.name),
            directives: vec![],
        })
    }
}

fn field_enum_values_from_child_entity(
    schema: &s::Document,
    field: &Field,
) -> Result<Vec<s::EnumValue>, APISchemaError> {
    fn resolve_supported_type_name(field_type: &s::Type) -> Option<&String> {
        match field_type {
            s::Type::NamedType(name) => Some(name),
            s::Type::ListType(_) => None,
            s::Type::NonNullType(of_type) => resolve_supported_type_name(of_type),
        }
    }

    let type_name = match ENV_VARS.graphql.disable_child_sorting {
        true => None,
        false => resolve_supported_type_name(&field.field_type),
    };

    Ok(match type_name {
        Some(name) => {
            let named_type = schema
                .get_named_type(name)
                .ok_or_else(|| APISchemaError::TypeNotFound(name.clone()))?;
            match named_type {
                s::TypeDefinition::Object(s::ObjectType { fields, .. })
                | s::TypeDefinition::Interface(s::InterfaceType { fields, .. }) => fields
                    .iter()
                    .filter_map(|f| {
                        enum_value_from_child_entity_field(schema, field.name.as_str(), f)
                    })
                    .collect(),
                _ => vec![],
            }
        }
        None => vec![],
    })
}

/// Create an input object type definition for the `where` argument of a
/// collection. The `name` is the name of the filter type, e.g.,
/// `User_filter` and the fields are all the possible filters. This function
/// adds fields for boolean `and` and `or` filters and for filtering by
/// block change to the given fields.
fn filter_type_defn(name: String, mut fields: Vec<s::InputValue>) -> s::Definition {
    fields.push(block_changed_filter_argument());

    if !ENV_VARS.graphql.disable_bool_filters {
        fields.push(s::InputValue {
            position: Pos::default(),
            description: None,
            name: "and".to_string(),
            value_type: s::Type::ListType(Box::new(s::Type::NamedType(name.clone()))),
            default_value: None,
            directives: vec![],
        });

        fields.push(s::InputValue {
            position: Pos::default(),
            description: None,
            name: "or".to_string(),
            value_type: s::Type::ListType(Box::new(s::Type::NamedType(name.clone()))),
            default_value: None,
            directives: vec![],
        });
    }

    let typedef = s::TypeDefinition::InputObject(s::InputObjectType {
        position: Pos::default(),
        description: None,
        name,
        directives: vec![],
        fields,
    });
    s::Definition::TypeDefinition(typedef)
}

/// Selector for the kind of field filters to generate
#[derive(Copy, Clone)]
enum FilterOps {
    /// Use ops for object and interface types
    Object,
    /// Use ops for aggregation types
    Aggregation,
}

impl FilterOps {
    fn for_type<'a>(&self, scalar_type: &'a s::ScalarType) -> FilterOpsSet<'a> {
        match self {
            Self::Object => FilterOpsSet::Object(&scalar_type.name),
            Self::Aggregation => FilterOpsSet::Aggregation(&scalar_type.name),
        }
    }

    fn for_kind(kind: TypeKind) -> FilterOps {
        match kind {
            TypeKind::Object | TypeKind::Interface => FilterOps::Object,
            TypeKind::Aggregation => FilterOps::Aggregation,
        }
    }

    /// Generates arguments for collection queries of a named type (e.g. User).
    fn collection_arguments(&self, type_name: &str) -> Vec<s::InputValue> {
        // `first` and `skip` should be non-nullable, but the Apollo graphql client
        // exhibts non-conforming behaviour by erroing if no value is provided for a
        // non-nullable field, regardless of the presence of a default.
        let mut skip = input_value("skip", "", s::Type::NamedType("Int".to_string()));
        skip.default_value = Some(s::Value::Int(0.into()));

        let mut first = input_value("first", "", s::Type::NamedType("Int".to_string()));
        first.default_value = Some(s::Value::Int(100.into()));

        let filter_type = s::Type::NamedType(format!("{}_filter", type_name));
        let filter = input_value("where", "", filter_type);

        let order_by = match self {
            FilterOps::Object => vec![
                input_value(
                    "orderBy",
                    "",
                    s::Type::NamedType(format!("{}_orderBy", type_name)),
                ),
                input_value(
                    "orderDirection",
                    "",
                    s::Type::NamedType("OrderDirection".to_string()),
                ),
            ],
            FilterOps::Aggregation => vec![input_value(
                "interval",
                "",
                s::Type::NonNullType(Box::new(s::Type::NamedType(
                    "Aggregation_interval".to_string(),
                ))),
            )],
        };

        let mut args = vec![skip, first];
        args.extend(order_by);
        args.push(filter);

        args
    }
}

#[derive(Copy, Clone)]
enum FilterOpsSet<'a> {
    Object(&'a str),
    Aggregation(&'a str),
}

impl<'a> FilterOpsSet<'a> {
    fn type_name(&self) -> &'a str {
        match self {
            Self::Object(type_name) | Self::Aggregation(type_name) => type_name,
        }
    }
}

/// Adds a `<type_name>_filter` enum type for the given fields to the
/// schema. Used for object and interface types
fn add_filter_type(
    api: &mut Schema,
    type_name: &str,
    fields: &[Field],
) -> Result<(), APISchemaError> {
    let filter_type_name = format!("{}_filter", type_name);
    if api.document.get_named_type(&filter_type_name).is_some() {
        return Err(APISchemaError::TypeExists(filter_type_name));
    }
    let filter_fields = field_input_values(api, fields, FilterOps::Object)?;

    let defn = filter_type_defn(filter_type_name, filter_fields);
    api.document.definitions.push(defn);

    Ok(())
}

fn add_aggregation_filter_type(
    api: &mut Schema,
    type_name: &str,
    agg: &Aggregation,
) -> Result<(), APISchemaError> {
    let filter_type_name = format!("{}_filter", type_name);
    if api.document.get_named_type(&filter_type_name).is_some() {
        return Err(APISchemaError::TypeExists(filter_type_name));
    }
    let filter_fields = field_input_values(api, &agg.fields, FilterOps::Aggregation)?;

    let defn = filter_type_defn(filter_type_name, filter_fields);
    api.document.definitions.push(defn);

    Ok(())
}

/// Generates `*_filter` input values for the given set of fields.
fn field_input_values(
    schema: &Schema,
    fields: &[Field],
    ops: FilterOps,
) -> Result<Vec<s::InputValue>, APISchemaError> {
    let mut input_values = vec![];
    for field in fields {
        input_values.extend(field_filter_input_values(schema, field, ops)?);
    }
    Ok(input_values)
}

/// Generates `*_filter` input values for the given field.
fn field_filter_input_values(
    schema: &Schema,
    field: &Field,
    ops: FilterOps,
) -> Result<Vec<s::InputValue>, APISchemaError> {
    let type_name = field.field_type.get_base_type();
    if field.is_list() {
        Ok(field_list_filter_input_values(schema, field)?.unwrap_or_default())
    } else {
        let named_type = schema
            .document
            .get_named_type(type_name)
            .ok_or_else(|| APISchemaError::TypeNotFound(type_name.to_string()))?;
        Ok(match named_type {
            s::TypeDefinition::Object(_) | s::TypeDefinition::Interface(_) => {
                let scalar_type = id_type_as_scalar(schema, named_type)?.unwrap();
                let mut input_values = if field.is_derived() {
                    // Only add `where` filter fields for object and interface fields
                    // if they are not @derivedFrom
                    vec![]
                } else {
                    // We allow filtering with `where: { other: "some-id" }` and
                    // `where: { others: ["some-id", "other-id"] }`. In both cases,
                    // we allow ID strings as the values to be passed to these
                    // filters.
                    field_scalar_filter_input_values(
                        &schema.document,
                        field,
                        ops.for_type(&scalar_type),
                    )
                };
                extend_with_child_filter_input_value(field, type_name, &mut input_values);
                input_values
            }
            s::TypeDefinition::Scalar(ref t) => {
                field_scalar_filter_input_values(&schema.document, field, ops.for_type(t))
            }
            s::TypeDefinition::Enum(ref t) => {
                field_enum_filter_input_values(&schema.document, field, t)
            }
            _ => vec![],
        })
    }
}

fn id_type_as_scalar(
    schema: &Schema,
    typedef: &s::TypeDefinition,
) -> Result<Option<s::ScalarType>, APISchemaError> {
    let id_type = match typedef {
        s::TypeDefinition::Object(obj_type) => IdType::try_from(obj_type)
            .map(Option::Some)
            .map_err(|_| APISchemaError::IllegalIdType(obj_type.name.to_owned())),
        s::TypeDefinition::Interface(intf_type) => {
            match schema
                .types_for_interface
                .get(&intf_type.name)
                .and_then(|obj_types| obj_types.first())
            {
                None => Ok(Some(IdType::String)),
                Some(obj_type) => IdType::try_from(obj_type)
                    .map(Option::Some)
                    .map_err(|_| APISchemaError::IllegalIdType(obj_type.name.to_owned())),
            }
        }
        _ => Ok(None),
    }?;
    let scalar_type = id_type.map(|id_type| match id_type {
        IdType::String | IdType::Bytes => s::ScalarType::new(String::from("String")),
        // It would be more logical to use "Int8" here, but currently, that
        // leads to values being turned into strings, not i64 which causes
        // database queries to fail in various places. Once this is fixed
        // (check e.g., `Value::coerce_scalar` in `graph/src/data/value.rs`)
        // we can turn that into "Int8". For now, queries can only query
        // Int8 id values up to i32::MAX.
        IdType::Int8 => s::ScalarType::new(String::from("Int")),
    });
    Ok(scalar_type)
}

fn field_filter_ops(set: FilterOpsSet<'_>) -> &'static [&'static str] {
    use FilterOpsSet::*;

    match set {
        Object("Boolean") => &["", "not", "in", "not_in"],
        Object("Bytes") => &[
            "",
            "not",
            "gt",
            "lt",
            "gte",
            "lte",
            "in",
            "not_in",
            "contains",
            "not_contains",
        ],
        Object("ID") => &["", "not", "gt", "lt", "gte", "lte", "in", "not_in"],
        Object("BigInt") | Object("BigDecimal") | Object("Int") | Object("Int8")
        | Object("Timestamp") => &["", "not", "gt", "lt", "gte", "lte", "in", "not_in"],
        Object("String") => &[
            "",
            "not",
            "gt",
            "lt",
            "gte",
            "lte",
            "in",
            "not_in",
            "contains",
            "contains_nocase",
            "not_contains",
            "not_contains_nocase",
            "starts_with",
            "starts_with_nocase",
            "not_starts_with",
            "not_starts_with_nocase",
            "ends_with",
            "ends_with_nocase",
            "not_ends_with",
            "not_ends_with_nocase",
        ],
        Aggregation("BigInt")
        | Aggregation("BigDecimal")
        | Aggregation("Int")
        | Aggregation("Int8")
        | Aggregation("Timestamp") => &["", "gt", "lt", "gte", "lte", "in"],
        Object(_) => &["", "not"],
        Aggregation(_) => &[""],
    }
}

/// Generates `*_filter` input values for the given scalar field.
fn field_scalar_filter_input_values(
    _schema: &s::Document,
    field: &Field,
    set: FilterOpsSet<'_>,
) -> Vec<s::InputValue> {
    field_filter_ops(set)
        .into_iter()
        .map(|filter_type| {
            let field_type = s::Type::NamedType(set.type_name().to_string());
            let value_type = match *filter_type {
                "in" | "not_in" => {
                    s::Type::ListType(Box::new(s::Type::NonNullType(Box::new(field_type))))
                }
                _ => field_type,
            };
            input_value(&field.name, filter_type, value_type)
        })
        .collect()
}

/// Appends a child filter to input values
fn extend_with_child_filter_input_value(
    field: &Field,
    field_type_name: &str,
    input_values: &mut Vec<s::InputValue>,
) {
    input_values.push(input_value(
        &format!("{}_", field.name),
        "",
        s::Type::NamedType(format!("{}_filter", field_type_name)),
    ));
}

/// Generates `*_filter` input values for the given enum field.
fn field_enum_filter_input_values(
    _schema: &s::Document,
    field: &Field,
    field_type: &s::EnumType,
) -> Vec<s::InputValue> {
    vec!["", "not", "in", "not_in"]
        .into_iter()
        .map(|filter_type| {
            let field_type = s::Type::NamedType(field_type.name.clone());
            let value_type = match filter_type {
                "in" | "not_in" => {
                    s::Type::ListType(Box::new(s::Type::NonNullType(Box::new(field_type))))
                }
                _ => field_type,
            };
            input_value(&field.name, filter_type, value_type)
        })
        .collect()
}

/// Generates `*_filter` input values for the given list field.
fn field_list_filter_input_values(
    schema: &Schema,
    field: &Field,
) -> Result<Option<Vec<s::InputValue>>, APISchemaError> {
    // Only add a filter field if the type of the field exists in the schema
    let typedef = match ast::get_type_definition_from_type(&schema.document, &field.field_type) {
        Some(typedef) => typedef,
        None => return Ok(None),
    };

    // Decide what type of values can be passed to the filter. In the case
    // one-to-many or many-to-many object or interface fields that are not
    // derived, we allow ID strings to be passed on.
    // Adds child filter only to object types.
    let (input_field_type, parent_type_name) = match typedef {
        s::TypeDefinition::Object(s::ObjectType { name, .. })
        | s::TypeDefinition::Interface(s::InterfaceType { name, .. }) => {
            if field.is_derived() {
                (None, Some(name.clone()))
            } else {
                let scalar_type = id_type_as_scalar(schema, typedef)?.unwrap();
                let named_type = s::Type::NamedType(scalar_type.name);
                (Some(named_type), Some(name.clone()))
            }
        }
        s::TypeDefinition::Scalar(ref t) => (Some(s::Type::NamedType(t.name.clone())), None),
        s::TypeDefinition::Enum(ref t) => (Some(s::Type::NamedType(t.name.clone())), None),
        s::TypeDefinition::InputObject(_) | s::TypeDefinition::Union(_) => (None, None),
    };

    let mut input_values: Vec<s::InputValue> = match input_field_type {
        None => {
            vec![]
        }
        Some(input_field_type) => vec![
            "",
            "not",
            "contains",
            "contains_nocase",
            "not_contains",
            "not_contains_nocase",
        ]
        .into_iter()
        .map(|filter_type| {
            input_value(
                &field.name,
                filter_type,
                s::Type::ListType(Box::new(s::Type::NonNullType(Box::new(
                    input_field_type.clone(),
                )))),
            )
        })
        .collect(),
    };

    if let Some(parent) = parent_type_name {
        extend_with_child_filter_input_value(field, &parent, &mut input_values);
    }

    Ok(Some(input_values))
}

/// Generates a `*_filter` input value for the given field name, suffix and value type.
fn input_value(name: &str, suffix: &'static str, value_type: s::Type) -> s::InputValue {
    s::InputValue {
        position: Pos::default(),
        description: None,
        name: if suffix.is_empty() {
            name.to_owned()
        } else {
            format!("{}_{}", name, suffix)
        },
        value_type,
        default_value: None,
        directives: vec![],
    }
}

/// Adds a root `Query` object type to the schema.
fn add_query_type(api: &mut s::Document, input_schema: &InputSchema) -> Result<(), APISchemaError> {
    let type_name = String::from("Query");

    if api.get_named_type(&type_name).is_some() {
        return Err(APISchemaError::TypeExists(type_name));
    }

    let mut fields = input_schema
        .object_types()
        .map(|(name, _)| name)
        .chain(input_schema.interface_types().map(|(name, _)| name))
        .flat_map(|name| query_fields_for_type(name, FilterOps::Object))
        .collect::<Vec<s::Field>>();
    let mut agg_fields = input_schema
        .aggregation_types()
        .map(|(name, _)| name)
        .flat_map(query_fields_for_agg_type)
        .collect::<Vec<s::Field>>();
    let mut fulltext_fields = input_schema
        .get_fulltext_directives()
        .map_err(|_| APISchemaError::FulltextSearchNonDeterministic)?
        .iter()
        .filter_map(|fulltext| query_field_for_fulltext(fulltext))
        .collect();
    fields.append(&mut agg_fields);
    fields.append(&mut fulltext_fields);
    fields.push(meta_field());

    let typedef = s::TypeDefinition::Object(s::ObjectType {
        position: Pos::default(),
        description: None,
        name: type_name,
        implements_interfaces: vec![],
        directives: vec![],
        fields,
    });
    let def = s::Definition::TypeDefinition(typedef);
    api.definitions.push(def);
    Ok(())
}

fn query_field_for_fulltext(fulltext: &s::Directive) -> Option<s::Field> {
    let name = fulltext.argument("name").unwrap().as_str().unwrap().into();

    let includes = fulltext.argument("include").unwrap().as_list().unwrap();
    // Only one include is allowed per fulltext directive
    let include = includes.iter().next().unwrap();
    let included_entity = include.as_object().unwrap();
    let entity_name = included_entity.get("entity").unwrap().as_str().unwrap();

    let mut arguments = vec![
        // text: String
        s::InputValue {
            position: Pos::default(),
            description: None,
            name: String::from("text"),
            value_type: s::Type::NonNullType(Box::new(s::Type::NamedType(String::from("String")))),
            default_value: None,
            directives: vec![],
        },
        // first: Int
        s::InputValue {
            position: Pos::default(),
            description: None,
            name: String::from("first"),
            value_type: s::Type::NamedType(String::from("Int")),
            default_value: Some(s::Value::Int(100.into())),
            directives: vec![],
        },
        // skip: Int
        s::InputValue {
            position: Pos::default(),
            description: None,
            name: String::from("skip"),
            value_type: s::Type::NamedType(String::from("Int")),
            default_value: Some(s::Value::Int(0.into())),
            directives: vec![],
        },
        // block: BlockHeight
        block_argument(),
        input_value(
            "where",
            "",
            s::Type::NamedType(format!("{}_filter", entity_name)),
        ),
    ];

    arguments.push(subgraph_error_argument());

    Some(s::Field {
        position: Pos::default(),
        description: None,
        name,
        arguments,
        field_type: s::Type::NonNullType(Box::new(s::Type::ListType(Box::new(
            s::Type::NonNullType(Box::new(s::Type::NamedType(entity_name.into()))),
        )))), // included entity type name
        directives: vec![fulltext.clone()],
    })
}

/// Adds a root `Subscription` object type to the schema.
fn add_subscription_type(
    api: &mut s::Document,
    input_schema: &InputSchema,
) -> Result<(), APISchemaError> {
    let type_name = String::from("Subscription");

    if api.get_named_type(&type_name).is_some() {
        return Err(APISchemaError::TypeExists(type_name));
    }

    let mut fields: Vec<s::Field> = input_schema
        .object_types()
        .map(|(name, _)| name)
        .chain(input_schema.interface_types().map(|(name, _)| name))
        .flat_map(|name| query_fields_for_type(name, FilterOps::Object))
        .collect();
    let mut agg_fields = input_schema
        .aggregation_types()
        .map(|(name, _)| name)
        .flat_map(query_fields_for_agg_type)
        .collect::<Vec<s::Field>>();
    fields.append(&mut agg_fields);
    fields.push(meta_field());

    let typedef = s::TypeDefinition::Object(s::ObjectType {
        position: Pos::default(),
        description: None,
        name: type_name,
        implements_interfaces: vec![],
        directives: vec![],
        fields,
    });
    let def = s::Definition::TypeDefinition(typedef);
    api.definitions.push(def);
    Ok(())
}

fn block_argument() -> s::InputValue {
    s::InputValue {
        position: Pos::default(),
        description: Some(
            "The block at which the query should be executed. \
             Can either be a `{ hash: Bytes }` value containing a block hash, \
             a `{ number: Int }` containing the block number, \
             or a `{ number_gte: Int }` containing the minimum block number. \
             In the case of `number_gte`, the query will be executed on the latest block only if \
             the subgraph has progressed to or past the minimum block number. \
             Defaults to the latest block when omitted."
                .to_owned(),
        ),
        name: "block".to_string(),
        value_type: s::Type::NamedType(BLOCK_HEIGHT.to_owned()),
        default_value: None,
        directives: vec![],
    }
}

fn block_changed_filter_argument() -> s::InputValue {
    s::InputValue {
        position: Pos::default(),
        description: Some("Filter for the block changed event.".to_owned()),
        name: "_change_block".to_string(),
        value_type: s::Type::NamedType(CHANGE_BLOCK_FILTER_NAME.to_owned()),
        default_value: None,
        directives: vec![],
    }
}

fn subgraph_error_argument() -> s::InputValue {
    s::InputValue {
        position: Pos::default(),
        description: Some(
            "Set to `allow` to receive data even if the subgraph has skipped over errors while syncing."
                .to_owned(),
        ),
        name: "subgraphError".to_string(),
        value_type: s::Type::NonNullType(Box::new(s::Type::NamedType(ERROR_POLICY_TYPE.to_string()))),
        default_value: Some(s::Value::Enum("deny".to_string())),
        directives: vec![],
    }
}

/// Generates `Query` fields for the given type name (e.g. `users` and `user`).
fn query_fields_for_type(type_name: &str, ops: FilterOps) -> Vec<s::Field> {
    let mut collection_arguments = ops.collection_arguments(type_name);
    collection_arguments.push(block_argument());

    let mut by_id_arguments = vec![
        s::InputValue {
            position: Pos::default(),
            description: None,
            name: "id".to_string(),
            value_type: s::Type::NonNullType(Box::new(s::Type::NamedType("ID".to_string()))),
            default_value: None,
            directives: vec![],
        },
        block_argument(),
    ];

    collection_arguments.push(subgraph_error_argument());
    by_id_arguments.push(subgraph_error_argument());

    // Name formatting must be updated in sync with `graph::data::schema::validate_fulltext_directive_name()`
    let (singular, plural) = camel_cased_names(type_name);
    vec![
        s::Field {
            position: Pos::default(),
            description: None,
            name: singular,
            arguments: by_id_arguments,
            field_type: s::Type::NamedType(type_name.to_owned()),
            directives: vec![],
        },
        s::Field {
            position: Pos::default(),
            description: None,
            name: plural,
            arguments: collection_arguments,
            field_type: s::Type::NonNullType(Box::new(s::Type::ListType(Box::new(
                s::Type::NonNullType(Box::new(s::Type::NamedType(type_name.to_owned()))),
            )))),
            directives: vec![],
        },
    ]
}

fn query_fields_for_agg_type(type_name: &str) -> Vec<s::Field> {
    let mut collection_arguments = FilterOps::Aggregation.collection_arguments(type_name);
    collection_arguments.push(block_argument());
    collection_arguments.push(subgraph_error_argument());

    let (_, plural) = camel_cased_names(type_name);
    vec![s::Field {
        position: Pos::default(),
        description: Some(format!("Collection of aggregated `{}` values", type_name)),
        name: plural,
        arguments: collection_arguments,
        field_type: s::Type::NonNullType(Box::new(s::Type::ListType(Box::new(
            s::Type::NonNullType(Box::new(s::Type::NamedType(type_name.to_owned()))),
        )))),
        directives: vec![],
    }]
}

fn meta_field() -> s::Field {
    lazy_static! {
        static ref META_FIELD: s::Field = s::Field {
            position: Pos::default(),
            description: Some("Access to subgraph metadata".to_string()),
            name: META_FIELD_NAME.to_string(),
            arguments: vec![
                // block: BlockHeight
                s::InputValue {
                    position: Pos::default(),
                    description: None,
                    name: String::from("block"),
                    value_type: s::Type::NamedType(BLOCK_HEIGHT.to_string()),
                    default_value: None,
                    directives: vec![],
                },
            ],
            field_type: s::Type::NamedType(META_FIELD_TYPE.to_string()),
            directives: vec![],
        };
    }
    META_FIELD.clone()
}

#[cfg(test)]
mod tests {
    use crate::{
        data::{
            graphql::{ext::FieldExt, ObjectTypeExt, TypeExt as _},
            subgraph::LATEST_VERSION,
        },
        prelude::{s, DeploymentHash},
        schema::{InputSchema, SCHEMA_TYPE_NAME},
    };
    use graphql_parser::schema::*;
    use lazy_static::lazy_static;

    use super::ApiSchema;
    use crate::schema::ast;

    lazy_static! {
        static ref ID: DeploymentHash = DeploymentHash::new("apiTest").unwrap();
    }

    #[track_caller]
    fn parse(raw: &str) -> ApiSchema {
        let input_schema = InputSchema::parse(LATEST_VERSION, raw, ID.clone())
            .expect("Failed to parse input schema");
        input_schema
            .api_schema()
            .expect("Failed to derive API schema")
    }

    /// Return a field from the `Query` type. If the field does not exist,
    /// fail the test
    #[track_caller]
    fn query_field<'a>(schema: &'a ApiSchema, name: &str) -> &'a s::Field {
        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, name),
            _ => None,
        }
        .expect(&format!("Schema should contain a field named `{}`", name))
    }

    #[test]
    fn api_schema_contains_built_in_scalar_types() {
        let schema = parse("type User @entity { id: ID! }");

        schema
            .get_named_type("Boolean")
            .expect("Boolean type is missing in API schema");
        schema
            .get_named_type("ID")
            .expect("ID type is missing in API schema");
        schema
            .get_named_type("Int")
            .expect("Int type is missing in API schema");
        schema
            .get_named_type("BigDecimal")
            .expect("BigDecimal type is missing in API schema");
        schema
            .get_named_type("String")
            .expect("String type is missing in API schema");
        schema
            .get_named_type("Int8")
            .expect("Int8 type is missing in API schema");
        schema
            .get_named_type("Timestamp")
            .expect("Timestamp type is missing in API schema");
    }

    #[test]
    fn api_schema_contains_order_direction_enum() {
        let schema = parse("type User @entity { id: ID!, name: String! }");

        let order_direction = schema
            .get_named_type("OrderDirection")
            .expect("OrderDirection type is missing in derived API schema");
        let enum_type = match order_direction {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("OrderDirection type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();
        assert_eq!(values, ["asc", "desc"]);
    }

    #[test]
    fn api_schema_contains_query_type() {
        let schema = parse("type User @entity { id: ID! }");
        schema
            .get_named_type("Query")
            .expect("Root Query type is missing in API schema");
    }

    #[test]
    fn api_schema_contains_field_order_by_enum() {
        let schema = parse("type User @entity { id: ID!, name: String! }");

        let user_order_by = schema
            .get_named_type("User_orderBy")
            .expect("User_orderBy type is missing in derived API schema");

        let enum_type = match user_order_by {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("User_orderBy type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();
        assert_eq!(values, ["id", "name"]);
    }

    #[test]
    fn api_schema_contains_field_order_by_enum_for_child_entity() {
        let schema = parse(
            r#"
              enum FurType {
                  NONE
                  FLUFFY
                  BRISTLY
              }

              type Pet @entity {
                  id: ID!
                  name: String!
                  mostHatedBy: [User!]!
                  mostLovedBy: [User!]!
              }

              interface Recipe {
                id: ID!
                name: String!
                author: User!
                lovedBy: [User!]!
                ingredients: [String!]!
              }

              type FoodRecipe implements Recipe @entity {
                id: ID!
                name: String!
                author: User!
                lovedBy: [User!]!
                ingredients: [String!]!
              }

              type DrinkRecipe implements Recipe @entity {
                id: ID!
                name: String!
                author: User!
                lovedBy: [User!]!
                ingredients: [String!]!
              }

              interface Meal {
                id: ID!
                name: String!
                mostHatedBy: [User!]!
                mostLovedBy: [User!]!
              }

              type Pizza implements Meal @entity {
                id: ID!
                name: String!
                toppings: [String!]!
                mostHatedBy: [User!]!
                mostLovedBy: [User!]!
              }

              type Burger implements Meal @entity {
                id: ID!
                name: String!
                bun: String!
                mostHatedBy: [User!]!
                mostLovedBy: [User!]!
              }

              type User @entity {
                  id: ID!
                  name: String!
                  favoritePetNames: [String!]
                  pets: [Pet!]!
                  favoriteFurType: FurType!
                  favoritePet: Pet!
                  leastFavoritePet: Pet @derivedFrom(field: "mostHatedBy")
                  mostFavoritePets: [Pet!] @derivedFrom(field: "mostLovedBy")
                  favoriteMeal: Meal!
                  leastFavoriteMeal: Meal @derivedFrom(field: "mostHatedBy")
                  mostFavoriteMeals: [Meal!] @derivedFrom(field: "mostLovedBy")
                  recipes: [Recipe!]! @derivedFrom(field: "author")
              }
            "#,
        );

        let user_order_by = schema
            .get_named_type("User_orderBy")
            .expect("User_orderBy type is missing in derived API schema");

        let enum_type = match user_order_by {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("User_orderBy type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();

        assert_eq!(
            values,
            [
                "id",
                "name",
                "favoritePetNames",
                "pets",
                "favoriteFurType",
                "favoritePet",
                "favoritePet__id",
                "favoritePet__name",
                "leastFavoritePet",
                "leastFavoritePet__id",
                "leastFavoritePet__name",
                "mostFavoritePets",
                "favoriteMeal",
                "favoriteMeal__id",
                "favoriteMeal__name",
                "leastFavoriteMeal",
                "leastFavoriteMeal__id",
                "leastFavoriteMeal__name",
                "mostFavoriteMeals",
                "recipes",
            ]
        );

        let meal_order_by = schema
            .get_named_type("Meal_orderBy")
            .expect("Meal_orderBy type is missing in derived API schema");

        let enum_type = match meal_order_by {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("Meal_orderBy type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();

        assert_eq!(values, ["id", "name", "mostHatedBy", "mostLovedBy",]);

        let recipe_order_by = schema
            .get_named_type("Recipe_orderBy")
            .expect("Recipe_orderBy type is missing in derived API schema");

        let enum_type = match recipe_order_by {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("Recipe_orderBy type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();

        assert_eq!(
            values,
            [
                "id",
                "name",
                "author",
                "author__id",
                "author__name",
                "author__favoriteFurType",
                "lovedBy",
                "ingredients"
            ]
        );
    }

    #[test]
    fn api_schema_contains_object_type_filter_enum() {
        let schema = parse(
            r#"
              enum FurType {
                  NONE
                  FLUFFY
                  BRISTLY
              }

              type Pet @entity {
                  id: ID!
                  name: String!
                  mostHatedBy: [User!]!
                  mostLovedBy: [User!]!
              }

              type User @entity {
                  id: ID!
                  name: String!
                  favoritePetNames: [String!]
                  pets: [Pet!]!
                  favoriteFurType: FurType!
                  favoritePet: Pet!
                  leastFavoritePet: Pet @derivedFrom(field: "mostHatedBy")
                  mostFavoritePets: [Pet!] @derivedFrom(field: "mostLovedBy")
              }
            "#,
        );

        let user_filter = schema
            .get_named_type("User_filter")
            .expect("User_filter type is missing in derived API schema");

        let user_filter_type = match user_filter {
            TypeDefinition::InputObject(t) => Some(t),
            _ => None,
        }
        .expect("User_filter type is not an input object");

        assert_eq!(
            user_filter_type
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<String>>(),
            [
                "id",
                "id_not",
                "id_gt",
                "id_lt",
                "id_gte",
                "id_lte",
                "id_in",
                "id_not_in",
                "name",
                "name_not",
                "name_gt",
                "name_lt",
                "name_gte",
                "name_lte",
                "name_in",
                "name_not_in",
                "name_contains",
                "name_contains_nocase",
                "name_not_contains",
                "name_not_contains_nocase",
                "name_starts_with",
                "name_starts_with_nocase",
                "name_not_starts_with",
                "name_not_starts_with_nocase",
                "name_ends_with",
                "name_ends_with_nocase",
                "name_not_ends_with",
                "name_not_ends_with_nocase",
                "favoritePetNames",
                "favoritePetNames_not",
                "favoritePetNames_contains",
                "favoritePetNames_contains_nocase",
                "favoritePetNames_not_contains",
                "favoritePetNames_not_contains_nocase",
                "pets",
                "pets_not",
                "pets_contains",
                "pets_contains_nocase",
                "pets_not_contains",
                "pets_not_contains_nocase",
                "pets_",
                "favoriteFurType",
                "favoriteFurType_not",
                "favoriteFurType_in",
                "favoriteFurType_not_in",
                "favoritePet",
                "favoritePet_not",
                "favoritePet_gt",
                "favoritePet_lt",
                "favoritePet_gte",
                "favoritePet_lte",
                "favoritePet_in",
                "favoritePet_not_in",
                "favoritePet_contains",
                "favoritePet_contains_nocase",
                "favoritePet_not_contains",
                "favoritePet_not_contains_nocase",
                "favoritePet_starts_with",
                "favoritePet_starts_with_nocase",
                "favoritePet_not_starts_with",
                "favoritePet_not_starts_with_nocase",
                "favoritePet_ends_with",
                "favoritePet_ends_with_nocase",
                "favoritePet_not_ends_with",
                "favoritePet_not_ends_with_nocase",
                "favoritePet_",
                "leastFavoritePet_",
                "mostFavoritePets_",
                "_change_block",
                "and",
                "or"
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );

        let pets_field = user_filter_type
            .fields
            .iter()
            .find(|field| field.name == "pets_")
            .expect("pets_ field is missing");

        assert_eq!(
            pets_field.value_type.to_string(),
            String::from("Pet_filter")
        );

        let pet_filter = schema
            .get_named_type("Pet_filter")
            .expect("Pet_filter type is missing in derived API schema");

        let pet_filter_type = match pet_filter {
            TypeDefinition::InputObject(t) => Some(t),
            _ => None,
        }
        .expect("Pet_filter type is not an input object");

        assert_eq!(
            pet_filter_type
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<String>>(),
            [
                "id",
                "id_not",
                "id_gt",
                "id_lt",
                "id_gte",
                "id_lte",
                "id_in",
                "id_not_in",
                "name",
                "name_not",
                "name_gt",
                "name_lt",
                "name_gte",
                "name_lte",
                "name_in",
                "name_not_in",
                "name_contains",
                "name_contains_nocase",
                "name_not_contains",
                "name_not_contains_nocase",
                "name_starts_with",
                "name_starts_with_nocase",
                "name_not_starts_with",
                "name_not_starts_with_nocase",
                "name_ends_with",
                "name_ends_with_nocase",
                "name_not_ends_with",
                "name_not_ends_with_nocase",
                "mostHatedBy",
                "mostHatedBy_not",
                "mostHatedBy_contains",
                "mostHatedBy_contains_nocase",
                "mostHatedBy_not_contains",
                "mostHatedBy_not_contains_nocase",
                "mostHatedBy_",
                "mostLovedBy",
                "mostLovedBy_not",
                "mostLovedBy_contains",
                "mostLovedBy_contains_nocase",
                "mostLovedBy_not_contains",
                "mostLovedBy_not_contains_nocase",
                "mostLovedBy_",
                "_change_block",
                "and",
                "or"
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );

        let change_block_filter = user_filter_type
            .fields
            .iter()
            .find(move |p| match p.name.as_str() {
                "_change_block" => true,
                _ => false,
            })
            .expect("_change_block field is missing in User_filter");

        match &change_block_filter.value_type {
            Type::NamedType(name) => assert_eq!(name.as_str(), "BlockChangedFilter"),
            _ => panic!("_change_block field is not a named type"),
        }

        schema
            .get_named_type("BlockChangedFilter")
            .expect("BlockChangedFilter type is missing in derived API schema");
    }

    #[test]
    fn api_schema_contains_object_type_with_field_interface() {
        let schema = parse(
            r#"
              interface Pet {
                  id: ID!
                  name: String!
                  owner: User!
              }

              type Dog implements Pet @entity {
                id: ID!
                name: String!
                owner: User!
            }

              type Cat implements Pet @entity {
                id: ID!
                name: String!
                owner: User!
              }

              type User @entity {
                  id: ID!
                  name: String!
                  pets: [Pet!]! @derivedFrom(field: "owner")
                  favoritePet: Pet!
              }
            "#,
        );

        let user_filter = schema
            .get_named_type("User_filter")
            .expect("User_filter type is missing in derived API schema");

        let user_filter_type = match user_filter {
            TypeDefinition::InputObject(t) => Some(t),
            _ => None,
        }
        .expect("User_filter type is not an input object");

        assert_eq!(
            user_filter_type
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<String>>(),
            [
                "id",
                "id_not",
                "id_gt",
                "id_lt",
                "id_gte",
                "id_lte",
                "id_in",
                "id_not_in",
                "name",
                "name_not",
                "name_gt",
                "name_lt",
                "name_gte",
                "name_lte",
                "name_in",
                "name_not_in",
                "name_contains",
                "name_contains_nocase",
                "name_not_contains",
                "name_not_contains_nocase",
                "name_starts_with",
                "name_starts_with_nocase",
                "name_not_starts_with",
                "name_not_starts_with_nocase",
                "name_ends_with",
                "name_ends_with_nocase",
                "name_not_ends_with",
                "name_not_ends_with_nocase",
                "pets_",
                "favoritePet",
                "favoritePet_not",
                "favoritePet_gt",
                "favoritePet_lt",
                "favoritePet_gte",
                "favoritePet_lte",
                "favoritePet_in",
                "favoritePet_not_in",
                "favoritePet_contains",
                "favoritePet_contains_nocase",
                "favoritePet_not_contains",
                "favoritePet_not_contains_nocase",
                "favoritePet_starts_with",
                "favoritePet_starts_with_nocase",
                "favoritePet_not_starts_with",
                "favoritePet_not_starts_with_nocase",
                "favoritePet_ends_with",
                "favoritePet_ends_with_nocase",
                "favoritePet_not_ends_with",
                "favoritePet_not_ends_with_nocase",
                "favoritePet_",
                "_change_block",
                "and",
                "or"
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );

        let change_block_filter = user_filter_type
            .fields
            .iter()
            .find(move |p| match p.name.as_str() {
                "_change_block" => true,
                _ => false,
            })
            .expect("_change_block field is missing in User_filter");

        match &change_block_filter.value_type {
            Type::NamedType(name) => assert_eq!(name.as_str(), "BlockChangedFilter"),
            _ => panic!("_change_block field is not a named type"),
        }

        schema
            .get_named_type("BlockChangedFilter")
            .expect("BlockChangedFilter type is missing in derived API schema");
    }

    #[test]
    fn api_schema_contains_object_fields_on_query_type() {
        let schema = parse(
            "type User @entity { id: ID!, name: String! } type UserProfile @entity { id: ID!, title: String! }",
        );

        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        let user_singular_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, "user"),
            _ => None,
        }
        .expect("\"user\" field is missing on Query type");

        assert_eq!(
            user_singular_field.field_type,
            Type::NamedType("User".to_string())
        );

        assert_eq!(
            user_singular_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.clone())
                .collect::<Vec<String>>(),
            vec![
                "id".to_string(),
                "block".to_string(),
                "subgraphError".to_string()
            ],
        );

        let user_plural_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, "users"),
            _ => None,
        }
        .expect("\"users\" field is missing on Query type");

        assert_eq!(
            user_plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("User".to_string()))
            )))))
        );

        assert_eq!(
            user_plural_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.clone())
                .collect::<Vec<String>>(),
            [
                "skip",
                "first",
                "orderBy",
                "orderDirection",
                "where",
                "block",
                "subgraphError",
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );

        let user_profile_singular_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, "userProfile"),
            _ => None,
        }
        .expect("\"userProfile\" field is missing on Query type");

        assert_eq!(
            user_profile_singular_field.field_type,
            Type::NamedType("UserProfile".to_string())
        );

        let user_profile_plural_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, "userProfiles"),
            _ => None,
        }
        .expect("\"userProfiles\" field is missing on Query type");

        assert_eq!(
            user_profile_plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("UserProfile".to_string()))
            )))))
        );
    }

    #[test]
    fn api_schema_contains_interface_fields_on_query_type() {
        let schema = parse(
            "
            interface Node { id: ID!, name: String! }
            type User implements Node @entity { id: ID!, name: String!, email: String }
            ",
        );

        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        let singular_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field(t, "node"),
            _ => None,
        }
        .expect("\"node\" field is missing on Query type");

        assert_eq!(
            singular_field.field_type,
            Type::NamedType("Node".to_string())
        );

        assert_eq!(
            singular_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.clone())
                .collect::<Vec<String>>(),
            vec![
                "id".to_string(),
                "block".to_string(),
                "subgraphError".to_string()
            ],
        );

        let plural_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field(t, "nodes"),
            _ => None,
        }
        .expect("\"nodes\" field is missing on Query type");

        assert_eq!(
            plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("Node".to_string()))
            )))))
        );

        assert_eq!(
            plural_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.clone())
                .collect::<Vec<String>>(),
            [
                "skip",
                "first",
                "orderBy",
                "orderDirection",
                "where",
                "block",
                "subgraphError"
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );
    }

    #[test]
    fn api_schema_contains_fulltext_query_field_on_query_type() {
        const SCHEMA: &str = r#"
type _Schema_ @fulltext(
  name: "metadata"
  language: en
  algorithm: rank
  include: [
    {
      entity: "Gravatar",
      fields: [
        { name: "displayName"},
        { name: "imageUrl"},
      ]
    }
  ]
)
type Gravatar @entity {
  id: ID!
  owner: Bytes!
  displayName: String!
  imageUrl: String!
}
"#;
        let schema = parse(SCHEMA);

        // The _Schema_ type must not be copied to the API schema as it will
        // cause GraphQL validation failures on clients
        assert_eq!(None, schema.get_named_type(SCHEMA_TYPE_NAME));
        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        let _metadata_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, &String::from("metadata")),
            _ => None,
        }
        .expect("\"metadata\" field is missing on Query type");
    }

    #[test]
    fn intf_implements_intf() {
        const SCHEMA: &str = r#"
          interface Legged {
            legs: Int!
          }

          interface Animal implements Legged {
            id: Bytes!
            legs: Int!
          }

          type Zoo @entity {
            id: Bytes!
            animals: [Animal!]
          }
          "#;
        // This used to fail in API schema construction; we just want to
        // make sure that generating an API schema works. The issue was that
        // `Zoo.animals` has an interface type, and that interface
        // implements another interface which we tried to look up as an
        // object type
        let _schema = parse(SCHEMA);
    }

    #[test]
    fn pluralize_plural_name() {
        const SCHEMA: &str = r#"
        type Stats @entity {
            id: Bytes!
        }
        "#;
        let schema = parse(SCHEMA);

        query_field(&schema, "stats");
        query_field(&schema, "stats_collection");
    }

    #[test]
    fn nested_filters() {
        const SCHEMA: &str = r#"
        type Musician @entity {
            id: Bytes!
            bands: [Band!]!
        }

        type Band @entity {
            id: Bytes!
            name: String!
            musicians: [Musician!]!
        }
        "#;
        let schema = parse(SCHEMA);

        let musicians = query_field(&schema, "musicians");
        let s::TypeDefinition::Object(musicians) =
            schema.get_type_definition_from_field(musicians).unwrap()
        else {
            panic!("Can not find type for 'musicians' field")
        };
        let bands = musicians.field("bands").unwrap();
        let filter = bands.argument("where").unwrap();
        assert_eq!("Band_filter", filter.value_type.get_base_type());

        query_field(&schema, "bands");
    }

    #[test]
    fn aggregation() {
        const SCHEMA: &str = r#"
        type Data @entity(timeseries: true) {
            id: Int8!
            timestamp: Timestamp!
            value: BigDecimal!
        }

        type Stats @aggregation(source: "Data", intervals: ["hour", "day"]) {
            id: Int8!
            timestamp: Timestamp!
            sum: BigDecimal! @aggregate(fn: "sum", arg: "value")
        }

        type Stuff @entity {
            id: Bytes!
            stats: [Stats!]!
        }
        "#;

        #[track_caller]
        fn assert_aggregation_field(schema: &ApiSchema, field: &s::Field, typename: &str) {
            let filter_type = format!("{typename}_filter");
            let interval = field.argument("interval").unwrap();
            assert_eq!("Aggregation_interval", interval.value_type.get_base_type());
            let filter = field.argument("where").unwrap();
            assert_eq!(&filter_type, filter.value_type.get_base_type());

            let s::TypeDefinition::InputObject(filter) = schema
                .get_type_definition_from_type(&filter.value_type)
                .unwrap()
            else {
                panic!("Can not find type for 'where' filter")
            };

            let mut fields = filter
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<_>>();
            fields.sort();
            assert_eq!(
                [
                    "_change_block",
                    "and",
                    "id",
                    "id_gt",
                    "id_gte",
                    "id_in",
                    "id_lt",
                    "id_lte",
                    "or",
                    "timestamp",
                    "timestamp_gt",
                    "timestamp_gte",
                    "timestamp_in",
                    "timestamp_lt",
                    "timestamp_lte",
                ],
                fields.as_slice()
            );

            let s::TypeDefinition::Object(field_type) =
                schema.get_type_definition_from_field(field).unwrap()
            else {
                panic!("Can not find type for 'stats' field")
            };
            assert_eq!("Stats", &field_type.name);
        }

        // The `Query` type must have a `stats_collection` field, and it
        // must look right for filtering an aggregation
        let schema = parse(SCHEMA);
        let stats = query_field(&schema, "stats_collection");
        assert_aggregation_field(&schema, stats, "Stats");

        // Make sure that Stuff.stats has collection arguments, in
        // particular a `where` filter
        let s::TypeDefinition::Object(stuff) = schema
            .get_type_definition_from_type(&s::Type::NamedType("Stuff".to_string()))
            .unwrap()
        else {
            panic!("Stuff type is missing")
        };
        let stats = stuff.field("stats").unwrap();
        assert_aggregation_field(&schema, stats, "Stats");
    }

    #[test]
    fn no_extra_filters_for_interface_children() {
        #[track_caller]
        fn query_field<'a>(schema: &'a ApiSchema, name: &str) -> &'a crate::schema::api::s::Field {
            let query_type = schema
                .get_named_type("Query")
                .expect("Query type is missing in derived API schema");

            match query_type {
                TypeDefinition::Object(t) => ast::get_field(t, name),
                _ => None,
            }
            .expect(&format!("Schema should contain a field named `{}`", name))
        }

        const SCHEMA: &str = r#"
        type DexProtocol implements Protocol @entity {
            id: Bytes!
            metrics: [Metric!]! @derivedFrom(field: "protocol")
            pools: [Pool!]!
        }

        type Metric @entity {
            id: Bytes!
            protocol: DexProtocol!
        }

        type Pool @entity {
            id: Bytes!
        }

        interface Protocol {
            id: Bytes!
            metrics: [Metric!]!  @derivedFrom(field: "protocol")
            pools: [Pool!]!
        }
        "#;
        let schema = parse(SCHEMA);

        // Even for interfaces, we pay attention to whether a field is
        // derived or not and change the filters in the API schema
        // accordingly. It doesn't really make sense but has been like this
        // for a long time and we'll have to support it.
        for protos in ["dexProtocols", "protocols"] {
            let groups = query_field(&schema, protos);
            let filter = groups.argument("where").unwrap();
            let s::TypeDefinition::InputObject(filter) = schema
                .get_type_definition_from_type(&filter.value_type)
                .unwrap()
            else {
                panic!("Can not find type for 'groups' filter")
            };
            let metrics_fields: Vec<_> = filter
                .fields
                .iter()
                .filter(|field| field.name.starts_with("metrics"))
                .map(|field| &field.name)
                .collect();
            assert_eq!(
                ["metrics_"],
                metrics_fields.as_slice(),
                "Field {protos} has additional metrics filters"
            );
            let mut pools_fields: Vec<_> = filter
                .fields
                .iter()
                .filter(|field| field.name.starts_with("pools"))
                .map(|field| &field.name)
                .collect();
            pools_fields.sort();
            assert_eq!(
                [
                    "pools",
                    "pools_",
                    "pools_contains",
                    "pools_contains_nocase",
                    "pools_not",
                    "pools_not_contains",
                    "pools_not_contains_nocase",
                ],
                pools_fields.as_slice(),
                "Field {protos} has the wrong pools filters"
            );
        }
    }
}

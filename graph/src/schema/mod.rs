use crate::data::graphql::ext::DocumentExt;
use crate::data::subgraph::DeploymentHash;
use crate::prelude::{anyhow, s};

use anyhow::Error;
use graphql_parser::{self, Pos};
use semver::Version;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::BTreeMap;
use std::fmt;
use std::iter::FromIterator;

/// Generate full-fledged API schemas from existing GraphQL schemas.
mod api;

/// Utilities for working with GraphQL schema ASTs.
pub mod ast;

mod entity_key;
mod entity_type;
mod fulltext;
mod input;

pub use api::{is_introspection_field, APISchemaError, INTROSPECTION_QUERY_TYPE};

pub use api::{ApiSchema, ErrorPolicy};
pub use entity_key::EntityKey;
pub use entity_type::{AsEntityTypeName, EntityType};
pub use fulltext::{FulltextAlgorithm, FulltextConfig, FulltextDefinition, FulltextLanguage};
pub use input::sqlexpr::{ExprVisitor, VisitExpr};
pub(crate) use input::POI_OBJECT;
pub use input::{
    kw, Aggregate, AggregateFn, Aggregation, AggregationInterval, AggregationMapping, Field,
    InputSchema, InterfaceType, ObjectOrInterface, ObjectType, TypeKind,
};

pub const SCHEMA_TYPE_NAME: &str = "_Schema_";
pub const INTROSPECTION_SCHEMA_FIELD_NAME: &str = "__schema";

pub const META_FIELD_TYPE: &str = "_Meta_";
pub const META_FIELD_NAME: &str = "_meta";

pub const SQL_FIELD_TYPE: &str = "SqlOutput";
pub const SQL_JSON_FIELD_TYPE: &str = "SqlJSONOutput";
pub const SQL_CSV_FIELD_TYPE: &str = "SqlCSVOutput";
pub const SQL_INPUT_TYPE: &str = "SqlInput";
pub const SQL_FIELD_NAME: &str = "sql";

pub const INTROSPECTION_TYPE_FIELD_NAME: &str = "__type";

pub const BLOCK_FIELD_TYPE: &str = "_Block_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = self.0.join(", ");
        write!(f, "{}", s)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaValidationError {
    #[error("Interface `{0}` not defined")]
    InterfaceUndefined(String),

    #[error("@entity directive missing on the following types: `{0}`")]
    EntityDirectivesMissing(Strings),
    #[error("The `{0}` argument of the @entity directive must be a boolean")]
    EntityDirectiveNonBooleanArgValue(String),

    #[error(
        "Entity type `{0}` does not satisfy interface `{1}` because it is missing \
         the following fields: {2}"
    )]
    InterfaceFieldsMissing(String, String, Strings), // (type, interface, missing_fields)
    #[error("Implementors of interface `{0}` use different id types `{1}`. They must all use the same type")]
    InterfaceImplementorsMixId(String, String),
    #[error("Field `{1}` in type `{0}` has invalid @derivedFrom: {2}")]
    InvalidDerivedFrom(String, String, String), // (type, field, reason)
    #[error("The following type names are reserved: `{0}`")]
    UsageOfReservedTypes(Strings),
    #[error("_Schema_ type is only for @fulltext and must not have any fields")]
    SchemaTypeWithFields,
    #[error("The _Schema_ type only allows @fulltext directives")]
    InvalidSchemaTypeDirectives,
    #[error("Type `{0}`, field `{1}`: type `{2}` is not defined")]
    FieldTypeUnknown(String, String, String), // (type_name, field_name, field_type)
    #[error("Imported type `{0}` does not exist in the `{1}` schema")]
    ImportedTypeUndefined(String, String), // (type_name, schema)
    #[error("Fulltext directive name undefined")]
    FulltextNameUndefined,
    #[error("Fulltext directive name overlaps with type: {0}")]
    FulltextNameConflict(String),
    #[error("Fulltext directive name overlaps with an existing entity field or a top-level query field: {0}")]
    FulltextNameCollision(String),
    #[error("Fulltext language is undefined")]
    FulltextLanguageUndefined,
    #[error("Fulltext language is invalid: {0}")]
    FulltextLanguageInvalid(String),
    #[error("Fulltext algorithm is undefined")]
    FulltextAlgorithmUndefined,
    #[error("Fulltext algorithm is invalid: {0}")]
    FulltextAlgorithmInvalid(String),
    #[error("Fulltext include is invalid")]
    FulltextIncludeInvalid,
    #[error("Fulltext directive requires an 'include' list")]
    FulltextIncludeUndefined,
    #[error("Fulltext 'include' list must contain an object")]
    FulltextIncludeObjectMissing,
    #[error(
        "Fulltext 'include' object must contain 'entity' (String) and 'fields' (List) attributes"
    )]
    FulltextIncludeEntityMissingOrIncorrectAttributes,
    #[error("Fulltext directive includes an entity not found on the subgraph schema")]
    FulltextIncludedEntityNotFound,
    #[error("Fulltext include field must have a 'name' attribute")]
    FulltextIncludedFieldMissingRequiredProperty,
    #[error("Fulltext entity field, {0}, not found or not a string")]
    FulltextIncludedFieldInvalid(String),
    #[error("Type {0} is missing an `id` field")]
    IdFieldMissing(String),
    #[error("{0}")]
    IllegalIdType(String),
    #[error("Timeseries {0} is missing a `timestamp` field")]
    TimestampFieldMissing(String),
    #[error("Aggregation {0}, field{1}: aggregates must use a numeric type, one of Int, Int8, BigInt, and BigDecimal")]
    NonNumericAggregate(String, String),
    #[error("Aggregation {0} is missing the `source` argument")]
    AggregationMissingSource(String),
    #[error(
        "Aggregation {0} has an invalid argument for `source`: it must be the name of a timeseries"
    )]
    AggregationInvalidSource(String),
    #[error("Aggregation {0} is missing an `intervals` argument for the timeseries directive")]
    AggregationMissingIntervals(String),
    #[error(
        "Aggregation {0} has an invalid argument for `intervals`: it must be a non-empty list of strings"
    )]
    AggregationWrongIntervals(String),
    #[error("Aggregation {0}: the interval {1} is not supported")]
    AggregationInvalidInterval(String, String),
    #[error("Aggregation {0} has no @aggregate fields")]
    PointlessAggregation(String),
    #[error(
        "Aggregation {0} has a derived field {1} but fields in aggregations can not be derived"
    )]
    AggregationDerivedField(String, String),
    #[error("Timeseries {0} is marked as mutable, it must be immutable")]
    MutableTimeseries(String),
    #[error("Timeseries {0} is missing a `timestamp` field")]
    TimeseriesMissingTimestamp(String),
    #[error("Type {0} has a `timestamp` field of type {1}, but it must be of type Timestamp")]
    InvalidTimestampType(String, String),
    #[error("Aggregaton {0} uses {1} as the source, but there is no timeseries of that name")]
    AggregationUnknownSource(String, String),
    #[error("Aggregation {0} uses {1} as the source, but that type is not a timeseries")]
    AggregationNonTimeseriesSource(String, String),
    #[error("Aggregation {0} uses {1} as the source, but that does not have a field {2}")]
    AggregationUnknownField(String, String, String),
    #[error("Field {1} in aggregation {0} has type {2} but its type in the source is {3}")]
    AggregationNonMatchingType(String, String, String, String),
    #[error("Field {1} in aggregation {0} has an invalid argument for `arg`: it must be a string")]
    AggregationInvalidArg(String, String),
    #[error("Field {1} in aggregation {0} uses the unknown aggregation function `{2}`")]
    AggregationInvalidFn(String, String, String),
    #[error("Field {1} in aggregation {0} is missing the `fn` argument")]
    AggregationMissingFn(String, String),
    #[error("Field {1} in aggregation {0} is missing the `arg` argument since the function {2} requires it")]
    AggregationMissingArg(String, String, String),
    #[error(
        "Field {1} in aggregation {0} has `arg` {2} but the source type does not have such a field"
    )]
    AggregationUnknownArg(String, String, String),
    #[error(
        "Field {1} in aggregation {0} has `arg` {2} of type {3} but it is of the wider type {4} in the source"
    )]
    AggregationNonMatchingArg(String, String, String, String, String),
    #[error("Field {1} in aggregation {0} has arg `{3}` but that is not a numeric field in {2}")]
    AggregationNonNumericArg(String, String, String, String),
    #[error("Field {1} in aggregation {0} has an invalid value for `cumulative`. It needs to be a boolean")]
    AggregationInvalidCumulative(String, String),
    #[error("Aggregations are not supported with spec version {0}; please migrate the subgraph to the latest version")]
    AggregationsNotSupported(Version),
    #[error("Using Int8 as the type for the `id` field is not supported with spec version {0}; please migrate the subgraph to the latest version")]
    IdTypeInt8NotSupported(Version),
    #[error("{0}")]
    ExprNotSupported(String),
    #[error("Expressions can't us the function {0}")]
    ExprIllegalFunction(String),
    #[error("Failed to parse expression: {0}")]
    ExprParseError(String),
}

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: DeploymentHash,
    pub document: s::Document,

    // Maps type name to implemented interfaces.
    pub interfaces_for_type: BTreeMap<String, Vec<s::InterfaceType>>,

    // Maps an interface name to the list of entities that implement it.
    pub types_for_interface_or_union: BTreeMap<String, Vec<s::ObjectType>>,
}

impl Schema {
    /// Create a new schema. The document must already have been validated
    //
    // TODO: The way some validation is expected to be done beforehand, and
    // some is done here makes it incredibly murky whether a `Schema` is
    // fully validated. The code should be changed to make sure that a
    // `Schema` is always fully valid
    pub fn new(id: DeploymentHash, document: s::Document) -> Result<Self, SchemaValidationError> {
        let (interfaces_for_type, types_for_interface_or_union) =
            Self::collect_interfaces(&document)?;

        let mut schema = Schema {
            id: id.clone(),
            document,
            interfaces_for_type,
            types_for_interface_or_union,
        };

        schema.add_subgraph_id_directives(id);

        Ok(schema)
    }

    fn collect_interfaces(
        document: &s::Document,
    ) -> Result<
        (
            BTreeMap<String, Vec<s::InterfaceType>>,
            BTreeMap<String, Vec<s::ObjectType>>,
        ),
        SchemaValidationError,
    > {
        // Initialize with an empty vec for each interface, so we don't
        // miss interfaces that have no implementors.
        let mut types_for_interface_or_union =
            BTreeMap::from_iter(document.definitions.iter().filter_map(|d| match d {
                s::Definition::TypeDefinition(s::TypeDefinition::Interface(t)) => {
                    Some((t.name.to_string(), vec![]))
                }
                _ => None,
            }));
        let mut interfaces_for_type = BTreeMap::<_, Vec<_>>::new();

        for object_type in document.get_object_type_definitions() {
            for implemented_interface in &object_type.implements_interfaces {
                let interface_type = document
                    .definitions
                    .iter()
                    .find_map(|def| match def {
                        s::Definition::TypeDefinition(s::TypeDefinition::Interface(i))
                            if i.name.eq(implemented_interface) =>
                        {
                            Some(i.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        SchemaValidationError::InterfaceUndefined(implemented_interface.clone())
                    })?;

                Self::validate_interface_implementation(object_type, &interface_type)?;

                interfaces_for_type
                    .entry(object_type.name.to_owned())
                    .or_default()
                    .push(interface_type);
                types_for_interface_or_union
                    .get_mut(implemented_interface)
                    .unwrap()
                    .push(object_type.clone());
            }
        }

        // we also load the union types
        // unions cannot be interfaces, so we don't need to worry about rewriting the above code
        for union in document.get_union_definitions() {
            let object_types: Vec<_> = document
                .definitions
                .iter()
                .filter_map(|def| match def {
                    s::Definition::TypeDefinition(s::TypeDefinition::Object(o))
                        if union.types.contains(&o.name) =>
                    {
                        Some(o.clone())
                    }
                    _ => None,
                })
                .collect();

            types_for_interface_or_union.insert(union.name.to_string(), object_types);
        }

        Ok((interfaces_for_type, types_for_interface_or_union))
    }

    pub fn parse(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(raw)?.into_static();

        Schema::new(id, document).map_err(Into::into)
    }

    /// Returned map has one an entry for each interface in the schema.
    pub fn types_for_interface_or_union(&self) -> &BTreeMap<String, Vec<s::ObjectType>> {
        &self.types_for_interface_or_union
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &str) -> Option<&Vec<s::InterfaceType>> {
        self.interfaces_for_type.get(type_name)
    }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    pub fn add_subgraph_id_directives(&mut self, id: DeploymentHash) {
        for definition in self.document.definitions.iter_mut() {
            let subgraph_id_argument = (String::from("id"), s::Value::String(id.to_string()));

            let subgraph_id_directive = s::Directive {
                name: "subgraphId".to_string(),
                position: Pos::default(),
                arguments: vec![subgraph_id_argument],
            };

            if let s::Definition::TypeDefinition(ref mut type_definition) = definition {
                let (name, directives) = match type_definition {
                    s::TypeDefinition::Object(object_type) => {
                        (&object_type.name, &mut object_type.directives)
                    }
                    s::TypeDefinition::Interface(interface_type) => {
                        (&interface_type.name, &mut interface_type.directives)
                    }
                    s::TypeDefinition::Enum(enum_type) => {
                        (&enum_type.name, &mut enum_type.directives)
                    }
                    s::TypeDefinition::Scalar(scalar_type) => {
                        (&scalar_type.name, &mut scalar_type.directives)
                    }
                    s::TypeDefinition::InputObject(input_object_type) => {
                        (&input_object_type.name, &mut input_object_type.directives)
                    }
                    s::TypeDefinition::Union(union_type) => {
                        (&union_type.name, &mut union_type.directives)
                    }
                };

                if !name.eq(SCHEMA_TYPE_NAME)
                    && !directives
                        .iter()
                        .any(|directive| directive.name.eq("subgraphId"))
                {
                    directives.push(subgraph_id_directive);
                }
            };
        }
    }

    /// Validate that `object` implements `interface`.
    fn validate_interface_implementation(
        object: &s::ObjectType,
        interface: &s::InterfaceType,
    ) -> Result<(), SchemaValidationError> {
        // Check that all fields in the interface exist in the object with same name and type.
        let mut missing_fields = vec![];
        for i in &interface.fields {
            if !object
                .fields
                .iter()
                .any(|o| o.name.eq(&i.name) && o.field_type.eq(&i.field_type))
            {
                missing_fields.push(i.to_string().trim().to_owned());
            }
        }
        if !missing_fields.is_empty() {
            Err(SchemaValidationError::InterfaceFieldsMissing(
                object.name.clone(),
                interface.name.clone(),
                Strings(missing_fields),
            ))
        } else {
            Ok(())
        }
    }

    fn subgraph_schema_object_type(&self) -> Option<&s::ObjectType> {
        self.document
            .get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq(SCHEMA_TYPE_NAME))
    }
}

#[test]
fn non_existing_interface() {
    let schema = "type Foo implements Bar @entity { foo: Int }";
    let res = Schema::parse(schema, DeploymentHash::new("dummy").unwrap());
    let error = res
        .unwrap_err()
        .downcast::<SchemaValidationError>()
        .unwrap();
    assert_eq!(
        error,
        SchemaValidationError::InterfaceUndefined("Bar".to_owned())
    );
}

#[test]
fn invalid_interface_implementation() {
    let schema = "
        interface Foo {
            x: Int,
            y: Int
        }

        type Bar implements Foo @entity {
            x: Boolean
        }
    ";
    let res = Schema::parse(schema, DeploymentHash::new("dummy").unwrap());
    assert_eq!(
        res.unwrap_err().to_string(),
        "Entity type `Bar` does not satisfy interface `Foo` because it is missing \
         the following fields: x: Int, y: Int",
    );
}

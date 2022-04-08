use crate::components::store::{EntityKey, EntityType, SubgraphStore};
use crate::data::graphql::ext::{DirectiveExt, DirectiveFinder, DocumentExt, TypeExt, ValueExt};
use crate::data::graphql::ObjectTypeExt;
use crate::data::store::{self, ValueType};
use crate::prelude::{
    anyhow, lazy_static,
    q::Value,
    s::{self, Definition, InterfaceType, ObjectType, TypeDefinition, *},
};

use anyhow::{Context, Error};
use graphql_parser::{self, Pos};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt;
use std::hash::Hash;
use std::iter::FromIterator;
use std::str::FromStr;
use std::sync::Arc;

use super::graphql::ObjectOrInterface;
use super::store::scalar;

pub const SCHEMA_TYPE_NAME: &str = "_Schema_";

pub const META_FIELD_TYPE: &str = "_Meta_";
pub const META_FIELD_NAME: &str = "_meta";

pub const BLOCK_FIELD_TYPE: &str = "_Block_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = (&self.0).join(", ");
        write!(f, "{}", s)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaValidationError {
    #[error("Interface `` not defined")]
    A,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FulltextAlgorithm {
    Rank,
    ProximityRank,
}

impl TryFrom<&str> for FulltextAlgorithm {
    type Error = String;
    fn try_from(algorithm: &str) -> Result<Self, Self::Error> {
        match algorithm {
            "rank" => Ok(FulltextAlgorithm::Rank),
            "proximityRank" => Ok(FulltextAlgorithm::ProximityRank),
            invalid => Err(format!(
                "The provided fulltext search algorithm {} is invalid. It must be one of: rank, proximityRank",
                invalid,
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FulltextConfig {
    pub language: (),
    pub algorithm: FulltextAlgorithm,
}

pub struct FulltextDefinition {
    pub config: FulltextConfig,
    pub included_fields: HashSet<String>,
    pub name: String,
}

impl From<&s::Directive> for FulltextDefinition {
    // Assumes the input is a Fulltext Directive that has already been validated because it makes
    // liberal use of unwrap() where specific types are expected
    fn from(directive: &Directive) -> Self {
        let name = directive.argument("name").unwrap().as_str().unwrap();

        let algorithm = FulltextAlgorithm::try_from(
            directive.argument("algorithm").unwrap().as_enum().unwrap(),
        )
        .unwrap();

        let language = ();

        let included_entity_list = directive.argument("include").unwrap().as_list().unwrap();
        // Currently fulltext query fields are limited to 1 entity, so we just take the first (and only) included Entity
        let included_entity = included_entity_list.first().unwrap().as_object().unwrap();
        let included_field_values = included_entity.get("fields").unwrap().as_list().unwrap();
        let included_fields: HashSet<String> = included_field_values
            .iter()
            .map(|field| {
                field
                    .as_object()
                    .unwrap()
                    .get("name")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .into()
            })
            .collect();

        FulltextDefinition {
            config: FulltextConfig {
                language,
                algorithm,
            },
            included_fields,
            name: name.into(),
        }
    }
}
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum SchemaImportError {
    #[error("Schema for imported subgraph `{0}` was not found")]
    ImportedSchemaNotFound(SchemaReference),
    #[error("Subgraph for imported schema `{0}` is not deployed")]
    ImportedSubgraphNotFound(SchemaReference),
}

/// The representation of a single type from an import statement. This
/// corresponds either to a string `"Thing"` or an object
/// `{name: "Thing", as: "Stuff"}`. The first form is equivalent to
/// `{name: "Thing", as: "Thing"}`
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ImportedType {
    /// The 'name'
    name: String,
    /// The 'as' alias or a copy of `name` if the user did not specify an alias
    alias: String,
    /// Whether the alias was explicitly given or is just a copy of the name
    explicit: bool,
}

impl ImportedType {
    fn parse(type_import: &Value) -> Option<Self> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaReference {
    subgraph: (),
}

impl fmt::Display for SchemaReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", 0)
    }
}

impl SchemaReference {
    fn new(subgraph: ()) -> Self {
        SchemaReference { subgraph }
    }

    pub fn resolve<S: SubgraphStore>(
        &self,
        store: Arc<S>,
    ) -> Result<Arc<Schema>, SchemaImportError> {
        store
            .input_schema(todo!())
            .map_err(|_| SchemaImportError::ImportedSchemaNotFound(self.clone()))
    }

    fn parse(value: &Value) -> Option<Self> {
        match value {
            Value::Object(map) => match map.get("id") {
                Some(Value::String(id)) => None,
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ApiSchema {
    schema: Schema,

    // Root types for the api schema.
    pub query_type: Arc<ObjectType>,
    pub subscription_type: Option<Arc<ObjectType>>,
    object_types: HashMap<String, Arc<ObjectType>>,
}

impl ApiSchema {
    /// `api_schema` will typically come from `fn api_schema` in the graphql
    /// crate.
    ///
    /// In addition, the API schema has an introspection schema mixed into
    /// `api_schema`. In particular, the `Query` type has fields called
    /// `__schema` and `__type`
    pub fn from_api_schema(mut api_schema: Schema) -> Result<Self, anyhow::Error> {
        add_introspection_schema(&mut api_schema.document);

        let query_type = api_schema
            .document
            .get_root_query_type()
            .context("no root `Query` in the schema")?
            .clone();
        let subscription_type = api_schema
            .document
            .get_root_subscription_type()
            .cloned()
            .map(Arc::new);

        let object_types = HashMap::from_iter(
            api_schema
                .document
                .get_object_type_definitions()
                .into_iter()
                .map(|obj_type| (obj_type.name.clone(), Arc::new(obj_type.clone()))),
        );

        Ok(Self {
            schema: api_schema,
            query_type: Arc::new(query_type),
            subscription_type,
            object_types,
        })
    }

    pub fn document(&self) -> &s::Document {
        &self.schema.document
    }

    pub fn id(&self) -> &() {
        &self.schema.id
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn types_for_interface(&self) -> &BTreeMap<EntityType, Vec<ObjectType>> {
        &self.schema.types_for_interface
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &EntityType) -> Option<&Vec<InterfaceType>> {
        None
    }

    /// Return an `Arc` around the `ObjectType` from our internal cache
    ///
    /// # Panics
    /// If `obj_type` is not part of this schema, this function panics
    pub fn object_type(&self, obj_type: &ObjectType) -> Arc<ObjectType> {
        self.object_types
            .get(&obj_type.name)
            .expect("ApiSchema.object_type is only used with existing types")
            .clone()
    }

    pub fn get_named_type(&self, name: &str) -> Option<&TypeDefinition> {
        self.schema.document.get_named_type(name)
    }

    pub fn get_root_query_type_def(&self) -> Option<&s::TypeDefinition> {
        None
    }

    pub fn object_or_interface(&self, name: &str) -> Option<ObjectOrInterface<'_>> {
        None
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
    pub fn definitions(&self) -> impl Iterator<Item = &s::Definition<'static, String>> {
        self.schema.document.definitions.iter()
    }
}

lazy_static! {
    static ref INTROSPECTION_SCHEMA: Document = {
        let schema = "";
        parse_schema(schema).expect("the schema `introspection.graphql` is invalid")
    };
}

fn add_introspection_schema(schema: &mut Document) {}

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: (),
    pub document: s::Document,

    // Maps type name to implemented interfaces.
    pub interfaces_for_type: BTreeMap<EntityType, Vec<InterfaceType>>,

    // Maps an interface name to the list of entities that implement it.
    pub types_for_interface: BTreeMap<EntityType, Vec<ObjectType>>,
}

impl Schema {
    /// Create a new schema. The document must already have been
    /// validated. This function is only useful for creating an introspection
    /// schema, and should not be used otherwise
    pub fn new(id: (), document: s::Document) -> Self {
        todo!()
    }

    /// Construct a value for the entity type's id attribute
    pub fn id_value(&self, key: &EntityKey) -> Result<store::Value, Error> {
        let base_type = self
            .document
            .get_object_type_definition(key.entity_type.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "Entity {}[{}]: unknown entity type `{}`",
                    key.entity_type,
                    key.entity_id,
                    key.entity_type
                )
            })?
            .field("id")
            .unwrap()
            .field_type
            .get_base_type();

        match base_type {
            "ID" | "String" => Ok(store::Value::String(key.entity_id.clone())),
            "Bytes" => Ok(store::Value::Bytes(scalar::Bytes::from_str(
                &key.entity_id,
            )?)),
            s => {
                return Err(anyhow!(
                    "Entity type {} uses illegal type {} for id column",
                    key.entity_type,
                    s
                ))
            }
        }
    }
}

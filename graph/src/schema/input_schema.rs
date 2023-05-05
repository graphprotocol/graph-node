use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use store::Entity;

use crate::cheap_clone::CheapClone;
use crate::components::store::{EntityKey, EntityType, LoadRelatedRequest};
use crate::data::graphql::ext::DirectiveFinder;
use crate::data::graphql::{DirectiveExt, DocumentExt, ObjectTypeExt, TypeExt, ValueExt};
use crate::data::store::{self, scalar, IntoEntityIterator, TryIntoEntityIterator};
use crate::data::subgraph::schema::POI_DIGEST;
use crate::prelude::q::Value;
use crate::prelude::{s, DeploymentHash};
use crate::schema::api_schema;
use crate::util::intern::AtomPool;

use super::fulltext::FulltextDefinition;
use super::{ApiSchema, Schema, SchemaValidationError};

/// The internal representation of a subgraph schema, i.e., the
/// `schema.graphql` file that is part of a subgraph. Any code that deals
/// with writing a subgraph should use this struct. Code that deals with
/// querying subgraphs will instead want to use an `ApiSchema` which can be
/// generated with the `api_schema` method on `InputSchema`
#[derive(Clone, Debug, PartialEq)]
pub struct InputSchema {
    inner: Arc<Inner>,
}

#[derive(Debug, PartialEq)]
pub struct Inner {
    schema: Schema,
    immutable_types: HashSet<EntityType>,
    pool: Arc<AtomPool>,
}

impl CheapClone for InputSchema {
    fn cheap_clone(&self) -> Self {
        InputSchema {
            inner: self.inner.cheap_clone(),
        }
    }
}

impl InputSchema {
    fn create(schema: Schema) -> Self {
        let immutable_types = HashSet::from_iter(
            schema
                .document
                .get_object_type_definitions()
                .into_iter()
                .filter(|obj_type| obj_type.is_immutable())
                .map(Into::into),
        );

        let pool = Arc::new(atom_pool(&schema.document));

        Self {
            inner: Arc::new(Inner {
                schema,
                immutable_types,
                pool,
            }),
        }
    }

    /// Create a new `InputSchema` from the GraphQL document that resulted
    /// from parsing a subgraph's `schema.graphql`. The document must have
    /// already been validated.
    pub fn new(id: DeploymentHash, document: s::Document) -> Result<Self, SchemaValidationError> {
        let schema = Schema::new(id, document)?;
        Ok(Self::create(schema))
    }

    /// A convenience function for creating an `InputSchema` from the string
    /// representation of the subgraph's GraphQL schema `raw` and its
    /// deployment hash `id`. An `InputSchema` that is constructed with this
    /// function still has to be validated after construction.
    pub fn parse(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        let schema = Schema::parse(raw, id)?;

        Ok(Self::create(schema))
    }

    /// Convenience for tests to construct an `InputSchema`
    ///
    /// # Panics
    ///
    /// If the `document` or `hash` can not be successfully converted
    #[cfg(debug_assertions)]
    #[track_caller]
    pub fn raw(document: &str, hash: &str) -> Self {
        let hash = DeploymentHash::new(hash).unwrap();
        Self::parse(document, hash).unwrap()
    }

    /// Generate the `ApiSchema` for use with GraphQL queries for this
    /// `InputSchema`
    pub fn api_schema(&self) -> Result<ApiSchema, anyhow::Error> {
        let mut schema = self.inner.schema.clone();
        schema.document = api_schema(&self.inner.schema.document)?;
        schema.add_subgraph_id_directives(schema.id.clone());
        ApiSchema::from_api_schema(schema)
    }

    /// Returns the field that has the relationship with the key requested
    /// This works as a reverse search for the Field related to the query
    ///
    /// example:
    ///
    /// type Account @entity {
    ///     wallets: [Wallet!]! @derivedFrom(field: "account")
    /// }
    /// type Wallet {
    ///     account: Account!
    ///     balance: Int!
    /// }
    ///
    /// When asked to load the related entities from "Account" in the field "wallets"
    /// This function will return the type "Wallet" with the field "account"
    pub fn get_field_related(&self, key: &LoadRelatedRequest) -> Result<(&str, &s::Field), Error> {
        let field = self
            .inner
            .schema
            .document
            .get_object_type_definition(key.entity_type.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "Entity {}[{}]: unknown entity type `{}`",
                    key.entity_type,
                    key.entity_id,
                    key.entity_type,
                )
            })?
            .field(&key.entity_field)
            .ok_or_else(|| {
                anyhow!(
                    "Entity {}[{}]: unknown field `{}`",
                    key.entity_type,
                    key.entity_id,
                    key.entity_field,
                )
            })?;
        if field.is_derived() {
            let derived_from = field.find_directive("derivedFrom").unwrap();
            let base_type = field.field_type.get_base_type();
            let field_name = derived_from.argument("field").unwrap();

            let field = self
                .inner
                .schema
                .document
                .get_object_type_definition(base_type)
                .ok_or_else(|| {
                    anyhow!(
                        "Entity {}[{}]: unknown entity type `{}`",
                        key.entity_type,
                        key.entity_id,
                        key.entity_type,
                    )
                })?
                .field(field_name.as_str().unwrap())
                .ok_or_else(|| {
                    anyhow!(
                        "Entity {}[{}]: unknown field `{}`",
                        key.entity_type,
                        key.entity_id,
                        key.entity_field,
                    )
                })?;

            Ok((base_type, field))
        } else {
            Err(anyhow!(
                "Entity {}[{}]: field `{}` is not derived",
                key.entity_type,
                key.entity_id,
                key.entity_field,
            ))
        }
    }

    /// Construct a value for the entity type's id attribute
    pub fn id_value(&self, key: &EntityKey) -> Result<store::Value, Error> {
        let base_type = self
            .inner
            .schema
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
            "ID" | "String" => Ok(store::Value::String(key.entity_id.to_string())),
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

    pub fn is_immutable(&self, entity_type: &EntityType) -> bool {
        self.inner.immutable_types.contains(entity_type)
    }

    pub fn get_named_type(&self, name: &str) -> Option<&s::TypeDefinition> {
        self.inner.schema.document.get_named_type(name)
    }

    pub fn types_for_interface(&self, intf: &s::InterfaceType) -> Option<&Vec<s::ObjectType>> {
        self.inner
            .schema
            .types_for_interface
            .get(&EntityType::new(intf.name.clone()))
    }

    pub fn find_object_type(&self, entity_type: &EntityType) -> Option<&s::ObjectType> {
        self.inner
            .schema
            .document
            .definitions
            .iter()
            .filter_map(|d| match d {
                s::Definition::TypeDefinition(s::TypeDefinition::Object(t)) => Some(t),
                _ => None,
            })
            .find(|object_type| entity_type.as_str() == object_type.name)
    }

    pub fn get_enum_definitions(&self) -> Vec<&s::EnumType> {
        self.inner.schema.document.get_enum_definitions()
    }

    pub fn get_object_type_definitions(&self) -> Vec<&s::ObjectType> {
        self.inner.schema.document.get_object_type_definitions()
    }

    pub fn interface_types(&self) -> &BTreeMap<EntityType, Vec<s::ObjectType>> {
        &self.inner.schema.types_for_interface
    }

    pub fn entity_fulltext_definitions(
        &self,
        entity: &str,
    ) -> Result<Vec<FulltextDefinition>, anyhow::Error> {
        Self::fulltext_definitions(&self.inner.schema.document, entity)
    }

    fn fulltext_definitions(
        document: &s::Document,
        entity: &str,
    ) -> Result<Vec<FulltextDefinition>, anyhow::Error> {
        Ok(document
            .get_fulltext_directives()?
            .into_iter()
            .filter(|directive| match directive.argument("include") {
                Some(Value::List(includes)) if !includes.is_empty() => {
                    includes.iter().any(|include| match include {
                        Value::Object(include) => match include.get("entity") {
                            Some(Value::String(fulltext_entity)) if fulltext_entity == entity => {
                                true
                            }
                            _ => false,
                        },
                        _ => false,
                    })
                }
                _ => false,
            })
            .map(FulltextDefinition::from)
            .collect())
    }

    pub fn id(&self) -> &DeploymentHash {
        &self.inner.schema.id
    }

    pub fn document_string(&self) -> String {
        self.inner.schema.document.to_string()
    }

    pub fn get_fulltext_directives(&self) -> Result<Vec<&s::Directive>, Error> {
        self.inner.schema.document.get_fulltext_directives()
    }

    pub(crate) fn validate(&self) -> Result<(), Vec<SchemaValidationError>> {
        self.inner.schema.validate()
    }

    pub fn make_entity<I: IntoEntityIterator>(&self, iter: I) -> Result<Entity, Error> {
        Entity::make(self.inner.pool.clone(), iter)
    }

    pub fn try_make_entity<
        E: std::error::Error + Send + Sync + 'static,
        I: TryIntoEntityIterator<E>,
    >(
        &self,
        iter: I,
    ) -> Result<Entity, Error> {
        Entity::try_make(self.inner.pool.clone(), iter)
    }
}

/// Create a new pool that contains the names of all the types defined
/// in the document and the names of all their fields
fn atom_pool(document: &s::Document) -> AtomPool {
    let mut pool = AtomPool::new();
    pool.intern(POI_DIGEST.as_str()); // Attribute of PoI object
    for definition in &document.definitions {
        match definition {
            s::Definition::TypeDefinition(typedef) => match typedef {
                s::TypeDefinition::Object(t) => {
                    pool.intern(&t.name);
                    for field in &t.fields {
                        pool.intern(&field.name);
                    }
                }
                s::TypeDefinition::Enum(t) => {
                    pool.intern(&t.name);
                }
                s::TypeDefinition::Interface(t) => {
                    pool.intern(&t.name);
                    for field in &t.fields {
                        pool.intern(&field.name);
                    }
                }
                s::TypeDefinition::InputObject(input_object) => {
                    pool.intern(&input_object.name);
                    for field in &input_object.fields {
                        pool.intern(&field.name);
                    }
                }
                s::TypeDefinition::Scalar(scalar_type) => {
                    pool.intern(&scalar_type.name);
                }
                s::TypeDefinition::Union(union_type) => {
                    pool.intern(&union_type.name);
                    for typ in &union_type.types {
                        pool.intern(typ);
                    }
                }
            },
            s::Definition::SchemaDefinition(_)
            | s::Definition::TypeExtension(_)
            | s::Definition::DirectiveDefinition(_) => { /* ignore, these only happen for introspection schemas */
            }
        }
    }

    for object_type in document.get_object_type_definitions() {
        for defn in InputSchema::fulltext_definitions(&document, &object_type.name).unwrap() {
            pool.intern(defn.name.as_str());
        }
    }

    pool
}

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
use crate::prelude::q::Value;
use crate::prelude::{s, DeploymentHash};
use crate::schema::api_schema;

use super::fulltext::FulltextDefinition;
use super::{ApiSchema, AtomPool, Schema, SchemaValidationError};

#[derive(Clone, Debug, PartialEq)]
pub struct InputSchema {
    inner: Arc<Inner>,
}

#[derive(Debug, PartialEq)]
pub struct Inner {
    schema: Schema,
    immutable_types: HashSet<EntityType>,
    pool: AtomPool,
}

impl std::ops::Deref for InputSchema {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
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
        let pool = AtomPool;
        Self {
            inner: Arc::new(Inner {
                schema,
                immutable_types,
                pool,
            }),
        }
    }
    pub fn new(id: DeploymentHash, document: s::Document) -> Result<Self, SchemaValidationError> {
        let schema = Schema::new(id, document)?;
        Ok(Self::create(schema))
    }

    pub fn parse(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        let schema = Schema::parse(raw, id)?;

        Ok(Self::create(schema))
    }
}

impl Inner {
    pub fn api_schema(&self) -> Result<ApiSchema, anyhow::Error> {
        let mut schema = self.schema.clone();
        schema.document = api_schema(&self.schema.document)?;
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
        self.immutable_types.contains(entity_type)
    }

    pub fn get_named_type(&self, name: &str) -> Option<&s::TypeDefinition> {
        self.schema.document.get_named_type(name)
    }

    pub fn types_for_interface(&self, intf: &s::InterfaceType) -> Option<&Vec<s::ObjectType>> {
        self.schema
            .types_for_interface
            .get(&EntityType::new(intf.name.clone()))
    }

    pub fn find_object_type(&self, entity_type: &EntityType) -> Option<&s::ObjectType> {
        self.schema
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
        self.schema.document.get_enum_definitions()
    }

    pub fn get_object_type_definitions(&self) -> Vec<&s::ObjectType> {
        self.schema.document.get_object_type_definitions()
    }

    pub fn interface_types(&self) -> &BTreeMap<EntityType, Vec<s::ObjectType>> {
        &self.schema.types_for_interface
    }

    pub fn entity_fulltext_definitions(
        &self,
        entity: &str,
    ) -> Result<Vec<FulltextDefinition>, anyhow::Error> {
        Ok(self
            .schema
            .document
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
        &self.schema.id
    }

    pub fn document_string(&self) -> String {
        self.schema.document.to_string()
    }

    pub fn get_fulltext_directives(&self) -> Result<Vec<&s::Directive>, Error> {
        self.schema.document.get_fulltext_directives()
    }

    pub(crate) fn validate(&self) -> Result<(), Vec<SchemaValidationError>> {
        self.schema.validate()
    }

    pub fn make_entity<I: IntoEntityIterator>(&self, iter: I) -> Entity {
        Entity::make(self.pool.clone(), iter)
    }

    pub fn try_make_entity<E, I: TryIntoEntityIterator<E>>(&self, iter: I) -> Result<Entity, E> {
        Entity::try_make(self.pool.clone(), iter)
    }
}

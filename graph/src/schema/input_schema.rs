use std::collections::BTreeMap;

use anyhow::Error;

use crate::components::store::{EntityKey, EntityType, LoadRelatedRequest};
use crate::data::graphql::DocumentExt;
use crate::data::schema::{FulltextDefinition, Schema, SchemaValidationError};
use crate::data::store;
use crate::prelude::{s, ApiSchema, DeploymentHash};
use crate::schema::api_schema;

#[derive(Clone, Debug, PartialEq)]
pub struct InputSchema {
    schema: Schema,
}

impl InputSchema {
    pub fn new(id: DeploymentHash, document: s::Document) -> Result<Self, SchemaValidationError> {
        let schema = Schema::new(id, document)?;
        Ok(Self { schema })
    }

    pub fn parse(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        let schema = Schema::parse(raw, id)?;

        Ok(Self { schema })
    }

    pub fn api_schema(&self) -> Result<ApiSchema, anyhow::Error> {
        let mut schema = self.schema.clone();
        schema.document = api_schema(&self.schema.document)?;
        schema.add_subgraph_id_directives(schema.id.clone());
        ApiSchema::from_api_schema(schema)
    }

    pub fn get_field_related(&self, key: &LoadRelatedRequest) -> Result<(&str, &s::Field), Error> {
        self.schema.get_field_related(key)
    }

    pub fn id_value(&self, key: &EntityKey) -> Result<store::Value, Error> {
        self.schema.id_value(key)
    }

    pub fn is_immutable(&self, entity_type: &EntityType) -> bool {
        self.schema.is_immutable(entity_type)
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
        Schema::entity_fulltext_definitions(entity, &self.schema.document)
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
}

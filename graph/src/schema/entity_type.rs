use std::{borrow::Borrow, fmt, sync::Arc};

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

use crate::{
    cheap_clone::CheapClone,
    data::{graphql::ObjectOrInterface, value::Word},
    prelude::s,
    util::intern::AtomPool,
};

/// The type name of an entity. This is the string that is used in the
/// subgraph's GraphQL schema as `type NAME @entity { .. }`
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityType(Word);

impl EntityType {
    /// Construct a new entity type. Ideally, this is only called when
    /// `entity_type` either comes from the GraphQL schema, or from
    /// the database from fields that are known to contain a valid entity type
    pub(crate) fn new(pool: &Arc<AtomPool>, entity_type: &str) -> Result<Self, Error> {
        match pool.lookup(entity_type) {
            Some(_) => Ok(EntityType(Word::from(entity_type))),
            None => bail!("entity type `{}` is not interned", entity_type),
        }
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_string(self) -> String {
        self.0.to_string()
    }

    pub fn is_poi(&self) -> bool {
        self.0.as_str() == "Poi$"
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Borrow<str> for EntityType {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl CheapClone for EntityType {}

impl std::fmt::Debug for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityType({})", self.0)
    }
}

pub trait AsEntityTypeName {
    fn name(&self) -> &str;
}

impl AsEntityTypeName for &str {
    fn name(&self) -> &str {
        self
    }
}

impl AsEntityTypeName for &String {
    fn name(&self) -> &str {
        self.as_str()
    }
}

impl AsEntityTypeName for &s::ObjectType {
    fn name(&self) -> &str {
        &self.name
    }
}

impl AsEntityTypeName for &s::InterfaceType {
    fn name(&self) -> &str {
        &self.name
    }
}

impl AsEntityTypeName for ObjectOrInterface<'_> {
    fn name(&self) -> &str {
        match self {
            ObjectOrInterface::Object(object) => &object.name,
            ObjectOrInterface::Interface(interface) => &interface.name,
        }
    }
}

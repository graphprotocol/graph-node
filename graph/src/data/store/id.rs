//! Types and helpers to deal with entity IDs which support a subset of the
//! types that more general values support
use anyhow::{anyhow, Error};

use crate::{
    data::graphql::{ObjectTypeExt, TypeExt},
    prelude::s,
};

/// The types that can be used for the `id` of an entity
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum IdType {
    String,
    Bytes,
}

impl<'a> TryFrom<&s::ObjectType> for IdType {
    type Error = Error;

    fn try_from(obj_type: &s::ObjectType) -> Result<Self, Self::Error> {
        let base_type = obj_type.field("id").unwrap().field_type.get_base_type();

        match base_type {
            "ID" | "String" => Ok(IdType::String),
            "Bytes" => Ok(IdType::Bytes),
            s => {
                return Err(anyhow!(
                    "Entity type {} uses illegal type {} for id column",
                    obj_type.name,
                    s
                ))
            }
        }
    }
}

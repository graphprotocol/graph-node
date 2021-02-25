use crate::prelude::Schema;
use crate::{components::store::EntityType, prelude::s};
use std::collections::BTreeMap;

use super::ObjectTypeExt;

#[derive(Copy, Clone, Debug)]
pub enum ObjectOrInterface<'a> {
    Object(&'a s::ObjectType),
    Interface(&'a s::InterfaceType),
}

impl<'a> From<&'a s::ObjectType> for ObjectOrInterface<'a> {
    fn from(object: &'a s::ObjectType) -> Self {
        ObjectOrInterface::Object(object)
    }
}

impl<'a> From<&'a s::InterfaceType> for ObjectOrInterface<'a> {
    fn from(interface: &'a s::InterfaceType) -> Self {
        ObjectOrInterface::Interface(interface)
    }
}

impl<'a> From<ObjectOrInterface<'a>> for EntityType {
    fn from(ooi: ObjectOrInterface) -> Self {
        match ooi {
            ObjectOrInterface::Object(ty) => EntityType::from(ty),
            ObjectOrInterface::Interface(ty) => EntityType::from(ty),
        }
    }
}

impl<'a> ObjectOrInterface<'a> {
    pub fn is_object(self) -> bool {
        match self {
            ObjectOrInterface::Object(_) => true,
            ObjectOrInterface::Interface(_) => false,
        }
    }

    pub fn is_interface(self) -> bool {
        match self {
            ObjectOrInterface::Object(_) => false,
            ObjectOrInterface::Interface(_) => true,
        }
    }

    pub fn name(self) -> &'a str {
        match self {
            ObjectOrInterface::Object(object) => &object.name,
            ObjectOrInterface::Interface(interface) => &interface.name,
        }
    }

    pub fn directives(self) -> &'a Vec<s::Directive> {
        match self {
            ObjectOrInterface::Object(object) => &object.directives,
            ObjectOrInterface::Interface(interface) => &interface.directives,
        }
    }

    pub fn fields(self) -> &'a Vec<s::Field> {
        match self {
            ObjectOrInterface::Object(object) => &object.fields,
            ObjectOrInterface::Interface(interface) => &interface.fields,
        }
    }

    pub fn field(&self, name: &String) -> Option<&s::Field> {
        self.fields().iter().find(|field| &field.name == name)
    }

    pub fn object_types(self, schema: &'a Schema) -> Option<Vec<&'a s::ObjectType>> {
        match self {
            ObjectOrInterface::Object(object) => Some(vec![object]),
            ObjectOrInterface::Interface(interface) => schema
                .types_for_interface()
                .get(&interface.into())
                .map(|object_types| object_types.iter().collect()),
        }
    }

    /// `typename` is the name of an object type. Matches if `self` is an object and has the same
    /// name, or if self is an interface implemented by `typename`.
    pub fn matches(
        self,
        typename: &str,
        types_for_interface: &BTreeMap<EntityType, Vec<s::ObjectType>>,
    ) -> bool {
        match self {
            ObjectOrInterface::Object(o) => o.name == typename,
            ObjectOrInterface::Interface(i) => types_for_interface[&i.into()]
                .iter()
                .any(|o| o.name == typename),
        }
    }

    pub fn is_meta(&self) -> bool {
        match self {
            ObjectOrInterface::Object(o) => o.is_meta(),
            ObjectOrInterface::Interface(i) => i.is_meta(),
        }
    }
}

use crate::prelude::Schema;
use graphql_parser::schema as s;
use std::collections::BTreeMap;

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

impl<'a> ObjectOrInterface<'a> {
    pub fn is_object(self) -> bool {
        match self {
            ObjectOrInterface::Object(_) => true,
            ObjectOrInterface::Interface(_) => false,
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

    pub fn field(&self, name: &s::Name) -> Option<&s::Field> {
        self.fields().iter().find(|field| &field.name == name)
    }

    pub fn object_types(self, schema: &'a Schema) -> Option<Vec<&'a s::ObjectType>> {
        match self {
            ObjectOrInterface::Object(object) => Some(vec![object]),
            ObjectOrInterface::Interface(interface) => schema
                .types_for_interface()
                .get(&interface.name)
                .map(|object_types| object_types.iter().collect()),
        }
    }

    /// `typename` is the name of an object type. Matches if `self` is an object and has the same
    /// name, or if self is an interface implemented by `typename`.
    pub fn matches(
        self,
        typename: &str,
        types_for_interface: &BTreeMap<s::Name, Vec<s::ObjectType>>,
    ) -> bool {
        match self {
            ObjectOrInterface::Object(o) => o.name == typename,
            ObjectOrInterface::Interface(i) => types_for_interface[&i.name]
                .iter()
                .any(|o| o.name == typename),
        }
    }
}

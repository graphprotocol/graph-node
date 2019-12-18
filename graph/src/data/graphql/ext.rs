use graphql_parser::schema::{
    Definition, Directive, Document, Field, InterfaceType, Name, ObjectType, Type, TypeDefinition,
};

use std::collections::HashMap;

pub trait ObjectTypeExt {
    fn field(&self, name: &Name) -> Option<&Field>;
}

impl ObjectTypeExt for ObjectType {
    fn field(&self, name: &Name) -> Option<&Field> {
        self.fields.iter().find(|field| &field.name == name)
    }
}

impl ObjectTypeExt for InterfaceType {
    fn field(&self, name: &Name) -> Option<&Field> {
        self.fields.iter().find(|field| &field.name == name)
    }
}

pub trait DocumentExt {
    fn get_object_type_definitions(&self) -> Vec<&ObjectType>;

    fn get_object_and_interface_type_fields(&self) -> HashMap<&Name, &Vec<Field>>;

    fn find_interface(&self, name: &str) -> Option<&InterfaceType>;
}

impl DocumentExt for Document {
    fn get_object_type_definitions(&self) -> Vec<&ObjectType> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
                _ => None,
            })
            .collect()
    }

    fn get_object_and_interface_type_fields(&self) -> HashMap<&Name, &Vec<Field>> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t)) => Some((&t.name, &t.fields)),
                Definition::TypeDefinition(TypeDefinition::Interface(t)) => {
                    Some((&t.name, &t.fields))
                }
                _ => None,
            })
            .collect()
    }

    fn find_interface(&self, name: &str) -> Option<&InterfaceType> {
        self.definitions.iter().find_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Interface(t)) if t.name == name => Some(t),
            _ => None,
        })
    }
}

pub trait TypeExt {
    fn get_base_type(&self) -> &Name;
}

impl TypeExt for Type {
    fn get_base_type(&self) -> &Name {
        match self {
            Type::NamedType(name) => name,
            Type::NonNullType(inner) => Self::get_base_type(&inner),
            Type::ListType(inner) => Self::get_base_type(&inner),
        }
    }
}

pub trait DirectiveFinder {
    fn find_directive(&self, name: Name) -> Option<&Directive>;
}

impl DirectiveFinder for ObjectType {
    fn find_directive(&self, name: Name) -> Option<&Directive> {
        self.directives
            .iter()
            .find(|directive| directive.name.eq(&name))
    }
}

impl DirectiveFinder for Field {
    fn find_directive(&self, name: Name) -> Option<&Directive> {
        self.directives
            .iter()
            .find(|directive| directive.name.eq(&name))
    }
}

use crate::data::schema::{META_FIELD_TYPE, SCHEMA_TYPE_NAME};
use graphql_parser::schema::{
    Definition, Directive, Document, EnumType, Field, InterfaceType, Name, ObjectType, Type,
    TypeDefinition, Value,
};

use super::ObjectOrInterface;

use std::collections::{BTreeMap, HashMap};

pub trait ObjectTypeExt {
    fn field(&self, name: &Name) -> Option<&Field>;
    fn is_meta(&self) -> bool;
}

impl ObjectTypeExt for ObjectType {
    fn field(&self, name: &Name) -> Option<&Field> {
        self.fields.iter().find(|field| &field.name == name)
    }

    fn is_meta(&self) -> bool {
        self.name == META_FIELD_TYPE
    }
}

impl ObjectTypeExt for InterfaceType {
    fn field(&self, name: &Name) -> Option<&Field> {
        self.fields.iter().find(|field| &field.name == name)
    }

    fn is_meta(&self) -> bool {
        false
    }
}

pub trait DocumentExt {
    fn get_object_type_definitions(&self) -> Vec<&ObjectType>;

    fn get_object_type_definition(&self, name: &str) -> Option<&ObjectType>;

    fn get_object_and_interface_type_fields(&self) -> HashMap<&Name, &Vec<Field>>;

    fn get_enum_definitions(&self) -> Vec<&EnumType>;

    fn find_interface(&self, name: &str) -> Option<&InterfaceType>;

    fn get_fulltext_directives<'a>(&'a self) -> Vec<&'a Directive>;

    fn get_root_query_type(&self) -> Option<&ObjectType>;

    fn get_root_subscription_type(&self) -> Option<&ObjectType>;

    fn object_or_interface(&self, name: &str) -> Option<ObjectOrInterface<'_>>;

    fn get_named_type(&self, name: &str) -> Option<&TypeDefinition>;
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

    fn get_object_type_definition(&self, name: &str) -> Option<&ObjectType> {
        self.get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq(name))
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

    fn get_enum_definitions(&self) -> Vec<&EnumType> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Enum(e)) => Some(e),
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

    fn get_fulltext_directives(&self) -> Vec<&Directive> {
        self.get_object_type_definition(SCHEMA_TYPE_NAME)
            .map_or(vec![], |subgraph_schema_type| {
                subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directives| directives.name.eq("fulltext"))
                    .collect()
            })
    }

    /// Returns the root query type (if there is one).
    fn get_root_query_type(&self) -> Option<&ObjectType> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t)) if t.name == "Query" => {
                    Some(t)
                }
                _ => None,
            })
            .peekable()
            .next()
    }

    fn get_root_subscription_type(&self) -> Option<&ObjectType> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t))
                    if t.name == "Subscription" =>
                {
                    Some(t)
                }
                _ => None,
            })
            .peekable()
            .next()
    }

    fn object_or_interface(&self, name: &str) -> Option<ObjectOrInterface<'_>> {
        match self.get_named_type(name) {
            Some(TypeDefinition::Object(t)) => Some(t.into()),
            Some(TypeDefinition::Interface(t)) => Some(t.into()),
            _ => None,
        }
    }

    fn get_named_type(&self, name: &str) -> Option<&TypeDefinition> {
        self.definitions
            .iter()
            .filter_map(|def| match def {
                Definition::TypeDefinition(typedef) => Some(typedef),
                _ => None,
            })
            .find(|typedef| match typedef {
                TypeDefinition::Object(t) => &t.name == name,
                TypeDefinition::Enum(t) => &t.name == name,
                TypeDefinition::InputObject(t) => &t.name == name,
                TypeDefinition::Interface(t) => &t.name == name,
                TypeDefinition::Scalar(t) => &t.name == name,
                TypeDefinition::Union(t) => &t.name == name,
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

pub trait DirectiveExt {
    fn argument(&self, name: &str) -> Option<&Value>;
}

impl DirectiveExt for Directive {
    fn argument(&self, name: &str) -> Option<&Value> {
        self.arguments
            .iter()
            .find(|(key, _value)| key == name)
            .map(|(_argument, value)| value)
    }
}

pub trait ValueExt {
    fn as_object(&self) -> Option<&BTreeMap<Name, Value>>;
    fn as_list(&self) -> Option<&Vec<Value>>;
    fn as_string(&self) -> Option<&String>;
    fn as_enum(&self) -> Option<&Name>;
}

impl ValueExt for Value {
    fn as_object(&self) -> Option<&BTreeMap<Name, Value>> {
        match self {
            Value::Object(object) => Some(object),
            _ => None,
        }
    }

    fn as_list(&self) -> Option<&Vec<Value>> {
        match self {
            Value::List(list) => Some(list),
            _ => None,
        }
    }

    fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(string) => Some(string),
            _ => None,
        }
    }

    fn as_enum(&self) -> Option<&Name> {
        match self {
            Value::Enum(e) => Some(e),
            _ => None,
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

impl DirectiveFinder for Vec<Directive> {
    fn find_directive(&self, name: Name) -> Option<&Directive> {
        self.iter().find(|directive| directive.name.eq(&name))
    }
}

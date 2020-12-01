use crate::data::schema::{META_FIELD_TYPE, SCHEMA_TYPE_NAME};
use graphql_parser::schema::{
    Definition, Directive, Document, EnumType, Field, InterfaceType, ObjectType, Type,
    TypeDefinition, Value,
};

use super::ObjectOrInterface;

use std::collections::{BTreeMap, HashMap};

pub trait ObjectTypeExt {
    fn field(&self, name: &String) -> Option<&Field<'static, String>>;
    fn is_meta(&self) -> bool;
}

impl ObjectTypeExt for ObjectType<'static, String> {
    fn field(&self, name: &String) -> Option<&Field<'static, String>> {
        self.fields.iter().find(|field| &field.name == name)
    }

    fn is_meta(&self) -> bool {
        self.name == META_FIELD_TYPE
    }
}

impl ObjectTypeExt for InterfaceType<'static, String> {
    fn field(&self, name: &String) -> Option<&Field<'static, String>> {
        self.fields.iter().find(|field| &field.name == name)
    }

    fn is_meta(&self) -> bool {
        false
    }
}

pub trait DocumentExt {
    fn get_object_type_definitions(&self) -> Vec<&ObjectType<'static, String>>;

    fn get_object_type_definition(&self, name: &str) -> Option<&ObjectType<'static, String>>;

    fn get_object_and_interface_type_fields(
        &self,
    ) -> HashMap<&String, &Vec<Field<'static, String>>>;

    fn get_enum_definitions(&self) -> Vec<&EnumType<'static, String>>;

    fn find_interface(&self, name: &str) -> Option<&InterfaceType<'static, String>>;

    fn get_fulltext_directives<'a>(&'a self) -> Vec<&'a Directive<'static, String>>;

    fn get_root_query_type(&self) -> Option<&ObjectType<'static, String>>;

    fn get_root_subscription_type(&self) -> Option<&ObjectType<'static, String>>;

    fn object_or_interface(&self, name: &str) -> Option<ObjectOrInterface<'_>>;

    fn get_named_type(&self, name: &str) -> Option<&TypeDefinition<'static, String>>;
}

impl DocumentExt for Document<'static, String> {
    fn get_object_type_definitions(&self) -> Vec<&ObjectType<'static, String>> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
                _ => None,
            })
            .collect()
    }

    fn get_object_type_definition(&self, name: &str) -> Option<&ObjectType<'static, String>> {
        self.get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq(name))
    }

    fn get_object_and_interface_type_fields(
        &self,
    ) -> HashMap<&String, &Vec<Field<'static, String>>> {
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

    fn get_enum_definitions(&self) -> Vec<&EnumType<'static, String>> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Enum(e)) => Some(e),
                _ => None,
            })
            .collect()
    }

    fn find_interface(&self, name: &str) -> Option<&InterfaceType<'static, String>> {
        self.definitions.iter().find_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Interface(t)) if t.name == name => Some(t),
            _ => None,
        })
    }

    fn get_fulltext_directives(&self) -> Vec<&Directive<'static, String>> {
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
    fn get_root_query_type(&self) -> Option<&ObjectType<'static, String>> {
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

    fn get_root_subscription_type(&self) -> Option<&ObjectType<'static, String>> {
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

    fn get_named_type(&self, name: &str) -> Option<&TypeDefinition<'static, String>> {
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
    fn get_base_type(&self) -> &String;
}

impl TypeExt for Type<'static, String> {
    fn get_base_type(&self) -> &String {
        match self {
            Type::NamedType(name) => name,
            Type::NonNullType(inner) => Self::get_base_type(&inner),
            Type::ListType(inner) => Self::get_base_type(&inner),
        }
    }
}

pub trait DirectiveExt {
    fn argument(&self, name: &str) -> Option<&Value<'static, String>>;
}

impl DirectiveExt for Directive<'static, String> {
    fn argument(&self, name: &str) -> Option<&Value<'static, String>> {
        self.arguments
            .iter()
            .find(|(key, _value)| key == name)
            .map(|(_argument, value)| value)
    }
}

pub trait ValueExt {
    fn as_object(&self) -> Option<&BTreeMap<String, Value<'static, String>>>;
    fn as_list(&self) -> Option<&Vec<Value<'static, String>>>;
    fn as_string(&self) -> Option<&String>;
    fn as_enum(&self) -> Option<&String>;
}

impl ValueExt for Value<'static, String> {
    fn as_object(&self) -> Option<&BTreeMap<String, Value<'static, String>>> {
        match self {
            Value::Object(object) => Some(object),
            _ => None,
        }
    }

    fn as_list(&self) -> Option<&Vec<Value<'static, String>>> {
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

    fn as_enum(&self) -> Option<&String> {
        match self {
            Value::Enum(e) => Some(e),
            _ => None,
        }
    }
}

pub trait DirectiveFinder {
    fn find_directive(&self, name: String) -> Option<&Directive<'static, String>>;
}

impl DirectiveFinder for ObjectType<'static, String> {
    fn find_directive(&self, name: String) -> Option<&Directive<'static, String>> {
        self.directives
            .iter()
            .find(|directive| directive.name.eq(&name))
    }
}

impl DirectiveFinder for Field<'static, String> {
    fn find_directive(&self, name: String) -> Option<&Directive<'static, String>> {
        self.directives
            .iter()
            .find(|directive| directive.name.eq(&name))
    }
}

impl DirectiveFinder for Vec<Directive<'static, String>> {
    fn find_directive(&self, name: String) -> Option<&Directive<'static, String>> {
        self.iter().find(|directive| directive.name.eq(&name))
    }
}

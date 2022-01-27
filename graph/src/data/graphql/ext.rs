use super::ObjectOrInterface;
use crate::data::schema::{META_FIELD_TYPE, SCHEMA_TYPE_NAME};
use crate::prelude::s::{
    Definition, Directive, Document, EnumType, Field, InterfaceType, ObjectType, Type,
    TypeDefinition, Value,
};
use crate::prelude::ValueType;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

lazy_static! {
    static ref ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH: bool = if cfg!(debug_assertions) {
        true
    } else {
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH").is_ok()
    };
}

pub trait ObjectTypeExt {
    fn field(&self, name: &str) -> Option<&Field>;
    fn is_meta(&self) -> bool;
}

impl ObjectTypeExt for ObjectType {
    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| &field.name == name)
    }

    fn is_meta(&self) -> bool {
        self.name == META_FIELD_TYPE
    }
}

impl ObjectTypeExt for InterfaceType {
    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| &field.name == name)
    }

    fn is_meta(&self) -> bool {
        false
    }
}

pub trait DocumentExt {
    fn get_object_type_definitions(&self) -> Vec<&ObjectType>;

    fn get_interface_type_definitions(&self) -> Vec<&InterfaceType>;

    fn get_object_type_definition(&self, name: &str) -> Option<&ObjectType>;

    fn get_object_and_interface_type_fields(&self) -> HashMap<&str, &Vec<Field>>;

    fn get_enum_definitions(&self) -> Vec<&EnumType>;

    fn find_interface(&self, name: &str) -> Option<&InterfaceType>;

    fn get_fulltext_directives<'a>(&'a self) -> Result<Vec<&'a Directive>, anyhow::Error>;

    fn get_root_query_type(&self) -> Option<&ObjectType>;

    fn get_root_subscription_type(&self) -> Option<&ObjectType>;

    fn object_or_interface(&self, name: &str) -> Option<ObjectOrInterface<'_>>;

    fn get_named_type(&self, name: &str) -> Option<&TypeDefinition>;

    fn scalar_value_type(&self, field_type: &Type) -> ValueType;
}

impl DocumentExt for Document {
    /// Returns all object type definitions in the schema.
    fn get_object_type_definitions(&self) -> Vec<&ObjectType> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
                _ => None,
            })
            .collect()
    }

    /// Returns all interface definitions in the schema.
    fn get_interface_type_definitions(&self) -> Vec<&InterfaceType> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Interface(t)) => Some(t),
                _ => None,
            })
            .collect()
    }

    fn get_object_type_definition(&self, name: &str) -> Option<&ObjectType> {
        self.get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq(name))
    }

    fn get_object_and_interface_type_fields(&self) -> HashMap<&str, &Vec<Field>> {
        self.definitions
            .iter()
            .filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Object(t)) => {
                    Some((t.name.as_str(), &t.fields))
                }
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

    fn get_fulltext_directives(&self) -> Result<Vec<&Directive>, anyhow::Error> {
        let directives = self.get_object_type_definition(SCHEMA_TYPE_NAME).map_or(
            vec![],
            |subgraph_schema_type| {
                subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directives| directives.name.eq("fulltext"))
                    .collect()
            },
        );
        if !*ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH && !directives.is_empty() {
            Err(anyhow::anyhow!("Fulltext search is not yet deterministic"))
        } else {
            Ok(directives)
        }
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

    fn scalar_value_type(&self, field_type: &Type) -> ValueType {
        use TypeDefinition as t;
        match field_type {
            Type::NamedType(name) => {
                ValueType::from_str(&name).unwrap_or_else(|_| match self.get_named_type(name) {
                    Some(t::Object(_)) | Some(t::Interface(_)) | Some(t::Enum(_)) => {
                        ValueType::String
                    }
                    Some(t::Scalar(_)) => unreachable!("user-defined scalars are not used"),
                    Some(t::Union(_)) => unreachable!("unions are not used"),
                    Some(t::InputObject(_)) => unreachable!("inputObjects are not used"),
                    None => unreachable!("names of field types have been validated"),
                })
            }
            Type::NonNullType(inner) => self.scalar_value_type(inner),
            Type::ListType(inner) => self.scalar_value_type(inner),
        }
    }
}

pub trait TypeExt {
    fn get_base_type(&self) -> &str;
    fn is_list(&self) -> bool;
    fn is_non_null(&self) -> bool;
}

impl TypeExt for Type {
    fn get_base_type(&self) -> &str {
        match self {
            Type::NamedType(name) => name,
            Type::NonNullType(inner) => Self::get_base_type(inner),
            Type::ListType(inner) => Self::get_base_type(inner),
        }
    }

    fn is_list(&self) -> bool {
        match self {
            Type::NamedType(_) => false,
            Type::NonNullType(inner) => inner.is_list(),
            Type::ListType(_) => true,
        }
    }

    // Returns true if the given type is a non-null type.
    fn is_non_null(&self) -> bool {
        match self {
            Type::NonNullType(_) => true,
            _ => false,
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
    fn as_object(&self) -> Option<&BTreeMap<String, Value>>;
    fn as_list(&self) -> Option<&Vec<Value>>;
    fn as_str(&self) -> Option<&str>;
    fn as_enum(&self) -> Option<&str>;
}

impl ValueExt for Value {
    fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
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

    fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(string) => Some(string),
            _ => None,
        }
    }

    fn as_enum(&self) -> Option<&str> {
        match self {
            Value::Enum(e) => Some(e),
            _ => None,
        }
    }
}

pub trait DirectiveFinder {
    fn find_directive(&self, name: &str) -> Option<&Directive>;
    fn is_derived(&self) -> bool;
}

impl DirectiveFinder for ObjectType {
    fn find_directive(&self, name: &str) -> Option<&Directive> {
        self.directives
            .iter()
            .find(|directive| directive.name.eq(&name))
    }

    fn is_derived(&self) -> bool {
        let is_derived = |directive: &Directive| directive.name.eq("derivedFrom");

        self.directives.iter().any(is_derived)
    }
}

impl DirectiveFinder for Field {
    fn find_directive(&self, name: &str) -> Option<&Directive> {
        self.directives
            .iter()
            .find(|directive| directive.name.eq(name))
    }

    fn is_derived(&self) -> bool {
        let is_derived = |directive: &Directive| directive.name.eq("derivedFrom");

        self.directives.iter().any(is_derived)
    }
}

impl DirectiveFinder for Vec<Directive> {
    fn find_directive(&self, name: &str) -> Option<&Directive> {
        self.iter().find(|directive| directive.name.eq(&name))
    }

    fn is_derived(&self) -> bool {
        let is_derived = |directive: &Directive| directive.name.eq("derivedFrom");

        self.iter().any(is_derived)
    }
}

#[cfg(test)]
mod directive_finder_tests {
    use graphql_parser::parse_schema;

    use super::*;

    const SCHEMA: &str = "
    type BuyEvent implements Event @derivedFrom(field: \"buyEvent\") {
        id: ID!,
        transaction: Transaction! @derivedFrom(field: \"buyEvent\")
    }";

    /// Makes sure that the DirectiveFinder::find_directive implementation for ObjectiveType and Field works
    #[test]
    fn find_directive_impls() {
        let ast = parse_schema::<String>(SCHEMA).unwrap();
        let object_types = ast.get_object_type_definitions();
        assert_eq!(object_types.len(), 1);
        let object_type = object_types[0];

        // The object type BuyEvent has a @derivedFrom directive
        assert!(object_type.find_directive("derivedFrom").is_some());

        // BuyEvent has no deprecated directive
        assert!(object_type.find_directive("deprecated").is_none());

        let fields = &object_type.fields;
        assert_eq!(fields.len(), 2);

        // Field 1 `id` is not derived
        assert!(fields[0].find_directive("derivedFrom").is_none());
        // Field 2 `transaction` is derived
        assert!(fields[1].find_directive("derivedFrom").is_some());
    }

    /// Makes sure that the DirectiveFinder::is_derived implementation for ObjectiveType and Field works
    #[test]
    fn is_derived_impls() {
        let ast = parse_schema::<String>(SCHEMA).unwrap();
        let object_types = ast.get_object_type_definitions();
        assert_eq!(object_types.len(), 1);
        let object_type = object_types[0];

        // The object type BuyEvent is derived
        assert!(object_type.is_derived());

        let fields = &object_type.fields;
        assert_eq!(fields.len(), 2);

        // Field 1 `id` is not derived
        assert!(!fields[0].is_derived());
        // Field 2 `transaction` is derived
        assert!(fields[1].is_derived());
    }
}

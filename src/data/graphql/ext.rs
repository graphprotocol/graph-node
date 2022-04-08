use crate::data::schema::{META_FIELD_TYPE, SCHEMA_TYPE_NAME};
use crate::prelude::s::{
    Definition, Directive, Document, EnumType, Field, InterfaceType, ObjectType, Type,
    TypeDefinition, Value,
};
use std::collections::{BTreeMap, HashMap};

pub trait ObjectTypeExt {
    fn field(&self, name: &str) -> Option<&Field>;
    fn is_meta(&self) -> bool;
}

impl ObjectTypeExt for ObjectType {
    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }

    fn is_meta(&self) -> bool {
        self.name == META_FIELD_TYPE
    }
}

impl ObjectTypeExt for InterfaceType {
    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }

    fn is_meta(&self) -> bool {
        false
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

pub trait TypeDefinitionExt {
    fn name(&self) -> &str;

    // Return `true` if this is the definition of a type from the
    // introspection schema
    fn is_introspection(&self) -> bool {
        self.name().starts_with("__")
    }
}

impl TypeDefinitionExt for TypeDefinition {
    fn name(&self) -> &str {
        match self {
            TypeDefinition::Scalar(t) => &t.name,
            TypeDefinition::Object(t) => &t.name,
            TypeDefinition::Interface(t) => &t.name,
            TypeDefinition::Union(t) => &t.name,
            TypeDefinition::Enum(t) => &t.name,
            TypeDefinition::InputObject(t) => &t.name,
        }
    }
}

pub trait FieldExt {
    // Return `true` if this is the name of one of the query fields from the
    // introspection schema
    fn is_introspection(&self) -> bool;
}

impl FieldExt for Field {
    fn is_introspection(&self) -> bool {
        &self.name == "__schema" || &self.name == "__type"
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

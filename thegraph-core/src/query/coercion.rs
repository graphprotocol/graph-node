use graphql_parser::query::Value;
use graphql_parser::schema::{Name, Type, TypeDefinition};

pub trait MaybeCoercible<T, N, V, U> {
    fn coerce<F>(&self, using_type: T, map_type: &F) -> Option<V>
    where
        F: Fn(N) -> Option<U>;
}

/// A GraphQL value that can be coerced according to a type.
#[derive(Debug)]
pub struct MaybeCoercibleValue<'a>(pub &'a Value);

impl<'a> MaybeCoercible<&'a TypeDefinition, &'a Name, Value, &'a TypeDefinition>
    for MaybeCoercibleValue<'a>
{
    fn coerce<F>(&self, using_type: &'a TypeDefinition, _map_type: &F) -> Option<Value>
    where
        F: Fn(&'a Name) -> Option<&'a TypeDefinition>,
    {
        match (using_type, self.0) {
            // Accept enum values if they match a value in the enum type
            (TypeDefinition::Enum(t), Value::Enum(name)) => t.values
                .iter()
                .find(|value| &value.name == name)
                .map(|_| self.0.clone()),

            // Reject non-enum values for enum types
            (TypeDefinition::Enum(_), _) => None,

            // Try to coerce Boolean values
            (TypeDefinition::Scalar(t), Value::Boolean(_)) => {
                if t.name == "Boolean" {
                    Some(self.0.clone())
                } else {
                    None
                }
            }

            // Try to coerce Int values
            (TypeDefinition::Scalar(t), Value::Int(_)) => {
                if t.name == "Int" {
                    Some(self.0.clone())
                } else {
                    None
                }
            }

            // Try to coerce Float values
            (TypeDefinition::Scalar(t), Value::Float(_)) => {
                if t.name == "Float" {
                    Some(self.0.clone())
                } else {
                    None
                }
            }

            // Try to coerce String values
            (TypeDefinition::Scalar(t), Value::String(_)) => {
                if t.name == "String" {
                    Some(self.0.clone())
                } else {
                    None
                }
            }

            // Try to coerce InputObject values
            (TypeDefinition::InputObject(_), v) => match v {
                Value::Object(_) => Some(self.0.clone()),
                _ => None,
            },

            // Everything else remains unimplemented
            _ => unimplemented!(),
        }
    }
}

impl<'a> MaybeCoercible<&'a Type, &'a Name, Value, &'a TypeDefinition> for MaybeCoercibleValue<'a> {
    fn coerce<F>(&self, using_type: &'a Type, map_type: &F) -> Option<Value>
    where
        F: Fn(&'a Name) -> Option<&'a TypeDefinition>,
    {
        match (using_type, self.0) {
            // Null values cannot be coerced into non-null types
            (Type::NonNullType(_), Value::Null) => None,

            // Non-null values may be coercible into non-null types
            (Type::NonNullType(t), _) => self.coerce(t.as_ref(), map_type),

            // Resolve named types, then try to coerce the value into the resolved type
            (Type::NamedType(name), _) => match map_type(name) {
                Some(t) => self.coerce(t, map_type),
                None => None,
            },

            // List values may be coercible if they are empty or their values are coercible
            // into the inner type
            (Type::ListType(t), Value::List(values)) => if values.is_empty() {
                Some(Value::List(values.clone()))
            } else {
                let mut coerced_values = vec![];

                // Coerce the list values individually
                for value in values {
                    if let Some(v) = MaybeCoercibleValue(value).coerce(t.as_ref(), map_type) {
                        coerced_values.push(v);
                    } else {
                        // Fail if not all values could be coerced
                        return None;
                    }
                }

                Some(Value::List(coerced_values))
            },

            // Everything else is unsupported for now
            _ => unimplemented!(),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use graphql_parser::Pos;
    use graphql_parser::query::Value;
    use graphql_parser::schema::{EnumType, EnumValue, InputObjectType, ScalarType, TypeDefinition};
    use std::collections::BTreeMap;
    use std::iter::FromIterator;

    use super::{MaybeCoercible, MaybeCoercibleValue};

    #[test]
    fn coercion_using_enum_type_definitions_is_correct() {
        let enum_type = TypeDefinition::Enum(EnumType {
            name: "Enum".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
            values: vec![
                EnumValue {
                    name: "ValidVariant".to_string(),
                    position: Pos::default(),
                    description: None,
                    directives: vec![],
                },
            ],
        });

        // We can coerce from Value::Enum -> TypeDefinition::Enum if the variant is valid
        assert_eq!(
            MaybeCoercibleValue(&Value::Enum("ValidVariant".to_string()))
                .coerce(&enum_type, &|_| None),
            Some(Value::Enum("ValidVariant".to_string()))
        );

        // We cannot coerce from Value::Enum -> TypeDefinition::Enum if the variant is invalid
        assert_eq!(
            MaybeCoercibleValue(&Value::Enum("InvalidVariant".to_string()))
                .coerce(&enum_type, &|_| None),
            None,
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Enum)
        assert_eq!(
            MaybeCoercibleValue(&Value::String("ValidVariant".to_string()))
                .coerce(&enum_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::String("InvalidVariant".to_string()))
                .coerce(&enum_type, &|_| None),
            None,
        );
    }

    #[test]
    fn coercion_using_boolean_type_definitions_is_correct() {
        let bool_type = TypeDefinition::Scalar(ScalarType {
            name: "Boolean".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
        });

        // We can coerce from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(true)).coerce(&bool_type, &|_| None),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(false)).coerce(&bool_type, &|_| None),
            Some(Value::Boolean(false))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            MaybeCoercibleValue(&Value::String("true".to_string())).coerce(&bool_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::String("false".to_string())).coerce(&bool_type, &|_| None),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(1.0)).coerce(&bool_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(0.0)).coerce(&bool_type, &|_| None),
            None,
        );
    }

    #[test]
    fn coercion_using_float_type_definitions_is_correct() {
        let float_type = TypeDefinition::Scalar(ScalarType {
            name: "Float".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
        });

        // We can coerce from Value::Float -> TypeDefinition::Scalar(Float)
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(23.7)).coerce(&float_type, &|_| None),
            Some(Value::Float(23.7))
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(-5.879)).coerce(&float_type, &|_| None),
            Some(Value::Float(-5.879))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Float)
        assert_eq!(
            MaybeCoercibleValue(&Value::String("23.7".to_string())).coerce(&float_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::String("-5.879".to_string()))
                .coerce(&float_type, &|_| None),
            None,
        );

        // We don't spport going from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(true)).coerce(&float_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(false)).coerce(&float_type, &|_| None),
            None,
        );
    }

    #[test]
    fn coercion_using_string_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType {
            name: "String".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
        });

        // We can coerce from Value::String -> TypeDefinition::Scalar(String)
        assert_eq!(
            MaybeCoercibleValue(&Value::String("foo".to_string())).coerce(&string_type, &|_| None),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::String("bar".to_string())).coerce(&string_type, &|_| None),
            Some(Value::String("bar".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(String)
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(true)).coerce(&string_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(false)).coerce(&string_type, &|_| None),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(String)
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(23.7)).coerce(&string_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(-5.879)).coerce(&string_type, &|_| None),
            None,
        );
    }

    #[test]
    fn coercion_using_input_object_type_definitions_is_correct() {
        let input_object_type = TypeDefinition::InputObject(InputObjectType {
            name: "InputObject".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
            fields: vec![],
        });

        // We can coerce from Value::Object -> TypeDefinition::InputObject
        let example_object = BTreeMap::from_iter(
            vec![
                ("Foo".to_string(), Value::Boolean(false)),
                ("Bar".to_string(), Value::Float(15.2)),
            ].into_iter(),
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Object(example_object.clone()))
                .coerce(&input_object_type, &|_| None),
            Some(Value::Object(example_object))
        );

        // We don't support going from Value::String -> TypeDefinition::InputObject
        assert_eq!(
            MaybeCoercibleValue(&Value::String("foo".to_string()))
                .coerce(&input_object_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::String("bar".to_string()))
                .coerce(&input_object_type, &|_| None),
            None,
        );

        // We don't support going from Value::Boolean -> TypeDefinition::InputObject
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(true)).coerce(&input_object_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Boolean(false)).coerce(&input_object_type, &|_| None),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::InputObject
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(23.7)).coerce(&input_object_type, &|_| None),
            None,
        );
        assert_eq!(
            MaybeCoercibleValue(&Value::Float(-5.879)).coerce(&input_object_type, &|_| None),
            None,
        );
    }
}

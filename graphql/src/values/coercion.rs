use crate::schema;
use graph::prelude::s::{EnumType, InputValue, ScalarType, Type, TypeDefinition, Value};
use graph::prelude::{q, QueryExecutionError};
use std::collections::{BTreeMap, HashMap};

/// A GraphQL value that can be coerced according to a type.
pub trait MaybeCoercible<T> {
    /// On error,  `self` is returned as `Err(self)`.
    fn coerce(self, using_type: &T) -> Result<Value, Value>;
}

impl MaybeCoercible<EnumType> for Value {
    fn coerce(self, using_type: &EnumType) -> Result<Value, Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::String(name) | Value::Enum(name)
                if using_type.values.iter().any(|value| value.name == name) =>
            {
                Ok(Value::Enum(name))
            }
            _ => Err(self),
        }
    }
}

impl MaybeCoercible<ScalarType> for Value {
    fn coerce(self, using_type: &ScalarType) -> Result<Value, Value> {
        match (using_type.name.as_str(), self) {
            (_, v @ Value::Null) => Ok(v),
            ("Boolean", v @ Value::Boolean(_)) => Ok(v),
            ("BigDecimal", Value::Float(f)) => Ok(Value::String(f.to_string())),
            ("BigDecimal", Value::Int(i)) => {
                Ok(Value::String(i.as_i64().ok_or(Value::Int(i))?.to_string()))
            }
            ("BigDecimal", v @ Value::String(_)) => Ok(v),
            ("Int", Value::Int(num)) => {
                let n = num.as_i64().ok_or_else(|| Value::Int(num.clone()))?;
                if i32::min_value() as i64 <= n && n <= i32::max_value() as i64 {
                    Ok(Value::Int((n as i32).into()))
                } else {
                    Err(Value::Int(num))
                }
            }
            ("String", v @ Value::String(_)) => Ok(v),
            ("ID", v @ Value::String(_)) => Ok(v),
            ("ID", Value::Int(n)) => {
                Ok(Value::String(n.as_i64().ok_or(Value::Int(n))?.to_string()))
            }
            ("Bytes", v @ Value::String(_)) => Ok(v),
            ("BigInt", v @ Value::String(_)) => Ok(v),
            ("BigInt", Value::Int(n)) => {
                Ok(Value::String(n.as_i64().ok_or(Value::Int(n))?.to_string()))
            }
            (_, v) => Err(v),
        }
    }
}

/// On error, the `value` is returned as `Err(value)`.
fn coerce_to_definition<'a>(
    value: Value,
    definition: &str,
    resolver: &impl Fn(&str) -> Option<&'a TypeDefinition>,
    variables: &HashMap<String, q::Value>,
) -> Result<Value, Value> {
    match resolver(definition).ok_or_else(|| value.clone())? {
        // Accept enum values if they match a value in the enum type
        TypeDefinition::Enum(t) => value.coerce(t),

        // Try to coerce Scalar values
        TypeDefinition::Scalar(t) => value.coerce(t),

        // Try to coerce InputObject values
        TypeDefinition::InputObject(t) => match value {
            Value::Object(object) => {
                let object_for_error = Value::Object(object.clone());
                let mut coerced_object = BTreeMap::new();
                for (name, value) in object {
                    let def = t
                        .fields
                        .iter()
                        .find(|f| f.name == name)
                        .ok_or_else(|| object_for_error.clone())?;
                    coerced_object.insert(
                        name.clone(),
                        match coerce_input_value(Some(value), def, resolver, variables) {
                            Err(_) | Ok(None) => return Err(object_for_error),
                            Ok(Some(v)) => v,
                        },
                    );
                }
                Ok(Value::Object(coerced_object))
            }
            _ => Err(value),
        },

        // Everything else remains unimplemented
        _ => Err(value),
    }
}

/// Coerces an argument into a GraphQL value.
///
/// `Ok(None)` happens when no value is found for a nullable type.
pub(crate) fn coerce_input_value<'a>(
    mut value: Option<Value>,
    def: &InputValue,
    resolver: &impl Fn(&str) -> Option<&'a TypeDefinition>,
    variable_values: &HashMap<String, q::Value>,
) -> Result<Option<Value>, QueryExecutionError> {
    if let Some(Value::Variable(name)) = value {
        value = variable_values.get(&name).cloned();
    };

    // Use the default value if necessary and present.
    value = value.or(def.default_value.clone());

    // Extract value, checking for null or missing.
    let value = match value {
        None => {
            return if schema::ast::is_non_null_type(&def.value_type) {
                Err(QueryExecutionError::MissingArgumentError(
                    def.position.clone(),
                    def.name.to_owned(),
                ))
            } else {
                Ok(None)
            };
        }
        Some(value) => value,
    };

    Ok(Some(
        coerce_value(value, &def.value_type, resolver, variable_values).map_err(|val| {
            QueryExecutionError::InvalidArgumentError(
                def.position.clone(),
                def.name.to_owned(),
                val,
            )
        })?,
    ))
}

/// On error, the `value` is returned as `Err(value)`.
pub(crate) fn coerce_value<'a>(
    value: Value,
    ty: &Type,
    resolver: &impl Fn(&str) -> Option<&'a TypeDefinition>,
    variable_values: &HashMap<String, q::Value>,
) -> Result<Value, Value> {
    match (ty, value) {
        // Null values cannot be coerced into non-null types.
        (Type::NonNullType(_), Value::Null) => Err(Value::Null),

        // Non-null values may be coercible into non-null types
        (Type::NonNullType(_), val) => {
            // We cannot bind `t` in the pattern above because "binding by-move and by-ref in the
            // same pattern is unstable". Refactor this and the others when Rust fixes this.
            let t = match ty {
                Type::NonNullType(ty) => ty,
                _ => unreachable!(),
            };
            coerce_value(val, t, resolver, variable_values)
        }

        // Nullable types can be null.
        (_, Value::Null) => Ok(Value::Null),

        // Resolve named types, then try to coerce the value into the resolved type
        (Type::NamedType(_), val) => {
            let name = match ty {
                Type::NamedType(name) => name,
                _ => unreachable!(),
            };
            coerce_to_definition(val, name, resolver, variable_values)
        }

        // List values are coercible if their values are coercible into the
        // inner type.
        (Type::ListType(_), Value::List(values)) => {
            let t = match ty {
                Type::ListType(ty) => ty,
                _ => unreachable!(),
            };
            let mut coerced_values = vec![];

            // Coerce the list values individually
            for value in values {
                coerced_values.push(coerce_value(value, t, resolver, variable_values)?);
            }

            Ok(Value::List(coerced_values))
        }

        // Otherwise the list type is not coercible.
        (Type::ListType(_), value) => Err(value),
    }
}

#[cfg(test)]
mod tests {
    use graphql_parser::query::Value;
    use graphql_parser::schema::{EnumType, EnumValue, ScalarType, TypeDefinition};
    use graphql_parser::Pos;
    use std::collections::HashMap;

    use super::coerce_to_definition;

    #[test]
    fn coercion_using_enum_type_definitions_is_correct() {
        let enum_type = TypeDefinition::Enum(EnumType {
            name: "Enum".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
            values: vec![EnumValue {
                name: "ValidVariant".to_string(),
                position: Pos::default(),
                description: None,
                directives: vec![],
            }],
        });
        let resolver = |_: &str| Some(&enum_type);

        // We can coerce from Value::Enum -> TypeDefinition::Enum if the variant is valid
        assert_eq!(
            coerce_to_definition(
                Value::Enum("ValidVariant".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::Enum("ValidVariant".to_string()))
        );

        // We cannot coerce from Value::Enum -> TypeDefinition::Enum if the variant is invalid
        assert!(coerce_to_definition(
            Value::Enum("InvalidVariant".to_string()),
            "",
            &resolver,
            &HashMap::new()
        )
        .is_err());

        // We also support going from Value::String -> TypeDefinition::Scalar(Enum)
        assert_eq!(
            coerce_to_definition(
                Value::String("ValidVariant".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::Enum("ValidVariant".to_string())),
        );

        // But we don't support invalid variants
        assert!(coerce_to_definition(
            Value::String("InvalidVariant".to_string()),
            "",
            &resolver,
            &HashMap::new()
        )
        .is_err());
    }

    #[test]
    fn coercion_using_boolean_type_definitions_is_correct() {
        let bool_type = TypeDefinition::Scalar(ScalarType {
            name: "Boolean".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
        });
        let resolver = |_: &str| Some(&bool_type);

        // We can coerce from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(Value::Boolean(true), "", &resolver, &HashMap::new()),
            Ok(Value::Boolean(true))
        );
        assert_eq!(
            coerce_to_definition(Value::Boolean(false), "", &resolver, &HashMap::new()),
            Ok(Value::Boolean(false))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Boolean)
        assert!(coerce_to_definition(
            Value::String("true".to_string()),
            "",
            &resolver,
            &HashMap::new()
        )
        .is_err());
        assert!(coerce_to_definition(
            Value::String("false".to_string()),
            "",
            &resolver,
            &HashMap::new()
        )
        .is_err());

        // We don't support going from Value::Float -> TypeDefinition::Scalar(Boolean)
        assert!(coerce_to_definition(Value::Float(1.0), "", &resolver, &HashMap::new()).is_err());
        assert!(coerce_to_definition(Value::Float(0.0), "", &resolver, &HashMap::new()).is_err());
    }

    #[test]
    fn coercion_using_big_decimal_type_definitions_is_correct() {
        let big_decimal_type = TypeDefinition::Scalar(ScalarType::new("BigDecimal".to_string()));
        let resolver = |_: &str| Some(&big_decimal_type);

        // We can coerce from Value::Float -> TypeDefinition::Scalar(BigDecimal)
        assert_eq!(
            coerce_to_definition(Value::Float(23.7), "", &resolver, &HashMap::new()),
            Ok(Value::String("23.7".to_string()))
        );
        assert_eq!(
            coerce_to_definition(Value::Float(-5.879), "", &resolver, &HashMap::new()),
            Ok(Value::String("-5.879".to_string()))
        );

        // We can coerce from Value::String -> TypeDefinition::Scalar(BigDecimal)
        assert_eq!(
            coerce_to_definition(
                Value::String("23.7".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("23.7".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                Value::String("-5.879".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("-5.879".to_string())),
        );

        // We can coerce from Value::Int -> TypeDefinition::Scalar(BigDecimal)
        assert_eq!(
            coerce_to_definition(Value::Int(23.into()), "", &resolver, &HashMap::new()),
            Ok(Value::String("23".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                Value::Int((-5 as i32).into()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("-5".to_string())),
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert!(
            coerce_to_definition(Value::Boolean(true), "", &resolver, &HashMap::new()).is_err()
        );
        assert!(
            coerce_to_definition(Value::Boolean(false), "", &resolver, &HashMap::new()).is_err()
        );
    }

    #[test]
    fn coercion_using_string_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType::new("String".to_string()));
        let resolver = |_: &str| Some(&string_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(
                Value::String("foo".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("foo".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                Value::String("bar".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("bar".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(String)
        assert!(
            coerce_to_definition(Value::Boolean(true), "", &resolver, &HashMap::new()).is_err()
        );
        assert!(
            coerce_to_definition(Value::Boolean(false), "", &resolver, &HashMap::new()).is_err()
        );

        // We don't support going from Value::Float -> TypeDefinition::Scalar(String)
        assert!(coerce_to_definition(Value::Float(23.7), "", &resolver, &HashMap::new()).is_err());
        assert!(
            coerce_to_definition(Value::Float(-5.879), "", &resolver, &HashMap::new()).is_err()
        );
    }

    #[test]
    fn coercion_using_id_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType::new("ID".to_owned()));
        let resolver = |_: &str| Some(&string_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(
                Value::String("foo".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("foo".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                Value::String("bar".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("bar".to_string()))
        );

        // And also from Value::Int
        assert_eq!(
            coerce_to_definition(Value::Int(1234.into()), "", &resolver, &HashMap::new()),
            Ok(Value::String("1234".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(ID)
        assert!(
            coerce_to_definition(Value::Boolean(true), "", &resolver, &HashMap::new()).is_err()
        );

        assert!(
            coerce_to_definition(Value::Boolean(false), "", &resolver, &HashMap::new()).is_err()
        );

        // We don't support going from Value::Float -> TypeDefinition::Scalar(ID)
        assert!(coerce_to_definition(Value::Float(23.7), "", &resolver, &HashMap::new()).is_err());
        assert!(
            coerce_to_definition(Value::Float(-5.879), "", &resolver, &HashMap::new()).is_err()
        );
    }

    #[test]
    fn coerce_big_int_scalar() {
        let big_int_type = TypeDefinition::Scalar(ScalarType::new("BigInt".to_string()));
        let resolver = |_: &str| Some(&big_int_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(BigInt)
        assert_eq!(
            coerce_to_definition(
                Value::String("1234".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("1234".to_string()))
        );

        // And also from Value::Int
        assert_eq!(
            coerce_to_definition(Value::Int(1234.into()), "", &resolver, &HashMap::new()),
            Ok(Value::String("1234".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                Value::Int((-1234 as i32).into()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("-1234".to_string()))
        );
    }

    #[test]
    fn coerce_bytes_scalar() {
        let bytes_type = TypeDefinition::Scalar(ScalarType::new("Bytes".to_string()));
        let resolver = |_: &str| Some(&bytes_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(Bytes)
        assert_eq!(
            coerce_to_definition(
                Value::String("0x21f".to_string()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::String("0x21f".to_string()))
        );
    }

    #[test]
    fn coerce_int_scalar() {
        let int_type = TypeDefinition::Scalar(ScalarType::new("Int".to_string()));
        let resolver = |_: &str| Some(&int_type);

        assert_eq!(
            coerce_to_definition(Value::Int(13289123.into()), "", &resolver, &HashMap::new()),
            Ok(Value::Int(13289123.into()))
        );
        assert_eq!(
            coerce_to_definition(
                Value::Int((-13289123 as i32).into()),
                "",
                &resolver,
                &HashMap::new()
            ),
            Ok(Value::Int((-13289123 as i32).into()))
        );
    }
}

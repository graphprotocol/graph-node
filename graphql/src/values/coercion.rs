use crate::schema;
use graph::prelude::QueryExecutionError;
use graphql_parser::query as q;
use graphql_parser::schema::{EnumType, InputValue, Name, ScalarType, Type, TypeDefinition, Value};
use std::collections::{BTreeMap, HashMap};

/// A GraphQL value that can be coerced according to a type.
pub trait MaybeCoercible<T> {
    fn coerce(&self, using_type: &T) -> Option<Value>;
}

impl MaybeCoercible<EnumType> for Value {
    fn coerce(&self, using_type: &EnumType) -> Option<Value> {
        match self {
            Value::Null => Some(Value::Null),
            Value::String(name) | Value::Enum(name) => using_type
                .values
                .iter()
                .find(|value| &value.name == name)
                .map(|_| Value::Enum(name.clone())),
            _ => None,
        }
    }
}

impl MaybeCoercible<ScalarType> for Value {
    fn coerce(&self, using_type: &ScalarType) -> Option<Value> {
        match (using_type.name.as_str(), self) {
            (_, v @ Value::Null) => Some(v.clone()),
            ("Boolean", v @ Value::Boolean(_)) => Some(v.clone()),
            ("BigDecimal", Value::Float(f)) => Some(Value::String(f.to_string())),
            ("BigDecimal", Value::Int(i)) => Some(Value::String(i.as_i64()?.to_string())),
            ("BigDecimal", v @ Value::String(_)) => Some(v.clone()),
            ("Int", Value::Int(num)) => {
                let num = num.as_i64()?;
                if i32::min_value() as i64 <= num && num <= i32::max_value() as i64 {
                    Some(Value::Int((num as i32).into()))
                } else {
                    None
                }
            }
            ("String", v @ Value::String(_)) => Some(v.clone()),
            ("ID", v @ Value::String(_)) => Some(v.clone()),
            ("ID", Value::Int(num)) => Some(Value::String(num.as_i64()?.to_string())),
            ("Bytes", v @ Value::String(_)) => Some(v.clone()),
            ("BigInt", v @ Value::String(_)) => Some(v.clone()),
            ("BigInt", Value::Int(num)) => Some(Value::String(num.as_i64()?.to_string())),
            _ => None,
        }
    }
}

fn coerce_to_definition<'a>(
    value: &Value,
    definition: &Name,
    resolver: &impl Fn(&Name) -> Option<&'a TypeDefinition>,
    variables: &HashMap<q::Name, q::Value>,
) -> Option<Value> {
    match resolver(definition)? {
        // Accept enum values if they match a value in the enum type
        TypeDefinition::Enum(t) => value.coerce(t),

        // Try to coerce Scalar values
        TypeDefinition::Scalar(t) => value.coerce(t),

        // Try to coerce InputObject values
        TypeDefinition::InputObject(t) => match value {
            Value::Object(object) => {
                let mut coerced_object = BTreeMap::new();
                for (name, value) in object {
                    let def = t.fields.iter().find(|f| f.name == *name)?;
                    coerced_object.insert(
                        name.clone(),
                        coerce_input_value(Some(value.clone()), def, resolver, variables)
                            .ok()??,
                    );
                }
                Some(Value::Object(coerced_object))
            }
            _ => None,
        },

        // Everything else remains unimplemented
        _ => None,
    }
}

/// Coerces an argument into a GraphQL value.
///
/// `Ok(None)` happens when no value is found for a nullable type.
pub(crate) fn coerce_input_value<'a>(
    mut value: Option<Value>,
    def: &InputValue,
    resolver: &impl Fn(&Name) -> Option<&'a TypeDefinition>,
    variable_values: &HashMap<q::Name, q::Value>,
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
        coerce_value(&value, &def.value_type, resolver, variable_values).ok_or_else(|| {
            QueryExecutionError::InvalidArgumentError(
                def.position.clone(),
                def.name.to_owned(),
                value.clone(),
            )
        })?,
    ))
}

pub(crate) fn coerce_value<'a>(
    value: &Value,
    ty: &Type,
    resolver: &impl Fn(&Name) -> Option<&'a TypeDefinition>,
    variable_values: &HashMap<q::Name, q::Value>,
) -> Option<Value> {
    match (ty, value) {
        // Null values cannot be coerced into non-null types.
        (Type::NonNullType(_), Value::Null) => None,

        // Non-null values may be coercible into non-null types
        (Type::NonNullType(t), _) => coerce_value(value, t, resolver, variable_values),

        // Nullable types can be null.
        (_, Value::Null) => Some(Value::Null),

        // Resolve named types, then try to coerce the value into the resolved type
        (Type::NamedType(name), _) => coerce_to_definition(value, name, resolver, variable_values),

        // List values are coercible if their values are coercible into the
        // inner type.
        (Type::ListType(t), Value::List(ref values)) => {
            let mut coerced_values = vec![];

            // Coerce the list values individually
            for value in values {
                if let Some(v) = coerce_value(value, t, resolver, variable_values) {
                    coerced_values.push(v);
                } else {
                    // Fail if not all values could be coerced
                    return None;
                }
            }

            Some(Value::List(coerced_values))
        }

        // Otherwise the list type is not coercible.
        (Type::ListType(_), _) => None,
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
        let resolver = |_: &String| Some(&enum_type);

        // We can coerce from Value::Enum -> TypeDefinition::Enum if the variant is valid
        assert_eq!(
            coerce_to_definition(
                &Value::Enum("ValidVariant".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::Enum("ValidVariant".to_string()))
        );

        // We cannot coerce from Value::Enum -> TypeDefinition::Enum if the variant is invalid
        assert_eq!(
            coerce_to_definition(
                &Value::Enum("InvalidVariant".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );

        // We also support going from Value::String -> TypeDefinition::Scalar(Enum)
        assert_eq!(
            coerce_to_definition(
                &Value::String("ValidVariant".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::Enum("ValidVariant".to_string())),
        );

        // But we don't support invalid variants
        assert_eq!(
            coerce_to_definition(
                &Value::String("InvalidVariant".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
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
        let resolver = |_: &String| Some(&bool_type);

        // We can coerce from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(true),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(false),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::Boolean(false))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(
                &Value::String("true".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::String("false".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(
                &Value::Float(1.0),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Float(0.0),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
    }

    #[test]
    fn coercion_using_big_decimal_type_definitions_is_correct() {
        let big_decimal_type = TypeDefinition::Scalar(ScalarType::new("BigDecimal".to_string()));
        let resolver = |_: &String| Some(&big_decimal_type);

        // We can coerce from Value::Float -> TypeDefinition::Scalar(BigDecimal)
        assert_eq!(
            coerce_to_definition(
                &Value::Float(23.7),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("23.7".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Float(-5.879),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("-5.879".to_string()))
        );

        // We can coerce from Value::String -> TypeDefinition::Scalar(BigDecimal)
        assert_eq!(
            coerce_to_definition(
                &Value::String("23.7".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("23.7".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::String("-5.879".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("-5.879".to_string())),
        );

        // We can coerce from Value::Int -> TypeDefinition::Scalar(BigDecimal)
        assert_eq!(
            coerce_to_definition(
                &Value::Int(23.into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("23".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Int((-5 as i32).into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("-5".to_string())),
        );

        // We don't spport going from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(true),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(false),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
    }

    #[test]
    fn coercion_using_string_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType::new("String".to_string()));
        let resolver = |_: &String| Some(&string_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(
                &Value::String("foo".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::String("bar".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("bar".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(true),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(false),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(
                &Value::Float(23.7),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Float(-5.879),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None
        );
    }

    #[test]
    fn coercion_using_id_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType::new("ID".to_owned()));
        let resolver = |_: &String| Some(&string_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(
                &Value::String("foo".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::String("bar".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("bar".to_string()))
        );

        // And also from Value::Int
        assert_eq!(
            coerce_to_definition(
                &Value::Int(1234.into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("1234".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(true),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Boolean(false),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None,
        );

        // We don't support going from Value::Float -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(
                &Value::Float(23.7),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Float(-5.879),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            None
        );
    }

    #[test]
    fn coerce_big_int_scalar() {
        let big_int_type = TypeDefinition::Scalar(ScalarType::new("BigInt".to_string()));
        let resolver = |_: &String| Some(&big_int_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(BigInt)
        assert_eq!(
            coerce_to_definition(
                &Value::String("1234".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("1234".to_string()))
        );

        // And also from Value::Int
        assert_eq!(
            coerce_to_definition(
                &Value::Int(1234.into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("1234".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Int((-1234 as i32).into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("-1234".to_string()))
        );
    }

    #[test]
    fn coerce_bytes_scalar() {
        let bytes_type = TypeDefinition::Scalar(ScalarType::new("Bytes".to_string()));
        let resolver = |_: &String| Some(&bytes_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(Bytes)
        assert_eq!(
            coerce_to_definition(
                &Value::String("0x21f".to_string()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::String("0x21f".to_string()))
        );
    }

    #[test]
    fn coerce_int_scalar() {
        let int_type = TypeDefinition::Scalar(ScalarType::new("Int".to_string()));
        let resolver = |_: &String| Some(&int_type);

        assert_eq!(
            coerce_to_definition(
                &Value::Int(13289123.into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::Int(13289123.into()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Int((-13289123 as i32).into()),
                &String::new(),
                &resolver,
                &HashMap::new()
            ),
            Some(Value::Int((-13289123 as i32).into()))
        );
    }
}

use graphql_parser::query::Value;
use graphql_parser::schema::{EnumType, Name, ScalarType, Type, TypeDefinition};

/// A GraphQL value that can be coerced according to a type.
pub trait MaybeCoercible<T> {
    fn coerce(&self, using_type: &T) -> Option<Value>;
}

impl MaybeCoercible<EnumType> for Value {
    fn coerce(&self, using_type: &EnumType) -> Option<Value> {
        match self {
            Value::String(name) => using_type
                .values
                .iter()
                .find(|value| &value.name == name)
                .map(|_| Value::Enum(name.clone())),
            Value::Enum(name) => using_type
                .values
                .iter()
                .find(|value| &value.name == name)
                .map(|_| self.clone()),
            _ => None,
        }
    }
}

impl MaybeCoercible<ScalarType> for Value {
    fn coerce(&self, using_type: &ScalarType) -> Option<Value> {
        match (using_type.name.as_str(), self) {
            ("Boolean", v @ Value::Boolean(_)) => Some(v.clone()),
            ("Float", v @ Value::Float(_)) => Some(v.clone()),
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
) -> Option<Value> {
    match resolver(definition)? {
        // Accept enum values if they match a value in the enum type
        TypeDefinition::Enum(t) => value.coerce(t),

        // Try to coerce Scalar values
        TypeDefinition::Scalar(t) => value.coerce(t),

        // Try to coerce InputObject values
        TypeDefinition::InputObject(_) => match value {
            Value::Object(_) => Some(value.clone()),
            _ => None,
        },

        // Everything else remains unimplemented
        _ => None,
    }
}

/// `R` is a name resolver.
pub(crate) fn coerce_value<'a>(
    value: &Value,
    ty: &Type,
    resolver: &impl Fn(&Name) -> Option<&'a TypeDefinition>,
) -> Option<Value> {
    match (ty, value) {
        // Null values cannot be coerced into non-null types.
        (Type::NonNullType(_), Value::Null) => None,

        // Non-null values may be coercible into non-null types
        (Type::NonNullType(t), _) => coerce_value(value, t, resolver),

        // Nullable types can be null.
        (_, Value::Null) => Some(Value::Null),

        // Resolve named types, then try to coerce the value into the resolved type
        (Type::NamedType(name), _) => coerce_to_definition(value, name, resolver),

        // List values are coercible if their values are coercible into the
        // inner type.
        (Type::ListType(t), Value::List(ref values)) => {
            let mut coerced_values = vec![];

            // Coerce the list values individually
            for value in values {
                if let Some(v) = coerce_value(value, t, resolver) {
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
    use graphql_parser::schema::{
        EnumType, EnumValue, InputObjectType, ScalarType, TypeDefinition,
    };
    use graphql_parser::Pos;
    use std::collections::BTreeMap;
    use std::iter::FromIterator;

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
                &resolver
            ),
            Some(Value::Enum("ValidVariant".to_string()))
        );

        // We cannot coerce from Value::Enum -> TypeDefinition::Enum if the variant is invalid
        assert_eq!(
            coerce_to_definition(
                &Value::Enum("InvalidVariant".to_string()),
                &String::new(),
                &resolver
            ),
            None,
        );

        // We also support going from Value::String -> TypeDefinition::Scalar(Enum)
        assert_eq!(
            coerce_to_definition(
                &Value::String("ValidVariant".to_string()),
                &String::new(),
                &resolver
            ),
            Some(Value::Enum("ValidVariant".to_string())),
        );

        // But we don't support invalid variants
        assert_eq!(
            coerce_to_definition(
                &Value::String("InvalidVariant".to_string()),
                &String::new(),
                &resolver
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
            coerce_to_definition(&Value::Boolean(true), &String::new(), &resolver,),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            coerce_to_definition(&Value::Boolean(false), &String::new(), &resolver),
            Some(Value::Boolean(false))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(
                &Value::String("true".to_string()),
                &String::new(),
                &resolver
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::String("false".to_string()),
                &String::new(),
                &resolver
            ),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(&Value::Float(1.0), &String::new(), &resolver),
            None,
        );
        assert_eq!(
            coerce_to_definition(&Value::Float(0.0), &String::new(), &resolver),
            None,
        );
    }

    #[test]
    fn coercion_using_float_type_definitions_is_correct() {
        let float_type = TypeDefinition::Scalar(ScalarType::new("Float".to_string()));
        let resolver = |_: &String| Some(&float_type);

        // We can coerce from Value::Float -> TypeDefinition::Scalar(Float)
        assert_eq!(
            coerce_to_definition(&Value::Float(23.7), &String::new(), &resolver),
            Some(Value::Float(23.7))
        );
        assert_eq!(
            coerce_to_definition(&Value::Float(-5.879), &String::new(), &resolver),
            Some(Value::Float(-5.879))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Float)
        assert_eq!(
            coerce_to_definition(
                &Value::String("23.7".to_string()),
                &String::new(),
                &resolver
            ),
            None,
        );
        assert_eq!(
            coerce_to_definition(
                &Value::String("-5.879".to_string()),
                &String::new(),
                &resolver
            ),
            None,
        );

        // We don't spport going from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(
            coerce_to_definition(&Value::Boolean(true), &String::new(), &resolver),
            None,
        );
        assert_eq!(
            coerce_to_definition(&Value::Boolean(false), &String::new(), &resolver),
            None,
        );
    }

    #[test]
    fn coercion_using_string_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType::new("String".to_string()));
        let resolver = |_: &String| Some(&string_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(&Value::String("foo".to_string()), &String::new(), &resolver),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            coerce_to_definition(&Value::String("bar".to_string()), &String::new(), &resolver),
            Some(Value::String("bar".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(&Value::Boolean(true), &String::new(), &resolver),
            None,
        );
        assert_eq!(
            coerce_to_definition(&Value::Boolean(false), &String::new(), &resolver),
            None,
        );

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(String)
        assert_eq!(
            coerce_to_definition(&Value::Float(23.7), &String::new(), &resolver),
            None
        );
        assert_eq!(
            coerce_to_definition(&Value::Float(-5.879), &String::new(), &resolver),
            None
        );
    }

    #[test]
    fn coercion_using_id_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType::new("ID".to_owned()));
        let resolver = |_: &String| Some(&string_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(&Value::String("foo".to_string()), &String::new(), &resolver),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            coerce_to_definition(&Value::String("bar".to_string()), &String::new(), &resolver),
            Some(Value::String("bar".to_string()))
        );

        // And also from Value::Int
        assert_eq!(
            coerce_to_definition(&Value::Int(1234.into()), &String::new(), &resolver),
            Some(Value::String("1234".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(&Value::Boolean(true), &String::new(), &resolver),
            None,
        );
        assert_eq!(
            coerce_to_definition(&Value::Boolean(false), &String::new(), &resolver),
            None,
        );

        // We don't support going from Value::Float -> TypeDefinition::Scalar(ID)
        assert_eq!(
            coerce_to_definition(&Value::Float(23.7), &String::new(), &resolver),
            None
        );
        assert_eq!(
            coerce_to_definition(&Value::Float(-5.879), &String::new(), &resolver),
            None
        );
    }

    /* #[test]
    fn coercion_using_input_object_type_definitions_is_correct() {
        let input_object_type =
            TypeDefinition::InputObject(InputObjectType::new("InputObject".to_owned()));
        let resolver = |_: &String| Some(&input_object_type);

        // We can coerce from Value::Object -> TypeDefinition::InputObject
        let example_object = BTreeMap::from_iter(
            vec![
                ("Foo".to_string(), Value::Boolean(false)),
                ("Bar".to_string(), Value::Float(15.2)),
            ]
            .into_iter(),
        );
        assert_eq!(
            Value::Object(example_object.clone()).coerce(&input_object_type),
            Some(Value::Object(example_object))
        );

        // We don't support going from Value::String -> TypeDefinition::InputObject
        assert_eq!(
            Value::String("foo".to_string()).coerce(&input_object_type),
            None,
        );
        assert_eq!(
            Value::String("bar".to_string()).coerce(&input_object_type),
            None,
        );

        // We don't support going from Value::Boolean -> TypeDefinition::InputObject
        assert_eq!(Value::Boolean(true).coerce(&input_object_type), None,);
        assert_eq!(Value::Boolean(false).coerce(&input_object_type), None,);

        // We don't spport going from Value::Float -> TypeDefinition::InputObject
        assert_eq!(Value::Float(23.7).coerce(&input_object_type), None,);
        assert_eq!(Value::Float(-5.879).coerce(&input_object_type), None,);
    }*/

    #[test]
    fn coerce_big_int_scalar() {
        let big_int_type = TypeDefinition::Scalar(ScalarType::new("BigInt".to_string()));
        let resolver = |_: &String| Some(&big_int_type);

        // We can coerce from Value::String -> TypeDefinition::Scalar(BigInt)
        assert_eq!(
            coerce_to_definition(
                &Value::String("1234".to_string()),
                &String::new(),
                &resolver
            ),
            Some(Value::String("1234".to_string()))
        );

        // And also from Value::Int
        assert_eq!(
            coerce_to_definition(&Value::Int(1234.into()), &String::new(), &resolver),
            Some(Value::String("1234".to_string()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Int((-1234 as i32).into()),
                &String::new(),
                &resolver
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
                &resolver
            ),
            Some(Value::String("0x21f".to_string()))
        );
    }

    #[test]
    fn coerce_int_scalar() {
        let int_type = TypeDefinition::Scalar(ScalarType::new("Int".to_string()));
        let resolver = |_: &String| Some(&int_type);

        assert_eq!(
            coerce_to_definition(&Value::Int(13289123.into()), &String::new(), &resolver),
            Some(Value::Int(13289123.into()))
        );
        assert_eq!(
            coerce_to_definition(
                &Value::Int((-13289123 as i32).into()),
                &String::new(),
                &resolver
            ),
            Some(Value::Int((-13289123 as i32).into()))
        );
    }
}

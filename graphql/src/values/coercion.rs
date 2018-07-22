use graphql_parser::query::Value;
use graphql_parser::schema::{EnumType, Name, ScalarType, Type, TypeDefinition};

/// A GraphQL value that can be coerced according to a type.
pub trait MaybeCoercible<T> {
    fn coerce(&self, using_type: &T) -> Option<Value>;
}

impl MaybeCoercible<EnumType> for Value {
    fn coerce(&self, using_type: &EnumType) -> Option<Value> {
        match self {
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
            ("Int", v @ Value::Int(_)) => Some(v.clone()),
            ("String", v @ Value::String(_)) => Some(v.clone()),
            ("ID", v @ Value::String(_)) => Some(v.clone()),
            _ => None,
        }
    }
}

impl MaybeCoercible<TypeDefinition> for Value {
    fn coerce(&self, using_type: &TypeDefinition) -> Option<Value> {
        match (using_type, self) {
            // Accept enum values if they match a value in the enum type
            (TypeDefinition::Enum(t), value) => value.coerce(t),

            // Try to coerce Scalar values
            (TypeDefinition::Scalar(t), value) => value.coerce(t),

            // Try to coerce InputObject values
            (TypeDefinition::InputObject(_), v) => match v {
                Value::Object(_) => Some(self.clone()),
                _ => None,
            },

            // Everything else remains unimplemented
            _ => unimplemented!(),
        }
    }
}

/// `R` is a name resolver.
pub(crate) fn coerce_value<'a, R>(value: &Value, ty: &Type, resolver: &R) -> Option<Value>
where
    R: Fn(&Name) -> Option<&'a TypeDefinition>,
{
    match (ty, value) {
        // Null values cannot be coerced into non-null types
        (Type::NonNullType(_), Value::Null) => None,

        // Non-null values may be coercible into non-null types
        (Type::NonNullType(t), _) => coerce_value(value, t, resolver),

        // Resolve named types, then try to coerce the value into the resolved type
        (Type::NamedType(name), _) => resolver(name).and_then(|def| value.coerce(def)),

        // List values may be coercible if they are empty or their values are coercible
        // into the inner type
        (Type::ListType(t), Value::List(ref values)) => if values.is_empty() {
            Some(Value::List(values.clone()))
        } else {
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
        },

        // Everything else is unsupported for now
        _ => unimplemented!(),
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

    use super::MaybeCoercible;

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

        // We can coerce from Value::Enum -> TypeDefinition::Enum if the variant is valid
        assert_eq!(
            Value::Enum("ValidVariant".to_string()).coerce(&enum_type),
            Some(Value::Enum("ValidVariant".to_string()))
        );

        // We cannot coerce from Value::Enum -> TypeDefinition::Enum if the variant is invalid
        assert_eq!(
            Value::Enum("InvalidVariant".to_string()).coerce(&enum_type),
            None,
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Enum)
        assert_eq!(
            Value::String("ValidVariant".to_string()).coerce(&enum_type),
            None,
        );
        assert_eq!(
            Value::String("InvalidVariant".to_string()).coerce(&enum_type),
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
            Value::Boolean(true).coerce(&bool_type),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            Value::Boolean(false).coerce(&bool_type),
            Some(Value::Boolean(false))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Boolean)
        assert_eq!(Value::String("true".to_string()).coerce(&bool_type), None,);
        assert_eq!(Value::String("false".to_string()).coerce(&bool_type), None,);

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(Boolean)
        assert_eq!(Value::Float(1.0).coerce(&bool_type), None,);
        assert_eq!(Value::Float(0.0).coerce(&bool_type), None,);
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
            Value::Float(23.7).coerce(&float_type),
            Some(Value::Float(23.7))
        );
        assert_eq!(
            Value::Float(-5.879).coerce(&float_type),
            Some(Value::Float(-5.879))
        );

        // We don't support going from Value::String -> TypeDefinition::Scalar(Float)
        assert_eq!(Value::String("23.7".to_string()).coerce(&float_type), None,);
        assert_eq!(
            Value::String("-5.879".to_string()).coerce(&float_type),
            None,
        );

        // We don't spport going from Value::Boolean -> TypeDefinition::Scalar(Boolean)
        assert_eq!(Value::Boolean(true).coerce(&float_type), None,);
        assert_eq!(Value::Boolean(false).coerce(&float_type), None,);
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
            Value::String("foo".to_string()).coerce(&string_type),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            Value::String("bar".to_string()).coerce(&string_type),
            Some(Value::String("bar".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(String)
        assert_eq!(Value::Boolean(true).coerce(&string_type), None,);
        assert_eq!(Value::Boolean(false).coerce(&string_type), None,);

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(String)
        assert_eq!(Value::Float(23.7).coerce(&string_type), None,);
        assert_eq!(Value::Float(-5.879).coerce(&string_type), None,);
    }

    #[test]
    fn coercion_using_id_type_definitions_is_correct() {
        let string_type = TypeDefinition::Scalar(ScalarType {
            name: "ID".to_string(),
            description: None,
            directives: vec![],
            position: Pos::default(),
        });

        // We can coerce from Value::String -> TypeDefinition::Scalar(ID)
        assert_eq!(
            Value::String("foo".to_string()).coerce(&string_type),
            Some(Value::String("foo".to_string()))
        );
        assert_eq!(
            Value::String("bar".to_string()).coerce(&string_type),
            Some(Value::String("bar".to_string()))
        );

        // We don't support going from Value::Boolean -> TypeDefinition::Scalar(ID)
        assert_eq!(Value::Boolean(true).coerce(&string_type), None,);
        assert_eq!(Value::Boolean(false).coerce(&string_type), None,);

        // We don't spport going from Value::Float -> TypeDefinition::Scalar(ID)
        assert_eq!(Value::Float(23.7).coerce(&string_type), None,);
        assert_eq!(Value::Float(-5.879).coerce(&string_type), None,);
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
    }
}

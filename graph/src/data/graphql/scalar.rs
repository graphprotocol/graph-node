use std::convert::TryFrom;

pub enum BuiltInScalarType {
    Boolean,
    Int,
    BigDecimal,
    String,
    BigInt,
    Bytes,
    ID,
}

impl TryFrom<&String> for BuiltInScalarType {
    type Error = ();

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match value.as_ref() {
            "Boolean" => Ok(BuiltInScalarType::Boolean),
            "Int" => Ok(BuiltInScalarType::Int),
            "BigDecimal" => Ok(BuiltInScalarType::BigDecimal),
            "String" => Ok(BuiltInScalarType::String),
            "BigInt" => Ok(BuiltInScalarType::BigInt),
            "Bytes" => Ok(BuiltInScalarType::Bytes),
            "ID" => Ok(BuiltInScalarType::ID),
            _ => Err(()),
        }
    }
}

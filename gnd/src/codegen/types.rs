//! Type conversion utilities for code generation.
//!
//! This module handles conversions between:
//! - GraphQL schema types and AssemblyScript types
//! - Ethereum ABI types and AssemblyScript types
//! - graph-node Value types and AssemblyScript types

/// Get the AssemblyScript type for a GraphQL Value type.
pub fn asc_type_for_value(value_type: &str) -> &'static str {
    match value_type {
        "Bytes" => "Bytes",
        "Boolean" => "boolean",
        "Int" => "i32",
        "Int8" => "i64",
        "BigInt" => "BigInt",
        "ID" | "String" => "string",
        "BigDecimal" => "BigDecimal",
        "Timestamp" => "i64",
        // Array types
        "[Bytes]" => "Array<Bytes>",
        "[Boolean]" => "Array<boolean>",
        "[Int]" => "Array<i32>",
        "[Int8]" => "Array<i64>",
        "[Timestamp]" => "Array<i64>",
        "[BigInt]" => "Array<BigInt>",
        "[ID]" | "[String]" => "Array<string>",
        "[BigDecimal]" => "Array<BigDecimal>",
        // Default for entity references and unknown types
        _ => {
            if value_type.starts_with('[') && value_type.ends_with(']') {
                "Array<string>" // Entity reference arrays
            } else {
                "string" // Entity references
            }
        }
    }
}

/// Generate code to convert a Value to AssemblyScript.
pub fn value_to_asc(code: &str, value_type: &str) -> String {
    match value_type {
        "Bytes" => format!("{}.toBytes()", code),
        "Boolean" => format!("{}.toBoolean()", code),
        "Int" => format!("{}.toI32()", code),
        "Int8" => format!("{}.toI64()", code),
        "BigInt" => format!("{}.toBigInt()", code),
        "ID" | "String" => format!("{}.toString()", code),
        "BigDecimal" => format!("{}.toBigDecimal()", code),
        "Timestamp" => format!("{}.toTimestamp()", code),
        // Array types
        "[Bytes]" => format!("{}.toBytesArray()", code),
        "[Boolean]" => format!("{}.toBooleanArray()", code),
        "[Int]" => format!("{}.toI32Array()", code),
        "[Int8]" => format!("{}.toI64Array()", code),
        "[Timestamp]" => format!("{}.toTimestampArray()", code),
        "[BigInt]" => format!("{}.toBigIntArray()", code),
        "[ID]" | "[String]" => format!("{}.toStringArray()", code),
        "[BigDecimal]" => format!("{}.toBigDecimalArray()", code),
        // Default for entity references
        _ => {
            if value_type.starts_with('[') {
                format!("{}.toStringArray()", code)
            } else {
                format!("{}.toString()", code)
            }
        }
    }
}

/// Generate code to convert an AssemblyScript value to a Value.
pub fn value_from_asc(code: &str, value_type: &str) -> String {
    match value_type {
        "Bytes" => format!("Value.fromBytes({})", code),
        "Boolean" => format!("Value.fromBoolean({})", code),
        "Int" => format!("Value.fromI32({})", code),
        "Int8" => format!("Value.fromI64({})", code),
        "BigInt" => format!("Value.fromBigInt({})", code),
        "ID" | "String" => format!("Value.fromString({})", code),
        "BigDecimal" => format!("Value.fromBigDecimal({})", code),
        "Timestamp" => format!("Value.fromTimestamp({})", code),
        // Array types
        "[Bytes]" => format!("Value.fromBytesArray({})", code),
        "[Boolean]" => format!("Value.fromBooleanArray({})", code),
        "[Int]" => format!("Value.fromI32Array({})", code),
        "[Int8]" => format!("Value.fromI64Array({})", code),
        "[Timestamp]" => format!("Value.fromTimestampArray({})", code),
        "[BigInt]" => format!("Value.fromBigIntArray({})", code),
        "[ID]" | "[String]" => format!("Value.fromStringArray({})", code),
        "[BigDecimal]" => format!("Value.fromBigDecimalArray({})", code),
        // Default for entity references
        _ => {
            if value_type.starts_with('[') {
                format!("Value.fromStringArray({})", code)
            } else {
                format!("Value.fromString({})", code)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asc_type_for_value() {
        assert_eq!(asc_type_for_value("String"), "string");
        assert_eq!(asc_type_for_value("ID"), "string");
        assert_eq!(asc_type_for_value("Int"), "i32");
        assert_eq!(asc_type_for_value("BigInt"), "BigInt");
        assert_eq!(asc_type_for_value("Bytes"), "Bytes");
        assert_eq!(asc_type_for_value("[String]"), "Array<string>");
        assert_eq!(asc_type_for_value("SomeEntity"), "string"); // Entity reference
    }

    #[test]
    fn test_value_to_asc() {
        assert_eq!(value_to_asc("value", "String"), "value.toString()");
        assert_eq!(value_to_asc("value", "BigInt"), "value.toBigInt()");
        assert_eq!(value_to_asc("value", "[Int]"), "value.toI32Array()");
    }

    #[test]
    fn test_value_from_asc() {
        assert_eq!(value_from_asc("value", "String"), "Value.fromString(value)");
        assert_eq!(value_from_asc("value", "BigInt"), "Value.fromBigInt(value)");
    }
}

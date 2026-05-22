//! Type conversion utilities for code generation.
//!
//! This module handles conversions between:
//! - GraphQL schema types and AssemblyScript types
//! - Ethereum ABI types and AssemblyScript types
//! - graph-node Value types and AssemblyScript types

/// Get the AssemblyScript type for a GraphQL Value type.
///
/// Supports scalar types, arrays, and nested arrays (matrices).
/// Value types use bracket notation: `String`, `[String]`, `[[String]]`
pub fn asc_type_for_value(value_type: &str) -> &'static str {
    match value_type {
        // Scalar types
        "Bytes" => "Bytes",
        "Boolean" => "boolean",
        "Int" => "i32",
        "Int8" => "i64",
        "BigInt" => "BigInt",
        "ID" | "String" => "string",
        "BigDecimal" => "BigDecimal",
        "Timestamp" => "i64",
        // Array types (single dimension)
        "[Bytes]" => "Array<Bytes>",
        "[Boolean]" => "Array<boolean>",
        "[Int]" => "Array<i32>",
        "[Int8]" => "Array<i64>",
        "[Timestamp]" => "Array<i64>",
        "[BigInt]" => "Array<BigInt>",
        "[ID]" | "[String]" => "Array<string>",
        "[BigDecimal]" => "Array<BigDecimal>",
        // Matrix types (nested arrays - 2D)
        "[[Bytes]]" => "Array<Array<Bytes>>",
        "[[Boolean]]" => "Array<Array<boolean>>",
        "[[Int]]" => "Array<Array<i32>>",
        "[[Int8]]" => "Array<Array<i64>>",
        "[[Timestamp]]" => "Array<Array<i64>>",
        "[[BigInt]]" => "Array<Array<BigInt>>",
        "[[ID]]" | "[[String]]" => "Array<Array<string>>",
        "[[BigDecimal]]" => "Array<Array<BigDecimal>>",
        // Default for entity references and unknown types
        _ => {
            if value_type.starts_with("[[") && value_type.ends_with("]]") {
                "Array<Array<string>>" // Entity reference matrices
            } else if value_type.starts_with('[') && value_type.ends_with(']') {
                "Array<string>" // Entity reference arrays
            } else {
                "string" // Entity references
            }
        }
    }
}

/// Generate code to convert a Value to AssemblyScript.
///
/// Supports scalar types, arrays, and nested arrays (matrices).
pub fn value_to_asc(code: &str, value_type: &str) -> String {
    match value_type {
        // Scalar types
        "Bytes" => format!("{}.toBytes()", code),
        "Boolean" => format!("{}.toBoolean()", code),
        "Int" => format!("{}.toI32()", code),
        "Int8" => format!("{}.toI64()", code),
        "BigInt" => format!("{}.toBigInt()", code),
        "ID" | "String" => format!("{}.toString()", code),
        "BigDecimal" => format!("{}.toBigDecimal()", code),
        "Timestamp" => format!("{}.toTimestamp()", code),
        // Array types (single dimension)
        "[Bytes]" => format!("{}.toBytesArray()", code),
        "[Boolean]" => format!("{}.toBooleanArray()", code),
        "[Int]" => format!("{}.toI32Array()", code),
        "[Int8]" => format!("{}.toI64Array()", code),
        "[Timestamp]" => format!("{}.toTimestampArray()", code),
        "[BigInt]" => format!("{}.toBigIntArray()", code),
        "[ID]" | "[String]" => format!("{}.toStringArray()", code),
        "[BigDecimal]" => format!("{}.toBigDecimalArray()", code),
        // Matrix types (nested arrays - 2D)
        "[[Bytes]]" => format!("{}.toBytesMatrix()", code),
        "[[Boolean]]" => format!("{}.toBooleanMatrix()", code),
        "[[Int]]" => format!("{}.toI32Matrix()", code),
        "[[Int8]]" => format!("{}.toI64Matrix()", code),
        "[[Timestamp]]" => format!("{}.toTimestampMatrix()", code),
        "[[BigInt]]" => format!("{}.toBigIntMatrix()", code),
        "[[ID]]" | "[[String]]" => format!("{}.toStringMatrix()", code),
        "[[BigDecimal]]" => format!("{}.toBigDecimalMatrix()", code),
        // Default for entity references
        _ => {
            if value_type.starts_with("[[") {
                format!("{}.toStringMatrix()", code)
            } else if value_type.starts_with('[') {
                format!("{}.toStringArray()", code)
            } else {
                format!("{}.toString()", code)
            }
        }
    }
}

/// Generate code to convert an AssemblyScript value to a Value.
///
/// Supports scalar types, arrays, and nested arrays (matrices).
pub fn value_from_asc(code: &str, value_type: &str) -> String {
    match value_type {
        // Scalar types
        "Bytes" => format!("Value.fromBytes({})", code),
        "Boolean" => format!("Value.fromBoolean({})", code),
        "Int" => format!("Value.fromI32({})", code),
        "Int8" => format!("Value.fromI64({})", code),
        "BigInt" => format!("Value.fromBigInt({})", code),
        "ID" | "String" => format!("Value.fromString({})", code),
        "BigDecimal" => format!("Value.fromBigDecimal({})", code),
        "Timestamp" => format!("Value.fromTimestamp({})", code),
        // Array types (single dimension)
        "[Bytes]" => format!("Value.fromBytesArray({})", code),
        "[Boolean]" => format!("Value.fromBooleanArray({})", code),
        "[Int]" => format!("Value.fromI32Array({})", code),
        "[Int8]" => format!("Value.fromI64Array({})", code),
        "[Timestamp]" => format!("Value.fromTimestampArray({})", code),
        "[BigInt]" => format!("Value.fromBigIntArray({})", code),
        "[ID]" | "[String]" => format!("Value.fromStringArray({})", code),
        "[BigDecimal]" => format!("Value.fromBigDecimalArray({})", code),
        // Matrix types (nested arrays - 2D)
        "[[Bytes]]" => format!("Value.fromBytesMatrix({})", code),
        "[[Boolean]]" => format!("Value.fromBooleanMatrix({})", code),
        "[[Int]]" => format!("Value.fromI32Matrix({})", code),
        "[[Int8]]" => format!("Value.fromI64Matrix({})", code),
        "[[Timestamp]]" => format!("Value.fromTimestampMatrix({})", code),
        "[[BigInt]]" => format!("Value.fromBigIntMatrix({})", code),
        "[[ID]]" | "[[String]]" => format!("Value.fromStringMatrix({})", code),
        "[[BigDecimal]]" => format!("Value.fromBigDecimalMatrix({})", code),
        // Default for entity references
        _ => {
            if value_type.starts_with("[[") {
                format!("Value.fromStringMatrix({})", code)
            } else if value_type.starts_with('[') {
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
        // Scalar types
        assert_eq!(asc_type_for_value("String"), "string");
        assert_eq!(asc_type_for_value("ID"), "string");
        assert_eq!(asc_type_for_value("Int"), "i32");
        assert_eq!(asc_type_for_value("BigInt"), "BigInt");
        assert_eq!(asc_type_for_value("Bytes"), "Bytes");
        assert_eq!(asc_type_for_value("Boolean"), "boolean");
        assert_eq!(asc_type_for_value("BigDecimal"), "BigDecimal");
        assert_eq!(asc_type_for_value("Int8"), "i64");
        assert_eq!(asc_type_for_value("Timestamp"), "i64");

        // Array types
        assert_eq!(asc_type_for_value("[String]"), "Array<string>");
        assert_eq!(asc_type_for_value("[ID]"), "Array<string>");
        assert_eq!(asc_type_for_value("[Int]"), "Array<i32>");
        assert_eq!(asc_type_for_value("[BigInt]"), "Array<BigInt>");
        assert_eq!(asc_type_for_value("[Bytes]"), "Array<Bytes>");
        assert_eq!(asc_type_for_value("[Boolean]"), "Array<boolean>");
        assert_eq!(asc_type_for_value("[BigDecimal]"), "Array<BigDecimal>");
        assert_eq!(asc_type_for_value("[Int8]"), "Array<i64>");
        assert_eq!(asc_type_for_value("[Timestamp]"), "Array<i64>");

        // Matrix types (nested arrays)
        assert_eq!(asc_type_for_value("[[String]]"), "Array<Array<string>>");
        assert_eq!(asc_type_for_value("[[ID]]"), "Array<Array<string>>");
        assert_eq!(asc_type_for_value("[[Int]]"), "Array<Array<i32>>");
        assert_eq!(asc_type_for_value("[[BigInt]]"), "Array<Array<BigInt>>");
        assert_eq!(asc_type_for_value("[[Bytes]]"), "Array<Array<Bytes>>");
        assert_eq!(asc_type_for_value("[[Boolean]]"), "Array<Array<boolean>>");
        assert_eq!(
            asc_type_for_value("[[BigDecimal]]"),
            "Array<Array<BigDecimal>>"
        );
        assert_eq!(asc_type_for_value("[[Int8]]"), "Array<Array<i64>>");
        assert_eq!(asc_type_for_value("[[Timestamp]]"), "Array<Array<i64>>");

        // Entity references
        assert_eq!(asc_type_for_value("SomeEntity"), "string");
        assert_eq!(asc_type_for_value("[SomeEntity]"), "Array<string>");
        assert_eq!(asc_type_for_value("[[SomeEntity]]"), "Array<Array<string>>");
    }

    #[test]
    fn test_value_to_asc() {
        // Scalar types
        assert_eq!(value_to_asc("value", "String"), "value.toString()");
        assert_eq!(value_to_asc("value", "BigInt"), "value.toBigInt()");
        assert_eq!(value_to_asc("value", "Bytes"), "value.toBytes()");
        assert_eq!(value_to_asc("value", "Boolean"), "value.toBoolean()");
        assert_eq!(value_to_asc("value", "Int"), "value.toI32()");
        assert_eq!(value_to_asc("value", "Int8"), "value.toI64()");

        // Array types
        assert_eq!(value_to_asc("value", "[Int]"), "value.toI32Array()");
        assert_eq!(value_to_asc("value", "[String]"), "value.toStringArray()");
        assert_eq!(value_to_asc("value", "[Bytes]"), "value.toBytesArray()");
        assert_eq!(value_to_asc("value", "[BigInt]"), "value.toBigIntArray()");

        // Matrix types
        assert_eq!(value_to_asc("value", "[[Int]]"), "value.toI32Matrix()");
        assert_eq!(
            value_to_asc("value", "[[String]]"),
            "value.toStringMatrix()"
        );
        assert_eq!(value_to_asc("value", "[[Bytes]]"), "value.toBytesMatrix()");
        assert_eq!(
            value_to_asc("value", "[[BigInt]]"),
            "value.toBigIntMatrix()"
        );
        assert_eq!(
            value_to_asc("value", "[[Boolean]]"),
            "value.toBooleanMatrix()"
        );
        assert_eq!(value_to_asc("value", "[[Int8]]"), "value.toI64Matrix()");
        assert_eq!(
            value_to_asc("value", "[[BigDecimal]]"),
            "value.toBigDecimalMatrix()"
        );

        // Entity reference matrices
        assert_eq!(
            value_to_asc("value", "[[SomeEntity]]"),
            "value.toStringMatrix()"
        );
    }

    #[test]
    fn test_value_from_asc() {
        // Scalar types
        assert_eq!(value_from_asc("value", "String"), "Value.fromString(value)");
        assert_eq!(value_from_asc("value", "BigInt"), "Value.fromBigInt(value)");
        assert_eq!(value_from_asc("value", "Bytes"), "Value.fromBytes(value)");
        assert_eq!(
            value_from_asc("value", "Boolean"),
            "Value.fromBoolean(value)"
        );

        // Array types
        assert_eq!(
            value_from_asc("value", "[Int]"),
            "Value.fromI32Array(value)"
        );
        assert_eq!(
            value_from_asc("value", "[String]"),
            "Value.fromStringArray(value)"
        );
        assert_eq!(
            value_from_asc("value", "[BigInt]"),
            "Value.fromBigIntArray(value)"
        );

        // Matrix types
        assert_eq!(
            value_from_asc("value", "[[Int]]"),
            "Value.fromI32Matrix(value)"
        );
        assert_eq!(
            value_from_asc("value", "[[String]]"),
            "Value.fromStringMatrix(value)"
        );
        assert_eq!(
            value_from_asc("value", "[[Bytes]]"),
            "Value.fromBytesMatrix(value)"
        );
        assert_eq!(
            value_from_asc("value", "[[BigInt]]"),
            "Value.fromBigIntMatrix(value)"
        );
        assert_eq!(
            value_from_asc("value", "[[Boolean]]"),
            "Value.fromBooleanMatrix(value)"
        );
        assert_eq!(
            value_from_asc("value", "[[Int8]]"),
            "Value.fromI64Matrix(value)"
        );
        assert_eq!(
            value_from_asc("value", "[[BigDecimal]]"),
            "Value.fromBigDecimalMatrix(value)"
        );

        // Entity reference matrices
        assert_eq!(
            value_from_asc("value", "[[SomeEntity]]"),
            "Value.fromStringMatrix(value)"
        );
    }
}

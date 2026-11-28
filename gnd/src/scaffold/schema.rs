//! Schema (schema.graphql) generation for scaffold.

use graph::abi::{DynSolType, EventParam};

use crate::abi::{IntWidth, classify_int_width};

use super::ScaffoldOptions;
use super::manifest::extract_events_from_abi;
use super::sanitize_field_name;

/// Generate the schema.graphql content.
pub fn generate_schema(options: &ScaffoldOptions) -> String {
    let events = extract_events_from_abi(options);

    if events.is_empty() {
        // Generate example entity with no event params
        return generate_example_entity(&[]);
    }

    if !options.index_events {
        // Generate example entity with first event's params
        return generate_example_entity(&events[0].inputs);
    }

    // Generate an entity for each event, disambiguating overloaded names.
    let mut schema = String::new();

    for resolved in super::disambiguate_events(events) {
        let entity = generate_event_entity(&resolved.entity_name, &resolved.event.inputs);
        schema.push_str(&entity);
        schema.push_str("\n\n");
    }

    schema.trim_end().to_string()
}

/// Generate an example entity for placeholder mode.
/// Uses first 2 event params if available, with type comments.
fn generate_example_entity(inputs: &[EventParam]) -> String {
    let mut fields = String::new();
    fields.push_str("  # Use Bytes when possible for better performance\n");
    fields.push_str("  id: Bytes!\n");
    fields.push_str("  count: BigInt!\n");

    // Include first 2 event params with type comments
    for input in inputs.iter().take(2) {
        let field_name = sanitize_field_name(&input.name);
        // Resolve from `selector_type` (a tuple's `ty` is the bare "tuple"), but
        // keep the `# {ty}` comment on the raw declared type. Bytes is the
        // fallback for a type alloy can't resolve.
        let ty = crate::abi::try_resolve_selector_type(&input.selector_type())
            .unwrap_or(DynSolType::Bytes);
        fields.push_str(&format!(
            "  {}: {}! # {}\n",
            field_name,
            graphql_type(&ty),
            input.ty
        ));
    }

    format!(
        "# Declare entity types as immutable when possible for better performance\n\
         type ExampleEntity @entity(immutable: true) {{\n{}}}\n",
        fields
    )
}

/// Generate an entity type for an event.
pub fn generate_event_entity(entity_name: &str, inputs: &[EventParam]) -> String {
    let mut fields = String::new();

    // ID field
    fields.push_str("  id: Bytes!\n");

    // Fields from event inputs (tuples are unrolled into a field per component).
    for leaf in super::flatten_event_inputs(inputs) {
        fields.push_str(&format!("  {}: {}!\n", leaf.field, graphql_type(&leaf.ty)));
    }

    // Standard blockchain fields
    fields.push_str("  blockNumber: BigInt!\n");
    fields.push_str("  blockTimestamp: BigInt!\n");
    fields.push_str("  transactionHash: Bytes!");

    format!(
        "# Declare entity types as immutable when possible for better performance\n\
         type {} @entity(immutable: true) {{\n{}\n}}",
        entity_name, fields
    )
}

/// Map a resolved Solidity type to the GraphQL scalar the scaffold emits.
///
/// The match is exhaustive on purpose: enabling alloy's `eip712` feature adds a
/// `CustomStruct` variant, and a missing arm should fail to compile rather than
/// silently fall through.
fn graphql_type(ty: &DynSolType) -> &'static str {
    match ty {
        DynSolType::Address | DynSolType::Bytes | DynSolType::FixedBytes(_) => "Bytes",
        // A Solidity `function` (bytes24). codegen decodes it as `ethereum.Tuple`
        // (`.toTuple()`), so schema and binding disagree; kept as the pre-existing
        // scaffold behavior, tracked in #6684.
        DynSolType::Function => "Bytes",
        DynSolType::Bool => "Boolean",
        DynSolType::String => "String",
        DynSolType::Int(bits) => int_scalar(true, *bits as u32),
        DynSolType::Uint(bits) => int_scalar(false, *bits as u32),
        // An indexed reference type arrives as a `bytes32` hash leaf, and a
        // placeholder tuple renders opaque: either way one `Bytes` field.
        DynSolType::Tuple(_) => "Bytes",
        DynSolType::Array(inner) | DynSolType::FixedArray(inner, _) => list_of(inner),
    }
}

/// The GraphQL list type for an array element. The `Int8` (i64) band collapses to
/// `[BigInt!]`: `ethereum.Value` has no i64 array accessor, so the codegen decodes
/// int arrays of that band via `toBigIntArray()` (see codegen/abi.rs). Nested
/// lists flatten to one dimension, matching graph-node's store, which has none.
fn list_of(inner: &DynSolType) -> &'static str {
    match graphql_type(inner) {
        "Int" => "[Int!]",
        "Boolean" => "[Boolean!]",
        "String" => "[String!]",
        "Int8" | "BigInt" => "[BigInt!]",
        // Bytes, and any nested list degraded to one dimension.
        _ => "[Bytes!]",
    }
}

/// Map a Solidity integer to the narrowest GraphQL scalar that holds it:
/// `Int` (i32), `Int8` (i64), or `BigInt`; see [`classify_int_width`] for the
/// cutoffs. Must match codegen's `asc_type_for_ethereum`, or the schema and the
/// generated bindings disagree.
fn int_scalar(signed: bool, bits: u32) -> &'static str {
    match classify_int_width(signed, bits) {
        IntWidth::I32 => "Int",
        IntWidth::I64 => "Int8",
        IntWidth::Big => "BigInt",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_generate_example_entity_no_inputs() {
        let schema = generate_example_entity(&[]);
        assert!(schema.contains("type ExampleEntity @entity(immutable: true)"));
        assert!(schema.contains("id: Bytes!"));
        assert!(schema.contains("count: BigInt!"));
        // Should not contain block fields
        assert!(!schema.contains("blockNumber"));
        assert!(!schema.contains("blockTimestamp"));
        assert!(!schema.contains("transactionHash"));
    }

    #[test]
    fn test_generate_example_entity_with_inputs() {
        let param = |name: &str, ty: &str, indexed: bool| EventParam {
            name: name.to_string(),
            ty: ty.to_string(),
            indexed,
            ..Default::default()
        };
        let inputs = vec![
            param("from", "address", true),
            param("to", "address", true),
            param("value", "uint256", false),
        ];

        let schema = generate_example_entity(&inputs);
        assert!(schema.contains("type ExampleEntity @entity(immutable: true)"));
        assert!(schema.contains("id: Bytes!"));
        assert!(schema.contains("count: BigInt!"));
        // First 2 params with type comments
        assert!(schema.contains("from: Bytes! # address"));
        assert!(schema.contains("to: Bytes! # address"));
        // Third param should NOT be included (only first 2)
        assert!(!schema.contains("value: BigInt!"));
    }

    #[test]
    fn test_generate_schema_placeholder_mode() {
        let abi = json!([
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [
                    {"name": "from", "type": "address", "indexed": true},
                    {"name": "to", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ]
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            index_events: false, // placeholder mode
            ..Default::default()
        };

        let schema = generate_schema(&options);

        assert!(schema.contains("type ExampleEntity @entity(immutable: true)"));
        // First 2 event params with type comments
        assert!(schema.contains("from: Bytes! # address"));
        assert!(schema.contains("to: Bytes! # address"));
    }

    #[test]
    fn test_generate_schema_with_events() {
        let abi = json!([
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [
                    {"name": "from", "type": "address", "indexed": true},
                    {"name": "to", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ]
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            index_events: true,
            ..Default::default()
        };

        let schema = generate_schema(&options);

        assert!(schema.contains("type Transfer @entity(immutable: true)"));
        assert!(schema.contains("from: Bytes!"));
        assert!(schema.contains("to: Bytes!"));
        assert!(schema.contains("value: BigInt!"));
    }

    /// Resolve a Solidity type string and map it, exercising the same path the
    /// generators use.
    fn graphql_type_of(solidity_type: &str) -> &'static str {
        graphql_type(&solidity_type.parse::<DynSolType>().unwrap())
    }

    #[test]
    fn test_graphql_type() {
        assert_eq!(graphql_type_of("address"), "Bytes");
        assert_eq!(graphql_type_of("bool"), "Boolean");
        assert_eq!(graphql_type_of("string"), "String");
        assert_eq!(graphql_type_of("bytes32"), "Bytes");
        assert_eq!(graphql_type_of("bytes"), "Bytes");
        assert_eq!(graphql_type_of("address[]"), "[Bytes!]");
        assert_eq!(graphql_type_of("uint256[]"), "[BigInt!]");
    }

    #[test]
    fn test_integer_width_mapping() {
        // Widths that fit an i32 -> Int.
        assert_eq!(graphql_type_of("int8"), "Int");
        assert_eq!(graphql_type_of("int32"), "Int");
        assert_eq!(graphql_type_of("uint8"), "Int");
        assert_eq!(graphql_type_of("uint24"), "Int");
        // Wider widths that still fit an i64 -> Int8.
        assert_eq!(graphql_type_of("uint32"), "Int8"); // first unsigned to overflow i32
        assert_eq!(graphql_type_of("int40"), "Int8");
        assert_eq!(graphql_type_of("int64"), "Int8");
        assert_eq!(graphql_type_of("uint56"), "Int8"); // largest unsigned that fits i64
        // Too wide for i64 -> BigInt.
        assert_eq!(graphql_type_of("int72"), "BigInt");
        assert_eq!(graphql_type_of("uint64"), "BigInt"); // 2^64-1 overflows i64
        assert_eq!(graphql_type_of("uint256"), "BigInt");
        // Bare `uint`/`int` canonicalize to the 256-bit form.
        assert_eq!(graphql_type_of("int"), "BigInt");
        assert_eq!(graphql_type_of("uint"), "BigInt");
        // Arrays: the i32 band keeps Int; the Int8 band collapses to BigInt (no
        // i64 array accessor), and wider stays BigInt.
        assert_eq!(graphql_type_of("int8[]"), "[Int!]");
        assert_eq!(graphql_type_of("int40[]"), "[BigInt!]");
        assert_eq!(graphql_type_of("uint64[]"), "[BigInt!]");
    }

    #[test]
    fn test_generate_schema_covers_all_type_mappings() {
        // One event exercising every schema-side mapping row, including the
        // fixed-size arrays the old `ends_with("[]")` matcher could not see and
        // that scaffolded to non-building scalar fields.
        let param = |name: &str, ty: &str| json!({"name": name, "type": ty, "indexed": false});
        let abi = json!([
            {
                "type": "event",
                "name": "AllTypes",
                "inputs": [
                    param("addr", "address"),
                    param("flag", "bool"),
                    param("text", "string"),
                    param("tiny", "uint8"),      // i32 band
                    param("mid", "uint32"),      // i64 band
                    param("big", "uint256"),     // BigInt
                    param("addrsDyn", "address[]"),
                    param("addrsFixed", "address[3]"),
                    param("numsFixed", "uint256[3]"),
                    param("tinyFixed", "uint8[3]"),
                    param("flagsFixed", "bool[2]"),
                    param("textFixed", "string[2]"),
                    param("hashesFixed", "bytes32[4]"),
                    param("tinyDyn", "int8[]"),  // i32 band keeps Int
                    param("midDyn", "int40[]"),  // i64 band collapses to BigInt
                ]
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            index_events: true,
            ..Default::default()
        };
        let schema = generate_schema(&options);

        for (field, ty) in [
            ("addr", "Bytes!"),
            ("flag", "Boolean!"),
            ("text", "String!"),
            ("tiny", "Int!"),
            ("mid", "Int8!"),
            ("big", "BigInt!"),
            ("addrsDyn", "[Bytes!]!"),
            ("addrsFixed", "[Bytes!]!"),
            ("numsFixed", "[BigInt!]!"),
            ("tinyFixed", "[Int!]!"),
            ("flagsFixed", "[Boolean!]!"),
            ("textFixed", "[String!]!"),
            ("hashesFixed", "[Bytes!]!"),
            ("tinyDyn", "[Int!]!"),
            ("midDyn", "[BigInt!]!"),
        ] {
            assert!(
                schema.contains(&format!("{field}: {ty}")),
                "expected `{field}: {ty}` in:\n{schema}"
            );
        }
    }

    #[test]
    fn test_generate_schema_unrolls_tuple() {
        let abi = json!([
            {
                "type": "event",
                "name": "Deposit",
                "inputs": [
                    {"name": "data", "type": "tuple", "components": [
                        {"name": "account", "type": "address"},
                        {"name": "amount", "type": "uint256"}
                    ]}
                ]
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            index_events: true,
            ..Default::default()
        };

        let schema = generate_schema(&options);
        // Tuple components become one field each, joined with `_`.
        assert!(schema.contains("data_account: Bytes!"), "{}", schema);
        assert!(schema.contains("data_amount: BigInt!"), "{}", schema);
        // The tuple itself is not emitted as a single field.
        assert!(!schema.contains("data: "), "{}", schema);
    }
}

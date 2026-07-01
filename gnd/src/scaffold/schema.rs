//! Schema (schema.graphql) generation for scaffold.

use super::ScaffoldOptions;
use super::manifest::{EventInput, extract_events_from_abi};
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
fn generate_example_entity(inputs: &[EventInput]) -> String {
    let mut fields = String::new();
    fields.push_str("  # Use Bytes when possible for better performance\n");
    fields.push_str("  id: Bytes!\n");
    fields.push_str("  count: BigInt!\n");

    // Include first 2 event params with type comments
    for input in inputs.iter().take(2) {
        let field_name = sanitize_field_name(&input.name);
        let graphql_type = solidity_to_graphql(&input.solidity_type);
        fields.push_str(&format!(
            "  {}: {}! # {}\n",
            field_name, graphql_type, input.solidity_type
        ));
    }

    format!(
        "# Declare entity types as immutable when possible for better performance\n\
         type ExampleEntity @entity(immutable: true) {{\n{}}}\n",
        fields
    )
}

/// Generate an entity type for an event.
pub fn generate_event_entity(entity_name: &str, inputs: &[EventInput]) -> String {
    let mut fields = String::new();

    // ID field
    fields.push_str("  id: Bytes!\n");

    // Fields from event inputs
    for input in inputs {
        let field_name = sanitize_field_name(&input.name);
        let graphql_type = solidity_to_graphql(&input.solidity_type);
        fields.push_str(&format!("  {}: {}!\n", field_name, graphql_type));
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

/// Convert Solidity type to GraphQL type.
fn solidity_to_graphql(solidity_type: &str) -> &'static str {
    // Handle arrays
    if solidity_type.ends_with("[]") {
        let inner = &solidity_type.strip_suffix("[]").unwrap();
        return match solidity_to_graphql(inner) {
            "Bytes" => "[Bytes!]",
            "BigInt" => "[BigInt!]",
            "Int" => "[Int!]",
            "String" => "[String!]",
            "Boolean" => "[Boolean!]",
            _ => "[Bytes!]",
        };
    }

    match solidity_type {
        // Address types
        "address" => "Bytes",

        // Boolean
        "bool" => "Boolean",

        // String
        "string" => "String",

        // Bytes types
        "bytes" => "Bytes",
        "bytes1" | "bytes2" | "bytes3" | "bytes4" | "bytes5" | "bytes6" | "bytes7" | "bytes8"
        | "bytes9" | "bytes10" | "bytes11" | "bytes12" | "bytes13" | "bytes14" | "bytes15"
        | "bytes16" | "bytes17" | "bytes18" | "bytes19" | "bytes20" | "bytes21" | "bytes22"
        | "bytes23" | "bytes24" | "bytes25" | "bytes26" | "bytes27" | "bytes28" | "bytes29"
        | "bytes30" | "bytes31" | "bytes32" => "Bytes",

        // Integers: small widths fit in an i32 (GraphQL Int), the rest need BigInt.
        t if t.starts_with("uint") || t.starts_with("int") => int_to_graphql(t),

        // Default to Bytes for unknown types
        _ => "Bytes",
    }
}

/// Map a Solidity integer type to GraphQL `Int` when it fits in an i32, else
/// `BigInt`. An i32 holds signed ints up to 32 bits and unsigned ints up to 24
/// bits, matching graph-cli's AssemblyScript type conversion.
fn int_to_graphql(solidity_type: &str) -> &'static str {
    let (signed, width) = match solidity_type.strip_prefix("uint") {
        Some(rest) => (false, rest),
        None => match solidity_type.strip_prefix("int") {
            Some(rest) => (true, rest),
            None => return "BigInt",
        },
    };

    // A bare `int` / `uint` is 256 bits.
    let bits: u32 = if width.is_empty() {
        256
    } else {
        width.parse().unwrap_or(256)
    };

    let fits_i32 = if signed { bits <= 32 } else { bits <= 24 };
    if fits_i32 { "Int" } else { "BigInt" }
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
        let inputs = vec![
            EventInput {
                name: "from".to_string(),
                solidity_type: "address".to_string(),
                indexed: true,
            },
            EventInput {
                name: "to".to_string(),
                solidity_type: "address".to_string(),
                indexed: true,
            },
            EventInput {
                name: "value".to_string(),
                solidity_type: "uint256".to_string(),
                indexed: false,
            },
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

    #[test]
    fn test_solidity_to_graphql() {
        assert_eq!(solidity_to_graphql("address"), "Bytes");
        assert_eq!(solidity_to_graphql("bool"), "Boolean");
        assert_eq!(solidity_to_graphql("string"), "String");
        assert_eq!(solidity_to_graphql("bytes32"), "Bytes");
        assert_eq!(solidity_to_graphql("bytes"), "Bytes");
        assert_eq!(solidity_to_graphql("address[]"), "[Bytes!]");
        assert_eq!(solidity_to_graphql("uint256[]"), "[BigInt!]");
    }

    #[test]
    fn test_integer_width_mapping() {
        // Small widths that fit in an i32 map to Int.
        assert_eq!(solidity_to_graphql("int8"), "Int");
        assert_eq!(solidity_to_graphql("int32"), "Int");
        assert_eq!(solidity_to_graphql("uint8"), "Int");
        assert_eq!(solidity_to_graphql("uint24"), "Int");
        // Wider integers need BigInt (uint32 does not fit an i32).
        assert_eq!(solidity_to_graphql("uint32"), "BigInt");
        assert_eq!(solidity_to_graphql("int40"), "BigInt");
        assert_eq!(solidity_to_graphql("uint256"), "BigInt");
        assert_eq!(solidity_to_graphql("int"), "BigInt");
        assert_eq!(solidity_to_graphql("uint"), "BigInt");
        // Arrays follow the element type.
        assert_eq!(solidity_to_graphql("int8[]"), "[Int!]");
        assert_eq!(solidity_to_graphql("uint64[]"), "[BigInt!]");
    }
}

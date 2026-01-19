//! Schema (schema.graphql) generation for scaffold.

use super::manifest::{extract_events_from_abi, EventInput};
use super::ScaffoldOptions;

/// Generate the schema.graphql content.
pub fn generate_schema(options: &ScaffoldOptions) -> String {
    let events = extract_events_from_abi(options);

    if events.is_empty() || !options.index_events {
        // Generate example entity
        return generate_example_entity();
    }

    // Generate entity for each event
    let mut schema = String::new();

    for event in events {
        let entity = generate_event_entity(&event.name, &event.inputs);
        schema.push_str(&entity);
        schema.push_str("\n\n");
    }

    schema.trim_end().to_string()
}

/// Generate an example entity when no events are found.
fn generate_example_entity() -> String {
    r#"type ExampleEntity @entity {
  id: Bytes!
  count: BigInt!
  blockNumber: BigInt!
  blockTimestamp: BigInt!
  transactionHash: Bytes!
}
"#
    .to_string()
}

/// Generate an entity type for an event.
fn generate_event_entity(event_name: &str, inputs: &[EventInput]) -> String {
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

    format!("type {} @entity {{\n{}\n}}", event_name, fields)
}

/// Convert Solidity type to GraphQL type.
fn solidity_to_graphql(solidity_type: &str) -> &'static str {
    // Handle arrays
    if solidity_type.ends_with("[]") {
        let inner = &solidity_type.strip_suffix("[]").unwrap();
        return match solidity_to_graphql(inner) {
            "Bytes" => "[Bytes!]",
            "BigInt" => "[BigInt!]",
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

        // Integer types - all map to BigInt for simplicity
        t if t.starts_with("uint") || t.starts_with("int") => "BigInt",

        // Default to Bytes for unknown types
        _ => "Bytes",
    }
}

/// Sanitize a field name to be a valid GraphQL identifier.
fn sanitize_field_name(name: &str) -> String {
    if name.is_empty() {
        return "value".to_string();
    }

    // GraphQL field names must start with a letter or underscore
    let mut result = String::new();

    for (i, c) in name.chars().enumerate() {
        if i == 0 && c.is_ascii_digit() {
            result.push('_');
        }
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push('_');
        }
    }

    // Convert to camelCase if starts with uppercase
    if result
        .chars()
        .next()
        .map(|c| c.is_uppercase())
        .unwrap_or(false)
    {
        let mut chars = result.chars();
        if let Some(first) = chars.next() {
            result = first.to_lowercase().collect::<String>() + chars.as_str();
        }
    }

    // Avoid reserved words
    match result.as_str() {
        "id" => "eventId".to_string(),
        "type" => "eventType".to_string(),
        _ => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_generate_example_entity() {
        let schema = generate_example_entity();
        assert!(schema.contains("type ExampleEntity @entity"));
        assert!(schema.contains("id: Bytes!"));
        assert!(schema.contains("blockNumber: BigInt!"));
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

        assert!(schema.contains("type Transfer @entity"));
        assert!(schema.contains("from: Bytes!"));
        assert!(schema.contains("to: Bytes!"));
        assert!(schema.contains("value: BigInt!"));
    }

    #[test]
    fn test_solidity_to_graphql() {
        assert_eq!(solidity_to_graphql("address"), "Bytes");
        assert_eq!(solidity_to_graphql("bool"), "Boolean");
        assert_eq!(solidity_to_graphql("string"), "String");
        assert_eq!(solidity_to_graphql("uint256"), "BigInt");
        assert_eq!(solidity_to_graphql("int8"), "BigInt");
        assert_eq!(solidity_to_graphql("bytes32"), "Bytes");
        assert_eq!(solidity_to_graphql("bytes"), "Bytes");
        assert_eq!(solidity_to_graphql("address[]"), "[Bytes!]");
        assert_eq!(solidity_to_graphql("uint256[]"), "[BigInt!]");
    }

    #[test]
    fn test_sanitize_field_name() {
        assert_eq!(sanitize_field_name("from"), "from");
        assert_eq!(sanitize_field_name("tokenId"), "tokenId");
        assert_eq!(sanitize_field_name("TokenId"), "tokenId");
        assert_eq!(sanitize_field_name("123value"), "_123value");
        assert_eq!(sanitize_field_name("id"), "eventId");
        assert_eq!(sanitize_field_name(""), "value");
    }
}

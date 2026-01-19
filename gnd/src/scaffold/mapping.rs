//! Mapping (AssemblyScript) generation for scaffold.

use super::manifest::extract_events_from_abi;
use super::ScaffoldOptions;

/// Generate the mapping.ts content.
pub fn generate_mapping(options: &ScaffoldOptions) -> String {
    let events = extract_events_from_abi(options);
    let contract_name = &options.contract_name;

    if events.is_empty() || !options.index_events {
        return generate_placeholder_mapping(contract_name);
    }

    generate_event_handlers(contract_name, &events, options)
}

/// Generate a placeholder mapping when no events are found.
fn generate_placeholder_mapping(contract_name: &str) -> String {
    format!(
        r#"import {{ BigInt, Bytes }} from "@graphprotocol/graph-ts"
import {{ ExampleEvent as ExampleEventEvent }} from "../generated/{contract_name}/{contract_name}"
import {{ ExampleEntity }} from "../generated/schema"

export function handle{contract_name}ExampleEvent(event: ExampleEventEvent): void {{
  // Entities can be loaded from the store using a string ID; this ID
  // needs to be unique across all entities of the same type
  let entity = ExampleEntity.load(event.transaction.hash.concatI32(event.logIndex.toI32()))

  // Entities only exist after they have been saved to the store;
  // `null` checks allow to create entities on demand
  if (!entity) {{
    entity = new ExampleEntity(event.transaction.hash.concatI32(event.logIndex.toI32()))

    // Entity fields can be set using simple assignments
    entity.count = BigInt.fromI32(0)
  }}

  // BigInt and BigDecimal math are supported
  entity.count = entity.count.plus(BigInt.fromI32(1))

  // Entity fields can be set based on event parameters
  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash

  // Entities can be written to the store with `.save()`
  entity.save()
}}
"#
    )
}

/// Generate event handlers for all events in the ABI.
fn generate_event_handlers(
    contract_name: &str,
    events: &[super::manifest::EventInfo],
    _options: &ScaffoldOptions,
) -> String {
    let mut imports = String::new();
    let mut handlers = String::new();

    // Import graph-ts types
    imports.push_str("import { BigInt, Bytes } from \"@graphprotocol/graph-ts\"\n");

    // Import event types
    let event_imports: Vec<String> = events
        .iter()
        .map(|e| format!("{} as {}Event", e.name, e.name))
        .collect();

    imports.push_str(&format!(
        "import {{ {} }} from \"../generated/{}/{}\"\n",
        event_imports.join(", "),
        contract_name,
        contract_name
    ));

    // Import entity types
    let entity_imports: Vec<String> = events.iter().map(|e| e.name.clone()).collect();

    imports.push_str(&format!(
        "import {{ {} }} from \"../generated/schema\"\n",
        entity_imports.join(", ")
    ));

    // Generate handler for each event
    for event in events {
        handlers.push('\n');
        handlers.push_str(&generate_single_handler(event));
    }

    format!("{}\n{}", imports, handlers)
}

/// Generate a handler function for a single event.
fn generate_single_handler(event: &super::manifest::EventInfo) -> String {
    let event_name = &event.name;

    // Generate field assignments from event parameters
    let mut field_assignments = String::new();
    for input in &event.inputs {
        let field_name = sanitize_param_name(&input.name);
        field_assignments.push_str(&format!(
            "  entity.{} = event.params.{}\n",
            field_name, input.name
        ));
    }

    format!(
        r#"export function handle{event_name}(event: {event_name}Event): void {{
  let entity = new {event_name}(
    event.transaction.hash.concatI32(event.logIndex.toI32())
  )

{field_assignments}  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash

  entity.save()
}}
"#
    )
}

/// Sanitize parameter name for use in AssemblyScript.
fn sanitize_param_name(name: &str) -> String {
    if name.is_empty() {
        return "value".to_string();
    }

    // Convert to camelCase if starts with uppercase
    let mut result = name.to_string();
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
    fn test_generate_placeholder_mapping() {
        let mapping = generate_placeholder_mapping("MyContract");

        assert!(mapping.contains("handleMyContractExampleEvent"));
        assert!(mapping.contains("ExampleEntity"));
        assert!(mapping.contains("@graphprotocol/graph-ts"));
    }

    #[test]
    fn test_generate_mapping_with_events() {
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
            contract_name: "Token".to_string(),
            abi: Some(abi),
            index_events: true,
            ..Default::default()
        };

        let mapping = generate_mapping(&options);

        assert!(mapping.contains("handleTransfer"));
        assert!(mapping.contains("Transfer as TransferEvent"));
        assert!(mapping.contains("event.params.from"));
        assert!(mapping.contains("event.params.to"));
        assert!(mapping.contains("event.params.value"));
    }

    #[test]
    fn test_sanitize_param_name() {
        assert_eq!(sanitize_param_name("from"), "from");
        assert_eq!(sanitize_param_name("TokenId"), "tokenId");
        assert_eq!(sanitize_param_name("id"), "eventId");
        assert_eq!(sanitize_param_name(""), "value");
    }
}

//! Mapping (AssemblyScript) generation for scaffold.

use super::manifest::{extract_events_from_abi, EventInfo};
use super::ScaffoldOptions;

/// Generate the mapping.ts content.
pub fn generate_mapping(options: &ScaffoldOptions) -> String {
    let events = extract_events_from_abi(options);
    let contract_name = &options.contract_name;

    if events.is_empty() {
        return generate_fallback_mapping(contract_name);
    }

    if !options.index_events {
        return generate_placeholder_mapping(contract_name, &events, options);
    }

    generate_event_handlers(contract_name, &events, options)
}

/// Generate a fallback mapping when no events are found in ABI.
fn generate_fallback_mapping(contract_name: &str) -> String {
    format!(
        r#"import {{ BigInt, Bytes }} from "@graphprotocol/graph-ts"
import {{ ExampleEvent as ExampleEventEvent }} from "../generated/{contract_name}/{contract_name}"
import {{ ExampleEntity }} from "../generated/schema"

export function handle{contract_name}ExampleEvent(event: ExampleEventEvent): void {{
  let entity = ExampleEntity.load(
    event.transaction.hash.concat(Bytes.fromByteArray(Bytes.fromBigInt(event.logIndex)))
  )

  if (!entity) {{
    entity = new ExampleEntity(
      event.transaction.hash.concat(Bytes.fromByteArray(Bytes.fromBigInt(event.logIndex)))
    )
    entity.count = BigInt.fromI32(0)
  }}

  entity.count = entity.count + BigInt.fromI32(1)
  entity.save()
}}
"#
    )
}

/// Generate placeholder mapping when events are found but index_events is false.
fn generate_placeholder_mapping(
    contract_name: &str,
    events: &[EventInfo],
    options: &ScaffoldOptions,
) -> String {
    let mut output = String::new();

    // Import graph-ts types
    output.push_str("import {\n  BigInt,\n  Bytes\n} from \"@graphprotocol/graph-ts\"\n");

    // Import contract class and all events
    output.push_str(&format!("import {{\n  {contract_name},\n"));
    for (i, event) in events.iter().enumerate() {
        let suffix = if i < events.len() - 1 { ",\n" } else { "\n" };
        output.push_str(&format!(
            "  {} as {}Event{}",
            event.name, event.name, suffix
        ));
    }
    output.push_str(&format!(
        "}} from \"../generated/{contract_name}/{contract_name}\"\n"
    ));

    // Import entity
    output.push_str("import { ExampleEntity } from \"../generated/schema\"\n");

    // Generate first handler with full example code
    let first_event = &events[0];
    output.push_str(&generate_first_placeholder_handler(
        first_event,
        contract_name,
        options,
    ));

    // Generate empty stub handlers for remaining events
    for event in events.iter().skip(1) {
        output.push_str(&format!(
            "\nexport function handle{}(event: {}Event): void {{}}\n",
            event.name, event.name
        ));
    }

    output
}

/// Generate the first handler with full example code.
fn generate_first_placeholder_handler(
    event: &EventInfo,
    contract_name: &str,
    options: &ScaffoldOptions,
) -> String {
    let event_name = &event.name;

    // Generate field assignments for first 2 event params
    let mut field_assignments = String::new();
    for input in event.inputs.iter().take(2) {
        let field_name = sanitize_param_name(&input.name);
        field_assignments.push_str(&format!(
            "  entity.{} = event.params.{}\n",
            field_name, input.name
        ));
    }

    // Extract callable functions from ABI for comments
    let callable_functions = extract_callable_functions(options);

    format!(
        r#"
export function handle{event_name}(event: {event_name}Event): void {{
  // Entities can be loaded from the store using their id; using 'Bytes' as
  // the id type is more efficient than 'ID' or 'String' and should be used
  // whenever possible.
  const id = event.transaction.hash.concat(
    Bytes.fromByteArray(Bytes.fromBigInt(event.logIndex))
  )
  let entity = ExampleEntity.load(id)

  // Entities only exist after they have been saved to the store;
  // `null` checks allow to create entities on demand
  if (!entity) {{
    entity = new ExampleEntity(id)

    // Entity fields can be set using simple assignments
    entity.count = BigInt.fromI32(0)
  }}

  // BigInt and BigDecimal math are supported
  entity.count = entity.count + BigInt.fromI32(1)

  // Entity fields can be set based on event parameters
{field_assignments}
  entity.save()

  // It is also possible to access smart contracts from mappings. For
  // example, the contract that has emitted the event can be connected to
  // with:
  //
  // let contract = {contract_name}.bind(event.address)
  //
  // The following functions can then be called on this contract to access
  // state variables and other data:
  //{callable_functions}
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

/// Extract callable functions from ABI for documentation comments.
fn extract_callable_functions(options: &ScaffoldOptions) -> String {
    let Some(abi) = &options.abi else {
        return "\n  // - None found".to_string();
    };

    let Some(items) = abi.as_array() else {
        return "\n  // - None found".to_string();
    };

    let mut functions = Vec::new();

    for item in items {
        let item_type = item.get("type").and_then(|t| t.as_str());

        // Include functions that are view/pure (callable without state change)
        if item_type == Some("function") {
            let state = item
                .get("stateMutability")
                .and_then(|s| s.as_str())
                .unwrap_or("");

            // Only include view and pure functions (callable from mappings)
            if state == "view" || state == "pure" {
                if let Some(name) = item.get("name").and_then(|n| n.as_str()) {
                    functions.push(format!("\n  // - contract.{}(...)", name));
                }
            }
        }
    }

    if functions.is_empty() {
        "\n  //\n  // - None".to_string()
    } else {
        functions.join("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_generate_fallback_mapping() {
        let mapping = generate_fallback_mapping("MyContract");

        assert!(mapping.contains("handleMyContractExampleEvent"));
        assert!(mapping.contains("ExampleEntity"));
        assert!(mapping.contains("@graphprotocol/graph-ts"));
        // Check for the correct ID pattern
        assert!(mapping.contains("Bytes.fromByteArray(Bytes.fromBigInt(event.logIndex))"));
    }

    #[test]
    fn test_generate_placeholder_mapping_with_events() {
        let abi = json!([
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [
                    {"name": "from", "type": "address", "indexed": true},
                    {"name": "to", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ]
            },
            {
                "type": "event",
                "name": "Approval",
                "inputs": [
                    {"name": "owner", "type": "address", "indexed": true},
                    {"name": "spender", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ]
            },
            {
                "type": "function",
                "name": "balanceOf",
                "stateMutability": "view",
                "inputs": [{"name": "owner", "type": "address"}]
            }
        ]);

        let options = ScaffoldOptions {
            contract_name: "Token".to_string(),
            abi: Some(abi),
            index_events: false, // placeholder mode
            ..Default::default()
        };

        let mapping = generate_mapping(&options);

        // Check imports
        assert!(mapping.contains("Token,"));
        assert!(mapping.contains("Transfer as TransferEvent"));
        assert!(mapping.contains("Approval as ApprovalEvent"));
        assert!(mapping.contains("ExampleEntity"));

        // First handler has full implementation
        assert!(mapping.contains("export function handleTransfer(event: TransferEvent): void {"));
        assert!(mapping.contains("entity.from = event.params.from"));
        assert!(mapping.contains("entity.to = event.params.to"));
        // Check for correct ID pattern
        assert!(mapping.contains("Bytes.fromByteArray(Bytes.fromBigInt(event.logIndex))"));
        // Check for callable functions comment
        assert!(mapping.contains("contract.balanceOf(...)"));

        // Second handler is empty stub
        assert!(mapping.contains("export function handleApproval(event: ApprovalEvent): void {}"));
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

    #[test]
    fn test_extract_callable_functions() {
        let abi = json!([
            {
                "type": "function",
                "name": "balanceOf",
                "stateMutability": "view"
            },
            {
                "type": "function",
                "name": "totalSupply",
                "stateMutability": "pure"
            },
            {
                "type": "function",
                "name": "transfer",
                "stateMutability": "nonpayable"
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            ..Default::default()
        };

        let functions = extract_callable_functions(&options);
        assert!(functions.contains("balanceOf"));
        assert!(functions.contains("totalSupply"));
        // transfer is nonpayable, should not be included
        assert!(!functions.contains("transfer"));
    }
}

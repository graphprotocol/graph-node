//! Manifest (subgraph.yaml) generation for scaffold.

use std::collections::HashMap;

use graph::abi::{Event, EventParam};

use super::ScaffoldOptions;
use crate::shared::handle_reserved_word;

/// Generate the subgraph.yaml manifest content.
pub fn generate_manifest(options: &ScaffoldOptions) -> String {
    let contract_name = &options.contract_name;
    let network = &options.network;
    let mapping_file = format!("./src/{}.ts", super::to_kebab_case(contract_name));
    let abi_file = format!("./abis/{}.json", contract_name);

    let mut source = String::new();

    if let Some(address) = &options.address {
        source.push_str(&format!("      address: \"{}\"\n", address));
    }

    source.push_str(&format!("      abi: {contract_name}\n"));

    if let Some(start_block) = options.start_block {
        source.push_str(&format!("      startBlock: {}\n", start_block));
    }

    // Resolve events once (disambiguating overloaded names) so the handlers and
    // entities lists stay consistent.
    let events = disambiguate_events(extract_events_from_abi(options));
    let event_handlers = get_event_handlers(contract_name, &events);

    format!(
        r#"specVersion: {spec_version}
indexerHints:
  prune: auto
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum
    name: {contract_name}
    network: {network}
    source:
{source}    mapping:
      kind: ethereum/events
      apiVersion: {api_version}
      language: wasm/assemblyscript
      entities:{entities}
      abis:
        - name: {contract_name}
          file: {abi_file}
      eventHandlers:{event_handlers}
      file: {mapping_file}
"#,
        spec_version = super::SPEC_VERSION,
        api_version = super::MAPPING_API_VERSION,
        entities = get_entities(&events),
    )
}

/// Get event handlers for the manifest.
fn get_event_handlers(contract_name: &str, events: &[ResolvedEvent]) -> String {
    if events.is_empty() {
        // Default placeholder handler
        return format!(
            r#"
        - event: ExampleEvent(indexed address,uint256)
          handler: handle{contract_name}ExampleEvent"#
        );
    }

    let mut handlers = String::new();
    for event in events {
        handlers.push_str(&format!(
            "\n        - event: {}\n          handler: handle{}",
            event.event.signature, event.alias
        ));
    }

    handlers
}

/// Get entities list for the manifest.
fn get_entities(events: &[ResolvedEvent]) -> String {
    if events.is_empty() {
        return "\n        - ExampleEntity".to_string();
    }

    let mut entities = String::new();
    for event in events {
        entities.push_str(&format!("\n        - {}", event.entity_name));
    }
    entities
}

/// Event info extracted from ABI.
#[derive(Debug, Clone)]
pub struct EventInfo {
    pub name: String,
    pub signature: String,
    pub inputs: Vec<EventParam>,
}

/// An event resolved to the concrete names the generators render.
///
/// Decided once (here / by collision resolution) so the schema, mapping and
/// manifest generators never derive names independently:
/// - `alias` names the handler function and the ABI event-type import; it is
///   disambiguated for events overloaded within a single ABI.
/// - `entity_name` names the GraphQL entity type, the `new` expression and the
///   schema import; it gains a contract prefix when it collides with an entity
///   that already exists in the subgraph.
/// - `declare_in_schema` is false when the event reuses an entity that already
///   exists (a merge), so the type must not be redeclared.
#[derive(Debug, Clone)]
pub struct ResolvedEvent {
    pub event: EventInfo,
    pub alias: String,
    pub entity_name: String,
    pub declare_in_schema: bool,
}

/// Resolve events for a fresh scaffold, disambiguating names that are overloaded
/// within one ABI by suffixing repeats (`Transfer`, `Transfer1`, ...). There are
/// no existing entities to collide with, so each entity is declared as-is.
pub fn disambiguate_events(events: Vec<EventInfo>) -> Vec<ResolvedEvent> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    events
        .into_iter()
        .map(|event| {
            let count = seen.entry(event.name.clone()).or_insert(0);
            let alias = if *count == 0 {
                event.name.clone()
            } else {
                format!("{}{}", event.name, count)
            };
            *count += 1;
            let entity_name = alias.clone();
            ResolvedEvent {
                event,
                alias,
                entity_name,
                declare_in_schema: true,
            }
        })
        .collect()
}

/// The `event.params.<name>` accessor for each input, mirroring the names the
/// ABI codegen gives the generated getters: reserved words are escaped and
/// unnamed params become `param<index>`, with a counter for any collisions.
/// Keeps the mapping's right-hand side in sync with the generated bindings.
pub fn event_param_accessors(inputs: &[EventParam]) -> Vec<String> {
    let mut seen: HashMap<String, u32> = HashMap::new();
    inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let base = if input.name.is_empty() {
                format!("param{index}")
            } else {
                handle_reserved_word(&input.name)
            };
            let count = seen.entry(base.clone()).or_insert(0);
            let name = if *count == 0 {
                base.clone()
            } else {
                format!("{base}{count}")
            };
            *count += 1;
            name
        })
        .collect()
}

/// Extract events from the ABI, in file order.
///
/// The ABI is preprocessed (artifact formats unwrapped, `anonymous` and
/// `param{index}` defaults added) so alloy can parse each event item into a
/// typed `Event`. A malformed event is skipped with a warning.
pub fn extract_events_from_abi(options: &ScaffoldOptions) -> Vec<EventInfo> {
    let Some(abi) = &options.abi else {
        return vec![];
    };

    let normalized = match crate::abi::preprocess_abi_value(abi.clone()) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("warning: could not parse ABI for scaffolding: {err}");
            return vec![];
        }
    };

    let Some(items) = normalized.as_array() else {
        return vec![];
    };

    let mut events = Vec::new();

    for item in items {
        if item.get("type").and_then(|t| t.as_str()) != Some("event") {
            continue;
        }

        match serde_json::from_value::<Event>(item.clone()) {
            Ok(event) => {
                let signature = crate::abi::event_signature_with_indexed(&event);
                events.push(EventInfo {
                    name: event.name,
                    signature,
                    inputs: event.inputs,
                });
            }
            Err(err) => {
                let name = item
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("<unnamed>");
                eprintln!("warning: skipping malformed event `{name}` in ABI: {err}");
            }
        }
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_generate_manifest_minimal() {
        let options = ScaffoldOptions::default();
        let manifest = generate_manifest(&options);

        assert!(manifest.contains("specVersion: 1.3.0"));
        assert!(manifest.contains("kind: ethereum"));
        assert!(manifest.contains("network: mainnet"));
        assert!(manifest.contains("kind: ethereum/events"));
    }

    #[test]
    fn test_generate_manifest_with_address() {
        let options = ScaffoldOptions {
            address: Some("0x1234567890123456789012345678901234567890".to_string()),
            network: "goerli".to_string(),
            contract_name: "MyToken".to_string(),
            start_block: Some(12345678),
            ..Default::default()
        };

        let manifest = generate_manifest(&options);

        assert!(manifest.contains("network: goerli"));
        assert!(manifest.contains("name: MyToken"));
        assert!(manifest.contains("0x1234567890123456789012345678901234567890"));
        assert!(manifest.contains("startBlock: 12345678"));

        // Verify address comes before abi
        let address_pos = manifest.find("address:").unwrap();
        let abi_pos = manifest.find("abi: MyToken").unwrap();
        assert!(address_pos < abi_pos, "address should come before abi");
    }

    #[test]
    fn test_get_entities_uses_event_names() {
        let abi = json!([
            {
                "type": "event",
                "name": "Transfer",
                "inputs": []
            },
            {
                "type": "event",
                "name": "Approval",
                "inputs": []
            }
        ]);

        // Even with index_events=false, entities should be event names
        let options = ScaffoldOptions {
            abi: Some(abi),
            index_events: false,
            ..Default::default()
        };

        let manifest = generate_manifest(&options);
        assert!(manifest.contains("- Transfer"));
        assert!(manifest.contains("- Approval"));
        // Should NOT contain ExampleEntity
        assert!(!manifest.contains("ExampleEntity"));
    }

    #[test]
    fn test_event_handlers_no_blank_lines() {
        let abi = json!([
            {
                "type": "event",
                "name": "Transfer",
                "inputs": []
            },
            {
                "type": "event",
                "name": "Approval",
                "inputs": []
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            ..Default::default()
        };

        let manifest = generate_manifest(&options);

        // Check there are no double newlines in the eventHandlers section
        assert!(!manifest.contains("handler: handleTransfer\n\n"));
    }

    #[test]
    fn test_extract_events_from_abi() {
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
                "type": "function",
                "name": "transfer"
            }
        ]);

        let options = ScaffoldOptions {
            abi: Some(abi),
            ..Default::default()
        };

        let events = extract_events_from_abi(&options);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "Transfer");
        assert_eq!(
            events[0].signature,
            "Transfer(indexed address,indexed address,uint256)"
        );
        assert_eq!(events[0].inputs.len(), 3);
    }

    #[test]
    fn test_extract_events_accepts_artifact_format() {
        // A Foundry/Hardhat artifact wrapper is unwrapped before parsing.
        let abi = json!({
            "abi": [
                {
                    "type": "event",
                    "name": "Transfer",
                    "inputs": [{"name": "from", "type": "address", "indexed": true}]
                }
            ]
        });
        let options = ScaffoldOptions {
            abi: Some(abi),
            ..Default::default()
        };
        let events = extract_events_from_abi(&options);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "Transfer");
    }

    #[test]
    fn test_extract_events_names_unnamed_params() {
        // Unnamed event params are named `param{index}`, matching graph-cli, so
        // the accessor and the generated getter agree. solc writes `"name": ""`
        // for these; a hand-written ABI may omit the key. Both are unnamed.
        let abi = json!([
            {
                "type": "event",
                "name": "Act",
                "anonymous": false,
                "inputs": [
                    {"name": "", "type": "uint256", "indexed": false, "internalType": "uint256"},
                    {"name": "", "type": "address", "indexed": false, "internalType": "address"},
                    {"type": "bool"}
                ]
            }
        ]);
        let options = ScaffoldOptions {
            abi: Some(abi),
            ..Default::default()
        };
        let events = extract_events_from_abi(&options);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].inputs[0].name, "param0");
        assert_eq!(events[0].inputs[1].name, "param1");
        assert_eq!(events[0].inputs[2].name, "param2");
    }

    #[test]
    fn test_generate_schema_unnamed_params_do_not_collide() {
        // Two unnamed params must not both sanitize to the field `value`: a
        // duplicate field is not valid GraphQL and fails the build.
        let abi = json!([
            {
                "type": "event",
                "name": "Act",
                "anonymous": false,
                "inputs": [
                    {"name": "", "type": "address", "indexed": false},
                    {"name": "", "type": "uint8", "indexed": false}
                ]
            }
        ]);
        let options = ScaffoldOptions {
            abi: Some(abi),
            index_events: true,
            ..Default::default()
        };
        let schema = super::super::generate_schema(&options);
        assert!(schema.contains("param0: Bytes!"), "{}", schema);
        assert!(schema.contains("param1: Int!"), "{}", schema);
        assert!(!schema.contains("value:"), "{}", schema);
    }

    #[test]
    fn test_extract_events_expands_tuple_signature() {
        // A tuple param expands to its components so the topic0 hash matches the
        // on-chain event; array suffixes are preserved.
        let abi = json!([
            {
                "type": "event",
                "name": "Filled",
                "inputs": [
                    {"name": "order", "type": "tuple", "components": [
                        {"name": "maker", "type": "address"},
                        {"name": "price", "type": "uint256"}
                    ]},
                    {"name": "fills", "type": "tuple[]", "components": [
                        {"name": "amount", "type": "uint256"}
                    ]},
                    {"name": "fee", "type": "uint256"}
                ]
            }
        ]);
        let options = ScaffoldOptions {
            abi: Some(abi),
            ..Default::default()
        };
        let events = extract_events_from_abi(&options);
        assert_eq!(
            events[0].signature,
            "Filled((address,uint256),(uint256)[],uint256)"
        );
    }

    #[test]
    fn test_disambiguate_events() {
        let ev = |name: &str| EventInfo {
            name: name.to_string(),
            signature: format!("{}()", name),
            inputs: vec![],
        };
        let resolved = disambiguate_events(vec![ev("Transfer"), ev("Transfer"), ev("Approval")]);
        // Repeated names are suffixed; the first occurrence is unchanged.
        assert_eq!(resolved[0].alias, "Transfer");
        assert_eq!(resolved[0].entity_name, "Transfer");
        assert_eq!(resolved[1].alias, "Transfer1");
        assert_eq!(resolved[1].entity_name, "Transfer1");
        assert_eq!(resolved[2].alias, "Approval");
        assert!(resolved.iter().all(|r| r.declare_in_schema));
    }
}

use crate::abi;
use crate::abi::DynSolValueExt;
use crate::abi::FunctionExt;
use crate::blockchain::block_stream::EntitySourceOperation;
use crate::data::subgraph::SPEC_VERSION_1_4_0;
use crate::prelude::{BlockPtr, Value};
use crate::{
    components::link_resolver::{LinkResolver, LinkResolverContext},
    data::subgraph::DeploymentHash,
    data::value::Word,
    prelude::Link,
};
use alloy::primitives::{Address, U256};
use alloy::rpc::types::Log;
use anyhow::{anyhow, Context, Error};
use graph_derive::CheapClone;
use lazy_static::lazy_static;
use num_bigint::Sign;
use regex::Regex;
use serde::de;
use serde::Deserialize;
use serde_json;
use slog::Logger;
use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};

/// Normalizes ABI JSON to handle compatibility issues between the legacy `ethabi`/`rust-web3`
/// parser and the stricter `alloy` parser.
///
/// Some deployed subgraph ABIs contain non-standard constructs that `ethabi` accepted but
/// `alloy` rejects. This function patches these issues to maintain backward compatibility:
///
/// 1. **`stateMutability: "undefined"`** - Some ABIs use "undefined" which is not a valid
///    Solidity state mutability. We replace it with "nonpayable".
///
/// 2. **Duplicate constructors** - Some ABIs contain multiple constructor definitions.
///    We keep only the first one.
///
/// 3. **Duplicate fallback functions** - Similar to constructors, some ABIs have multiple
///    fallback definitions. We keep only the first one.
///
/// 4. **`indexed` field in non-event params** - The `indexed` field is only valid for event
///    parameters, but some ABIs include it on function inputs/outputs. We strip it from
///    non-event items.
///
/// These issues were identified by validating ABIs across deployed subgraphs in production
/// before the migration to alloy.
fn normalize_abi_json(json_bytes: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    let mut value: serde_json::Value = serde_json::from_slice(json_bytes)?;

    if let Some(array) = value.as_array_mut() {
        let mut found_constructor = false;
        let mut found_fallback = false;
        let mut indices_to_remove = Vec::new();

        for (index, item) in array.iter_mut().enumerate() {
            if let Some(obj) = item.as_object_mut() {
                if let Some(state_mutability) = obj.get_mut("stateMutability") {
                    if let Some(s) = state_mutability.as_str() {
                        if s == "undefined" {
                            *state_mutability = serde_json::Value::String("nonpayable".to_string());
                        }
                    }
                }

                let item_type = obj.get("type").and_then(|t| t.as_str());

                match item_type {
                    Some("constructor") if found_constructor => indices_to_remove.push(index),
                    Some("constructor") => found_constructor = true,
                    Some("fallback") if found_fallback => indices_to_remove.push(index),
                    Some("fallback") => found_fallback = true,
                    _ => {}
                }

                if item_type != Some("event") {
                    strip_indexed_from_params(obj.get_mut("inputs"));
                    strip_indexed_from_params(obj.get_mut("outputs"));
                }
            }
        }

        for index in indices_to_remove.iter().rev() {
            array.remove(*index);
        }
    }

    Ok(serde_json::to_vec(&value)?)
}

fn strip_indexed_from_params(params: Option<&mut serde_json::Value>) {
    if let Some(serde_json::Value::Array(arr)) = params {
        for param in arr.iter_mut() {
            if let Some(obj) = param.as_object_mut() {
                obj.remove("indexed");
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MappingABI {
    pub name: String,
    pub contract: abi::JsonAbi,
}

impl MappingABI {
    pub fn function(
        &self,
        contract_name: &str,
        name: &str,
        signature: Option<&str>,
    ) -> Result<&abi::Function, Error> {
        let contract = &self.contract;
        let function = match signature {
            // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
            // functions this always picks the same overloaded variant, which is incorrect
            // and may lead to encoding/decoding errors
            None => contract
                .function(name)
                .and_then(|matches| matches.first())
                .with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" called from WASM runtime",
                        contract_name, name
                    )
                })?,

            // Behavior for apiVersion >= 0.0.04: look up function by signature of
            // the form `functionName(uint256,string) returns (bytes32,string)`; this
            // correctly picks the correct variant of an overloaded function
            Some(ref signature) => contract
                .function(name)
                .with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" called from WASM runtime",
                        contract_name, name
                    )
                })?
                .iter()
                .find(|f| signature == &f.signature_compat())
                .with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" with signature `{}` \
                             called from WASM runtime",
                        contract_name, name, signature,
                    )
                })?,
        };
        Ok(function)
    }
}

/// Helper struct for working with ABI JSON to extract struct field information on demand
#[derive(Clone, Debug)]
pub struct AbiJson {
    abi: serde_json::Value,
}

impl AbiJson {
    pub fn new(abi_bytes: &[u8]) -> Result<Self, Error> {
        let abi = serde_json::from_slice(abi_bytes).with_context(|| "Failed to parse ABI JSON")?;
        Ok(Self { abi })
    }

    /// Extract event name from event signature
    /// e.g., "Transfer(address,address,uint256)" -> "Transfer"
    fn extract_event_name(signature: &str) -> &str {
        signature.split('(').next().unwrap_or(signature).trim()
    }

    /// Get struct field information for a specific event parameter
    pub fn get_struct_field_info(
        &self,
        event_signature: &str,
        param_name: &str,
    ) -> Result<Option<StructFieldInfo>, Error> {
        let event_name = Self::extract_event_name(event_signature);

        let Some(abi_array) = self.abi.as_array() else {
            return Ok(None);
        };

        for item in abi_array {
            // Only process events
            if item.get("type").and_then(|t| t.as_str()) == Some("event") {
                if let Some(item_event_name) = item.get("name").and_then(|n| n.as_str()) {
                    if item_event_name == event_name {
                        // Found the event, now look for the parameter
                        if let Some(inputs) = item.get("inputs").and_then(|i| i.as_array()) {
                            for input in inputs {
                                if let Some(input_param_name) =
                                    input.get("name").and_then(|n| n.as_str())
                                {
                                    if input_param_name == param_name {
                                        // Found the parameter, check if it's a struct
                                        if let Some(param_type) =
                                            input.get("type").and_then(|t| t.as_str())
                                        {
                                            if param_type == "tuple" {
                                                if let Some(components) = input.get("components") {
                                                    // Parse the ParamType from the JSON (simplified for now)
                                                    let param_type = abi::DynSolType::Tuple(vec![]);
                                                    return StructFieldInfo::from_components(
                                                        param_name.to_string(),
                                                        param_type,
                                                        components,
                                                    )
                                                    .map(Some);
                                                }
                                            }
                                        }
                                        // Parameter found but not a struct
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                        // Event found but parameter not found
                        return Ok(None);
                    }
                }
            }
        }

        // Event not found
        Ok(None)
    }

    /// Get nested struct field information by resolving a field path
    /// e.g., field_path = ["complexAsset", "base", "addr"]
    /// returns Some(vec![0, 0]) if complexAsset.base is at index 0 and base.addr is at index 0
    pub fn get_nested_struct_field_info(
        &self,
        event_signature: &str,
        field_path: &[&str],
    ) -> Result<Option<Vec<usize>>, Error> {
        if field_path.is_empty() {
            return Ok(None);
        }

        let event_name = Self::extract_event_name(event_signature);
        let param_name = field_path[0];
        let nested_path = &field_path[1..];

        let Some(abi_array) = self.abi.as_array() else {
            return Ok(None);
        };

        for item in abi_array {
            // Only process events
            if item.get("type").and_then(|t| t.as_str()) == Some("event") {
                if let Some(item_event_name) = item.get("name").and_then(|n| n.as_str()) {
                    if item_event_name == event_name {
                        // Found the event, now look for the parameter
                        if let Some(inputs) = item.get("inputs").and_then(|i| i.as_array()) {
                            for input in inputs {
                                if let Some(input_param_name) =
                                    input.get("name").and_then(|n| n.as_str())
                                {
                                    if input_param_name == param_name {
                                        // Found the parameter, check if it's a struct
                                        if let Some(param_type) =
                                            input.get("type").and_then(|t| t.as_str())
                                        {
                                            if param_type == "tuple" {
                                                if let Some(components) = input.get("components") {
                                                    // If no nested path, this is the end
                                                    if nested_path.is_empty() {
                                                        return Ok(Some(vec![]));
                                                    }
                                                    // Recursively resolve the nested path
                                                    return self
                                                        .resolve_field_path(components, nested_path)
                                                        .map(Some);
                                                }
                                            }
                                        }
                                        // Parameter found but not a struct
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                        // Event found but parameter not found
                        return Ok(None);
                    }
                }
            }
        }

        // Event not found
        Ok(None)
    }

    /// Recursively resolve a field path within ABI components
    /// Supports both numeric indices and field names
    /// Returns the index path to access the final field
    fn resolve_field_path(
        &self,
        components: &serde_json::Value,
        field_path: &[&str],
    ) -> Result<Vec<usize>, Error> {
        if field_path.is_empty() {
            return Ok(vec![]);
        }

        let field_accessor = field_path[0];
        let remaining_path = &field_path[1..];

        let Some(components_array) = components.as_array() else {
            return Err(anyhow!("Expected components array"));
        };

        // Check if it's a numeric index
        if let Ok(index) = field_accessor.parse::<usize>() {
            // Validate the index
            if index >= components_array.len() {
                return Err(anyhow!(
                    "Index {} out of bounds for struct with {} fields",
                    index,
                    components_array.len()
                ));
            }

            // If there are more fields to resolve
            if !remaining_path.is_empty() {
                let component = &components_array[index];

                // Check if this component is a tuple that can be further accessed
                if let Some(component_type) = component.get("type").and_then(|t| t.as_str()) {
                    if component_type == "tuple" {
                        if let Some(nested_components) = component.get("components") {
                            // Recursively resolve the remaining path
                            let mut result = vec![index];
                            let nested_result =
                                self.resolve_field_path(nested_components, remaining_path)?;
                            result.extend(nested_result);
                            return Ok(result);
                        } else {
                            return Err(anyhow!(
                                "Field at index {} is a tuple but has no components",
                                index
                            ));
                        }
                    } else {
                        return Err(anyhow!(
                            "Field at index {} is not a struct (type: {}), cannot access nested field '{}'",
                            index,
                            component_type,
                            remaining_path[0]
                        ));
                    }
                }
            }

            // This is the final field
            return Ok(vec![index]);
        }

        // It's a field name - find it in the current level
        for (index, component) in components_array.iter().enumerate() {
            if let Some(component_name) = component.get("name").and_then(|n| n.as_str()) {
                if component_name == field_accessor {
                    // Found the field
                    if remaining_path.is_empty() {
                        // This is the final field, return its index
                        return Ok(vec![index]);
                    } else {
                        // We need to go deeper - check if this component is a tuple
                        if let Some(component_type) = component.get("type").and_then(|t| t.as_str())
                        {
                            if component_type == "tuple" {
                                if let Some(nested_components) = component.get("components") {
                                    // Recursively resolve the remaining path
                                    let mut result = vec![index];
                                    let nested_result =
                                        self.resolve_field_path(nested_components, remaining_path)?;
                                    result.extend(nested_result);
                                    return Ok(result);
                                } else {
                                    return Err(anyhow!(
                                        "Tuple field '{}' has no components",
                                        field_accessor
                                    ));
                                }
                            } else {
                                return Err(anyhow!(
                                    "Field '{}' is not a struct (type: {}), cannot access nested field '{}'",
                                    field_accessor,
                                    component_type,
                                    remaining_path[0]
                                ));
                            }
                        }
                    }
                }
            }
        }

        // Field not found at this level
        let available_fields: Vec<String> = components_array
            .iter()
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
            .map(String::from)
            .collect();

        Err(anyhow!(
            "Field '{}' not found. Available fields: {:?}",
            field_accessor,
            available_fields
        ))
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedMappingABI {
    pub name: String,
    pub file: Link,
}

impl UnresolvedMappingABI {
    pub async fn resolve(
        self,
        deployment_hash: &DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<(MappingABI, AbiJson), anyhow::Error> {
        let contract_bytes = resolver
            .cat(
                &LinkResolverContext::new(deployment_hash, logger),
                &self.file,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to resolve ABI {} from {}",
                    self.name, self.file.link
                )
            })?;
        // Normalize the ABI to handle compatibility issues between ethabi and alloy parsers.
        // See `normalize_abi_json` for details on the specific issues being addressed.
        let normalized_bytes = normalize_abi_json(&contract_bytes)
            .with_context(|| format!("failed to normalize ABI JSON for {}", self.name))?;

        let contract = serde_json::from_slice(&normalized_bytes)
            .with_context(|| format!("failed to load ABI {}", self.name))?;

        // Parse ABI JSON for on-demand struct field extraction
        let abi_json = AbiJson::new(&normalized_bytes)
            .with_context(|| format!("Failed to parse ABI JSON for {}", self.name))?;

        Ok((
            MappingABI {
                name: self.name,
                contract,
            },
            abi_json,
        ))
    }
}

/// Internal representation of declared calls. In the manifest that's
/// written as part of an event handler as
/// ```yaml
/// calls:
///   - myCall1: Contract[address].function(arg1, arg2, ...)
///   - ..
/// ```
///
/// The `address` and `arg` fields can be either `event.address` or
/// `event.params.<name>`. Each entry under `calls` gets turned into a
/// `CallDcl`
#[derive(Clone, CheapClone, Debug, Default, Hash, Eq, PartialEq)]
pub struct CallDecls {
    pub decls: Arc<Vec<CallDecl>>,
    readonly: (),
}

/// A single call declaration, like `myCall1:
/// Contract[address].function(arg1, arg2, ...)`
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct CallDecl {
    /// A user-defined label
    pub label: String,
    /// The call expression
    pub expr: CallExpr,
    readonly: (),
}

impl CallDecl {
    pub fn validate_args(&self) -> Result<(), Error> {
        self.expr.validate_args()
    }

    pub fn address_for_log(
        &self,
        log: &Log,
        params: &[abi::DynSolParam],
    ) -> Result<Address, Error> {
        self.address_for_log_with_abi(log, params)
    }

    pub fn address_for_log_with_abi(
        &self,
        log: &Log,
        params: &[abi::DynSolParam],
    ) -> Result<Address, Error> {
        let address = match &self.expr.address {
            CallArg::HexAddress(address) => *address,
            CallArg::Ethereum(arg) => match arg {
                EthereumArg::Address => log.address(),
                EthereumArg::Param(name) => {
                    let value = &params
                        .iter()
                        .find(|param| &param.name == name.as_str())
                        .ok_or_else(|| {
                            anyhow!(
                                "In declarative call '{}': unknown param {}",
                                self.label,
                                name
                            )
                        })?
                        .value;

                    let address = value.as_address().ok_or_else(|| {
                        anyhow!(
                            "In declarative call '{}': param {} is not an address",
                            self.label,
                            name
                        )
                    })?;

                    Address::from(address.into_array())
                }
                EthereumArg::StructField(param_name, field_accesses) => {
                    let param = params
                        .iter()
                        .find(|param| &param.name == param_name.as_str())
                        .ok_or_else(|| {
                            anyhow!(
                                "In declarative call '{}': unknown param {}",
                                self.label,
                                param_name
                            )
                        })?;

                    Self::extract_nested_struct_field_as_address(
                        &param.value,
                        field_accesses,
                        &self.label,
                    )?
                }
            },
            CallArg::Subgraph(_) => {
                return Err(anyhow!(
                "In declarative call '{}': Subgraph params are not supported for event handlers",
                self.label
            ))
            }
        };
        Ok(address)
    }

    pub fn args_for_log(
        &self,
        log: &Log,
        params: &[abi::DynSolParam],
    ) -> Result<Vec<abi::DynSolValue>, Error> {
        self.args_for_log_with_abi(log, params)
    }

    pub fn args_for_log_with_abi(
        &self,
        log: &Log,
        params: &[abi::DynSolParam],
    ) -> Result<Vec<abi::DynSolValue>, Error> {
        use abi::DynSolValue;
        self.expr
            .args
            .iter()
            .map(|arg| match arg {
                CallArg::HexAddress(address) => Ok(DynSolValue::Address(*address)),
                CallArg::Ethereum(arg) => match arg {
                    EthereumArg::Address => Ok(DynSolValue::Address(log.address())),
                    EthereumArg::Param(name) => {
                        let value = params
                            .iter()
                            .find(|param| &param.name == name.as_str())
                            .ok_or_else(|| anyhow!("In declarative call '{}': unknown param {}", self.label, name))?
                            .value
                            .clone();
                        Ok(value)
                    }
                    EthereumArg::StructField(param_name, field_accesses) => {
                        let param = params
                            .iter()
                            .find(|param| &param.name == param_name.as_str())
                            .ok_or_else(|| anyhow!("In declarative call '{}': unknown param {}", self.label, param_name))?;

                        Self::extract_nested_struct_field(
                            &param.value,
                            field_accesses,
                            &self.label,
                        )
                    }
                },
                CallArg::Subgraph(_) => Err(anyhow!(
                    "In declarative call '{}': Subgraph params are not supported for event handlers",
                    self.label
                )),
            })
            .collect()
    }

    pub fn get_function(
        &self,
        mapping: &dyn FindMappingABI,
    ) -> Result<abi::Function, anyhow::Error> {
        let contract_name = self.expr.abi.to_string();
        let function_name = self.expr.func.as_str();
        let abi = mapping.find_abi(&contract_name)?;

        // TODO: Handle overloaded functions
        // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
        // functions this always picks the same overloaded variant, which is incorrect
        // and may lead to encoding/decoding errors
        abi.contract
            .function(function_name)
            .and_then(|matches| matches.first())
            .cloned()
            .with_context(|| {
                format!(
                    "Unknown function \"{}::{}\" called from WASM runtime",
                    contract_name, function_name
                )
            })
    }

    pub fn address_for_entity_handler(
        &self,
        entity: &EntitySourceOperation,
    ) -> Result<Address, Error> {
        match &self.expr.address {
            // Static hex address - just return it directly
            CallArg::HexAddress(address) => Ok(*address),

            // Ethereum params not allowed here
            CallArg::Ethereum(_) => Err(anyhow!(
                "Ethereum params are not supported for entity handler calls"
            )),

            // Look up address from entity parameter
            CallArg::Subgraph(SubgraphArg::EntityParam(name)) => {
                // Get the value for this parameter
                let value = entity
                    .entity
                    .get(name.as_str())
                    .ok_or_else(|| anyhow!("entity missing required param '{name}'"))?;

                // Make sure it's a bytes value and convert to address
                match value {
                    Value::Bytes(bytes) => {
                        let address = Address::from_slice(bytes.as_slice());
                        Ok(address)
                    }
                    _ => Err(anyhow!("param '{name}' must be an address")),
                }
            }
        }
    }

    /// Processes arguments for an entity handler, converting them to the expected token types.
    /// Returns an error if argument count mismatches or if conversion fails.
    pub fn args_for_entity_handler(
        &self,
        entity: &EntitySourceOperation,
        param_types: Vec<abi::DynSolType>,
    ) -> Result<Vec<abi::DynSolValue>, Error> {
        self.validate_entity_handler_args(&param_types)?;

        self.expr
            .args
            .iter()
            .zip(param_types.into_iter())
            .map(|(arg, expected_type)| {
                self.process_entity_handler_arg(arg, &expected_type, entity)
            })
            .collect()
    }

    /// Validates that the number of provided arguments matches the expected parameter types.
    fn validate_entity_handler_args(&self, param_types: &[abi::DynSolType]) -> Result<(), Error> {
        if self.expr.args.len() != param_types.len() {
            return Err(anyhow!(
                "mismatched number of arguments: expected {}, got {}",
                param_types.len(),
                self.expr.args.len()
            ));
        }
        Ok(())
    }

    /// Processes a single entity handler argument based on its type (HexAddress, Ethereum, or Subgraph).
    /// Returns error for unsupported Ethereum params.
    fn process_entity_handler_arg(
        &self,
        arg: &CallArg,
        expected_type: &abi::DynSolType,
        entity: &EntitySourceOperation,
    ) -> Result<abi::DynSolValue, Error> {
        match arg {
            CallArg::HexAddress(address) => self.process_hex_address(*address, expected_type),
            CallArg::Ethereum(_) => Err(anyhow!(
                "Ethereum params are not supported for entity handler calls"
            )),
            CallArg::Subgraph(SubgraphArg::EntityParam(name)) => {
                self.process_entity_param(name, expected_type, entity)
            }
        }
    }

    /// Converts a hex address to a token, ensuring it matches the expected parameter type.
    fn process_hex_address(
        &self,
        address: Address,
        expected_type: &abi::DynSolType,
    ) -> Result<abi::DynSolValue, Error> {
        match expected_type {
            abi::DynSolType::Address => Ok(abi::DynSolValue::Address(address)),
            _ => Err(anyhow!(
                "type mismatch: hex address provided for non-address parameter"
            )),
        }
    }

    /// Retrieves and processes an entity parameter, converting it to the expected token type.
    fn process_entity_param(
        &self,
        name: &str,
        expected_type: &abi::DynSolType,
        entity: &EntitySourceOperation,
    ) -> Result<abi::DynSolValue, Error> {
        let value = entity
            .entity
            .get(name)
            .ok_or_else(|| anyhow!("entity missing required param '{name}'"))?;

        self.convert_entity_value_to_token(value, expected_type, name)
    }

    /// Converts a `Value` to the appropriate `Token` type based on the expected parameter type.
    /// Handles various type conversions including primitives, bytes, and arrays.
    fn convert_entity_value_to_token(
        &self,
        value: &Value,
        expected_type: &abi::DynSolType,
        param_name: &str,
    ) -> Result<abi::DynSolValue, Error> {
        use abi::DynSolType;
        use abi::DynSolValue;

        match (expected_type, value) {
            (DynSolType::Address, Value::Bytes(b)) => {
                Ok(DynSolValue::Address(b.as_slice().try_into()?))
            }
            (DynSolType::Bytes, Value::Bytes(b)) => Ok(DynSolValue::Bytes(b.as_ref().to_vec())),
            (DynSolType::FixedBytes(size), Value::Bytes(b)) if b.len() == *size => {
                DynSolValue::fixed_bytes_from_slice(b.as_ref())
            }
            (DynSolType::String, Value::String(s)) => Ok(DynSolValue::String(s.to_string())),
            (DynSolType::Bool, Value::Bool(b)) => Ok(DynSolValue::Bool(*b)),
            (DynSolType::Int(_), Value::Int(i)) => {
                let x = abi::I256::try_from(*i)?;
                Ok(DynSolValue::Int(x, x.bits() as usize))
            }
            (DynSolType::Int(_), Value::Int8(i)) => {
                let x = abi::I256::try_from(*i)?;
                Ok(DynSolValue::Int(x, x.bits() as usize))
            }
            (DynSolType::Int(_), Value::BigInt(i)) => {
                let x =
                    abi::I256::from_le_bytes(i.to_signed_u256().to_le_bytes::<{ U256::BYTES }>());
                Ok(DynSolValue::Int(x, x.bits() as usize))
            }
            (DynSolType::Uint(_), Value::Int(i)) if *i >= 0 => {
                let x = U256::try_from(*i)?;
                Ok(DynSolValue::Uint(x, x.bit_len()))
            }
            (DynSolType::Uint(_), Value::BigInt(i)) if i.sign() == Sign::Plus => {
                let x = i.to_unsigned_u256()?;
                Ok(DynSolValue::Uint(x, x.bit_len()))
            }
            (DynSolType::Array(inner_type), Value::List(values)) => {
                self.process_entity_array_values(values, inner_type.as_ref(), param_name)
            }
            _ => Err(anyhow!(
                "type mismatch for param '{param_name}': cannot convert {:?} to {:?}",
                value,
                expected_type
            )),
        }
    }

    fn process_entity_array_values(
        &self,
        values: &[Value],
        inner_type: &abi::DynSolType,
        param_name: &str,
    ) -> Result<abi::DynSolValue, Error> {
        let tokens: Result<Vec<abi::DynSolValue>, Error> = values
            .iter()
            .enumerate()
            .map(|(idx, v)| {
                self.convert_entity_value_to_token(v, inner_type, &format!("{param_name}[{idx}]"))
            })
            .collect();
        Ok(abi::DynSolValue::Array(tokens?))
    }

    /// Extracts a nested field value from a struct parameter with mixed numeric/named access
    fn extract_nested_struct_field_as_address(
        struct_token: &abi::DynSolValue,
        field_accesses: &[usize],
        call_label: &str,
    ) -> Result<Address, Error> {
        let field_token =
            Self::extract_nested_struct_field(struct_token, field_accesses, call_label)?;
        let address = field_token.as_address().ok_or_else(|| {
            anyhow!(
                "In declarative call '{}': nested struct field is not an address",
                call_label
            )
        })?;
        Ok(address)
    }

    /// Extracts a nested field value from a struct parameter using numeric indices
    fn extract_nested_struct_field(
        struct_token: &abi::DynSolValue,
        field_accesses: &[usize],
        call_label: &str,
    ) -> Result<abi::DynSolValue, Error> {
        assert!(
            !field_accesses.is_empty(),
            "Internal error: empty field access path should be caught at parse time"
        );

        let mut current_token = struct_token;

        for (index, &field_index) in field_accesses.iter().enumerate() {
            match current_token {
                abi::DynSolValue::Tuple(fields) => {
                    let field_token = fields
                        .get(field_index)
                        .ok_or_else(|| {
                            anyhow!(
                                "In declarative call '{}': struct field index {} out of bounds (struct has {} fields) at access step {}",
                                call_label, field_index, fields.len(), index
                            )
                        })?;

                    // If this is the last field access, return the token
                    if index == field_accesses.len() - 1 {
                        return Ok(field_token.clone());
                    }

                    // Otherwise, continue with the next level
                    current_token = field_token;
                }
                _ => {
                    return Err(anyhow!(
                        "In declarative call '{}': cannot access field on non-struct/tuple at access step {} (field path: {:?})",
                        call_label, index, field_accesses
                    ));
                }
            }
        }

        // This should never be reached due to empty check at the beginning
        unreachable!()
    }
}

/// Unresolved representation of declared calls stored as raw strings
/// Used during initial manifest parsing before ABI context is available
#[derive(Clone, CheapClone, Debug, Default, Eq, PartialEq)]
pub struct UnresolvedCallDecls {
    pub raw_decls: Arc<std::collections::HashMap<String, String>>,
    readonly: (),
}

impl UnresolvedCallDecls {
    /// Parse the raw call declarations into CallDecls using ABI context
    pub fn resolve(
        self,
        abi_json: &AbiJson,
        event_signature: Option<&str>,
        spec_version: &semver::Version,
    ) -> Result<CallDecls, anyhow::Error> {
        let decls: Result<Vec<CallDecl>, anyhow::Error> = self
            .raw_decls
            .iter()
            .map(|(label, expr)| {
                CallExpr::parse(expr, abi_json, event_signature, spec_version)
                    .map(|expr| CallDecl {
                        label: label.clone(),
                        expr,
                        readonly: (),
                    })
                    .with_context(|| format!("Error in declared call '{}':", label))
            })
            .collect();

        Ok(CallDecls {
            decls: Arc::new(decls?),
            readonly: (),
        })
    }

    /// Check if the unresolved calls are empty
    pub fn is_empty(&self) -> bool {
        self.raw_decls.is_empty()
    }
}

impl<'de> de::Deserialize<'de> for UnresolvedCallDecls {
    fn deserialize<D>(deserializer: D) -> Result<UnresolvedCallDecls, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let raw_decls: std::collections::HashMap<String, String> =
            de::Deserialize::deserialize(deserializer)?;
        Ok(UnresolvedCallDecls {
            raw_decls: Arc::new(raw_decls),
            readonly: (),
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct CallExpr {
    pub abi: Word,
    pub address: CallArg,
    pub func: Word,
    pub args: Vec<CallArg>,
    readonly: (),
}

impl CallExpr {
    fn validate_args(&self) -> Result<(), anyhow::Error> {
        // Consider address along with args for checking Ethereum/Subgraph mixing
        let has_ethereum = matches!(self.address, CallArg::Ethereum(_))
            || self
                .args
                .iter()
                .any(|arg| matches!(arg, CallArg::Ethereum(_)));

        let has_subgraph = matches!(self.address, CallArg::Subgraph(_))
            || self
                .args
                .iter()
                .any(|arg| matches!(arg, CallArg::Subgraph(_)));

        if has_ethereum && has_subgraph {
            return Err(anyhow!(
                "Cannot mix Ethereum and Subgraph args in the same call expression"
            ));
        }

        Ok(())
    }

    /// Parse a call expression with ABI context to resolve field names at parse time
    pub fn parse(
        s: &str,
        abi_json: &AbiJson,
        event_signature: Option<&str>,
        spec_version: &semver::Version,
    ) -> Result<Self, anyhow::Error> {
        // Parse the expression manually to inject ABI context for field name resolution
        // Format: Contract[address].function(arg1, arg2, ...)

        // Find the contract name and opening bracket
        let bracket_pos = s.find('[').ok_or_else(|| {
            anyhow!(
                "Invalid call expression '{}': missing '[' after contract name",
                s
            )
        })?;
        let abi = s[..bracket_pos].trim();

        if abi.is_empty() {
            return Err(anyhow!(
                "Invalid call expression '{}': missing contract name before '['",
                s
            ));
        }

        // Find the closing bracket and extract the address part
        let bracket_end = s.find(']').ok_or_else(|| {
            anyhow!(
                "Invalid call expression '{}': missing ']' to close address",
                s
            )
        })?;
        let address_str = &s[bracket_pos + 1..bracket_end];

        if address_str.is_empty() {
            return Err(anyhow!(
                "Invalid call expression '{}': empty address in '{}[{}]'",
                s,
                abi,
                address_str
            ));
        }

        // Parse the address with ABI context
        let address = CallArg::parse_with_abi(address_str, abi_json, event_signature, spec_version)
            .with_context(|| {
                format!(
                    "Failed to parse address '{}' in call expression '{}'",
                    address_str, s
                )
            })?;

        // Find the function name and arguments
        let dot_pos = s[bracket_end..].find('.').ok_or_else(|| {
            anyhow!(
                "Invalid call expression '{}': missing '.' after address '{}[{}]'",
                s,
                abi,
                address_str
            )
        })?;
        let func_start = bracket_end + dot_pos + 1;

        let paren_pos = s[func_start..].find('(').ok_or_else(|| {
            anyhow!(
                "Invalid call expression '{}': missing '(' to start function arguments",
                s
            )
        })?;
        let func = &s[func_start..func_start + paren_pos];

        if func.is_empty() {
            return Err(anyhow!(
                "Invalid call expression '{}': missing function name after '{}[{}].'",
                s,
                abi,
                address_str
            ));
        }

        // Find the closing parenthesis and extract arguments
        let paren_end = s.rfind(')').ok_or_else(|| {
            anyhow!(
                "Invalid call expression '{}': missing ')' to close function arguments",
                s
            )
        })?;
        let args_str = &s[func_start + paren_pos + 1..paren_end];

        // Parse arguments with ABI context
        let mut args = Vec::new();
        if !args_str.trim().is_empty() {
            for (i, arg_str) in args_str.split(',').enumerate() {
                let arg_str = arg_str.trim();
                let arg = CallArg::parse_with_abi(arg_str, abi_json, event_signature, spec_version)
                    .with_context(|| {
                        format!(
                            "Failed to parse argument {} '{}' in call expression '{}'",
                            i + 1,
                            arg_str,
                            s
                        )
                    })?;
                args.push(arg);
            }
        }

        let expr = CallExpr {
            abi: Word::from(abi),
            address,
            func: Word::from(func),
            args,
            readonly: (),
        };

        expr.validate_args().with_context(|| {
            format!(
                "Invalid call expression '{}': argument validation failed",
                s
            )
        })?;
        Ok(expr)
    }
}
/// Parse expressions of the form `Contract[address].function(arg1, arg2,
/// ...)` where the `address` and the args are either `event.address` or
/// `event.params.<name>`.
///
/// The parser is pretty awful as it generates error messages that aren't
/// very helpful. We should replace all this with a real parser, most likely
/// `combine` which is what `graphql_parser` uses
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum CallArg {
    // Hard-coded hex address
    HexAddress(Address),
    // Ethereum-specific variants
    Ethereum(EthereumArg),
    // Subgraph datasource specific variants
    Subgraph(SubgraphArg),
}

/// Information about struct field mappings extracted from ABI JSON components
#[derive(Clone, Debug, PartialEq)]
pub struct StructFieldInfo {
    /// Original parameter name from the event
    pub param_name: String,
    /// Mapping from field names to their indices in the tuple
    pub field_mappings: HashMap<String, usize>,
    /// The alloy DynSolType for type validation
    pub param_type: abi::DynSolType,
}

impl StructFieldInfo {
    /// Create a new StructFieldInfo from ABI JSON components
    pub fn from_components(
        param_name: String,
        param_type: abi::DynSolType,
        components: &serde_json::Value,
    ) -> Result<Self, Error> {
        let mut field_mappings = HashMap::new();

        if let Some(components_array) = components.as_array() {
            for (index, component) in components_array.iter().enumerate() {
                if let Some(field_name) = component.get("name").and_then(|n| n.as_str()) {
                    field_mappings.insert(field_name.to_string(), index);
                }
            }
        }

        Ok(StructFieldInfo {
            param_name,
            field_mappings,
            param_type,
        })
    }

    /// Resolve a field name to its tuple index
    pub fn resolve_field_name(&self, field_name: &str) -> Option<usize> {
        self.field_mappings.get(field_name).copied()
    }

    /// Get all available field names
    pub fn get_field_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.field_mappings.keys().cloned().collect();
        names.sort();
        names
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum EthereumArg {
    Address,
    Param(Word),
    /// Struct field access with numeric indices (field names resolved at parse time)
    StructField(Word, Vec<usize>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum SubgraphArg {
    EntityParam(Word),
}

lazy_static! {
    // Matches a 40-character hexadecimal string prefixed with '0x', typical for Ethereum addresses
    static ref ADDR_RE: Regex = Regex::new(r"^0x[0-9a-fA-F]{40}$").unwrap();
}

impl CallArg {
    /// Parse a call argument with ABI context to resolve field names at parse time
    pub fn parse_with_abi(
        s: &str,
        abi_json: &AbiJson,
        event_signature: Option<&str>,
        spec_version: &semver::Version,
    ) -> Result<Self, anyhow::Error> {
        // Handle hex addresses first
        if ADDR_RE.is_match(s) {
            if let Ok(parsed_address) = Address::from_str(s) {
                return Ok(CallArg::HexAddress(parsed_address));
            }
        }

        // Context validation
        let starts_with_event = s.starts_with("event.");
        let starts_with_entity = s.starts_with("entity.");

        match event_signature {
            None => {
                // In entity handler context: forbid event.* expressions
                if starts_with_event {
                    return Err(anyhow!(
                        "'event.*' expressions not allowed in entity handler context"
                    ));
                }
            }
            Some(_) => {
                // In event handler context: require event.* expressions (or hex addresses)
                if starts_with_entity {
                    return Err(anyhow!(
                        "'entity.*' expressions not allowed in event handler context"
                    ));
                }
                if !starts_with_event && !ADDR_RE.is_match(s) {
                    return Err(anyhow!(
                        "In event handler context, only 'event.*' expressions and hex addresses are allowed"
                    ));
                }
            }
        }

        let mut parts = s.split('.');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("event"), Some("address"), None) => Ok(CallArg::Ethereum(EthereumArg::Address)),
            (Some("event"), Some("params"), Some(param)) => {
                // Check if there are any additional parts for struct field access
                let remaining_parts: Vec<&str> = parts.collect();
                if remaining_parts.is_empty() {
                    // Simple parameter access: event.params.foo
                    Ok(CallArg::Ethereum(EthereumArg::Param(Word::from(param))))
                } else {
                    // Struct field access: event.params.foo.bar.0.baz...
                    // Validate spec version before allowing any struct field access
                    if spec_version < &SPEC_VERSION_1_4_0 {
                        return Err(anyhow!(
                                "Struct field access 'event.params.{}.*' in declarative calls is only supported for specVersion >= 1.4.0, current version is {}. Event: '{}'",
                                param,
                                spec_version,
                                event_signature.unwrap_or("unknown")
                            ));
                    }

                    // Resolve field path - supports both numeric and named fields
                    let field_indices = if let Some(signature) = event_signature {
                        // Build field path: [param, field1, field2, ...]
                        let mut field_path = vec![param];
                        field_path.extend(remaining_parts.clone());

                        let resolved_indices = abi_json
                            .get_nested_struct_field_info(signature, &field_path)
                            .with_context(|| {
                                format!(
                                    "Failed to resolve nested field path for event '{}', path '{}'",
                                    signature,
                                    field_path.join(".")
                                )
                            })?;

                        match resolved_indices {
                            Some(indices) => indices,
                            None => {
                                return Err(anyhow!(
                                    "Cannot resolve field path 'event.params.{}' for event '{}'",
                                    field_path.join("."),
                                    signature
                                ));
                            }
                        }
                    } else {
                        // No ABI context - only allow numeric indices
                        let all_numeric = remaining_parts
                            .iter()
                            .all(|part| part.parse::<usize>().is_ok());
                        if !all_numeric {
                            return Err(anyhow!(
                                "Field access 'event.params.{}.{}' requires event signature context for named field resolution",
                                param,
                                remaining_parts.join(".")
                            ));
                        }
                        remaining_parts
                            .into_iter()
                            .map(|part| part.parse::<usize>())
                            .collect::<Result<Vec<_>, _>>()
                            .with_context(|| format!("Failed to parse numeric field indices"))?
                    };
                    Ok(CallArg::Ethereum(EthereumArg::StructField(
                        Word::from(param),
                        field_indices,
                    )))
                }
            }
            (Some("entity"), Some(param), None) => Ok(CallArg::Subgraph(SubgraphArg::EntityParam(
                Word::from(param),
            ))),
            _ => Err(anyhow!("invalid call argument `{}`", s)),
        }
    }
}

pub trait FindMappingABI {
    fn find_abi(&self, abi_name: &str) -> Result<Arc<MappingABI>, Error>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeclaredCall {
    /// The user-supplied label from the manifest
    label: String,
    contract_name: String,
    address: Address,
    function: abi::Function,
    args: Vec<abi::DynSolValue>,
}

impl DeclaredCall {
    pub fn from_log_trigger(
        mapping: &dyn FindMappingABI,
        call_decls: &CallDecls,
        log: &alloy::rpc::types::Log,
        params: &[abi::DynSolParam],
    ) -> Result<Vec<DeclaredCall>, anyhow::Error> {
        Self::from_log_trigger_with_event(mapping, call_decls, log, params)
    }

    pub fn from_log_trigger_with_event(
        mapping: &dyn FindMappingABI,
        call_decls: &CallDecls,
        log: &Log,
        params: &[abi::DynSolParam],
    ) -> Result<Vec<DeclaredCall>, anyhow::Error> {
        Self::create_calls(mapping, call_decls, |decl, _| {
            Ok((
                decl.address_for_log_with_abi(log, params)?,
                decl.args_for_log_with_abi(log, params)?,
            ))
        })
    }

    pub fn from_entity_trigger(
        mapping: &dyn FindMappingABI,
        call_decls: &CallDecls,
        entity: &EntitySourceOperation,
    ) -> Result<Vec<DeclaredCall>, Error> {
        Self::create_calls(mapping, call_decls, |decl, function| {
            let param_types = function
                .inputs
                .iter()
                .map(|param| param.selector_type().parse())
                .collect::<Result<Vec<_>, _>>()?;

            Ok((
                decl.address_for_entity_handler(entity)?,
                decl.args_for_entity_handler(entity, param_types)
                    .context(format!(
                        "Failed to parse arguments for call to function \"{}\" of contract \"{}\"",
                        decl.expr.func.as_str(),
                        decl.expr.abi.to_string()
                    ))?,
            ))
        })
    }

    fn create_calls<F>(
        mapping: &dyn FindMappingABI,
        call_decls: &CallDecls,
        get_address_and_args: F,
    ) -> Result<Vec<DeclaredCall>, anyhow::Error>
    where
        F: Fn(&CallDecl, &abi::Function) -> Result<(Address, Vec<abi::DynSolValue>), anyhow::Error>,
    {
        let mut calls = Vec::new();
        for decl in call_decls.decls.iter() {
            let contract_name = decl.expr.abi.to_string();
            let function = decl.get_function(mapping)?;
            let (address, args) = get_address_and_args(decl, &function)?;

            calls.push(DeclaredCall {
                label: decl.label.clone(),
                contract_name,
                address,
                function: function.clone(),
                args,
            });
        }
        Ok(calls)
    }

    pub fn as_eth_call(self, block_ptr: BlockPtr, gas: Option<u32>) -> (ContractCall, String) {
        (
            ContractCall {
                contract_name: self.contract_name,
                address: self.address,
                block_ptr,
                function: self.function,
                args: self.args,
                gas,
            },
            self.label,
        )
    }
}
#[derive(Clone, Debug)]
pub struct ContractCall {
    pub contract_name: String,
    pub address: Address,
    pub block_ptr: BlockPtr,
    pub function: abi::Function,
    pub args: Vec<abi::DynSolValue>,
    pub gas: Option<u32>,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;

    use crate::data::subgraph::SPEC_VERSION_1_3_0;

    use super::*;

    const EV_TRANSFER: Option<&str> = Some("Transfer(address,tuple)");
    const EV_COMPLEX_ASSET: Option<&str> =
        Some("ComplexAssetCreated(((address,uint256,bool),string,uint256[]),uint256)");

    /// Test helper for parsing CallExpr expressions with predefined ABI and
    /// event context.
    ///
    /// This struct simplifies testing by providing a fluent API for parsing
    /// call expressions with the test ABI (from
    /// `create_test_mapping_abi()`). It handles three main contexts:
    /// - Event handler context with Transfer event (default)
    /// - Event handler context with ComplexAssetCreated event
    ///   (`for_complex_asset()`)
    /// - Entity handler context with no event (`for_subgraph()`)
    ///
    /// # Examples
    /// ```ignore
    /// let parser = ExprParser::new();
    /// // Parse and expect success
    /// let expr = parser.ok("Contract[event.params.asset.addr].test()");
    ///
    /// // Parse and expect error, get error message
    /// let error_msg = parser.err("Contract[invalid].test()");
    ///
    /// // Test with different spec version
    /// let result = parser.parse_with_version(expr, &old_version);
    ///
    /// // Test entity handler context
    /// let entity_parser = ExprParser::new().for_subgraph();
    /// let expr = entity_parser.ok("Contract[entity.addr].test()");
    /// ```
    struct ExprParser {
        abi: super::AbiJson,
        event: Option<String>,
    }

    impl ExprParser {
        /// Creates a new parser with the test ABI and Transfer event context
        fn new() -> Self {
            let abi = create_test_mapping_abi();
            Self {
                abi,
                event: EV_TRANSFER.map(|s| s.to_string()),
            }
        }

        /// Switches to entity handler context (no event signature)
        fn for_subgraph(mut self) -> Self {
            self.event = None;
            self
        }

        /// Switches to ComplexAssetCreated event context for testing nested
        /// structs
        fn for_complex_asset(mut self) -> Self {
            self.event = EV_COMPLEX_ASSET.map(|s| s.to_string());
            self
        }

        /// Parses an expression using the default spec version (1.4.0)
        fn parse(&self, expression: &str) -> Result<CallExpr, Error> {
            self.parse_with_version(expression, &SPEC_VERSION_1_4_0)
        }

        /// Parses an expression with a specific spec version for testing
        /// version compatibility
        fn parse_with_version(
            &self,
            expression: &str,
            spec_version: &semver::Version,
        ) -> Result<CallExpr, Error> {
            CallExpr::parse(expression, &self.abi, self.event.as_deref(), spec_version)
        }

        /// Parses an expression and panics if it fails, returning the
        /// parsed CallExpr. Use this when the expression is expected to
        /// parse successfully.
        #[track_caller]
        fn ok(&self, expression: &str) -> CallExpr {
            let result = self.parse(expression);
            assert!(
                result.is_ok(),
                "Expression '{}' should have parsed successfully: {:#}",
                expression,
                result.unwrap_err()
            );
            result.unwrap()
        }

        /// Parses an expression and panics if it succeeds, returning the
        /// error message. Use this when testing error cases and you want to
        /// verify the error message.
        #[track_caller]
        fn err(&self, expression: &str) -> String {
            match self.parse(expression) {
                Ok(expr) => {
                    panic!(
                        "Expression '{}' should have failed to parse but yielded {:#?}",
                        expression, expr
                    );
                }
                Err(e) => {
                    format!("{:#}", e)
                }
            }
        }
    }

    /// Test helper for parsing CallArg expressions with the test ABI.
    ///
    /// This struct is specifically for testing argument parsing (e.g.,
    /// `event.params.asset.addr`) as opposed to full call expressions. It
    /// uses the same test ABI as ExprParser.
    ///
    /// # Examples
    /// ```ignore
    /// let parser = ArgParser::new();
    /// // Parse an event parameter argument
    /// let arg = parser.ok("event.params.asset.addr", Some("Transfer(address,tuple)"));
    ///
    /// // Test entity context argument
    /// let arg = parser.ok("entity.contractAddress", None);
    ///
    /// // Test error cases
    /// let error = parser.err("invalid.arg", Some("Transfer(address,tuple)"));
    /// ```
    struct ArgParser {
        abi: super::AbiJson,
    }

    impl ArgParser {
        /// Creates a new argument parser with the test ABI
        fn new() -> Self {
            let abi = create_test_mapping_abi();
            Self { abi }
        }

        /// Parses a call argument with optional event signature context
        fn parse(&self, expression: &str, event_signature: Option<&str>) -> Result<CallArg, Error> {
            CallArg::parse_with_abi(expression, &self.abi, event_signature, &SPEC_VERSION_1_4_0)
        }

        /// Parses an argument and panics if it fails, returning the parsed
        /// CallArg. Use this when the argument is expected to parse
        /// successfully.
        fn ok(&self, expression: &str, event_signature: Option<&str>) -> CallArg {
            let result = self.parse(expression, event_signature);
            assert!(
                result.is_ok(),
                "Expression '{}' should have parsed successfully: {}",
                expression,
                result.unwrap_err()
            );
            result.unwrap()
        }

        /// Parses an argument and panics if it succeeds, returning the
        /// error message. Use this when testing error cases and you want to
        /// verify the error message.
        fn err(&self, expression: &str, event_signature: Option<&str>) -> String {
            match self.parse(expression, event_signature) {
                Ok(arg) => {
                    panic!(
                        "Expression '{}' should have failed to parse but yielded {:#?}",
                        expression, arg
                    );
                }
                Err(e) => {
                    format!("{:#}", e)
                }
            }
        }
    }

    #[test]
    fn test_ethereum_call_expr() {
        let parser = ExprParser::new();
        let expr: CallExpr = parser.ok("ERC20[event.address].balanceOf(event.params.token)");
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(expr.address, CallArg::Ethereum(EthereumArg::Address));
        assert_eq!(expr.func, "balanceOf");
        assert_eq!(
            expr.args,
            vec![CallArg::Ethereum(EthereumArg::Param("token".into()))]
        );

        let expr: CallExpr =
            parser.ok("Pool[event.params.pool].fees(event.params.token0, event.params.token1)");
        assert_eq!(expr.abi, "Pool");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::Param("pool".into()))
        );
        assert_eq!(expr.func, "fees");
        assert_eq!(
            expr.args,
            vec![
                CallArg::Ethereum(EthereumArg::Param("token0".into())),
                CallArg::Ethereum(EthereumArg::Param("token1".into()))
            ]
        );
    }

    #[test]
    fn test_subgraph_call_expr() {
        let parser = ExprParser::new().for_subgraph();

        let expr: CallExpr = parser.ok("Token[entity.id].symbol()");
        assert_eq!(expr.abi, "Token");
        assert_eq!(
            expr.address,
            CallArg::Subgraph(SubgraphArg::EntityParam("id".into()))
        );
        assert_eq!(expr.func, "symbol");
        assert_eq!(expr.args, vec![]);

        let expr: CallExpr = parser.ok("Pair[entity.pair].getReserves(entity.token0)");
        assert_eq!(expr.abi, "Pair");
        assert_eq!(
            expr.address,
            CallArg::Subgraph(SubgraphArg::EntityParam("pair".into()))
        );
        assert_eq!(expr.func, "getReserves");
        assert_eq!(
            expr.args,
            vec![CallArg::Subgraph(SubgraphArg::EntityParam("token0".into()))]
        );
    }

    #[test]
    fn test_hex_address_call_expr() {
        let parser = ExprParser::new();

        let addr = "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF";
        let hex_address = CallArg::HexAddress(Address::from_str(addr).unwrap());

        // Test HexAddress in address position
        let expr: CallExpr = parser.ok(&format!("Pool[{}].growth()", addr));
        assert_eq!(expr.abi, "Pool");
        assert_eq!(expr.address, hex_address.clone());
        assert_eq!(expr.func, "growth");
        assert_eq!(expr.args, vec![]);

        // Test HexAddress in argument position
        let expr: CallExpr = parser.ok(&format!(
            "Pool[event.address].approve({}, event.params.amount)",
            addr
        ));
        assert_eq!(expr.abi, "Pool");
        assert_eq!(expr.address, CallArg::Ethereum(EthereumArg::Address));
        assert_eq!(expr.func, "approve");
        assert_eq!(expr.args.len(), 2);
        assert_eq!(expr.args[0], hex_address);
    }

    #[test]
    fn test_invalid_call_args() {
        let parser = ArgParser::new();
        // Invalid hex address
        parser.err("Pool[0xinvalid].test()", EV_TRANSFER);

        // Invalid event path
        parser.err("Pool[event.invalid].test()", EV_TRANSFER);

        // Invalid entity path
        parser.err("Pool[entity].test()", EV_TRANSFER);

        // Empty address
        parser.err("Pool[].test()", EV_TRANSFER);

        // Invalid parameter format
        parser.err("Pool[event.params].test()", EV_TRANSFER);
    }

    #[test]
    fn test_simple_args() {
        let parser = ArgParser::new();

        // Test valid hex address
        let addr = "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF";
        let arg = parser.ok(addr, EV_TRANSFER);
        assert!(matches!(arg, CallArg::HexAddress(_)));

        // Test Ethereum Address
        let arg = parser.ok("event.address", EV_TRANSFER);
        assert!(matches!(arg, CallArg::Ethereum(EthereumArg::Address)));

        // Test Ethereum Param
        let arg = parser.ok("event.params.token", EV_TRANSFER);
        assert!(matches!(arg, CallArg::Ethereum(EthereumArg::Param(_))));

        // Test Subgraph EntityParam
        let arg = parser.ok("entity.token", None);
        assert!(matches!(
            arg,
            CallArg::Subgraph(SubgraphArg::EntityParam(_))
        ));
    }

    #[test]
    fn test_struct_field_access_functions() {
        use crate::abi::DynSolValue;
        use alloy::primitives::{Address, U256};

        let parser = ExprParser::new();

        let tuple_fields = vec![
            DynSolValue::Uint(U256::from(8u8), 8), // index 0: uint8
            DynSolValue::Address(Address::from([1u8; 20])), // index 1: address
            DynSolValue::Uint(U256::from(1000u64), 256), // index 2: uint256
        ];

        // Test extract_struct_field with numeric indices
        let struct_token = DynSolValue::Tuple(tuple_fields.clone());

        // Test accessing index 0 (uint8)
        let result =
            CallDecl::extract_nested_struct_field(&struct_token, &[0], "testCall").unwrap();
        assert_eq!(result, tuple_fields[0]);

        // Test accessing index 1 (address)
        let result =
            CallDecl::extract_nested_struct_field(&struct_token, &[1], "testCall").unwrap();
        assert_eq!(result, tuple_fields[1]);

        // Test accessing index 2 (uint256)
        let result =
            CallDecl::extract_nested_struct_field(&struct_token, &[2], "testCall").unwrap();
        assert_eq!(result, tuple_fields[2]);

        // Test that it works in a declarative call context
        let expr: CallExpr = parser.ok("ERC20[event.params.asset.1].name()");
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![1]))
        );
        assert_eq!(expr.func, "name");
        assert_eq!(expr.args, vec![]);
    }

    #[test]
    fn test_invalid_struct_field_parsing() {
        let parser = ArgParser::new();
        // Test invalid patterns
        parser.err("event.params", EV_TRANSFER);
        parser.err("event.invalid.param.field", EV_TRANSFER);
    }

    #[test]
    fn test_declarative_call_error_context() {
        use crate::abi::{DynSolParam, DynSolValue};
        use alloy::primitives::U256;
        use alloy::rpc::types::Log;

        let parser = ExprParser::new();

        // Create a test call declaration
        let call_decl = CallDecl {
            label: "myTokenCall".to_string(),
            expr: parser.ok("ERC20[event.params.asset.1].name()"),
            readonly: (),
        };

        // Test scenario 1: Unknown parameter
        let inner_log = alloy::primitives::Log {
            address: Address::ZERO,
            data: alloy::primitives::LogData::new_unchecked(vec![].into(), vec![].into()),
        };
        let log = Log {
            inner: inner_log,
            block_hash: Some(B256::ZERO),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(B256::ZERO),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };
        let params = vec![]; // Empty params - 'asset' param is missing

        let result = call_decl.address_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'myTokenCall'"));
        assert!(error_msg.contains("unknown param asset"));

        // Test scenario 2: Struct field access error
        let params = vec![DynSolParam {
            name: "asset".to_string(),
            value: DynSolValue::Tuple(vec![DynSolValue::Uint(U256::from(1u8), 8)]), // Only 1 field, but trying to access index 1
        }];

        let result = call_decl.address_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'myTokenCall'"));
        assert!(error_msg.contains("out of bounds"));
        assert!(error_msg.contains("struct has 1 fields"));

        // Test scenario 3: Non-address field access
        let params = vec![DynSolParam {
            name: "asset".to_string(),
            value: DynSolValue::Tuple(vec![
                DynSolValue::Uint(U256::from(1u8), 8),
                DynSolValue::Uint(U256::from(2u8), 8), // Index 1 is uint, not address
            ]),
        }];

        let result = call_decl.address_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'myTokenCall'"));
        assert!(error_msg.contains("nested struct field is not an address"));

        // Test scenario 4: Field index out of bounds is caught at parse time
        let parser = parser.for_complex_asset();
        let error_msg =
            parser.err("ERC20[event.address].transfer(event.params.complexAsset.base.3)");
        assert!(error_msg.contains("Index 3 out of bounds for struct with 3 fields"));

        // Test scenario 5: Runtime struct field extraction error - out of bounds
        let expr = parser.ok("ERC20[event.address].transfer(event.params.complexAsset.base.2)");
        let call_decl_with_args = CallDecl {
            label: "transferCall".to_string(),
            expr,
            readonly: (),
        };

        // Create a structure where base has only 2 fields instead of 3
        // The parser thinks there should be 3 fields based on ABI, but at runtime we provide only 2
        let base_struct = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::from([1u8; 20])), // addr at index 0
            DynSolValue::Uint(U256::from(100u64), 256),     // amount at index 1
                                                            // Missing the active field at index 2!
        ]);

        let params = vec![DynSolParam {
            name: "complexAsset".to_string(),
            value: DynSolValue::Tuple(vec![
                base_struct,                                 // base with only 2 fields
                DynSolValue::String("metadata".to_string()), // metadata at index 1
                DynSolValue::Array(vec![]),                  // values at index 2
            ]),
        }];

        let result = call_decl_with_args.args_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'transferCall'"));
        assert!(error_msg.contains("out of bounds"));
        assert!(error_msg.contains("struct has 2 fields"));
    }

    #[test]
    fn test_struct_field_extraction_comprehensive() {
        use crate::abi::DynSolValue;
        use alloy::primitives::{Address, U256};

        // Create a complex nested structure for comprehensive testing:
        // struct Asset {
        //   uint8 kind;          // index 0
        //   Token token;         // index 1 (nested struct)
        //   uint256 amount;      // index 2
        // }
        // struct Token {
        //   address addr;        // index 0
        //   string name;         // index 1
        // }
        let inner_struct = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::from([0x42; 20])), // token.addr
            DynSolValue::String("TokenName".to_string()),    // token.name
        ]);

        let outer_struct = DynSolValue::Tuple(vec![
            DynSolValue::Uint(U256::from(1u8), 8),       // asset.kind
            inner_struct,                                // asset.token
            DynSolValue::Uint(U256::from(1000u64), 256), // asset.amount
        ]);

        // Test cases: (path, expected_value, description)
        let test_cases = vec![
            (
                vec![0],
                DynSolValue::Uint(U256::from(1u8), 8),
                "Simple field access",
            ),
            (
                vec![1, 0],
                DynSolValue::Address(Address::from([0x42; 20])),
                "Nested field access",
            ),
            (
                vec![1, 1],
                DynSolValue::String("TokenName".to_string()),
                "Nested string field",
            ),
            (
                vec![2],
                DynSolValue::Uint(U256::from(1000u64), 256),
                "Last field access",
            ),
        ];

        for (path, expected, description) in test_cases {
            let result = CallDecl::extract_nested_struct_field(&outer_struct, &path, "testCall")
                .unwrap_or_else(|e| panic!("Failed {}: {}", description, e));
            assert_eq!(result, expected, "Failed: {}", description);
        }

        // Test error cases
        let error_cases = vec![
            (vec![3], "out of bounds (struct has 3 fields)"),
            (vec![1, 2], "struct has 2 fields"),
            (vec![0, 0], "cannot access field on non-struct/tuple"),
        ];

        for (path, expected_error) in error_cases {
            let result = CallDecl::extract_nested_struct_field(&outer_struct, &path, "testCall");
            assert!(result.is_err(), "Expected error for path: {:?}", path);
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains(expected_error),
                "Error message should contain '{}'. Got: {}",
                expected_error,
                error_msg
            );
        }
    }

    #[test]
    fn test_abi_aware_named_field_resolution() {
        let parser = ExprParser::new();

        // Test 1: Named field resolution with ABI context
        let expr = parser.ok("TestContract[event.params.asset.addr].name()");

        assert_eq!(expr.abi, "TestContract");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![0])) // addr -> 0
        );
        assert_eq!(expr.func, "name");
        assert_eq!(expr.args, vec![]);

        // Test 2: Mixed named and numeric access in arguments
        let expr = parser.ok(
            "TestContract[event.address].transfer(event.params.asset.amount, event.params.asset.1)",
        );

        assert_eq!(expr.abi, "TestContract");
        assert_eq!(expr.address, CallArg::Ethereum(EthereumArg::Address));
        assert_eq!(expr.func, "transfer");
        assert_eq!(
            expr.args,
            vec![
                CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![1])), // amount -> 1
                CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![1])), // numeric 1
            ]
        );
    }

    #[test]
    fn test_abi_aware_error_handling() {
        let parser = ExprParser::new();

        // Test 1: Invalid field name provides helpful suggestions
        let error_msg = parser.err("TestContract[event.params.asset.invalid].name()");
        assert!(error_msg.contains("Field 'invalid' not found"));
        assert!(error_msg.contains("Available fields:"));

        // Test 2: Named field access without event context
        let error_msg = parser
            .for_subgraph()
            .err("TestContract[event.params.asset.addr].name()");
        assert!(error_msg.contains("'event.*' expressions not allowed in entity handler context"));
    }

    #[test]
    fn test_parse_function_error_messages() {
        const SV: &semver::Version = &SPEC_VERSION_1_4_0;
        const EV: Option<&str> = Some("Test()");

        // Create a minimal ABI for testing
        let abi_json = r#"[{"anonymous": false, "inputs": [], "name": "Test", "type": "event"}]"#;
        let abi_json_helper = AbiJson::new(abi_json.as_bytes()).unwrap();

        let parse = |expr: &str| {
            let result = CallExpr::parse(expr, &abi_json_helper, EV, SV);
            assert!(
                result.is_err(),
                "Expression {} should have failed to parse",
                expr
            );
            result.unwrap_err().to_string()
        };

        // Test 1: Missing opening bracket
        let error_msg = parse("TestContract event.address].test()");
        assert!(error_msg.contains("Invalid call expression"));
        assert!(error_msg.contains("missing '[' after contract name"));

        // Test 2: Missing closing bracket
        let error_msg = parse("TestContract[event.address.test()");
        assert!(error_msg.contains("missing ']' to close address"));

        // Test 3: Empty contract name
        let error_msg = parse("[event.address].test()");
        assert!(error_msg.contains("missing contract name before '['"));

        // Test 4: Empty address
        let error_msg = parse("TestContract[].test()");
        assert!(error_msg.contains("empty address"));

        // Test 5: Missing function name
        let error_msg = parse("TestContract[event.address].()");
        assert!(error_msg.contains("missing function name"));

        // Test 6: Missing opening parenthesis
        let error_msg = parse("TestContract[event.address].test");
        assert!(error_msg.contains("missing '(' to start function arguments"));

        // Test 7: Missing closing parenthesis
        let error_msg = parse("TestContract[event.address].test(");
        assert!(error_msg.contains("missing ')' to close function arguments"));

        // Test 8: Invalid argument should show argument position
        let error_msg = parse("TestContract[event.address].test(invalid.arg)");
        assert!(error_msg.contains("Failed to parse argument 1"));
        assert!(error_msg.contains("'invalid.arg'"));
    }

    #[test]
    fn test_call_expr_abi_context_comprehensive() {
        // Comprehensive test for CallExpr parsing with ABI context
        let parser = ExprParser::new().for_complex_asset();

        // Test 1: Parse-time field name resolution
        let expr = parser.ok("Contract[event.params.complexAsset.base.addr].test()");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 0]))
        );

        // Test 2: Mixed named and numeric field access
        let expr = parser.ok(
            "Contract[event.address].test(event.params.complexAsset.0.1, event.params.complexAsset.base.active)"
        );
        assert_eq!(
            expr.args,
            vec![
                CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 1])), // base.amount
                CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 2])), // base.active
            ]
        );

        // Test 3: Error - Invalid field name with helpful suggestions
        let error_msg = parser.err("Contract[event.params.complexAsset.invalid].test()");
        assert!(error_msg.contains("Field 'invalid' not found"));
        // Check that it mentions available fields (the exact format may vary)
        assert!(
            error_msg.contains("base")
                && error_msg.contains("metadata")
                && error_msg.contains("values")
        );

        // Test 4: Error - Accessing nested field on non-struct
        let error_msg = parser.err("Contract[event.params.complexAsset.metadata.something].test()");
        assert!(error_msg.contains("is not a struct"));

        // Test 5: Error - Out of bounds numeric access
        let error_msg = parser.err("Contract[event.params.complexAsset.3].test()");
        assert!(error_msg.contains("out of bounds"));

        // Test 6: Deep nesting with mixed access
        let expr = parser.ok(
            "Contract[event.params.complexAsset.base.0].test(event.params.complexAsset.0.amount)",
        );
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 0])) // base.addr
        );
        assert_eq!(
            expr.args,
            vec![
                CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 1])) // base.amount
            ]
        );

        // Test 7: Version check - struct field access requires v1.4.0+
        let result = parser.parse_with_version(
            "Contract[event.params.complexAsset.base.addr].test()",
            &SPEC_VERSION_1_3_0,
        );
        assert!(result.is_err());
        let error_msg = format!("{:#}", result.unwrap_err());
        assert!(error_msg.contains("only supported for specVersion >= 1.4.0"));

        // Test 8: Entity handler context - no event.* expressions allowed
        let entity_parser = ExprParser::new().for_subgraph();
        let error_msg = entity_parser.err("Contract[event.params.something].test()");
        assert!(error_msg.contains("'event.*' expressions not allowed in entity handler context"));

        // Test 9: Successful entity handler expression
        let expr = entity_parser.ok("Contract[entity.contractAddress].test(entity.amount)");
        assert!(matches!(expr.address, CallArg::Subgraph(_)));
        assert!(matches!(expr.args[0], CallArg::Subgraph(_)));
    }

    #[test]
    fn complex_asset() {
        let parser = ExprParser::new().for_complex_asset();

        // Test 1: All named field access: event.params.complexAsset.base.addr
        let expr =
            parser.ok("Contract[event.address].getMetadata(event.params.complexAsset.base.addr)");
        assert_eq!(
            expr.args[0],
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 0])) // base=0, addr=0
        );

        // Test 2: All numeric field access: event.params.complexAsset.0.0
        let expr = parser.ok("Contract[event.address].getMetadata(event.params.complexAsset.0.0)");
        assert_eq!(
            expr.args[0],
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 0]))
        );

        // Test 3: Mixed access - numeric then named: event.params.complexAsset.0.addr
        let expr = parser.ok("Contract[event.address].transfer(event.params.complexAsset.0.addr)");
        assert_eq!(
            expr.args[0],
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 0])) // 0=base, addr=0
        );

        // Test 4: Mixed access - named then numeric: event.params.complexAsset.base.1
        let expr =
            parser.ok("Contract[event.address].updateAmount(event.params.complexAsset.base.1)");
        assert_eq!(
            expr.args[0],
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![0, 1])) // base=0, 1=amount
        );

        // Test 5: Access non-nested field by name: event.params.complexAsset.metadata
        let expr =
            parser.ok("Contract[event.address].setMetadata(event.params.complexAsset.metadata)");
        assert_eq!(
            expr.args[0],
            CallArg::Ethereum(EthereumArg::StructField("complexAsset".into(), vec![1])) // metadata=1
        );

        // Test 6: Error case - invalid field name
        let error_msg =
            parser.err("Contract[event.address].test(event.params.complexAsset.invalid)");
        assert!(error_msg.contains("Field 'invalid' not found"));

        // Test 7: Error case - accessing nested field on non-tuple
        let error_msg = parser
            .err("Contract[event.address].test(event.params.complexAsset.metadata.something)");
        assert!(error_msg.contains("is not a struct"));
    }

    #[test]
    fn test_normalize_abi_json_with_undefined_state_mutability() {
        let abi_with_undefined = r#"[
            {
                "type": "function",
                "name": "testFunction",
                "inputs": [],
                "outputs": [],
                "stateMutability": "undefined"
            },
            {
                "type": "function",
                "name": "normalFunction",
                "inputs": [],
                "outputs": [],
                "stateMutability": "view"
            }
        ]"#;

        let normalized = normalize_abi_json(abi_with_undefined.as_bytes()).unwrap();
        let result: serde_json::Value = serde_json::from_slice(&normalized).unwrap();

        if let Some(array) = result.as_array() {
            assert_eq!(array[0]["stateMutability"], "nonpayable");
            assert_eq!(array[1]["stateMutability"], "view");
        } else {
            panic!("Expected JSON array");
        }

        let json_abi: abi::JsonAbi = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(json_abi.len(), 2);
    }

    #[test]
    fn test_normalize_abi_json_with_duplicate_constructors() {
        let abi_with_duplicate_constructors = r#"[
            {
                "type": "constructor",
                "inputs": [{"name": "param1", "type": "address"}],
                "stateMutability": "nonpayable"
            },
            {
                "type": "function",
                "name": "someFunction",
                "inputs": [],
                "outputs": [],
                "stateMutability": "view"
            },
            {
                "type": "constructor",
                "inputs": [{"name": "param2", "type": "uint256"}],
                "stateMutability": "nonpayable"
            }
        ]"#;

        let normalized = normalize_abi_json(abi_with_duplicate_constructors.as_bytes()).unwrap();
        let result: serde_json::Value = serde_json::from_slice(&normalized).unwrap();

        if let Some(array) = result.as_array() {
            assert_eq!(array.len(), 2);
            assert_eq!(array[0]["type"], "constructor");
            assert_eq!(array[0]["inputs"][0]["name"], "param1");
            assert_eq!(array[1]["type"], "function");
        } else {
            panic!("Expected JSON array");
        }

        let json_abi: abi::JsonAbi = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(json_abi.len(), 2);
    }

    #[test]
    fn test_normalize_abi_json_with_duplicate_fallbacks() {
        let abi_with_duplicate_fallbacks = r#"[
            {
                "type": "fallback",
                "stateMutability": "payable"
            },
            {
                "type": "function",
                "name": "someFunction",
                "inputs": [],
                "outputs": [],
                "stateMutability": "view"
            },
            {
                "type": "fallback",
                "stateMutability": "nonpayable"
            }
        ]"#;

        let normalized = normalize_abi_json(abi_with_duplicate_fallbacks.as_bytes()).unwrap();
        let result: serde_json::Value = serde_json::from_slice(&normalized).unwrap();

        if let Some(array) = result.as_array() {
            assert_eq!(array.len(), 2);
            assert_eq!(array[0]["type"], "fallback");
            assert_eq!(array[0]["stateMutability"], "payable");
            assert_eq!(array[1]["type"], "function");
        } else {
            panic!("Expected JSON array");
        }

        let json_abi: abi::JsonAbi = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(json_abi.len(), 2);
    }

    #[test]
    fn test_normalize_abi_json_strips_indexed_from_non_events() {
        let abi_with_indexed_in_function = r#"[
            {
                "type": "function",
                "name": "testFunction",
                "inputs": [{"name": "x", "type": "uint256", "indexed": true}],
                "outputs": [{"name": "y", "type": "address", "indexed": false}],
                "stateMutability": "view"
            },
            {
                "type": "event",
                "name": "TestEvent",
                "anonymous": false,
                "inputs": [{"name": "from", "type": "address", "indexed": true}]
            }
        ]"#;

        let normalized = normalize_abi_json(abi_with_indexed_in_function.as_bytes()).unwrap();
        let result: serde_json::Value = serde_json::from_slice(&normalized).unwrap();

        if let Some(array) = result.as_array() {
            assert!(array[0]["inputs"][0].get("indexed").is_none());
            assert!(array[0]["outputs"][0].get("indexed").is_none());
            assert_eq!(array[1]["inputs"][0]["indexed"], true);
        } else {
            panic!("Expected JSON array");
        }

        let json_abi: abi::JsonAbi = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(json_abi.len(), 2);
    }

    // Helper function to create consistent test ABI
    fn create_test_mapping_abi() -> AbiJson {
        const ABI_JSON: &str = r#"[
            {
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": false,
                        "name": "from",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "name": "asset",
                        "type": "tuple",
                        "components": [
                            {
                                "name": "addr",
                                "type": "address"
                            },
                            {
                                "name": "amount",
                                "type": "uint256"
                            },
                            {
                                "name": "active",
                                "type": "bool"
                            }
                        ]
                    }
                ],
                "name": "Transfer",
                "type": "event"
            },
            {
                "type": "event",
                "name": "ComplexAssetCreated",
                "inputs": [
                    {
                        "name": "complexAsset",
                        "type": "tuple",
                        "indexed": false,
                        "internalType": "struct DeclaredCallsContract.ComplexAsset",
                        "components": [
                            {
                                "name": "base",
                                "type": "tuple",
                                "internalType": "struct DeclaredCallsContract.Asset",
                                "components": [
                                        {
                                            "name": "addr",
                                            "type": "address",
                                            "internalType": "address"
                                        },
                                        {
                                            "name": "amount",
                                            "type": "uint256",
                                            "internalType": "uint256"
                                        },
                                        {
                                            "name": "active",
                                            "type": "bool",
                                            "internalType": "bool"
                                        }
                                ]
                            },
                            {
                                "name": "metadata",
                                "type": "string",
                                "internalType": "string"
                            },
                            {
                                "name": "values",
                                "type": "uint256[]",
                                "internalType": "uint256[]"
                            }
                        ]
                    }
                ]
            }
        ]"#;

        let abi_json_helper = AbiJson::new(ABI_JSON.as_bytes()).unwrap();

        abi_json_helper
    }
}

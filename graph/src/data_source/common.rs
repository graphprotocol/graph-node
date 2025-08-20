use crate::blockchain::block_stream::EntitySourceOperation;
use crate::data::subgraph::SPEC_VERSION_1_4_0;
use crate::prelude::{BlockPtr, Value};
use crate::{components::link_resolver::LinkResolver, data::value::Word, prelude::Link};
use anyhow::{anyhow, Context, Error};
use ethabi::{Address, Contract, Function, LogParam, ParamType, Token};
use graph_derive::CheapClone;
use lazy_static::lazy_static;
use num_bigint::Sign;
use regex::Regex;
use serde::de;
use serde::Deserialize;
use serde_json;
use slog::{warn, Logger};
use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};
use web3::types::{Log, H160};

#[derive(Clone, Debug, PartialEq)]
pub struct MappingABI {
    pub name: String,
    pub contract: Contract,
    /// Struct field mappings extracted from ABI JSON
    pub struct_field_mappings: HashMap<String, HashMap<String, StructFieldInfo>>,
}

impl MappingABI {
    pub fn function(
        &self,
        contract_name: &str,
        name: &str,
        signature: Option<&str>,
    ) -> Result<&Function, Error> {
        let contract = &self.contract;
        let function = match signature {
            // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
            // functions this always picks the same overloaded variant, which is incorrect
            // and may lead to encoding/decoding errors
            None => contract.function(name).with_context(|| {
                format!(
                    "Unknown function \"{}::{}\" called from WASM runtime",
                    contract_name, name
                )
            })?,

            // Behavior for apiVersion >= 0.0.04: look up function by signature of
            // the form `functionName(uint256,string) returns (bytes32,string)`; this
            // correctly picks the correct variant of an overloaded function
            Some(ref signature) => contract
                .functions_by_name(name)
                .with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" called from WASM runtime",
                        contract_name, name
                    )
                })?
                .iter()
                .find(|f| signature == &f.signature())
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

    /// Parse struct field mappings from ABI JSON
    fn parse_struct_field_mappings(
        abi_json: &str,
        logger: &Logger,
    ) -> Result<HashMap<String, HashMap<String, StructFieldInfo>>, Error> {
        let abi: serde_json::Value =
            serde_json::from_str(abi_json).with_context(|| "Failed to parse ABI JSON")?;

        let mut event_mappings = HashMap::new();

        if let Some(abi_array) = abi.as_array() {
            for item in abi_array {
                // Only process events
                if item.get("type").and_then(|t| t.as_str()) == Some("event") {
                    if let (Some(event_name), Some(inputs)) = (
                        item.get("name").and_then(|n| n.as_str()),
                        item.get("inputs").and_then(|i| i.as_array()),
                    ) {
                        let mut param_mappings = HashMap::new();

                        for input in inputs {
                            if let (Some(param_name), Some(param_type)) = (
                                input.get("name").and_then(|n| n.as_str()),
                                input.get("type").and_then(|t| t.as_str()),
                            ) {
                                // Check if this is a tuple type (struct)
                                if param_type == "tuple" {
                                    if let Some(components) = input.get("components") {
                                        // Parse the ParamType from the JSON (we'll use a placeholder for now)
                                        let param_type = ParamType::Tuple(vec![]); // Simplified for now

                                        match StructFieldInfo::from_components(
                                            param_name.to_string(),
                                            param_type,
                                            components,
                                        ) {
                                            Ok(field_info) => {
                                                param_mappings
                                                    .insert(param_name.to_string(), field_info);
                                            }
                                            Err(e) => {
                                                // Log error but continue processing other parameters
                                                warn!(logger, "Failed to parse struct field info for {}.{}: {}", event_name, param_name, e);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if !param_mappings.is_empty() {
                            event_mappings.insert(event_name.to_string(), param_mappings);
                        }
                    }
                }
            }
        }

        Ok(event_mappings)
    }

    /// Get struct field info for a specific event parameter
    pub fn get_struct_field_info(
        &self,
        event_name: &str,
        param_name: &str,
    ) -> Option<&StructFieldInfo> {
        self.struct_field_mappings
            .get(event_name)
            .and_then(|event_params| event_params.get(param_name))
    }

    /// Get all struct parameters for an event
    pub fn get_event_struct_params(
        &self,
        event_name: &str,
    ) -> Option<&HashMap<String, StructFieldInfo>> {
        self.struct_field_mappings.get(event_name)
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
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<MappingABI, anyhow::Error> {
        let contract_bytes = resolver.cat(logger, &self.file).await.with_context(|| {
            format!(
                "failed to resolve ABI {} from {}",
                self.name, self.file.link
            )
        })?;
        let contract = Contract::load(&*contract_bytes)
            .with_context(|| format!("failed to load ABI {}", self.name))?;

        // Parse struct field mappings from the original ABI JSON
        let abi_json = std::str::from_utf8(&contract_bytes)
            .with_context(|| format!("ABI {} contains invalid UTF-8", self.name))?;
        let struct_field_mappings = MappingABI::parse_struct_field_mappings(abi_json, logger)
            .unwrap_or_else(|e| {
                warn!(
                    logger,
                    "Failed to parse struct field mappings for {}: {}", self.name, e
                );
                HashMap::new()
            });

        Ok(MappingABI {
            name: self.name,
            contract,
            struct_field_mappings,
        })
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

    pub fn address_for_log(&self, log: &Log, params: &[LogParam]) -> Result<H160, Error> {
        self.address_for_log_with_abi(log, params)
    }

    pub fn address_for_log_with_abi(&self, log: &Log, params: &[LogParam]) -> Result<H160, Error> {
        let address = match &self.expr.address {
            CallArg::HexAddress(address) => *address,
            CallArg::Ethereum(arg) => match arg {
                EthereumArg::Address => log.address,
                EthereumArg::Param(name) => {
                    let value = params
                        .iter()
                        .find(|param| &param.name == name.as_str())
                        .ok_or_else(|| {
                            anyhow!(
                                "In declarative call '{}': unknown param {}",
                                self.label,
                                name
                            )
                        })?
                        .value
                        .clone();
                    value.into_address().ok_or_else(|| {
                        anyhow!(
                            "In declarative call '{}': param {} is not an address",
                            self.label,
                            name
                        )
                    })?
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

    pub fn args_for_log(&self, log: &Log, params: &[LogParam]) -> Result<Vec<Token>, Error> {
        self.args_for_log_with_abi(log, params)
    }

    pub fn args_for_log_with_abi(
        &self,
        log: &Log,
        params: &[LogParam],
    ) -> Result<Vec<Token>, Error> {
        self.expr
            .args
            .iter()
            .map(|arg| match arg {
                CallArg::HexAddress(address) => Ok(Token::Address(*address)),
                CallArg::Ethereum(arg) => match arg {
                    EthereumArg::Address => Ok(Token::Address(log.address)),
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

    pub fn get_function(&self, mapping: &dyn FindMappingABI) -> Result<Function, anyhow::Error> {
        let contract_name = self.expr.abi.to_string();
        let function_name = self.expr.func.as_str();
        let abi = mapping.find_abi(&contract_name)?;

        // TODO: Handle overloaded functions
        // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
        // functions this always picks the same overloaded variant, which is incorrect
        // and may lead to encoding/decoding errors
        abi.contract
            .function(function_name)
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
    ) -> Result<H160, Error> {
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
                        let address = H160::from_slice(bytes.as_slice());
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
        param_types: Vec<ParamType>,
    ) -> Result<Vec<Token>, Error> {
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
    fn validate_entity_handler_args(&self, param_types: &[ParamType]) -> Result<(), Error> {
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
        expected_type: &ParamType,
        entity: &EntitySourceOperation,
    ) -> Result<Token, Error> {
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
        address: H160,
        expected_type: &ParamType,
    ) -> Result<Token, Error> {
        match expected_type {
            ParamType::Address => Ok(Token::Address(address)),
            _ => Err(anyhow!(
                "type mismatch: hex address provided for non-address parameter"
            )),
        }
    }

    /// Retrieves and processes an entity parameter, converting it to the expected token type.
    fn process_entity_param(
        &self,
        name: &str,
        expected_type: &ParamType,
        entity: &EntitySourceOperation,
    ) -> Result<Token, Error> {
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
        expected_type: &ParamType,
        param_name: &str,
    ) -> Result<Token, Error> {
        match (expected_type, value) {
            (ParamType::Address, Value::Bytes(b)) => {
                Ok(Token::Address(H160::from_slice(b.as_slice())))
            }
            (ParamType::Bytes, Value::Bytes(b)) => Ok(Token::Bytes(b.as_ref().to_vec())),
            (ParamType::FixedBytes(size), Value::Bytes(b)) if b.len() == *size => {
                Ok(Token::FixedBytes(b.as_ref().to_vec()))
            }
            (ParamType::String, Value::String(s)) => Ok(Token::String(s.to_string())),
            (ParamType::Bool, Value::Bool(b)) => Ok(Token::Bool(*b)),
            (ParamType::Int(_), Value::Int(i)) => Ok(Token::Int((*i).into())),
            (ParamType::Int(_), Value::Int8(i)) => Ok(Token::Int((*i).into())),
            (ParamType::Int(_), Value::BigInt(i)) => Ok(Token::Int(i.to_signed_u256())),
            (ParamType::Uint(_), Value::Int(i)) if *i >= 0 => Ok(Token::Uint((*i).into())),
            (ParamType::Uint(_), Value::BigInt(i)) if i.sign() == Sign::Plus => {
                Ok(Token::Uint(i.to_unsigned_u256()))
            }
            (ParamType::Array(inner_type), Value::List(values)) => {
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
        inner_type: &ParamType,
        param_name: &str,
    ) -> Result<Token, Error> {
        let tokens: Result<Vec<Token>, Error> = values
            .iter()
            .enumerate()
            .map(|(idx, v)| {
                self.convert_entity_value_to_token(v, inner_type, &format!("{param_name}[{idx}]"))
            })
            .collect();
        Ok(Token::Array(tokens?))
    }

    /// Extracts a nested field value from a struct parameter with mixed numeric/named access
    fn extract_nested_struct_field_as_address(
        struct_token: &Token,
        field_accesses: &[usize],
        call_label: &str,
    ) -> Result<H160, Error> {
        let field_token =
            Self::extract_nested_struct_field(struct_token, field_accesses, call_label)?;
        field_token.into_address().ok_or_else(|| {
            anyhow!(
                "In declarative call '{}': nested struct field is not an address",
                call_label
            )
        })
    }

    /// Extracts a nested field value from a struct parameter using numeric indices
    fn extract_nested_struct_field(
        struct_token: &Token,
        field_accesses: &[usize],
        call_label: &str,
    ) -> Result<Token, Error> {
        assert!(
            !field_accesses.is_empty(),
            "Internal error: empty field access path should be caught at parse time"
        );

        let mut current_token = struct_token;

        for (index, &field_index) in field_accesses.iter().enumerate() {
            match current_token {
                Token::Tuple(fields) => {
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

impl<'de> de::Deserialize<'de> for CallDecls {
    fn deserialize<D>(deserializer: D) -> Result<CallDecls, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let decls: std::collections::HashMap<String, String> =
            de::Deserialize::deserialize(deserializer)?;
        let decls = decls
            .into_iter()
            .map(|(name, expr)| {
                expr.parse::<CallExpr>().map(|expr| CallDecl {
                    label: name,
                    expr,
                    readonly: (),
                })
            })
            .collect::<Result<_, _>>()
            .map(|decls| Arc::new(decls))
            .map_err(de::Error::custom)?;
        Ok(CallDecls {
            decls,
            readonly: (),
        })
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
        &self,
        mapping: &MappingABI,
        event_name: Option<&str>,
        spec_version: Option<&semver::Version>,
    ) -> Result<CallDecls, anyhow::Error> {
        let decls: Result<Vec<CallDecl>, anyhow::Error> = self
            .raw_decls
            .iter()
            .map(|(label, expr)| {
                CallExpr::parse(expr, mapping, event_name, spec_version).map(|expr| CallDecl {
                    label: label.clone(),
                    expr,
                    readonly: (),
                })
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
        mapping: &MappingABI,
        event_name: Option<&str>,
        spec_version: Option<&semver::Version>,
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
        let address = CallArg::parse_with_abi(address_str, mapping, event_name, spec_version)
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
                let arg = CallArg::parse_with_abi(arg_str, mapping, event_name, spec_version)
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
impl FromStr for CallExpr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex = Regex::new(
                r"(?x)
                (?P<abi>[a-zA-Z0-9_]+)\[
                    (?P<address>[^]]+)\]
                \.
                (?P<func>[a-zA-Z0-9_]+)\(
                    (?P<args>[^)]*)
                \)"
            )
            .unwrap();
        }
        let x = RE
            .captures(s)
            .ok_or_else(|| anyhow!("invalid call expression `{s}`"))?;
        let abi = Word::from(x.name("abi").unwrap().as_str());
        let address = x.name("address").unwrap().as_str().parse()?;
        let func = Word::from(x.name("func").unwrap().as_str());
        let args: Vec<CallArg> = x
            .name("args")
            .unwrap()
            .as_str()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().parse::<CallArg>())
            .collect::<Result<_, _>>()?;

        let call_expr = CallExpr {
            abi,
            address,
            func,
            args,
            readonly: (),
        };

        // Validate the arguments after constructing the CallExpr
        call_expr.validate_args()?;

        Ok(call_expr)
    }
}

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
    /// The ethabi ParamType for type validation
    pub param_type: ParamType,
}

impl StructFieldInfo {
    /// Create a new StructFieldInfo from ABI JSON components
    pub fn from_components(
        param_name: String,
        param_type: ParamType,
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
        mapping_abi: &MappingABI,
        event_name: Option<&str>,
        spec_version: Option<&semver::Version>,
    ) -> Result<Self, anyhow::Error> {
        // Handle hex addresses first
        if ADDR_RE.is_match(s) {
            if let Ok(parsed_address) = Address::from_str(s) {
                return Ok(CallArg::HexAddress(parsed_address));
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
                    // Resolve field names to indices using ABI context
                    let mut field_indices = Vec::new();

                    // Get struct field info for this event and parameter
                    let struct_field_info = event_name
                        .and_then(|event| mapping_abi.struct_field_mappings.get(event))
                        .and_then(|event_mappings| event_mappings.get(param));

                    for part in remaining_parts {
                        if let Ok(index) = part.parse::<usize>() {
                            // Numeric index - use directly
                            field_indices.push(index);
                        } else {
                            // Named field - validate spec version before resolving
                            if let Some(spec_ver) = spec_version {
                                if spec_ver < &SPEC_VERSION_1_4_0 {
                                    return Err(anyhow!(
                                        "Struct field access by name '{}' in declarative calls is only supported for specVersion >= 1.4.0, current version is {}",
                                        part,
                                        spec_ver
                                    ));
                                }
                            }

                            // Named field - resolve to index using ABI context
                            if let Some(field_info) = struct_field_info {
                                if let Some(field_index) = field_info.resolve_field_name(part) {
                                    field_indices.push(field_index);
                                } else {
                                    // Field name not found - provide helpful error
                                    let available_names = field_info.get_field_names();
                                    return Err(anyhow!(
                                        "Unknown field '{}' in struct parameter '{}' for event '{}'. Available field names: [{}]",
                                        part,
                                        param,
                                        event_name.unwrap_or("unknown"),
                                        available_names.join(", ")
                                    ));
                                }
                            } else {
                                return Err(anyhow!(
                                    "Cannot access field '{}' requires ABI struct information for parameter '{}' in event '{}'",
                                    part,
                                    param,
                                    event_name.unwrap_or("unknown")
                                ));
                            }
                        }
                    }
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

impl FromStr for CallArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if ADDR_RE.is_match(s) {
            if let Ok(parsed_address) = Address::from_str(s) {
                return Ok(CallArg::HexAddress(parsed_address));
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
                    let mut field_accesses = Vec::new();
                    for part in remaining_parts {
                        if let Ok(index) = part.parse::<usize>() {
                            field_accesses.push(index);
                        } else {
                            return Err(anyhow!(
                                "Cannot access field '{}' by name in event.params.{}. Use numeric index instead (e.g., event.params.{}.0, event.params.{}.1, etc.)",
                                part, param, param, param
                            ));
                        }
                    }
                    Ok(CallArg::Ethereum(EthereumArg::StructField(
                        Word::from(param),
                        field_accesses,
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
    function: Function,
    args: Vec<Token>,
}

impl DeclaredCall {
    pub fn from_log_trigger(
        mapping: &dyn FindMappingABI,
        call_decls: &CallDecls,
        log: &Log,
        params: &[LogParam],
    ) -> Result<Vec<DeclaredCall>, anyhow::Error> {
        Self::from_log_trigger_with_event(mapping, call_decls, log, params)
    }

    pub fn from_log_trigger_with_event(
        mapping: &dyn FindMappingABI,
        call_decls: &CallDecls,
        log: &Log,
        params: &[LogParam],
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
    ) -> Result<Vec<DeclaredCall>, anyhow::Error> {
        Self::create_calls(mapping, call_decls, |decl, function| {
            let param_types = function
                .inputs
                .iter()
                .map(|param| param.kind.clone())
                .collect::<Vec<_>>();

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
        F: Fn(&CallDecl, &Function) -> Result<(Address, Vec<Token>), anyhow::Error>,
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
    pub function: Function,
    pub args: Vec<Token>,
    pub gas: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ethereum_call_expr() {
        let expr: CallExpr = "ERC20[event.address].balanceOf(event.params.token)"
            .parse()
            .unwrap();
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(expr.address, CallArg::Ethereum(EthereumArg::Address));
        assert_eq!(expr.func, "balanceOf");
        assert_eq!(
            expr.args,
            vec![CallArg::Ethereum(EthereumArg::Param("token".into()))]
        );

        let expr: CallExpr =
            "Pool[event.params.pool].fees(event.params.token0, event.params.token1)"
                .parse()
                .unwrap();
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
        let expr: CallExpr = "Token[entity.id].symbol()".parse().unwrap();
        assert_eq!(expr.abi, "Token");
        assert_eq!(
            expr.address,
            CallArg::Subgraph(SubgraphArg::EntityParam("id".into()))
        );
        assert_eq!(expr.func, "symbol");
        assert_eq!(expr.args, vec![]);

        let expr: CallExpr = "Pair[entity.pair].getReserves(entity.token0)"
            .parse()
            .unwrap();
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
        let addr = "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF";
        let hex_address = CallArg::HexAddress(web3::types::H160::from_str(addr).unwrap());

        // Test HexAddress in address position
        let expr: CallExpr = format!("Pool[{}].growth()", addr).parse().unwrap();
        assert_eq!(expr.abi, "Pool");
        assert_eq!(expr.address, hex_address.clone());
        assert_eq!(expr.func, "growth");
        assert_eq!(expr.args, vec![]);

        // Test HexAddress in argument position
        let expr: CallExpr = format!("Pool[event.address].approve({}, event.params.amount)", addr)
            .parse()
            .unwrap();
        assert_eq!(expr.abi, "Pool");
        assert_eq!(expr.address, CallArg::Ethereum(EthereumArg::Address));
        assert_eq!(expr.func, "approve");
        assert_eq!(expr.args.len(), 2);
        assert_eq!(expr.args[0], hex_address);
    }

    #[test]
    fn test_invalid_call_args() {
        // Invalid hex address
        assert!("Pool[0xinvalid].test()".parse::<CallExpr>().is_err());

        // Invalid event path
        assert!("Pool[event.invalid].test()".parse::<CallExpr>().is_err());

        // Invalid entity path
        assert!("Pool[entity].test()".parse::<CallExpr>().is_err());

        // Empty address
        assert!("Pool[].test()".parse::<CallExpr>().is_err());

        // Invalid parameter format
        assert!("Pool[event.params].test()".parse::<CallExpr>().is_err());
    }

    #[test]
    fn test_from_str() {
        // Test valid hex address
        let addr = "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF";
        let arg = CallArg::from_str(addr).unwrap();
        assert!(matches!(arg, CallArg::HexAddress(_)));

        // Test Ethereum Address
        let arg = CallArg::from_str("event.address").unwrap();
        assert!(matches!(arg, CallArg::Ethereum(EthereumArg::Address)));

        // Test Ethereum Param
        let arg = CallArg::from_str("event.params.token").unwrap();
        assert!(matches!(arg, CallArg::Ethereum(EthereumArg::Param(_))));

        // Test Subgraph EntityParam
        let arg = CallArg::from_str("entity.token").unwrap();
        assert!(matches!(
            arg,
            CallArg::Subgraph(SubgraphArg::EntityParam(_))
        ));
    }

    #[test]
    fn test_struct_field_access_parsing() {
        // Test struct field access with numeric indices
        let arg = CallArg::from_str("event.params.myStruct.1").unwrap();
        assert!(matches!(
            arg,
            CallArg::Ethereum(EthereumArg::StructField(_, _))
        ));
        if let CallArg::Ethereum(EthereumArg::StructField(param, field_accesses)) = arg {
            assert_eq!(param.as_str(), "myStruct");
            assert_eq!(field_accesses, vec![1]);
        }

        // Test struct field access with index 0
        let arg = CallArg::from_str("event.params.asset.0").unwrap();
        assert!(matches!(
            arg,
            CallArg::Ethereum(EthereumArg::StructField(_, _))
        ));
        if let CallArg::Ethereum(EthereumArg::StructField(param, field_accesses)) = arg {
            assert_eq!(param.as_str(), "asset");
            assert_eq!(field_accesses, vec![0]);
        }
    }

    #[test]
    fn test_struct_field_call_expr_parsing() {
        // Test struct field access with numeric indices: ERC20[event.params.asset.1].name()
        let expr: CallExpr = "ERC20[event.params.asset.1].name()".parse().unwrap();
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![1]))
        );
        assert_eq!(expr.func, "name");
        assert_eq!(expr.args, vec![]);

        // Test struct field access in arguments with numeric indices
        let expr: CallExpr =
            "Contract[event.address].transfer(event.params.data.0, event.params.data.1)"
                .parse()
                .unwrap();
        assert_eq!(expr.abi, "Contract");
        assert_eq!(expr.address, CallArg::Ethereum(EthereumArg::Address));
        assert_eq!(expr.func, "transfer");
        assert_eq!(
            expr.args,
            vec![
                CallArg::Ethereum(EthereumArg::StructField("data".into(), vec![0])),
                CallArg::Ethereum(EthereumArg::StructField("data".into(), vec![1]))
            ]
        );
    }

    #[test]
    fn test_struct_field_access_functions() {
        use ethabi::Token;

        let tuple_fields = vec![
            Token::Uint(ethabi::Uint::from(8u8)),     // index 0: uint8
            Token::Address([1u8; 20].into()),         // index 1: address
            Token::Uint(ethabi::Uint::from(1000u64)), // index 2: uint256
        ];

        // Test extract_struct_field with numeric indices
        let struct_token = Token::Tuple(tuple_fields.clone());

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
        let expr: CallExpr = "ERC20[event.params.asset.1].name()".parse().unwrap();
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![1]))
        );
        assert_eq!(expr.func, "name");
        assert_eq!(expr.args, vec![]);
    }

    #[test]
    fn test_struct_field_access_errors() {
        use ethabi::Token;

        // Test that numeric field syntax parses successfully
        let result = CallArg::from_str("event.params.asset.0");
        assert!(result.is_ok(), "Should parse successfully");

        // Test accessing non-tuple as struct
        let non_tuple = Token::Address([1u8; 20].into());
        let result = CallDecl::extract_nested_struct_field(&non_tuple, &[0], "testCall");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("cannot access field on non-struct/tuple"));

        // Test out of bounds numeric index
        let tuple_fields = vec![Token::Uint(ethabi::Uint::from(123u64))];
        let struct_token = Token::Tuple(tuple_fields);
        let result = CallDecl::extract_nested_struct_field(&struct_token, &[1], "testCall");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'testCall'"));
        assert!(error_msg.contains("out of bounds"));
        assert!(error_msg.contains("struct has 1 fields"));

        // Test out of bounds field access
        let tuple_fields = vec![Token::Address([1u8; 20].into())];
        let struct_token = Token::Tuple(tuple_fields);
        let result = CallDecl::extract_nested_struct_field(
            &struct_token,
            &[1], // Index 1 is out of bounds (only has index 0)
            "testCall",
        );
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'testCall'"));
        assert!(error_msg.contains("out of bounds"));
        assert!(error_msg.contains("struct has 1 fields"));
    }

    #[test]
    fn test_invalid_struct_field_parsing() {
        // Test arbitrary nesting with numeric indices is supported
        let result = CallArg::from_str("event.params.asset.0.1");
        assert!(result.is_ok(), "Arbitrary nesting should now be supported");
        if let Ok(CallArg::Ethereum(EthereumArg::StructField(param, field_accesses))) = result {
            assert_eq!(param.as_str(), "asset");
            assert_eq!(field_accesses.len(), 2);
            assert_eq!(field_accesses[0], 0);
            assert_eq!(field_accesses[1], 1);
        }

        // Test invalid patterns
        let result = CallArg::from_str("event.params");
        assert!(result.is_err());

        let result = CallArg::from_str("event.invalid.param.field");
        assert!(result.is_err());
    }

    #[test]
    fn test_declarative_call_error_context() {
        use crate::prelude::web3::types::{Log, H160, H256};
        use ethabi::{LogParam, Token};

        // Create a test call declaration
        let call_decl = CallDecl {
            label: "myTokenCall".to_string(),
            expr: "ERC20[event.params.asset.1].name()".parse().unwrap(),
            readonly: (),
        };

        // Test scenario 1: Unknown parameter
        let log = Log {
            address: H160::zero(),
            topics: vec![],
            data: vec![].into(),
            block_hash: Some(H256::zero()),
            block_number: Some(1.into()),
            transaction_hash: Some(H256::zero()),
            transaction_index: Some(0.into()),
            log_index: Some(0.into()),
            transaction_log_index: Some(0.into()),
            log_type: None,
            removed: Some(false),
        };
        let params = vec![]; // Empty params - 'asset' param is missing

        let result = call_decl.address_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'myTokenCall'"));
        assert!(error_msg.contains("unknown param asset"));

        // Test scenario 2: Struct field access error
        let params = vec![LogParam {
            name: "asset".to_string(),
            value: Token::Tuple(vec![Token::Uint(ethabi::Uint::from(1u8))]), // Only 1 field, but trying to access index 1
        }];

        let result = call_decl.address_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'myTokenCall'"));
        assert!(error_msg.contains("out of bounds"));
        assert!(error_msg.contains("struct has 1 fields"));

        // Test scenario 3: Non-address field access
        let params = vec![LogParam {
            name: "asset".to_string(),
            value: Token::Tuple(vec![
                Token::Uint(ethabi::Uint::from(1u8)),
                Token::Uint(ethabi::Uint::from(2u8)), // Index 1 is uint, not address
            ]),
        }];

        let result = call_decl.address_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'myTokenCall'"));
        assert!(error_msg.contains("nested struct field is not an address"));

        // Test scenario 4: Out of bounds field index in args_for_log
        let call_decl_with_args = CallDecl {
            label: "transferCall".to_string(),
            expr: "ERC20[event.address].transfer(event.params.data.5)"
                .parse()
                .unwrap(),
            readonly: (),
        };

        let params = vec![LogParam {
            name: "data".to_string(),
            value: Token::Tuple(vec![Token::Address([1u8; 20].into())]),
        }];

        let result = call_decl_with_args.args_for_log(&log, &params);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'transferCall'"));
        assert!(error_msg.contains("out of bounds"));
    }

    #[test]
    fn test_nested_struct_access() {
        // Test accessing deeply nested structs using numeric indices
        // Scenario: event param is a struct containing another struct at index 1, and we want field 0 of that inner struct
        let arg = CallArg::from_str("event.params.outer.1.0").unwrap();
        assert!(matches!(
            arg,
            CallArg::Ethereum(EthereumArg::StructField(_, _))
        ));
        if let CallArg::Ethereum(EthereumArg::StructField(param, path)) = arg {
            assert_eq!(param.as_str(), "outer");
            assert_eq!(path, vec![1, 0]);
        }

        // Test parsing deeply nested struct access
        let expr: CallExpr = "ERC20[event.params.data.2.1].symbol()".parse().unwrap();
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("data".into(), vec![2, 1]))
        );
        assert_eq!(expr.func, "symbol");
    }

    #[test]
    fn test_named_field_access_without_abi() {
        // Test various forms of named field access rejection when no ABI context is available
        let test_cases = vec![
            ("event.params.asset.addr", "addr"),
            ("event.params.data.1.user.id", "user"), // Mixed numeric and named
            ("event.params.transfers.0.recipient", "recipient"), // After numeric index
        ];

        for (input, expected_field) in test_cases {
            let result = CallArg::from_str(input);
            assert!(result.is_err(), "Expected error for input: {}", input);
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains(&format!("Cannot access field '{}' by name", expected_field)),
                "Error message for '{}' should mention field '{}'. Got: {}",
                input,
                expected_field,
                error_msg
            );
        }

        // Test declarative call expressions also reject named access
        let call_test_cases = vec![
            ("ERC20[event.params.token.address].name()", "address"),
            (
                "Contract[event.address].transfer(event.params.transfers.0.recipient)",
                "recipient",
            ),
        ];

        for (input, expected_field) in call_test_cases {
            let result: Result<CallExpr, _> = input.parse();
            assert!(
                result.is_err(),
                "Expected error for call expression: {}",
                input
            );
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains(&format!("Cannot access field '{}' by name", expected_field)),
                "Call expression error for '{}' should mention field '{}'. Got: {}",
                input,
                expected_field,
                error_msg
            );
        }
    }

    #[test]
    fn test_struct_field_extraction_comprehensive() {
        use ethabi::Token;

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
        let inner_struct = Token::Tuple(vec![
            Token::Address([0x42; 20].into()),      // token.addr
            Token::String("TokenName".to_string()), // token.name
        ]);

        let outer_struct = Token::Tuple(vec![
            Token::Uint(ethabi::Uint::from(1u8)),     // asset.kind
            inner_struct,                             // asset.token
            Token::Uint(ethabi::Uint::from(1000u64)), // asset.amount
        ]);

        // Test cases: (path, expected_value, description)
        let test_cases = vec![
            (
                vec![0],
                Token::Uint(ethabi::Uint::from(1u8)),
                "Simple field access",
            ),
            (
                vec![1, 0],
                Token::Address([0x42; 20].into()),
                "Nested field access",
            ),
            (
                vec![1, 1],
                Token::String("TokenName".to_string()),
                "Nested string field",
            ),
            (
                vec![2],
                Token::Uint(ethabi::Uint::from(1000u64)),
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
        let mapping_abi = create_test_mapping_abi();

        // Test 1: Named field resolution with ABI context
        let expr = CallExpr::parse(
            "TestContract[event.params.asset.addr].name()",
            &mapping_abi,
            Some("Transfer"),
            None,
        )
        .unwrap();

        assert_eq!(expr.abi, "TestContract");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::StructField("asset".into(), vec![0])) // addr -> 0
        );
        assert_eq!(expr.func, "name");
        assert_eq!(expr.args, vec![]);

        // Test 2: Mixed named and numeric access in arguments
        let expr = CallExpr::parse(
            "TestContract[event.address].transfer(event.params.asset.amount, event.params.asset.1)",
            &mapping_abi,
            Some("Transfer"),
            None,
        )
        .unwrap();

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
        let mapping_abi = create_test_mapping_abi();

        // Test 1: Invalid field name provides helpful suggestions
        let result = CallExpr::parse(
            "TestContract[event.params.asset.invalid].name()",
            &mapping_abi,
            Some("Transfer"),
            None,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        let error_msg = format!("{:#}", error);
        assert!(error_msg.contains("Unknown field 'invalid'"));
        assert!(error_msg.contains("Available field names: [active, addr, amount]"));

        // Test 2: Named field access without event context
        let result = CallExpr::parse(
            "TestContract[event.params.asset.addr].name()",
            &mapping_abi,
            None, // No event name
            None,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        let error_msg = format!("{:#}", error);
        assert!(error_msg.contains("Cannot access field 'addr' requires ABI struct information"));
    }

    // Helper function to create consistent test ABI
    fn create_test_mapping_abi() -> MappingABI {
        use ethabi::{Contract, ParamType};
        use std::collections::HashMap;

        let abi_json = r#"[
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
            }
        ]"#;

        let contract = Contract::load(abi_json.as_bytes()).unwrap();
        let mut struct_field_mappings = HashMap::new();
        let mut event_mappings = HashMap::new();

        let asset_struct_info = StructFieldInfo {
            param_name: "asset".to_string(),
            field_mappings: {
                let mut mappings = HashMap::new();
                mappings.insert("addr".to_string(), 0);
                mappings.insert("amount".to_string(), 1);
                mappings.insert("active".to_string(), 2);
                mappings
            },
            param_type: ParamType::Tuple(vec![
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Bool,
            ]),
        };

        event_mappings.insert("asset".to_string(), asset_struct_info);
        struct_field_mappings.insert("Transfer".to_string(), event_mappings);

        MappingABI {
            name: "TestContract".to_string(),
            contract,
            struct_field_mappings,
        }
    }

    #[test]
    fn test_parse_function_error_messages() {
        use ethabi::Contract;
        use std::collections::HashMap;

        // Create a minimal ABI for testing
        let abi_json = r#"[{"anonymous": false, "inputs": [], "name": "Test", "type": "event"}]"#;
        let contract = Contract::load(abi_json.as_bytes()).unwrap();
        let mapping_abi = MappingABI {
            name: "TestContract".to_string(),
            contract,
            struct_field_mappings: HashMap::new(),
        };

        // Test 1: Missing opening bracket
        let result = CallExpr::parse(
            "TestContract event.address].test()",
            &mapping_abi,
            None,
            None,
        );
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid call expression"));
        assert!(error_msg.contains("missing '[' after contract name"));

        // Test 2: Missing closing bracket
        let result = CallExpr::parse(
            "TestContract[event.address.test()",
            &mapping_abi,
            None,
            None,
        );
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("missing ']' to close address"));

        // Test 3: Empty contract name
        let result = CallExpr::parse("[event.address].test()", &mapping_abi, None, None);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("missing contract name before '['"));

        // Test 4: Empty address
        let result = CallExpr::parse("TestContract[].test()", &mapping_abi, None, None);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("empty address"));

        // Test 5: Missing function name
        let result = CallExpr::parse("TestContract[event.address].()", &mapping_abi, None, None);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("missing function name"));

        // Test 6: Missing opening parenthesis
        let result = CallExpr::parse("TestContract[event.address].test", &mapping_abi, None, None);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("missing '(' to start function arguments"));

        // Test 7: Missing closing parenthesis
        let result = CallExpr::parse(
            "TestContract[event.address].test(",
            &mapping_abi,
            None,
            None,
        );
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("missing ')' to close function arguments"));

        // Test 8: Invalid argument should show argument position
        let result = CallExpr::parse(
            "TestContract[event.address].test(invalid.arg)",
            &mapping_abi,
            None,
            None,
        );
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Failed to parse argument 1"));
        assert!(error_msg.contains("'invalid.arg'"));
    }
}

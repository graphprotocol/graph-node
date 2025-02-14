use crate::abi;
use crate::abi::DynSolValueExt;
use crate::abi::FunctionExt;
use crate::blockchain::block_stream::EntitySourceOperation;
use crate::prelude::{BlockPtr, Value};
use crate::{components::link_resolver::LinkResolver, data::value::Word, prelude::Link};
use anyhow::{anyhow, Context, Error};
use graph_derive::CheapClone;
use lazy_static::lazy_static;
use num_bigint::Sign;
use regex::Regex;
use serde::de;
use serde::Deserialize;
use slog::Logger;
use std::{str::FromStr, sync::Arc};
use web3::types::Address;
use web3::types::{Log, H160};

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
        let contract = serde_json::from_slice(&*contract_bytes)
            .with_context(|| format!("failed to load ABI {}", self.name))?;
        Ok(MappingABI {
            name: self.name,
            contract,
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

    pub fn address_for_log(&self, log: &Log, params: &[abi::DynSolParam]) -> Result<H160, Error> {
        let address = match &self.expr.address {
            CallArg::HexAddress(address) => *address,
            CallArg::Ethereum(arg) => match arg {
                EthereumArg::Address => log.address,
                EthereumArg::Param(name) => {
                    let value = &params
                        .iter()
                        .find(|param| &param.name == name.as_str())
                        .ok_or_else(|| anyhow!("unknown param '{name}'"))?
                        .value;

                    let address = value
                        .as_address()
                        .ok_or_else(|| anyhow!("param '{name}' is not an address"))?;

                    Address::from(address.into_array())
                }
            },
            CallArg::Subgraph(_) => {
                return Err(anyhow!(
                    "Subgraph params are not supported for when declaring calls for event handlers"
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
        use abi::DynSolValue;

        self.expr
            .args
            .iter()
            .map(|arg| match arg {
                CallArg::HexAddress(address) => {
                    Ok(DynSolValue::Address(address.to_fixed_bytes().into()))
                }
                CallArg::Ethereum(arg) => match arg {
                    EthereumArg::Address => {
                        Ok(DynSolValue::Address(log.address.to_fixed_bytes().into()))
                    }
                    EthereumArg::Param(name) => {
                        let value = params
                            .iter()
                            .find(|param| &param.name == name.as_str())
                            .ok_or_else(|| anyhow!("unknown param {name}"))?
                            .value
                            .clone();
                        Ok(value)
                    }
                },
                CallArg::Subgraph(_) => Err(anyhow!(
                    "Subgraph params are not supported for when declaring calls for event handlers"
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
        address: H160,
        expected_type: &abi::DynSolType,
    ) -> Result<abi::DynSolValue, Error> {
        match expected_type {
            abi::DynSolType::Address => {
                Ok(abi::DynSolValue::Address(address.to_fixed_bytes().into()))
            }
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
                let x = abi::I256::from_limbs(i.to_signed_u256().0);
                Ok(DynSolValue::Int(x, x.bits() as usize))
            }
            (DynSolType::Uint(_), Value::Int(i)) if *i >= 0 => {
                let x = abi::U256::try_from(*i)?;
                Ok(DynSolValue::Uint(x, x.bit_len()))
            }
            (DynSolType::Uint(_), Value::BigInt(i)) if i.sign() == Sign::Plus => {
                let x = abi::U256::from_limbs(i.to_unsigned_u256().0);
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

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum EthereumArg {
    Address,
    Param(Word),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum SubgraphArg {
    EntityParam(Word),
}

lazy_static! {
    // Matches a 40-character hexadecimal string prefixed with '0x', typical for Ethereum addresses
    static ref ADDR_RE: Regex = Regex::new(r"^0x[0-9a-fA-F]{40}$").unwrap();
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
                Ok(CallArg::Ethereum(EthereumArg::Param(Word::from(param))))
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
        log: &Log,
        params: &[abi::DynSolParam],
    ) -> Result<Vec<DeclaredCall>, Error> {
        Self::create_calls(mapping, call_decls, |decl, _| {
            Ok((
                decl.address_for_log(log, params)?,
                decl.args_for_log(log, params)?,
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
}

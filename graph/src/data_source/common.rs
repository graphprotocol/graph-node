use crate::blockchain::block_stream::EntitySourceOperation;
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
use slog::Logger;
use std::{str::FromStr, sync::Arc};
use web3::types::{Log, H160};

#[derive(Clone, Debug, PartialEq)]
pub struct MappingABI {
    pub name: String,
    pub contract: Contract,
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

    pub fn address_for_log(&self, log: &Log, params: &[LogParam]) -> Result<H160, Error> {
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
                EthereumArg::ParamField(param_name, field_name) => {
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

                    Self::extract_struct_field_as_address(&param.value, field_name, &self.label)?
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
                    EthereumArg::ParamField(param_name, field_name) => {
                        let param = params
                            .iter()
                            .find(|param| &param.name == param_name.as_str())
                            .ok_or_else(|| anyhow!("In declarative call '{}': unknown param {}", self.label, param_name))?;

                        Self::extract_struct_field(&param.value, field_name, &self.label)
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

    /// Extracts a field value from a struct parameter and converts it to an address
    fn extract_struct_field_as_address(
        struct_token: &Token,
        field_name: &str,
        call_label: &str,
    ) -> Result<H160, Error> {
        let field_token = Self::extract_struct_field(struct_token, field_name, call_label)?;
        field_token.into_address().ok_or_else(|| {
            anyhow!(
                "In declarative call '{}': struct field {} is not an address",
                call_label,
                field_name
            )
        })
    }

    /// Extracts a field value from a struct parameter using numeric index
    fn extract_struct_field(
        struct_token: &Token,
        field_index_str: &str,
        call_label: &str,
    ) -> Result<Token, Error> {
        match struct_token {
            Token::Tuple(fields) => {
                // Parse field name as numeric index (like "0", "1", "2")
                let field_index = field_index_str.parse::<usize>().map_err(|_| {
                    anyhow!(
                        "In declarative call '{}': struct field '{}' must be a numeric index (0, 1, 2, ...)",
                        call_label, field_index_str
                    )
                })?;

                fields
                    .get(field_index)
                    .ok_or_else(|| {
                        anyhow!(
                            "In declarative call '{}': struct field index {} out of bounds (struct has {} fields)",
                            call_label, field_index, fields.len()
                        )
                    })
                    .map(|token| token.clone())
            }
            _ => Err(anyhow!(
                "In declarative call '{}': parameter is not a struct/tuple, cannot access field {}",
                call_label,
                field_index_str
            )),
        }
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
    ParamField(Word, Word),
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
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("event"), Some("address"), None, None) => {
                Ok(CallArg::Ethereum(EthereumArg::Address))
            }
            (Some("event"), Some("params"), Some(param), None) => {
                Ok(CallArg::Ethereum(EthereumArg::Param(Word::from(param))))
            }
            (Some("event"), Some("params"), Some(param), Some(field)) if parts.next().is_none() => {
                Ok(CallArg::Ethereum(EthereumArg::ParamField(
                    Word::from(param),
                    Word::from(field),
                )))
            }
            (Some("entity"), Some(param), None, None) => Ok(CallArg::Subgraph(
                SubgraphArg::EntityParam(Word::from(param)),
            )),
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
            CallArg::Ethereum(EthereumArg::ParamField(_, _))
        ));
        if let CallArg::Ethereum(EthereumArg::ParamField(param, field)) = arg {
            assert_eq!(param.as_str(), "myStruct");
            assert_eq!(field.as_str(), "1");
        }

        // Test struct field access with index 0
        let arg = CallArg::from_str("event.params.asset.0").unwrap();
        assert!(matches!(
            arg,
            CallArg::Ethereum(EthereumArg::ParamField(_, _))
        ));
        if let CallArg::Ethereum(EthereumArg::ParamField(param, field)) = arg {
            assert_eq!(param.as_str(), "asset");
            assert_eq!(field.as_str(), "0");
        }
    }

    #[test]
    fn test_struct_field_call_expr_parsing() {
        // Test struct field access with numeric indices: ERC20[event.params.asset.1].name()
        let expr: CallExpr = "ERC20[event.params.asset.1].name()".parse().unwrap();
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::ParamField("asset".into(), "1".into()))
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
                CallArg::Ethereum(EthereumArg::ParamField("data".into(), "0".into())),
                CallArg::Ethereum(EthereumArg::ParamField("data".into(), "1".into()))
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
        let result = CallDecl::extract_struct_field(&struct_token, "0", "testCall").unwrap();
        assert!(matches!(result, Token::Uint(_)));

        // Test accessing index 1 (address)
        let result = CallDecl::extract_struct_field(&struct_token, "1", "testCall").unwrap();
        assert!(matches!(result, Token::Address(_)));

        // Test accessing index 2 (uint256)
        let result = CallDecl::extract_struct_field(&struct_token, "2", "testCall").unwrap();
        assert!(matches!(result, Token::Uint(_)));
    }

    #[test]
    fn test_struct_field_access_errors() {
        use ethabi::Token;

        // Test accessing non-tuple as struct
        let non_tuple = Token::Address([1u8; 20].into());
        let result = CallDecl::extract_struct_field(&non_tuple, "0", "testCall");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("parameter is not a struct/tuple"));

        // Test out of bounds numeric index
        let tuple_fields = vec![Token::Uint(ethabi::Uint::from(123u64))];
        let struct_token = Token::Tuple(tuple_fields);
        let result = CallDecl::extract_struct_field(&struct_token, "1", "testCall");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));

        // Test invalid field name (non-numeric)
        let tuple_fields = vec![Token::Address([1u8; 20].into())];
        let struct_token = Token::Tuple(tuple_fields);
        let result = CallDecl::extract_struct_field(&struct_token, "addr", "testCall");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be a numeric index"));

        // Test invalid field name (non-numeric)
        let tuple_fields = vec![Token::Address([1u8; 20].into())];
        let struct_token = Token::Tuple(tuple_fields);
        let result = CallDecl::extract_struct_field(&struct_token, "unknown", "testCall");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be a numeric index"));
    }

    #[test]
    fn test_invalid_struct_field_parsing() {
        // Test too many parts (more than 4)
        let result = CallArg::from_str("event.params.asset.addr.extra");
        assert!(result.is_err());

        // Test invalid patterns
        let result = CallArg::from_str("event.params");
        assert!(result.is_err());

        let result = CallArg::from_str("event.invalid.param.field");
        assert!(result.is_err());
    }

    #[test]
    fn test_helpful_error_messages() {
        // Test that users get helpful error messages when they use the old syntax
        let result = CallArg::from_str("event.params.asset.addr");
        assert!(result.is_ok(), "Should parse successfully");

        // Test error message when using invalid field names
        use ethabi::Token;
        let struct_token = Token::Tuple(vec![Token::Address([1u8; 20].into())]);
        let result = CallDecl::extract_struct_field(&struct_token, "addr", "testCall");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'testCall'"));
        assert!(error_msg.contains("must be a numeric index"));
        assert!(error_msg.contains("0, 1, 2"));

        // Test helpful error for out of bounds
        let result = CallDecl::extract_struct_field(&struct_token, "5", "testCall");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("In declarative call 'testCall'"));
        assert!(error_msg.contains("out of bounds"));
        assert!(error_msg.contains("struct has 1 fields"));
    }

    #[test]
    fn test_struct_field_extraction_with_mixed_types() {
        // Test extracting fields from a struct with mixed types: (uint8, address, uint256)
        use ethabi::Token;

        // Create a mock struct that represents (uint8, address, uint256)
        let mixed_struct = Token::Tuple(vec![
            Token::Uint(ethabi::Uint::from(1u8)), // uint8 field at index 0
            Token::Address([0x42; 20].into()),    // address field at index 1
            Token::Uint(ethabi::Uint::from(1000000u64)), // uint256 field at index 2
        ]);

        // Test extracting each field by its numeric index
        let extracted = CallDecl::extract_struct_field(&mixed_struct, "0", "testCall").unwrap();
        assert!(matches!(extracted, Token::Uint(_)));

        let extracted = CallDecl::extract_struct_field(&mixed_struct, "1", "testCall").unwrap();
        assert!(matches!(extracted, Token::Address(_)));
        if let Token::Address(addr) = extracted {
            assert_eq!(addr, [0x42; 20].into());
        }

        let extracted = CallDecl::extract_struct_field(&mixed_struct, "2", "testCall").unwrap();
        assert!(matches!(extracted, Token::Uint(_)));

        // Test that it works in a declarative call context
        let expr: CallExpr = "ERC20[event.params.asset.1].name()".parse().unwrap();
        assert_eq!(expr.abi, "ERC20");
        assert_eq!(
            expr.address,
            CallArg::Ethereum(EthereumArg::ParamField("asset".into(), "1".into()))
        );
        assert_eq!(expr.func, "name");
        assert_eq!(expr.args, vec![]);
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
        assert!(error_msg.contains("struct field 1 is not an address"));

        // Test scenario 4: Invalid field name in args_for_log
        let call_decl_with_args = CallDecl {
            label: "transferCall".to_string(),
            expr: "ERC20[event.address].transfer(event.params.data.invalid)"
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
        assert!(error_msg.contains("must be a numeric index"));
    }
}

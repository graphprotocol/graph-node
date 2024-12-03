use crate::{components::link_resolver::LinkResolver, data::value::Word, prelude::Link};
use anyhow::{anyhow, Context, Error};
use ethabi::{Address, Contract, Function, LogParam, Token};
use graph_derive::CheapClone;
use lazy_static::lazy_static;
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
        let contract = Contract::load(&*contract_bytes)?;
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
    pub fn address(&self, log: &Log, params: &[LogParam]) -> Result<H160, Error> {
        let address = match &self.expr.address {
            CallArg::Address => log.address,
            CallArg::HexAddress(address) => *address,
            CallArg::Param(name) => {
                let value = params
                    .iter()
                    .find(|param| &param.name == name.as_str())
                    .ok_or_else(|| anyhow!("unknown param {name}"))?
                    .value
                    .clone();
                value
                    .into_address()
                    .ok_or_else(|| anyhow!("param {name} is not an address"))?
            }
        };
        Ok(address)
    }

    pub fn args(&self, log: &Log, params: &[LogParam]) -> Result<Vec<Token>, Error> {
        self.expr
            .args
            .iter()
            .map(|arg| match arg {
                CallArg::Address => Ok(Token::Address(log.address)),
                CallArg::HexAddress(address) => Ok(Token::Address(*address)),
                CallArg::Param(name) => {
                    let value = params
                        .iter()
                        .find(|param| &param.name == name.as_str())
                        .ok_or_else(|| anyhow!("unknown param {name}"))?
                        .value
                        .clone();
                    Ok(value)
                }
            })
            .collect()
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
        Ok(CallExpr {
            abi,
            address,
            func,
            args,
            readonly: (),
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum CallArg {
    HexAddress(Address),
    Address,
    Param(Word),
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
            (Some("event"), Some("address"), None) => Ok(CallArg::Address),
            (Some("event"), Some("params"), Some(param)) => Ok(CallArg::Param(Word::from(param))),
            _ => Err(anyhow!("invalid call argument `{}`", s)),
        }
    }
}

pub trait FindMappingABI {
    fn find_abi(&self, abi_name: &str) -> Result<Arc<MappingABI>, Error>;
}

#[test]
fn test_call_expr() {
    let expr: CallExpr = "ERC20[event.address].balanceOf(event.params.token)"
        .parse()
        .unwrap();
    assert_eq!(expr.abi, "ERC20");
    assert_eq!(expr.address, CallArg::Address);
    assert_eq!(expr.func, "balanceOf");
    assert_eq!(expr.args, vec![CallArg::Param("token".into())]);

    let expr: CallExpr = "Pool[event.params.pool].fees(event.params.token0, event.params.token1)"
        .parse()
        .unwrap();
    assert_eq!(expr.abi, "Pool");
    assert_eq!(expr.address, CallArg::Param("pool".into()));
    assert_eq!(expr.func, "fees");
    assert_eq!(
        expr.args,
        vec![
            CallArg::Param("token0".into()),
            CallArg::Param("token1".into())
        ]
    );

    let expr: CallExpr = "Pool[event.address].growth()".parse().unwrap();
    assert_eq!(expr.abi, "Pool");
    assert_eq!(expr.address, CallArg::Address);
    assert_eq!(expr.func, "growth");
    assert_eq!(expr.args, vec![]);

    let expr: CallExpr = "Pool[0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF].growth(0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF)"
        .parse()
        .unwrap();
    let call_arg =
        CallArg::HexAddress(H160::from_str("0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF").unwrap());
    assert_eq!(expr.abi, "Pool");
    assert_eq!(expr.address, call_arg);
    assert_eq!(expr.func, "growth");
    assert_eq!(expr.args, vec![call_arg]);
}

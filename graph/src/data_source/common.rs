use crate::{components::link_resolver::LinkResolver, prelude::Link};
use anyhow::{Context, Error};
use ethabi::{Contract, Function};
use serde::Deserialize;
use slog::Logger;
use std::sync::Arc;

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

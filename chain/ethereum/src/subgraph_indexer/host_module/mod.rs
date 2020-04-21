use std::time::Instant;

use ethabi::Token;

use graph::prelude::{block_on, debug, ethabi, info, EthereumContractCall};
use graph_runtime_wasm::{
  host_module, AscHeap, AscPtr, AscType, FromAscObj, HostModuleError, WasmiModule,
};

mod asc;
use asc::*;

pub struct EthereumModule {}

impl EthereumModule {
  pub fn new() -> Self {
    Self {}
  }
}

host_module! {
  EthereumModule,
  ETHEREUM_MODULE_FUNCS,
  ethereum,
  {
    call => call [0, 1],
  }
}

impl EthereumModule {
  fn call(
    &self,
    module: &mut WasmiModule,
    call_ptr: u32,
    // block: &LightEthereumBlock,
    // unresolved_call: UnresolvedContractCall,
  ) -> Result<Option<Vec<Token>>, HostModuleError> {
    let unresolved_call = if module.ctx.api_version >= Version::new(0, 0, 4) {
      module.asc_get::<_, AscUnresolvedContractCall_0_0_4>(call_ptr)
    } else {
      module.asc_get::<_, AscUnresolvedContractCall>(call_ptr)
    };

    let start_time = Instant::now();

    // Obtain the path to the contract ABI
    let contract = self
      .abis
      .iter()
      .find(|abi| abi.name == unresolved_call.contract_name)
      .ok_or_else(|| {
        HostModuleError(format_err!(
          "Could not find ABI for contract \"{}\", try adding it to the 'abis' section \
                 of the subgraph manifest",
          unresolved_call.contract_name
        ))
      })?
      .contract
      .clone();

    let function = match unresolved_call.function_signature {
      // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
      // functions this always picks the same overloaded variant, which is incorrect
      // and may lead to encoding/decoding errors
      None => contract
        .function(unresolved_call.function_name.as_str())
        .map_err(|e| {
          HostModuleError(format_err!(
            "Unknown function \"{}::{}\" called from WASM runtime: {}",
            unresolved_call.contract_name,
            unresolved_call.function_name,
            e
          ))
        })?,

      // Behavior for apiVersion >= 0.0.04: look up function by signature of
      // the form `functionName(uint256,string) returns (bytes32,string)`; this
      // correctly picks the correct variant of an overloaded function
      Some(ref function_signature) => contract
        .functions_by_name(unresolved_call.function_name.as_str())
        .map_err(|e| {
          HostModuleError(format_err!(
            "Unknown function \"{}::{}\" called from WASM runtime: {}",
            unresolved_call.contract_name,
            unresolved_call.function_name,
            e
          ))
        })?
        .iter()
        .find(|f| function_signature == &f.signature())
        .ok_or_else(|| {
          HostModuleError(format_err!(
            "Unknown function \"{}::{}\" with signature `{}` \
                     called from WASM runtime",
            unresolved_call.contract_name,
            unresolved_call.function_name,
            function_signature,
          ))
        })?,
    };

    let call = EthereumContractCall {
      address: unresolved_call.contract_address.clone(),
      block_ptr: block.into(),
      function: function.clone(),
      args: unresolved_call.function_args.clone(),
    };

    // Run Ethereum call in tokio runtime
    let eth_adapter = self.ethereum_adapter.clone();
    let logger1 = module.ctx.logger.clone();
    let call_cache = self.call_cache.clone();
    let result = match graph::block_on_allow_panic(future::lazy(move || {
      eth_adapter.contract_call(&logger1, call, call_cache)
    })) {
      Ok(tokens) => Ok(Some(tokens)),
      Err(EthereumContractCallError::Revert(reason)) => {
        info!(module.ctx.logger, "Contract call reverted"; "reason" => reason);
        Ok(None)
      }
      Err(e) => Err(HostModuleError(format_err!(
        "Failed to call function \"{}\" of contract \"{}\": {}",
        unresolved_call.function_name,
        unresolved_call.contract_name,
        e
      ))),
    };

    debug!(module.ctx.logger, "Contract call finished";
          "address" => &unresolved_call.contract_address.to_string(),
          "contract" => &unresolved_call.contract_name,
          "function" => &unresolved_call.function_name,
          "function_signature" => &unresolved_call.function_signature,
          "time" => format!("{}ms", start_time.elapsed().as_millis()));

    result
  }
}

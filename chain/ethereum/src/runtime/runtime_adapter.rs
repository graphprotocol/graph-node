use std::{sync::Arc, time::Instant};

use crate::adapter::EthereumRpcError;
use crate::data_source::MappingABI;
use crate::{
    capabilities::NodeCapabilities, network::EthereumNetworkAdapters, Chain, ContractCall,
    ContractCallError, DataSource, EthereumAdapter, EthereumAdapterTrait, ENV_VARS,
};
use anyhow::{anyhow, Context, Error};
use blockchain::HostFn;
use graph::blockchain::ChainIdentifier;
use graph::components::subgraph::HostMetrics;
use graph::data::store::ethereum::call;
use graph::data::store::scalar::BigInt;
use graph::data::subgraph::API_VERSION_0_0_9;
use graph::futures03::compat::Future01CompatExt;
use graph::prelude::web3::types::H160;
use graph::runtime::gas::Gas;
use graph::runtime::{AscIndexId, IndexForAscTypeId};
use graph::slog::debug;
use graph::{
    blockchain::{self, BlockPtr, HostFnCtx},
    cheap_clone::CheapClone,
    prelude::{
        ethabi::{self, Address, Token},
        EthereumCallCache,
    },
    runtime::{asc_get, asc_new, AscPtr, HostExportError},
    semver::Version,
    slog::Logger,
};
use graph_runtime_wasm::asc_abi::class::{AscBigInt, AscEnumArray, AscWrapped, EthereumValueKind};
use itertools::Itertools;

use super::abi::{AscUnresolvedContractCall, AscUnresolvedContractCall_0_0_4};

/// Gas limit for `eth_call`. The value of 50_000_000 is a protocol-wide parameter so this
/// should be changed only for debugging purposes and never on an indexer in the network. This
/// value was chosen because it is the Geth default
/// https://github.com/ethereum/go-ethereum/blob/e4b687cf462870538743b3218906940ae590e7fd/eth/ethconfig/config.go#L91.
/// It is not safe to set something higher because Geth will silently override the gas limit
/// with the default. This means that we do not support indexing against a Geth node with
/// `RPCGasCap` set below 50 million.
// See also f0af4ab0-6b7c-4b68-9141-5b79346a5f61.
const ETH_CALL_GAS: u32 = 50_000_000;

// When making an ethereum call, the maximum ethereum gas is ETH_CALL_GAS which is 50 million. One
// unit of Ethereum gas is at least 100ns according to these benchmarks [1], so 1000 of our gas. In
// the worst case an Ethereum call could therefore consume 50 billion of our gas. However the
// averarge call a subgraph makes is much cheaper or even cached in the call cache. So this cost is
// set to 5 billion gas as a compromise. This allows for 2000 calls per handler with the current
// limits.
//
// [1] - https://www.sciencedirect.com/science/article/abs/pii/S0166531620300900
pub const ETHEREUM_CALL: Gas = Gas::new(5_000_000_000);

// TODO: Determine the appropriate gas cost for `ETH_GET_BALANCE`, initially aligned with `ETHEREUM_CALL`.
pub const ETH_GET_BALANCE: Gas = Gas::new(5_000_000_000);

// TODO: Determine the appropriate gas cost for `ETH_HAS_CODE`, initially aligned with `ETHEREUM_CALL`.
pub const ETH_HAS_CODE: Gas = Gas::new(5_000_000_000);

pub struct RuntimeAdapter {
    pub eth_adapters: Arc<EthereumNetworkAdapters>,
    pub call_cache: Arc<dyn EthereumCallCache>,
    pub chain_identifier: Arc<ChainIdentifier>,
}

pub fn eth_call_gas(chain_identifier: &ChainIdentifier) -> Option<u32> {
    // Check if the current network version is in the eth_call_no_gas list
    let should_skip_gas = ENV_VARS
        .eth_call_no_gas
        .contains(&chain_identifier.net_version);

    if should_skip_gas {
        None
    } else {
        Some(ETH_CALL_GAS)
    }
}

impl blockchain::RuntimeAdapter<Chain> for RuntimeAdapter {
    fn host_fns(&self, ds: &DataSource) -> Result<Vec<HostFn>, Error> {
        let abis = ds.mapping.abis.clone();
        let call_cache = self.call_cache.cheap_clone();
        let eth_adapters = self.eth_adapters.cheap_clone();
        let archive = ds.mapping.requires_archive()?;
        let eth_call_gas = eth_call_gas(&self.chain_identifier);

        let ethereum_call = HostFn {
            name: "ethereum.call",
            func: Arc::new(move |ctx, wasm_ptr| {
                // Ethereum calls should prioritise call-only adapters if one is available.
                let eth_adapter = eth_adapters.call_or_cheapest(Some(&NodeCapabilities {
                    archive,
                    traces: false,
                }))?;
                ethereum_call(
                    &eth_adapter,
                    call_cache.cheap_clone(),
                    ctx,
                    wasm_ptr,
                    &abis,
                    eth_call_gas,
                )
                .map(|ptr| ptr.wasm_ptr())
            }),
        };

        let eth_adapters = self.eth_adapters.cheap_clone();
        let ethereum_get_balance = HostFn {
            name: "ethereum.getBalance",
            func: Arc::new(move |ctx, wasm_ptr| {
                let eth_adapter = eth_adapters.unverified_cheapest_with(&NodeCapabilities {
                    archive,
                    traces: false,
                })?;
                eth_get_balance(&eth_adapter, ctx, wasm_ptr).map(|ptr| ptr.wasm_ptr())
            }),
        };

        let eth_adapters = self.eth_adapters.cheap_clone();
        let ethereum_get_code = HostFn {
            name: "ethereum.hasCode",
            func: Arc::new(move |ctx, wasm_ptr| {
                let eth_adapter = eth_adapters.unverified_cheapest_with(&NodeCapabilities {
                    archive,
                    traces: false,
                })?;
                eth_has_code(&eth_adapter, ctx, wasm_ptr).map(|ptr| ptr.wasm_ptr())
            }),
        };

        Ok(vec![ethereum_call, ethereum_get_balance, ethereum_get_code])
    }
}

/// function ethereum.call(call: SmartContractCall): Array<Token> | null
fn ethereum_call(
    eth_adapter: &EthereumAdapter,
    call_cache: Arc<dyn EthereumCallCache>,
    ctx: HostFnCtx,
    wasm_ptr: u32,
    abis: &[Arc<MappingABI>],
    eth_call_gas: Option<u32>,
) -> Result<AscEnumArray<EthereumValueKind>, HostExportError> {
    ctx.gas
        .consume_host_fn_with_metrics(ETHEREUM_CALL, "ethereum_call")?;

    // For apiVersion >= 0.0.4 the call passed from the mapping includes the
    // function signature; subgraphs using an apiVersion < 0.0.4 don't pass
    // the signature along with the call.
    let call: UnresolvedContractCall = if ctx.heap.api_version() >= Version::new(0, 0, 4) {
        asc_get::<_, AscUnresolvedContractCall_0_0_4, _>(ctx.heap, wasm_ptr.into(), &ctx.gas, 0)?
    } else {
        asc_get::<_, AscUnresolvedContractCall, _>(ctx.heap, wasm_ptr.into(), &ctx.gas, 0)?
    };

    let result = eth_call(
        eth_adapter,
        call_cache,
        &ctx.logger,
        &ctx.block_ptr,
        call,
        abis,
        eth_call_gas,
        ctx.metrics.cheap_clone(),
    )?;
    match result {
        Some(tokens) => Ok(asc_new(ctx.heap, tokens.as_slice(), &ctx.gas)?),
        None => Ok(AscPtr::null()),
    }
}

fn eth_get_balance(
    eth_adapter: &EthereumAdapter,
    ctx: HostFnCtx<'_>,
    wasm_ptr: u32,
) -> Result<AscPtr<AscBigInt>, HostExportError> {
    ctx.gas
        .consume_host_fn_with_metrics(ETH_GET_BALANCE, "eth_get_balance")?;

    if ctx.heap.api_version() < API_VERSION_0_0_9 {
        return Err(HostExportError::Deterministic(anyhow!(
            "ethereum.getBalance call is not supported before API version 0.0.9"
        )));
    }

    let logger = &ctx.logger;
    let block_ptr = &ctx.block_ptr;

    let address: H160 = asc_get(ctx.heap, wasm_ptr.into(), &ctx.gas, 0)?;

    let result = graph::block_on(
        eth_adapter
            .get_balance(logger, address, block_ptr.clone())
            .compat(),
    );

    match result {
        Ok(v) => {
            let bigint = BigInt::from_unsigned_u256(&v);
            Ok(asc_new(ctx.heap, &bigint, &ctx.gas)?)
        }
        // Retry on any kind of error
        Err(EthereumRpcError::Web3Error(e)) => Err(HostExportError::PossibleReorg(e.into())),
        Err(EthereumRpcError::Timeout) => Err(HostExportError::PossibleReorg(
            EthereumRpcError::Timeout.into(),
        )),
    }
}

fn eth_has_code(
    eth_adapter: &EthereumAdapter,
    ctx: HostFnCtx<'_>,
    wasm_ptr: u32,
) -> Result<AscPtr<AscWrapped<bool>>, HostExportError> {
    ctx.gas
        .consume_host_fn_with_metrics(ETH_HAS_CODE, "eth_has_code")?;

    if ctx.heap.api_version() < API_VERSION_0_0_9 {
        return Err(HostExportError::Deterministic(anyhow!(
            "ethereum.hasCode call is not supported before API version 0.0.9"
        )));
    }

    let logger = &ctx.logger;
    let block_ptr = &ctx.block_ptr;

    let address: H160 = asc_get(ctx.heap, wasm_ptr.into(), &ctx.gas, 0)?;

    let result = graph::block_on(
        eth_adapter
            .get_code(logger, address, block_ptr.clone())
            .compat(),
    )
    .map(|v| !v.0.is_empty());

    match result {
        Ok(v) => Ok(asc_new(ctx.heap, &AscWrapped { inner: v }, &ctx.gas)?),
        // Retry on any kind of error
        Err(EthereumRpcError::Web3Error(e)) => Err(HostExportError::PossibleReorg(e.into())),
        Err(EthereumRpcError::Timeout) => Err(HostExportError::PossibleReorg(
            EthereumRpcError::Timeout.into(),
        )),
    }
}

/// Returns `Ok(None)` if the call was reverted.
fn eth_call(
    eth_adapter: &EthereumAdapter,
    call_cache: Arc<dyn EthereumCallCache>,
    logger: &Logger,
    block_ptr: &BlockPtr,
    unresolved_call: UnresolvedContractCall,
    abis: &[Arc<MappingABI>],
    eth_call_gas: Option<u32>,
    metrics: Arc<HostMetrics>,
) -> Result<Option<Vec<Token>>, HostExportError> {
    // Helpers to log the result of the call at the end
    fn tokens_as_string(tokens: &[Token]) -> String {
        tokens.iter().map(|arg| arg.to_string()).join(", ")
    }

    fn result_as_string(result: &Result<Option<Vec<Token>>, HostExportError>) -> String {
        match result {
            Ok(Some(tokens)) => format!("({})", tokens_as_string(&tokens)),
            Ok(None) => "none".to_string(),
            Err(_) => "error".to_string(),
        }
    }

    let start_time = Instant::now();

    // Obtain the path to the contract ABI
    let abi = abis
        .iter()
        .find(|abi| abi.name == unresolved_call.contract_name)
        .with_context(|| {
            format!(
                "Could not find ABI for contract \"{}\", try adding it to the 'abis' section \
                     of the subgraph manifest",
                unresolved_call.contract_name
            )
        })
        .map_err(HostExportError::Deterministic)?;

    let function = abi
        .function(
            &unresolved_call.contract_name,
            &unresolved_call.function_name,
            unresolved_call.function_signature.as_deref(),
        )
        .map_err(HostExportError::Deterministic)?;

    let call = ContractCall {
        contract_name: unresolved_call.contract_name.clone(),
        address: unresolved_call.contract_address,
        block_ptr: block_ptr.cheap_clone(),
        function: function.clone(),
        args: unresolved_call.function_args.clone(),
        gas: eth_call_gas,
    };

    // Run Ethereum call in tokio runtime
    let logger1 = logger.clone();
    let call_cache = call_cache.clone();
    let (result, source) =
        match graph::block_on(eth_adapter.contract_call(&logger1, &call, call_cache)) {
            Ok((result, source)) => (Ok(result), source),
            Err(e) => (Err(e), call::Source::Rpc),
        };
    let result = match result {
            Ok(res) => Ok(res),

            // Any error reported by the Ethereum node could be due to the block no longer being on
            // the main chain. This is very unespecific but we don't want to risk failing a
            // subgraph due to a transient error such as a reorg.
            Err(ContractCallError::Web3Error(e)) => Err(HostExportError::PossibleReorg(anyhow::anyhow!(
                "Ethereum node returned an error when calling function \"{}\" of contract \"{}\": {}",
                unresolved_call.function_name,
                unresolved_call.contract_name,
                e
            ))),

            // Also retry on timeouts.
            Err(ContractCallError::Timeout) => Err(HostExportError::PossibleReorg(anyhow::anyhow!(
                "Ethereum node did not respond when calling function \"{}\" of contract \"{}\"",
                unresolved_call.function_name,
                unresolved_call.contract_name,
            ))),

            Err(e) => Err(HostExportError::Unknown(anyhow::anyhow!(
                "Failed to call function \"{}\" of contract \"{}\": {}",
                unresolved_call.function_name,
                unresolved_call.contract_name,
                e
            ))),
        };

    let elapsed = start_time.elapsed();

    if source.observe() {
        metrics.observe_eth_call_execution_time(
            elapsed.as_secs_f64(),
            &unresolved_call.contract_name,
            &unresolved_call.function_name,
        );
    }

    debug!(logger, "Contract call finished";
              "address" => format!("0x{:x}", &unresolved_call.contract_address),
              "contract" => &unresolved_call.contract_name,
              "signature" => &unresolved_call.function_signature,
              "args" => format!("[{}]", tokens_as_string(&unresolved_call.function_args)),
              "time_ms" => format!("{}ms", elapsed.as_millis()),
              "result" => result_as_string(&result),
              "block_hash" => block_ptr.hash_hex(),
              "block_number" => block_ptr.block_number(),
              "source" => source.to_string());

    result
}

#[derive(Clone, Debug)]
pub struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_signature: Option<String>,
    pub function_args: Vec<ethabi::Token>,
}

impl AscIndexId for AscUnresolvedContractCall {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SmartContractCall;
}

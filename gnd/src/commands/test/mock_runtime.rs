//! Wraps Ethereum host functions to convert `PossibleReorg` errors into
//! `Deterministic` ones so they surface immediately instead of causing
//! infinite restart loops.

use anyhow::anyhow;
use graph::blockchain::{self, ChainIdentifier, HostFn, HostFnCtx};
use graph::data_source;
use graph::futures03::FutureExt;
use graph::prelude::EthereumCallCache;
use graph::runtime::HostExportError;
use graph_chain_ethereum::chain::{EthereumRuntimeAdapterBuilder, RuntimeAdapterBuilder};
use graph_chain_ethereum::network::EthereumNetworkAdapters;
use graph_chain_ethereum::Chain;
use std::sync::Arc;

const WRAPPED_HOST_FNS: &[&str] = &["ethereum.call", "ethereum.getBalance", "ethereum.hasCode"];

pub struct TestRuntimeAdapterBuilder;

impl RuntimeAdapterBuilder for TestRuntimeAdapterBuilder {
    fn build(
        &self,
        eth_adapters: Arc<EthereumNetworkAdapters>,
        call_cache: Arc<dyn EthereumCallCache>,
        chain_identifier: Arc<ChainIdentifier>,
    ) -> Arc<dyn blockchain::RuntimeAdapter<Chain>> {
        let real_adapter =
            EthereumRuntimeAdapterBuilder {}.build(eth_adapters, call_cache, chain_identifier);
        Arc::new(TestRuntimeAdapter { real_adapter })
    }
}

struct TestRuntimeAdapter {
    real_adapter: Arc<dyn blockchain::RuntimeAdapter<Chain>>,
}

impl TestRuntimeAdapter {
    /// Convert `PossibleReorg` → `Deterministic` for a single host function.
    fn wrap_possible_reorg(real: HostFn) -> HostFn {
        let real_func = real.func.clone();
        let name = real.name;
        HostFn {
            name,
            func: Arc::new(move |ctx: HostFnCtx<'_>, wasm_ptr: u32| {
                let real_func = real_func.clone();
                async move {
                    match real_func(ctx, wasm_ptr).await {
                        Ok(result) => Ok(result),
                        Err(HostExportError::PossibleReorg(e)) => {
                            Err(HostExportError::Deterministic(anyhow!(
                                "{}. Add mock data to your test JSON.",
                                e
                            )))
                        }
                        Err(other) => Err(other),
                    }
                }
                .boxed()
            }),
        }
    }
}

impl blockchain::RuntimeAdapter<Chain> for TestRuntimeAdapter {
    fn host_fns(&self, ds: &data_source::DataSource<Chain>) -> Result<Vec<HostFn>, anyhow::Error> {
        let mut fns = self.real_adapter.host_fns(ds)?;

        // PossibleReorg → Deterministic for mock-backed host functions.
        for hf in &mut fns {
            if WRAPPED_HOST_FNS.contains(&hf.name) {
                *hf = Self::wrap_possible_reorg(hf.clone());
            }
        }

        Ok(fns)
    }
}

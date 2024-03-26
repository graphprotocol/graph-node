use std::sync::Arc;

use anyhow::Error;
use graph::{
    blockchain::{self, HostFn},
    prelude::{CheapClone, EthereumCallCache},
};
use graph_chain_ethereum::{
    network::EthereumNetworkAdapters,
    runtime::runtime_adapter::{eth_get_balance, ethereum_call},
    NodeCapabilities,
};

use crate::{Chain, DataSource};

pub struct RuntimeAdapter {
    pub eth_adapters: Arc<EthereumNetworkAdapters>,
    pub call_cache: Arc<dyn EthereumCallCache>,
}

impl blockchain::RuntimeAdapter<Chain> for RuntimeAdapter {
    fn host_fns(&self, ds: &DataSource) -> Result<Vec<HostFn>, Error> {
        let abis = ds
            .mapping
            .abis
            .iter()
            .map(|mapping| {
                Arc::new(graph_chain_ethereum::MappingABI::from(
                    mapping.as_ref().clone(),
                ))
            })
            .collect::<Vec<Arc<graph_chain_ethereum::MappingABI>>>();
        let call_cache = self.call_cache.cheap_clone();
        let eth_adapters = self.eth_adapters.cheap_clone();

        let ethereum_call = HostFn {
            name: "ethereum.call",
            func: Arc::new(move |ctx, wasm_ptr| {
                // Ethereum calls should prioritise call-only adapters if one is available.
                let eth_adapter = eth_adapters.call_or_cheapest(Some(&NodeCapabilities {
                    archive: true,
                    traces: false,
                }))?;
                ethereum_call(
                    &eth_adapter,
                    call_cache.cheap_clone(),
                    ctx,
                    wasm_ptr,
                    &abis,
                    None,
                )
                .map(|ptr| ptr.wasm_ptr())
            }),
        };

        let eth_adapters = self.eth_adapters.cheap_clone();
        let ethereum_get_balance = HostFn {
            name: "ethereum.getBalance",
            func: Arc::new(move |ctx, wasm_ptr| {
                let eth_adapter = eth_adapters.cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })?;
                eth_get_balance(&eth_adapter, ctx, wasm_ptr).map(|ptr| ptr.wasm_ptr())
            }),
        };

        Ok(vec![ethereum_call, ethereum_get_balance])
    }
}

use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::prelude::*;
use graph::{blockchain as bc, components::metrics::CounterVec};
use mockall::automock;
use mockall::predicate::*;
use std::cmp;
use std::collections::HashSet;
use web3::types::Address;

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block: NearBlockFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn from_data_sources<'a>(data_sources: impl Iterator<Item = &'a DataSource> + Clone) -> Self {
        let mut this = Self::default();
        this.extend(data_sources);
        this
    }

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.block
            .extend(NearBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NearBlockFilter {
    pub contract_addresses: HashSet<(BlockNumber, Address)>,
    pub trigger_every_block: bool,
}

impl NearBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.source.address.is_some())
            .fold(Self::default(), |mut filter_opt, data_source| {
                let has_block_handler_with_call_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .into_iter()
                    .any(|block_handler| match block_handler.filter {
                        Some(ref filter) if *filter == BlockHandlerFilter::Call => return true,
                        _ => return false,
                    });

                let has_block_handler_without_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .into_iter()
                    .any(|block_handler| block_handler.filter.is_none());

                filter_opt.extend(Self {
                    trigger_every_block: has_block_handler_without_filter,
                    contract_addresses: if has_block_handler_with_call_filter {
                        vec![(
                            data_source.source.start_block,
                            data_source.source.address.unwrap().to_owned(),
                        )]
                        .into_iter()
                        .collect()
                    } else {
                        HashSet::default()
                    },
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: NearBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
        self.contract_addresses = self.contract_addresses.iter().cloned().fold(
            HashSet::new(),
            |mut addresses, (start_block, address)| {
                match other
                    .contract_addresses
                    .iter()
                    .cloned()
                    .find(|(_, other_address)| &address == other_address)
                {
                    Some((other_start_block, address)) => {
                        addresses.insert((cmp::min(other_start_block, start_block), address));
                    }
                    None => {
                        addresses.insert((start_block, address));
                    }
                }
                addresses
            },
        );
    }
}

#[derive(Clone)]
pub struct SubgraphNearRpcMetrics {
    errors: Box<CounterVec>,
}

impl SubgraphNearRpcMetrics {
    pub fn new(registry: Arc<dyn MetricsRegistry>, subgraph_hash: &str) -> Self {
        let errors = registry
            .new_deployment_counter_vec(
                "deployment_near_rpc_errors",
                "Counts NEAR errors for a subgraph deployment",
                &subgraph_hash,
                vec![String::from("method")],
            )
            .unwrap();
        Self { errors }
    }

    pub fn add_error(&self, method: &str) {
        self.errors.with_label_values(vec![method].as_slice()).inc();
    }
}

#[automock]
#[async_trait]
pub trait NearAdapter: Send + Sync + 'static {
    /// The `provider.label` from the adapter's configuration
    fn provider(&self) -> &str;
}

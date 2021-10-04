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
    pub trigger_every_block: bool,
}

impl NearBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.source.address.is_some())
            .fold(Self::default(), |mut filter_opt, data_source| {
                filter_opt.extend(Self {
                    trigger_every_block: true,
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: NearBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
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

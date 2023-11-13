mod hosts;

use anyhow::ensure;
use futures01::sync::mpsc::Sender;
use graph::{
    blockchain::{Blockchain, TriggerData as _},
    data_source::{
        causality_region::CausalityRegionSeq, offchain, CausalityRegion, DataSource,
        DataSourceTemplate, TriggerData,
    },
    prelude::*,
};
use hosts::{OffchainHosts, OnchainHosts};
use std::collections::HashMap;

pub(super) struct SubgraphInstance<C: Blockchain, T: RuntimeHostBuilder<C>> {
    subgraph_id: DeploymentHash,
    network: String,
    host_builder: T,
    pub templates: Arc<Vec<DataSourceTemplate<C>>>,
    /// The data sources declared in the subgraph manifest. This does not include dynamic data sources.
    pub(super) static_data_sources: Arc<Vec<DataSource<C>>>,
    host_metrics: Arc<HostMetrics>,

    /// The hosts represent the data sources in the subgraph. There is one host per data source.
    /// Data sources with no mappings (e.g. direct substreams) have no host.
    ///
    /// Onchain hosts must be created in increasing order of block number. `fn hosts_for_trigger`
    /// will return the onchain hosts in the same order as they were inserted.
    onchain_hosts: OnchainHosts<C, T>,

    offchain_hosts: OffchainHosts<C, T>,

    /// Maps the hash of a module to a channel to the thread in which the module is instantiated.
    module_cache: HashMap<[u8; 32], Sender<T::Req>>,

    /// This manages the sequence of causality regions for the subgraph.
    causality_region_seq: CausalityRegionSeq,
}

impl<T, C> SubgraphInstance<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    /// All onchain data sources that are part of this subgraph. This includes data sources
    /// that are included in the subgraph manifest and dynamic data sources.
    pub fn onchain_data_sources(&self) -> impl Iterator<Item = &C::DataSource> + Clone {
        let host_data_sources = self
            .onchain_hosts
            .hosts()
            .iter()
            .map(|h| h.data_source().as_onchain().unwrap());

        // Datasources that are defined in the subgraph manifest but does not correspond to any host
        // in the subgraph. Currently these are only substreams data sources.
        let substreams_data_sources = self
            .static_data_sources
            .iter()
            .filter(|ds| ds.runtime().is_none())
            .filter_map(|ds| ds.as_onchain());

        host_data_sources.chain(substreams_data_sources)
    }

    pub fn new(
        manifest: SubgraphManifest<C>,
        host_builder: T,
        host_metrics: Arc<HostMetrics>,
        causality_region_seq: CausalityRegionSeq,
    ) -> Self {
        let subgraph_id = manifest.id.clone();
        let network = manifest.network_name();
        let templates = Arc::new(manifest.templates);

        SubgraphInstance {
            host_builder,
            subgraph_id,
            network,
            static_data_sources: Arc::new(manifest.data_sources),
            onchain_hosts: OnchainHosts::new(),
            offchain_hosts: OffchainHosts::new(),
            module_cache: HashMap::new(),
            templates,
            host_metrics,
            causality_region_seq,
        }
    }

    // If `data_source.runtime()` is `None`, returns `Ok(None)`.
    fn new_host(
        &mut self,
        logger: Logger,
        data_source: DataSource<C>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        let module_bytes = match &data_source.runtime() {
            None => return Ok(None),
            Some(ref module_bytes) => module_bytes.cheap_clone(),
        };

        let mapping_request_sender = {
            let module_hash = tiny_keccak::keccak256(module_bytes.as_ref());
            if let Some(sender) = self.module_cache.get(&module_hash) {
                sender.clone()
            } else {
                let sender = T::spawn_mapping(
                    module_bytes.as_ref(),
                    logger,
                    self.subgraph_id.clone(),
                    self.host_metrics.cheap_clone(),
                )?;
                self.module_cache.insert(module_hash, sender.clone());
                sender
            }
        };

        let host = self.host_builder.build(
            self.network.clone(),
            self.subgraph_id.clone(),
            data_source,
            self.templates.cheap_clone(),
            mapping_request_sender,
            self.host_metrics.cheap_clone(),
        )?;
        Ok(Some(Arc::new(host)))
    }

    pub(super) fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource<C>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        // Protect against creating more than the allowed maximum number of data sources
        if self.hosts_len() >= ENV_VARS.subgraph_max_data_sources {
            anyhow::bail!(
                "Limit of {} data sources per subgraph exceeded",
                ENV_VARS.subgraph_max_data_sources,
            );
        }

        let is_onchain = data_source.is_onchain();
        let Some(host) = self.new_host(logger.clone(), data_source)? else {
            return Ok(None);
        };

        // Check for duplicates and add the host.
        if is_onchain {
            // `onchain_hosts` will remain ordered by the creation block.
            // See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
            ensure!(
                self.onchain_hosts
                    .last()
                    .and_then(|h| h.creation_block_number())
                    <= host.data_source().creation_block(),
            );

            if self.onchain_hosts.contains(&host) {
                Ok(None)
            } else {
                self.onchain_hosts.push(host.cheap_clone());
                Ok(Some(host))
            }
        } else {
            if self.offchain_hosts.contains(&host) {
                Ok(None)
            } else {
                self.offchain_hosts.push(host.cheap_clone());
                Ok(Some(host))
            }
        }
    }

    /// Reverts any DataSources that have been added from the block forwards (inclusively)
    /// This function also reverts the done_at status if it was 'done' on this block or later.
    /// It only returns the offchain::Source because we don't currently need to know which
    /// DataSources were removed, the source is used so that the offchain DDS can be found again.
    pub(super) fn revert_data_sources(
        &mut self,
        reverted_block: BlockNumber,
    ) -> Vec<offchain::Source> {
        self.revert_onchain_hosts(reverted_block);
        self.offchain_hosts.remove_ge_block(reverted_block);

        // Any File DataSources (Dynamic Data Sources), will have their own causality region
        // which currently is the next number of the sequence but that should be an internal detail.
        // Regardless of the sequence logic, if the current causality region is ONCHAIN then there are
        // no others and therefore the remaining code is a noop and we can just stop here.
        if self.causality_region_seq.0 == CausalityRegion::ONCHAIN {
            return vec![];
        }

        self.offchain_hosts
            .all()
            .filter(|host| matches!(host.done_at(), Some(done_at) if done_at >= reverted_block))
            .map(|host| {
                host.set_done_at(None);
                host.data_source().as_offchain().unwrap().source.clone()
            })
            .collect()
    }

    /// Because onchain hosts are ordered, removing them based on creation block is cheap and simple.
    fn revert_onchain_hosts(&mut self, reverted_block: BlockNumber) {
        // `onchain_hosts` is ordered by the creation block.
        // See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
        while self
            .onchain_hosts
            .last()
            .filter(|h| h.creation_block_number() >= Some(reverted_block))
            .is_some()
        {
            self.onchain_hosts.pop();
        }
    }

    /// Returns all hosts which match the trigger's address.
    /// This is a performance optimization to reduce the number of calls to `match_and_decode`.
    pub fn hosts_for_trigger(
        &self,
        trigger: &TriggerData<C>,
    ) -> Box<dyn Iterator<Item = &T::Host> + Send + '_> {
        match trigger {
            TriggerData::Onchain(trigger) => self
                .onchain_hosts
                .matches_by_address(trigger.address_match()),
            TriggerData::Offchain(trigger) => self
                .offchain_hosts
                .matches_by_address(trigger.source.address().as_ref().map(|a| a.as_slice())),
        }
    }

    pub(super) fn causality_region_next_value(&mut self) -> CausalityRegion {
        self.causality_region_seq.next_val()
    }

    pub fn hosts_len(&self) -> usize {
        self.onchain_hosts.len() + self.offchain_hosts.len()
    }

    pub fn first_host(&self) -> Option<&Arc<T::Host>> {
        self.onchain_hosts.hosts().first()
    }
}

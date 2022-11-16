use futures01::sync::mpsc::Sender;
use graph::{
    blockchain::Blockchain,
    data_source::{
        causality_region::CausalityRegionSeq, offchain, CausalityRegion, DataSource,
        DataSourceTemplate,
    },
    prelude::*,
};
use std::collections::HashMap;

use super::OffchainMonitor;

pub struct SubgraphInstance<C: Blockchain, T: RuntimeHostBuilder<C>> {
    subgraph_id: DeploymentHash,
    network: String,
    host_builder: T,
    templates: Arc<Vec<DataSourceTemplate<C>>>,
    host_metrics: Arc<HostMetrics>,

    /// Runtime hosts, one for each data source mapping.
    ///
    /// The runtime hosts are created and added in the same order the
    /// data sources appear in the subgraph manifest. Incoming block
    /// stream events are processed by the mappings in this same order.
    hosts: Vec<Arc<T::Host>>,

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
    pub fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest<C>,
        host_builder: T,
        host_metrics: Arc<HostMetrics>,
        offchain_monitor: &mut OffchainMonitor,
        causality_region_seq: CausalityRegionSeq,
    ) -> Result<Self, Error> {
        let subgraph_id = manifest.id.clone();
        let network = manifest.network_name();
        let templates = Arc::new(manifest.templates);

        let mut this = SubgraphInstance {
            host_builder,
            subgraph_id,
            network,
            hosts: Vec::new(),
            module_cache: HashMap::new(),
            templates,
            host_metrics,
            causality_region_seq,
        };

        // Create a new runtime host for each data source in the subgraph manifest;
        // we use the same order here as in the subgraph manifest to make the
        // event processing behavior predictable
        for ds in manifest.data_sources {
            // TODO: This is duplicating code from `IndexingContext::add_dynamic_data_source` and
            // `SubgraphInstance::add_dynamic_data_source`. Ideally this should be refactored into
            // `IndexingContext`.

            let runtime = ds.runtime();
            let module_bytes = match runtime {
                None => continue,
                Some(ref module_bytes) => module_bytes,
            };

            if let DataSource::Offchain(ds) = &ds {
                // monitor data source only if it's not processed.
                if !ds.is_processed() {
                    offchain_monitor.add_source(ds.source.clone())?;
                }
            }

            let host = this.new_host(logger.cheap_clone(), ds, module_bytes)?;
            this.hosts.push(Arc::new(host));
        }

        Ok(this)
    }

    // module_bytes is the same as data_source.runtime().unwrap(), this is to ensure that this
    // function is only called for data_sources for which data_source.runtime().is_some() is true.
    fn new_host(
        &mut self,
        logger: Logger,
        data_source: DataSource<C>,
        module_bytes: &Arc<Vec<u8>>,
    ) -> Result<T::Host, Error> {
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
        self.host_builder.build(
            self.network.clone(),
            self.subgraph_id.clone(),
            data_source,
            self.templates.cheap_clone(),
            mapping_request_sender,
            self.host_metrics.cheap_clone(),
        )
    }

    pub(super) fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource<C>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        // Protect against creating more than the allowed maximum number of data sources
        if self.hosts.len() >= ENV_VARS.subgraph_max_data_sources {
            anyhow::bail!(
                "Limit of {} data sources per subgraph exceeded",
                ENV_VARS.subgraph_max_data_sources,
            );
        }

        // `hosts` will remain ordered by the creation block.
        // See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
        assert!(
            self.hosts.last().and_then(|h| h.creation_block_number())
                <= data_source.creation_block()
        );

        let module_bytes = match &data_source.runtime() {
            None => return Ok(None),
            Some(ref module_bytes) => module_bytes.cheap_clone(),
        };

        let host = Arc::new(self.new_host(logger.clone(), data_source, &module_bytes)?);

        Ok(if self.hosts.contains(&host) {
            None
        } else {
            self.hosts.push(host.clone());
            Some(host)
        })
    }

    /// Reverts any DataSources that have been added from the block forwards (inclusively)
    /// This function also reverts the done_at status if it was 'done' on this block or later.
    /// It only returns the offchain::Source because we don't currently need to know which
    /// DataSources were removed, the source is used so that the offchain DDS can be found again.
    pub(super) fn revert_data_sources(
        &mut self,
        reverted_block: BlockNumber,
    ) -> Vec<offchain::Source> {
        self.revert_hosts_cheap(reverted_block);

        // The following code handles resetting offchain datasources so in most
        // cases this is enough processing.
        // At some point we prolly need to improve the linear search but for now this
        // should be fine. *IT'S FINE*
        //
        // Any File DataSources (Dynamic Data Sources), will have their own causality region
        // which currently is the next number of the sequence but that should be an internal detail.
        // Regardless of the sequence logic, if the current causality region is ONCHAIN then there are
        // no others and therefore the remaining code is a noop and we can just stop here.
        if self.causality_region_seq.0 == CausalityRegion::ONCHAIN {
            return vec![];
        }

        self.hosts
            .iter()
            .filter(|host| matches!(host.done_at(), Some(done_at) if done_at >= reverted_block))
            .map(|host| {
                host.set_done_at(None);
                // Safe to call unwrap() because only offchain DataSources have done_at = Some
                host.data_source().as_offchain().unwrap().source.clone()
            })
            .collect()
    }

    /// Because hosts are ordered, removing them based on creation block is cheap and simple.
    fn revert_hosts_cheap(&mut self, reverted_block: BlockNumber) {
        // `hosts` is ordered by the creation block.
        // See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
        while self
            .hosts
            .last()
            .filter(|h| h.creation_block_number() >= Some(reverted_block))
            .is_some()
        {
            self.hosts.pop();
        }
    }

    pub fn hosts(&self) -> &[Arc<T::Host>] {
        &self.hosts
    }

    pub(super) fn causality_region_next_value(&mut self) -> CausalityRegion {
        self.causality_region_seq.next_val()
    }
}

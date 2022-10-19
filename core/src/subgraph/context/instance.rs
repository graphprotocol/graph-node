use futures01::sync::mpsc::Sender;
use graph::{
    blockchain::Blockchain,
    data_source::{DataSource, DataSourceTemplate},
    prelude::*,
};
use std::collections::HashMap;

use super::OffchainMonitor;

pub(crate) struct SubgraphInstance<C: Blockchain, T: RuntimeHostBuilder<C>> {
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
        if let Some(max_data_sources) = ENV_VARS.subgraph_max_data_sources {
            if self.hosts.len() >= max_data_sources {
                anyhow::bail!(
                    "Limit of {} data sources per subgraph exceeded",
                    max_data_sources,
                );
            }
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

    pub(super) fn revert_data_sources(&mut self, reverted_block: BlockNumber) {
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

    pub(super) fn hosts(&self) -> &[Arc<T::Host>] {
        &self.hosts
    }
}

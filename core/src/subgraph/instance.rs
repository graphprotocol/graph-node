use futures01::sync::mpsc::Sender;
use graph::components::subgraph::ProofOfIndexingVersion;
use graph::data::subgraph::SPEC_VERSION_0_0_6;

use std::collections::HashMap;

use graph::{
    blockchain::Blockchain,
    components::{
        store::SubgraphFork,
        subgraph::{MappingError, SharedProofOfIndexing},
    },
    prelude::ENV_VARS,
};
use graph::{blockchain::DataSource, prelude::*};

use super::metrics::SubgraphInstanceMetrics;
use super::TriggerProcessor;

pub struct SubgraphInstance<C: Blockchain, T: RuntimeHostBuilder<C>, TP: TriggerProcessor<C, T>> {
    subgraph_id: DeploymentHash,
    network: String,
    pub poi_version: ProofOfIndexingVersion,
    host_builder: T,
    pub(crate) trigger_processor: TP,

    /// Runtime hosts, one for each data source mapping.
    ///
    /// The runtime hosts are created and added in the same order the
    /// data sources appear in the subgraph manifest. Incoming block
    /// stream events are processed by the mappings in this same order.
    hosts: Vec<Arc<T::Host>>,

    /// Maps the hash of a module to a channel to the thread in which the module is instantiated.
    module_cache: HashMap<[u8; 32], Sender<T::Req>>,
}

impl<T, C, TP> SubgraphInstance<C, T, TP>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
    TP: TriggerProcessor<C, T>,
{
    pub(crate) fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest<C>,
        host_builder: T,
        trigger_processor: TP,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<Self, Error> {
        let subgraph_id = manifest.id.clone();
        let network = manifest.network_name();
        let templates = Arc::new(manifest.templates);

        let poi_version = if manifest.spec_version.ge(&SPEC_VERSION_0_0_6) {
            ProofOfIndexingVersion::Fast
        } else {
            ProofOfIndexingVersion::Legacy
        };

        let mut this = SubgraphInstance {
            host_builder,
            subgraph_id,
            network,
            hosts: Vec::new(),
            module_cache: HashMap::new(),
            poi_version,
            trigger_processor,
        };

        // Create a new runtime host for each data source in the subgraph manifest;
        // we use the same order here as in the subgraph manifest to make the
        // event processing behavior predictable
        for ds in manifest.data_sources {
            let runtime = ds.runtime();
            let module_bytes = match runtime {
                None => continue,
                Some(ref module_bytes) => module_bytes,
            };

            let host = this.new_host(
                logger.cheap_clone(),
                ds,
                module_bytes,
                templates.cheap_clone(),
                host_metrics.cheap_clone(),
            )?;
            this.hosts.push(Arc::new(host))
        }

        Ok(this)
    }

    // module_bytes is the same as data_source.runtime().unwrap(), this is to ensure that this
    // function is only called for data_sources for which data_source.runtime().is_some() is true.
    fn new_host(
        &mut self,
        logger: Logger,
        data_source: C::DataSource,
        module_bytes: &Arc<Vec<u8>>,
        templates: Arc<Vec<C::DataSourceTemplate>>,
        host_metrics: Arc<HostMetrics>,
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
                    host_metrics.clone(),
                )?;
                self.module_cache.insert(module_hash, sender.clone());
                sender
            }
        };
        self.host_builder.build(
            self.network.clone(),
            self.subgraph_id.clone(),
            data_source,
            templates,
            mapping_request_sender,
            host_metrics,
        )
    }

    pub(crate) async fn process_trigger(
        &self,
        logger: &Logger,
        block: &Arc<C::Block>,
        trigger: &C::TriggerData,
        state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        self.trigger_processor
            .process_trigger(
                logger,
                &self.hosts,
                block,
                trigger,
                state,
                proof_of_indexing,
                causality_region,
                debug_fork,
                subgraph_metrics,
            )
            .await
    }

    pub(crate) fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: C::DataSource,
        templates: Arc<Vec<C::DataSourceTemplate>>,
        metrics: Arc<HostMetrics>,
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

        let host = Arc::new(self.new_host(
            logger.clone(),
            data_source,
            &module_bytes,
            templates,
            metrics,
        )?);

        Ok(if self.hosts.contains(&host) {
            None
        } else {
            self.hosts.push(host.clone());
            Some(host)
        })
    }

    pub(crate) fn revert_data_sources(&mut self, reverted_block: BlockNumber) {
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

    pub(crate) fn network(&self) -> &str {
        &self.network
    }
}

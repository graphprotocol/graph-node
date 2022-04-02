use futures01::sync::mpsc::Sender;

use std::collections::HashMap;
use std::time::Instant;

use graph::{blockchain::DataSource, prelude::*};
use graph::{
    blockchain::{Block, Blockchain},
    components::{
        store::SubgraphFork,
        subgraph::{MappingError, SharedProofOfIndexing},
    },
    prelude::ENV_VARS,
};

use super::metrics::SubgraphInstanceMetrics;

pub struct SubgraphInstance<C: Blockchain, T: RuntimeHostBuilder<C>> {
    subgraph_id: DeploymentHash,
    network: String,
    host_builder: T,

    /// Runtime hosts, one for each data source mapping.
    ///
    /// The runtime hosts are created and added in the same order the
    /// data sources appear in the subgraph manifest. Incoming block
    /// stream events are processed by the mappings in this same order.
    hosts: Vec<Arc<T::Host>>,

    /// Maps the hash of a module to a channel to the thread in which the module is instantiated.
    module_cache: HashMap<[u8; 32], Sender<T::Req>>,
}

impl<T, C: Blockchain> SubgraphInstance<C, T>
where
    T: RuntimeHostBuilder<C>,
{
    pub(crate) fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest<C>,
        host_builder: T,
        host_metrics: Arc<HostMetrics>,
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
        };

        // Create a new runtime host for each data source in the subgraph manifest;
        // we use the same order here as in the subgraph manifest to make the
        // event processing behavior predictable
        for ds in manifest.data_sources {
            let host = this.new_host(
                logger.cheap_clone(),
                ds,
                templates.cheap_clone(),
                host_metrics.cheap_clone(),
            )?;
            this.hosts.push(Arc::new(host))
        }

        Ok(this)
    }

    fn new_host(
        &mut self,
        logger: Logger,
        data_source: C::DataSource,
        templates: Arc<Vec<C::DataSourceTemplate>>,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<T::Host, Error> {
        let mapping_request_sender = {
            let module_bytes = data_source.runtime();
            let module_hash = tiny_keccak::keccak256(module_bytes);
            if let Some(sender) = self.module_cache.get(&module_hash) {
                sender.clone()
            } else {
                let sender = T::spawn_mapping(
                    module_bytes.to_owned(),
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
        Self::process_trigger_in_runtime_hosts(
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

    pub(crate) async fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<C::Block>,
        trigger: &C::TriggerData,
        mut state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        let error_count = state.deterministic_errors.len();

        if let Some(proof_of_indexing) = proof_of_indexing {
            proof_of_indexing
                .borrow_mut()
                .start_handler(causality_region);
        }

        for host in hosts {
            let mapping_trigger = match host.match_and_decode(trigger, block, logger)? {
                // Trigger matches and was decoded as a mapping trigger.
                Some(mapping_trigger) => mapping_trigger,

                // Trigger does not match, do not process it.
                None => continue,
            };

            let start = Instant::now();
            state = host
                .process_mapping_trigger(
                    logger,
                    block.ptr(),
                    mapping_trigger,
                    state,
                    proof_of_indexing.cheap_clone(),
                    debug_fork,
                )
                .await?;
            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.observe_trigger_processing_duration(elapsed);
        }

        if let Some(proof_of_indexing) = proof_of_indexing {
            if state.deterministic_errors.len() != error_count {
                assert!(state.deterministic_errors.len() == error_count + 1);

                // If a deterministic error has happened, write a new
                // ProofOfIndexingEvent::DeterministicError to the SharedProofOfIndexing.
                proof_of_indexing
                    .borrow_mut()
                    .write_deterministic_error(&logger, causality_region);
            }
        }

        Ok(state)
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

        let host = Arc::new(self.new_host(logger.clone(), data_source, templates, metrics)?);

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

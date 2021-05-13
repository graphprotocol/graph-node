use futures01::sync::mpsc::Sender;
use lazy_static::lazy_static;

use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use graph::{
    blockchain::Blockchain,
    components::subgraph::{MappingError, SharedProofOfIndexing},
};
use graph::{blockchain::DataSource, prelude::*};

lazy_static! {
    static ref MAX_DATA_SOURCES: Option<usize> = env::var("GRAPH_SUBGRAPH_MAX_DATA_SOURCES")
        .ok()
        .map(|s| usize::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_SUBGRAPH_MAX_DATA_SOURCES")));
}

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
        manifest: SubgraphManifest<C::DataSource>,
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
        templates: Arc<Vec<DataSourceTemplate>>,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<T::Host, Error> {
        let mapping_request_sender = {
            let module_bytes = data_source.mapping().runtime.as_ref();
            let module_hash = tiny_keccak::keccak256(module_bytes);
            if let Some(sender) = self.module_cache.get(&module_hash) {
                sender.clone()
            } else {
                let sender = T::spawn_mapping(
                    module_bytes.clone(),
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
        block: &Arc<BlockFinality>,
        trigger: EthereumTrigger,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        Self::process_trigger_in_runtime_hosts(
            logger,
            &self.hosts,
            block,
            trigger,
            state,
            proof_of_indexing,
        )
        .await
    }

    pub(crate) async fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<BlockFinality>,
        trigger: EthereumTrigger,
        mut state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        let block = Arc::new(block.light_block());
        for host in hosts {
            let mapping_trigger =
                match host.match_and_decode(&trigger, block.cheap_clone(), logger)? {
                    // Trigger matches and was decoded as a mapping trigger.
                    Some(mapping_trigger) => mapping_trigger,

                    // Trigger does not match, do not process it.
                    None => continue,
                };

            state = host
                .process_mapping_trigger(
                    logger,
                    block.block_ptr(),
                    mapping_trigger,
                    state,
                    proof_of_indexing.cheap_clone(),
                )
                .await?;
        }

        Ok(state)
    }

    pub(crate) fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: C::DataSource,
        templates: Arc<Vec<DataSourceTemplate>>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        // Protect against creating more than the allowed maximum number of data sources
        if let Some(max_data_sources) = *MAX_DATA_SOURCES {
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

        let host =
            Arc::new(self.new_host(logger.clone(), data_source, templates, metrics.clone())?);

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
}

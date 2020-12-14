use async_trait::async_trait;
use futures01::sync::mpsc::Sender;
use lazy_static::lazy_static;

use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use web3::types::{Log, H256};

lazy_static! {
    static ref MAX_DATA_SOURCES: Option<usize> = env::var("GRAPH_SUBGRAPH_MAX_DATA_SOURCES")
        .ok()
        .map(|s| usize::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_SUBGRAPH_MAX_DATA_SOURCES")));
}

pub struct SubgraphInstance<T: RuntimeHostBuilder> {
    subgraph_id: SubgraphDeploymentId,
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

impl<T> SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    pub(crate) fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest,
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
        let (hosts, errors): (_, Vec<_>) = manifest
            .data_sources
            .into_iter()
            .map(|d| this.new_host(logger.clone(), d, templates.clone(), host_metrics.clone()))
            .partition(|res| res.is_ok());

        if !errors.is_empty() {
            let joined_errors = errors
                .into_iter()
                .map(Result::unwrap_err)
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!("Errors loading data sources: {}", joined_errors));
        }

        this.hosts = hosts
            .into_iter()
            .map(Result::unwrap)
            .map(Arc::new)
            .collect();

        Ok(this)
    }

    fn new_host(
        &mut self,
        logger: Logger,
        data_source: DataSource,
        templates: Arc<Vec<DataSourceTemplate>>,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<T::Host, Error> {
        let mapping_request_sender = {
            let module_bytes = data_source.mapping.runtime.as_ref();
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
}

#[async_trait]
impl<T> SubgraphInstanceTrait<T::Host> for SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool {
        self.hosts.iter().any(|host| host.matches_log(log))
    }

    async fn process_trigger(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
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

    async fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        mut state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        match trigger {
            EthereumTrigger::Log(log) => {
                let log = Arc::new(log);

                let transaction = block
                    .transaction_for_log(&log)
                    .map(Arc::new)
                    .context("Found no transaction for event")?;
                let matching_hosts = hosts.iter().filter(|host| host.matches_log(&log));
                let hosts_count = matching_hosts.clone().count();

                if hosts_count > 1 {
                    info!(
                        logger,
                        "{} matching runtime hosts found for log trigger.", hosts_count;
                        "address" => &log.address.to_string(),
                        "topic0" => &log.topics.iter().next().unwrap_or(&H256::zero()).to_string(),
                        "transaction" => log.transaction_hash.unwrap_or(H256::zero()).to_string(),
                    );
                }

                // Process the log in each host in the same order the corresponding data
                // sources appear in the subgraph manifest
                let transaction = Arc::new(transaction);
                for (i, host) in matching_hosts.enumerate() {
                    let host_context = format!("{}/{}", i + 1, hosts_count);
                    let logger = logger.new(o!("runtime_host" => host_context));
                    state = host
                        .process_log(
                            &logger,
                            block,
                            &transaction,
                            &log,
                            state,
                            proof_of_indexing.cheap_clone(),
                        )
                        .await?;
                }
            }
            EthereumTrigger::Call(call) => {
                let call = Arc::new(call);

                let transaction = block
                    .transaction_for_call(&call)
                    .context("Found no transaction for call")?;
                let transaction = Arc::new(transaction);
                let matching_hosts = hosts.iter().filter(|host| host.matches_call(&call));
                let hosts_count = matching_hosts.clone().count();

                if hosts_count > 1 {
                    info!(
                        logger,
                        "{} matching runtime hosts found for call trigger.", hosts_count;
                        "from" => &call.from.to_string(),
                        "to" => &call.to.to_string(),
                        "transaction" => &call.transaction_hash.map(|hash| hash.to_string()).unwrap_or("unkown".to_string()),
                    );
                }

                for (i, host) in matching_hosts.enumerate() {
                    let host_context = format!("{}/{}", i + 1, hosts_count);
                    let logger = logger.new(o!("runtime_host" => host_context));
                    state = host
                        .process_call(
                            &logger,
                            block,
                            &transaction,
                            &call,
                            state,
                            proof_of_indexing.cheap_clone(),
                        )
                        .await?;
                }
            }
            EthereumTrigger::Block(ptr, trigger_type) => {
                let matching_hosts = hosts
                    .iter()
                    .filter(|host| host.matches_block(&trigger_type, ptr.number));
                let hosts_count = matching_hosts.clone().count();

                if hosts_count > 1 {
                    info!(
                        logger,
                        "{} matching runtime hosts found for block trigger.", hosts_count;
                        "number" => &ptr.number,
                        "hash" => &ptr.number,
                    );
                }

                for (i, host) in matching_hosts.enumerate() {
                    let host_context = format!("{}/{}", i + 1, hosts_count);
                    let logger = logger.new(o!("runtime_host" => host_context));
                    state = host
                        .process_block(
                            &logger,
                            block,
                            &trigger_type,
                            state,
                            proof_of_indexing.cheap_clone(),
                        )
                        .await?;
                }
            }
        }
        Ok(state)
    }

    fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource,
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
            self.hosts.last().and_then(|h| h.creation_block_number()) <= data_source.creation_block
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

    fn revert_data_sources(&mut self, reverted_block: u64) {
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

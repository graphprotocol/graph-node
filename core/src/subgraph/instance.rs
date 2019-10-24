use futures::sync::mpsc::Sender;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use web3::types::Log;

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

    /// Maps a serialized module to a channel to the thread in which the module is instantiated.
    module_cache: HashMap<Vec<u8>, Sender<T::Req>>,
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
        let network = manifest.network_name()?;
        let templates = manifest.templates;

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
            return Err(format_err!(
                "Errors loading data sources: {}",
                joined_errors
            ));
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
        top_level_templates: Vec<DataSourceTemplate>,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<T::Host, Error> {
        let mapping_request_sender = {
            let module_bytes = data_source.mapping.runtime.as_ref().clone().to_bytes()?;
            if let Some(sender) = self.module_cache.get(&module_bytes) {
                sender.clone()
            } else {
                let sender = T::spawn_mapping(
                    data_source.mapping.runtime.as_ref().clone(),
                    logger,
                    self.subgraph_id.clone(),
                    host_metrics.clone(),
                )?;
                self.module_cache.insert(module_bytes, sender.clone());
                sender
            }
        };
        self.host_builder.build(
            self.network.clone(),
            self.subgraph_id.clone(),
            data_source,
            top_level_templates,
            mapping_request_sender,
            host_metrics,
        )
    }
}

impl<T> SubgraphInstanceTrait<T::Host> for SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool {
        self.hosts.iter().any(|host| host.matches_log(log))
    }

    fn process_trigger(
        &self,
        logger: &Logger,
        block: Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
        Self::process_trigger_in_runtime_hosts(
            logger,
            self.hosts.iter().cloned(),
            block,
            trigger,
            state,
        )
    }

    fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: impl Iterator<Item = Arc<T::Host>>,
        block: Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
        let logger = logger.to_owned();
        match trigger {
            EthereumTrigger::Log(log) => {
                let transaction = block
                    .transaction_for_log(&log)
                    .map(Arc::new)
                    .ok_or_else(|| format_err!("Found no transaction for event"));
                let matching_hosts: Vec<_> = hosts.filter(|host| host.matches_log(&log)).collect();
                let log = Arc::new(log);

                // Process the log in each host in the same order the corresponding data
                // sources appear in the subgraph manifest
                Box::new(future::result(transaction).and_then(|transaction| {
                    stream::iter_ok(matching_hosts).fold(state, move |state, host| {
                        host.process_log(
                            logger.clone(),
                            block.clone(),
                            transaction.clone(),
                            log.clone(),
                            state,
                        )
                    })
                }))
            }
            EthereumTrigger::Call(call) => {
                let transaction = block
                    .transaction_for_call(&call)
                    .map(Arc::new)
                    .ok_or_else(|| format_err!("Found no transaction for call"));
                let matching_hosts: Vec<_> = hosts
                    .into_iter()
                    .filter(|host| host.matches_call(&call))
                    .collect();
                let call = Arc::new(call);

                Box::new(future::result(transaction).and_then(|transaction| {
                    stream::iter_ok(matching_hosts).fold(state, move |state, host| {
                        host.process_call(
                            logger.clone(),
                            block.clone(),
                            transaction.clone(),
                            call.clone(),
                            state,
                        )
                    })
                }))
            }
            EthereumTrigger::Block(ptr, trigger_type) => {
                let matching_hosts: Vec<_> = hosts
                    .into_iter()
                    .filter(|host| host.matches_block(trigger_type.clone(), ptr.number))
                    .collect();

                Box::new(
                    stream::iter_ok(matching_hosts).fold(state, move |state, host| {
                        host.process_block(
                            logger.clone(),
                            block.clone(),
                            trigger_type.clone(),
                            state,
                        )
                    }),
                )
            }
        }
    }

    fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource,
        top_level_templates: Vec<DataSourceTemplate>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Arc<T::Host>, Error> {
        // Protect against creating more than the allowed maximum number of data sources
        if let Some(max_data_sources) = *MAX_DATA_SOURCES {
            if self.hosts.len() >= max_data_sources {
                return Err(format_err!(
                    "Limit of {} data sources per subgraph exceeded",
                    max_data_sources
                ));
            }
        }

        let host = Arc::new(self.new_host(
            logger.clone(),
            data_source,
            top_level_templates,
            metrics.clone(),
        )?);
        self.hosts.push(host.clone());
        Ok(host)
    }
}

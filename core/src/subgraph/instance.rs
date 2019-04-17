use lazy_static::lazy_static;
use std::env;
use std::str::FromStr;

use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::web3::types::Log;

lazy_static! {
    static ref MAX_DATA_SOURCES: Option<usize> = env::var("GRAPH_SUBGRAPH_MAX_DATA_SOURCES")
        .ok()
        .map(|s| usize::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_SUBGRAPH_MAX_DATA_SOURCES")));
}

pub struct SubgraphInstance<T>
where
    T: RuntimeHostBuilder + Sync,
{
    /// Runtime hosts, one for each data source mapping.
    ///
    /// The runtime hosts are created and added in the same order the
    /// data sources appear in the subgraph manifest. Incoming block
    /// stream events are processed by the mappings in this same order.
    hosts: Vec<Arc<T::Host>>,
}

impl<T> SubgraphInstanceTrait<T> for SubgraphInstance<T>
where
    T: RuntimeHostBuilder + Sync,
{
    fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest,
        host_builder: &T,
    ) -> Result<Self, Error> {
        let manifest_id = manifest.id.clone();

        // Create a new runtime host for each data source in the subgraph manifest;
        // we use the same order here as in the subgraph manifest to make the
        // event processing behavior predictable
        let (hosts, errors): (_, Vec<_>) = manifest
            .data_sources
            .into_iter()
            .map(|d| host_builder.build(&logger, manifest_id.clone(), d))
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

        Ok(SubgraphInstance {
            hosts: hosts
                .into_iter()
                .map(Result::unwrap)
                .map(Arc::new)
                .collect(),
        })
    }

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool {
        self.hosts.iter().any(|host| host.matches_log(log))
    }

    fn process_trigger(
        &self,
        logger: &Logger,
        block: Arc<EthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send> {
        Self::process_trigger_in_runtime_hosts(logger, self.hosts.iter().cloned(), block, trigger, state)
    }

    fn process_trigger_in_runtime_hosts<I>(
        logger: &Logger,
        hosts: I,
        block: Arc<EthereumBlock>,
        log: Log,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send>
    where
        I: IntoIterator<Item = Arc<T::Host>>,
    {
        let logger = logger.to_owned();
        match trigger {
            EthereumTrigger::Log(log) => {
                let transaction = block
                    .transaction_for_log(&log)
                    .map(Arc::new)
                    .ok_or_else(|| format_err!("Found no transaction for event"));
                let matching_hosts: Vec<_> = hosts
                    .iter()
                    .filter(|host| host.matches_log(&log))
                    .cloned()
                    .collect();
                let log = Arc::new(log);
                // Process the log in each host in the same order the corresponding data sources appear
                // in the subgraph manifest
                let eops = future::result(transaction)
                    .and_then(|transaction| {
                        stream::iter_ok(matching_hosts)
                            .fold(state, move |state, host| {
                                host.process_log(
                                    logger.clone(),
                                    block.clone(),
                                    transaction.clone(),
                                    log.clone(),
                                    state,
                                )
                            })
                    });
                Box::new(eops)
            }
            EthereumTrigger::Call(call) => {
                let transaction = block
                    .transaction_for_call(&call)
                    .map(Arc::new)
                    .ok_or_else(|| format_err!("Found no transaction for call"));
                let matching_hosts: Vec<_> = hosts
                    .iter()
                    .filter(|host| host.matches_call(&call))
                    .cloned()
                    .collect();
                let call = Arc::new(call);
                let eops = future::result(transaction)
                    .and_then(|transaction| {
                        stream::iter_ok(matching_hosts)
                            .fold(state, move |state, host| {
                                host.process_call(
                                    logger.clone(),
                                    block.clone(),
                                    transaction.clone(),
                                    call.clone(),
                                    state,
                                )
                            })
                    });
                Box::new(eops)
            }
            EthereumTrigger::Block(trigger_type) => {
                let matching_hosts: Vec<_> = hosts
                    .iter()
                    .filter(|host| host.matches_block(trigger_type.clone()))
                    .cloned()
                    .collect();
                let eops = stream::iter_ok(matching_hosts)
                    .fold(state, move |state, host| {
                        host.process_block(
                            logger.clone(),
                            block.clone(),
                            trigger_type.clone(),
                            state,
                        )
                    },
                );
                Box::new(eops)
            }
        }
    }

    fn add_dynamic_data_sources(&mut self, runtime_hosts: Vec<Arc<T::Host>>) -> Result<(), Error> {
        // Protect against creating more than the allowed maximum number of data sources
        if let Some(max_data_sources) = *MAX_DATA_SOURCES {
            if self.hosts.len() + runtime_hosts.len() > max_data_sources {
                return Err(format_err!(
                    "Limit of {} data sources per subgraph exceeded",
                    max_data_sources
                ));
            }
        }

        // Add the runtime hosts
        self.hosts.extend(runtime_hosts);

        Ok(())
    }
}

pub mod instance;

use crate::polling_monitor::{
    ipfs_service::IpfsService, spawn_monitor, PollingMonitor, PollingMonitorMetrics,
};
use anyhow::{self, Error};
use bytes::Bytes;
use cid::Cid;
use graph::{
    blockchain::{Blockchain, BlockchainKind, DataSource as _},
    components::{
        store::{DeploymentId, SubgraphFork},
        subgraph::{MappingError, SharedProofOfIndexing},
    },
    data_source::{offchain, DataSource, TriggerData},
    prelude::{
        BlockNumber, BlockState, CancelGuard, DeploymentHash, MetricsRegistry, RuntimeHost,
        RuntimeHostBuilder, SubgraphInstanceMetrics, TriggerProcessor,
    },
    slog::Logger,
    tokio::sync::mpsc,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use self::instance::SubgraphInstance;

pub type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

// The context keeps track of mutable in-memory state that is retained across blocks.
//
// Currently most of the changes are applied in `runner.rs`, but ideally more of that would be
// refactored into the context so it wouldn't need `pub` fields. The entity cache should probaby
// also be moved here.
pub(crate) struct IndexingContext<C, T>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
{
    instance: SubgraphInstance<C, T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
    pub offchain_monitor: OffchainMonitor,
    trigger_processor: Box<dyn TriggerProcessor<C, T>>,
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> IndexingContext<C, T> {
    pub fn new(
        instance: SubgraphInstance<C, T>,
        instances: SharedInstanceKeepAliveMap,
        filter: C::TriggerFilter,
        offchain_monitor: OffchainMonitor,
        trigger_processor: Box<dyn TriggerProcessor<C, T>>,
    ) -> Self {
        Self {
            instance,
            instances,
            filter,
            offchain_monitor,
            trigger_processor,
        }
    }

    #[inline]
    pub fn filter_triggers(&self, triggers: &mut Vec<C::TriggerData>, block: &Arc<C::Block>) {
        filter_triggers(
            triggers,
            block,
            self.instance
                .hosts
                .iter()
                .map(|h| h.data_source())
                .collect(),
        );
    }

    pub async fn process_trigger(
        &self,
        logger: &Logger,
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        self.process_trigger_in_hosts(
            logger,
            &self.instance.hosts(),
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

    pub async fn process_trigger_in_hosts(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        self.trigger_processor
            .process_trigger(
                logger,
                hosts,
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

    // Removes data sources hosts with a creation block greater or equal to `reverted_block`, so
    // that they are no longer candidates for `process_trigger`.
    //
    // This does not currently affect the `offchain_monitor` or the `filter`, so they will continue
    // to include data sources that have been reverted. This is not ideal for performance, but it
    // does not affect correctness since triggers that have no matching host will be ignored by
    // `process_trigger`.
    pub fn revert_data_sources(&mut self, reverted_block: BlockNumber) {
        self.instance.revert_data_sources(reverted_block)
    }

    pub fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource<C>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        let source = data_source.as_offchain().map(|ds| ds.source.clone());
        let host = self.instance.add_dynamic_data_source(logger, data_source)?;

        if host.is_some() {
            if let Some(source) = source {
                self.offchain_monitor.add_source(&source)?;
            }
        }

        Ok(host)
    }
}

/// Filters a set of triggers, removing any that do not match any datasources,
/// which is useful when using the static filters feature to remove false positives.
/// This is potentially hot code, hence the mutation here to avoid cloning.
fn filter_triggers<C: Blockchain>(
    triggers: &mut Vec<C::TriggerData>,
    block: &Arc<C::Block>,
    data_sources: Vec<&DataSource<C>>,
) {
    // Ethereum is the only protocol with support for dynamic data sources so in
    // every other case we can just skip the filtering.
    if !matches!(C::KIND, BlockchainKind::Ethereum) {
        return;
    }

    triggers.retain_mut(|td| {
        data_sources.iter().any(|ds| match ds {
            &DataSource::Onchain(data_source) => data_source.is_address_match(td, &block),
            &DataSource::Offchain(_) => true,
        })
    });
}

pub(crate) struct OffchainMonitor {
    ipfs_monitor: PollingMonitor<Cid>,
    ipfs_monitor_rx: mpsc::Receiver<(Cid, Bytes)>,
}

impl OffchainMonitor {
    pub fn new(
        logger: Logger,
        registry: Arc<dyn MetricsRegistry>,
        subgraph_hash: &DeploymentHash,
        ipfs_service: IpfsService,
    ) -> Self {
        let (ipfs_monitor_tx, ipfs_monitor_rx) = mpsc::channel(10);
        let ipfs_monitor = spawn_monitor(
            ipfs_service,
            ipfs_monitor_tx,
            logger,
            PollingMonitorMetrics::new(registry, subgraph_hash),
        );
        Self {
            ipfs_monitor,
            ipfs_monitor_rx,
        }
    }

    fn add_source(&mut self, source: &offchain::Source) -> Result<(), Error> {
        match source {
            offchain::Source::Ipfs(cid) => self.ipfs_monitor.monitor(cid.clone()),
        };
        Ok(())
    }

    pub fn ready_offchain_events(&mut self) -> Result<Vec<offchain::TriggerData>, Error> {
        use graph::tokio::sync::mpsc::error::TryRecvError;

        let mut triggers = vec![];
        loop {
            match self.ipfs_monitor_rx.try_recv() {
                Ok((cid, data)) => triggers.push(offchain::TriggerData {
                    source: offchain::Source::Ipfs(cid),
                    data: Arc::new(data),
                }),
                Err(TryRecvError::Disconnected) => {
                    anyhow::bail!("ipfs monitor unexpectedly terminated")
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        Ok(triggers)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use graph::{
        blockchain::BlockchainKind,
        data_source::DataSource,
        prelude::{
            ethabi::Contract, web3::types::H160, EthereumBlock, EthereumBlockWithCalls,
            EthereumCall, Link,
        },
    };
    use graph_chain_ethereum::{
        chain::BlockFinality, trigger::EthereumTrigger, Mapping, MappingABI,
    };
    use semver::Version;

    use super::filter_triggers;

    #[test]
    fn trigger_filter() {
        let addr1 = H160::from_low_u64_le(1);
        let addr2 = H160::from_low_u64_le(2);
        let no_match_addr = H160::from_low_u64_le(3);

        let pass_all_ds: DataSource<graph_chain_ethereum::Chain> =
            DataSource::Onchain(new_data_source(None));

        let addr1_ds: DataSource<graph_chain_ethereum::Chain> =
            DataSource::Onchain(new_data_source(Some(addr1)));

        let addr2_ds: DataSource<graph_chain_ethereum::Chain> =
            DataSource::Onchain(new_data_source(Some(addr2)));

        // This isn't used atm.
        let block = Arc::new(BlockFinality::NonFinal(EthereumBlockWithCalls {
            ethereum_block: EthereumBlock::default(),
            calls: None,
        }));

        struct Case<'a> {
            name: &'a str,
            data_sources: Vec<&'a DataSource<graph_chain_ethereum::Chain>>,
            triggers: Vec<EthereumTrigger>,
            expected_result: Vec<EthereumTrigger>,
        }

        let cases: Vec<Case<'_>> = vec![
            Case {
                name: "pass all",
                data_sources: vec![&pass_all_ds, &addr1_ds, &addr2_ds],
                triggers: vec![new_eth_call(no_match_addr, no_match_addr)],
                expected_result: vec![new_eth_call(no_match_addr, no_match_addr)],
            },
            Case {
                name: "addr1 only",
                data_sources: vec![&addr1_ds],
                triggers: vec![
                    new_eth_call(addr1, addr2),
                    new_eth_call(no_match_addr, addr2),
                    new_eth_call(addr1, no_match_addr),
                    new_eth_call(no_match_addr, addr1),
                ],
                expected_result: vec![new_eth_call(no_match_addr, addr1)],
            },
            Case {
                name: "addr1 && addr2",
                data_sources: vec![&addr1_ds, &addr2_ds],
                triggers: vec![
                    new_eth_call(addr1, addr2),
                    new_eth_call(no_match_addr, addr2),
                    new_eth_call(addr1, no_match_addr),
                    new_eth_call(no_match_addr, addr1),
                ],
                expected_result: vec![
                    new_eth_call(addr1, addr2),
                    new_eth_call(no_match_addr, addr2),
                    new_eth_call(no_match_addr, addr1),
                ],
            },
            Case {
                name: "no matches",
                data_sources: vec![&addr2_ds],
                triggers: vec![
                    new_eth_call(no_match_addr, addr1),
                    new_eth_call(addr1, no_match_addr),
                ],
                expected_result: vec![],
            },
        ];

        for mut case in cases.into_iter() {
            filter_triggers(&mut case.triggers, &block, case.data_sources);
            assert_eq!(case.triggers, case.expected_result, "case: {:?}", case.name);
        }
    }

    fn new_eth_call(from: H160, to: H160) -> EthereumTrigger {
        let mut call = EthereumCall::default();
        call.from = from;
        call.to = to;

        EthereumTrigger::Call(Arc::new(call))
    }

    fn new_data_source(address: Option<H160>) -> graph_chain_ethereum::DataSource {
        graph_chain_ethereum::DataSource {
            kind: BlockchainKind::Ethereum.to_string(),
            network: None,
            name: "asd".to_string(),
            manifest_idx: 0,
            address,
            start_block: 0,
            mapping: Mapping {
                api_version: Version::parse("1.0.0").expect("unable to parse version"),
                language: "".to_string(),
                entities: vec![],
                block_handlers: vec![],
                runtime: Arc::new(vec![]),
                link: Link::default(),
                kind: "".to_string(),
                abis: vec![],
                call_handlers: vec![],
                event_handlers: vec![],
            },
            context: Arc::new(None),
            creation_block: None,
            contract_abi: Arc::new(MappingABI {
                name: "mock_abi".to_string(),
                contract: Contract::load(
                    r#"[
            {
                "inputs": [
                    {
                        "name": "a",
                        "type": "address"
                    }
                ],
                "type": "constructor"
            }
        ]"#
                    .as_bytes(),
                )
                .unwrap(),
            }),
        }
    }
}

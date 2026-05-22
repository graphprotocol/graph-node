use crate::{ENV_VARS, chain::BlockFinality};
use crate::{EthereumAdapter, EthereumAdapterTrait as _};
use async_trait::async_trait;
use futures::future::select_ok;
use graph::blockchain::BlockchainKind;
use graph::blockchain::client::ChainClient;
use graph::components::network_provider::ChainName;
use graph::prelude::alloy::primitives::B256;
use graph::slog::o;
use graph::util::backoff::ExponentialBackoff;
use graph::{
    blockchain::{BlockHash, BlockIngestor, BlockPtr, IngestorError},
    cheap_clone::CheapClone,
    prelude::{
        ChainStore, Error, EthereumBlockWithCalls, LogCode, Logger, debug, error, info, tokio,
        trace, warn,
    },
};
use std::{sync::Arc, time::Duration};

pub struct PollingBlockIngestor {
    logger: Logger,
    ancestor_count: i32,
    chain_client: Arc<ChainClient<crate::chain::Chain>>,
    chain_store: Arc<dyn ChainStore>,
    polling_interval: Duration,
    network_name: ChainName,
}

impl PollingBlockIngestor {
    pub fn new(
        logger: Logger,
        ancestor_count: i32,
        chain_client: Arc<ChainClient<crate::chain::Chain>>,
        chain_store: Arc<dyn ChainStore>,
        polling_interval: Duration,
        network_name: ChainName,
    ) -> Result<PollingBlockIngestor, Error> {
        Ok(PollingBlockIngestor {
            logger,
            ancestor_count,
            chain_client,
            chain_store,
            polling_interval,
            network_name,
        })
    }

    async fn cleanup_cached_blocks(&self) {
        match self
            .chain_store
            .cleanup_cached_blocks(self.ancestor_count)
            .await
        {
            Ok(Some((min_block, count))) => {
                if count > 0 {
                    info!(
                        self.logger,
                        "Cleaned {} blocks from the block cache. \
                                 Only blocks with number greater than {} remain",
                        count,
                        min_block
                    );
                }
            }
            Ok(None) => { /* nothing was cleaned, ignore */ }
            Err(e) => warn!(
                self.logger,
                "Failed to clean blocks from block cache: {}", e
            ),
        }
    }

    async fn do_poll(
        &self,
        logger: &Logger,
        eth_adapter: Arc<EthereumAdapter>,
    ) -> Result<(), IngestorError> {
        trace!(&logger, "BlockIngestor::do_poll");

        // Get chain head ptr from store
        let head_block_ptr_opt = self.chain_store.cheap_clone().chain_head_ptr().await?;

        // To check if there is a new block or not, fetch only the block header since that's cheaper
        // than the full block. This is worthwhile because most of the time there won't be a new
        // block, as we expect the poll interval to be much shorter than the block time.
        let latest_block = self.latest_block(logger, &eth_adapter).await?;

        if let Some(head_block) = head_block_ptr_opt.as_ref() {
            // If latest block matches head block in store, nothing needs to be done
            if &latest_block == head_block {
                return Ok(());
            }

            if latest_block.number < head_block.number {
                // An ingestor might wait or move forward, but it never
                // wavers and goes back. More seriously, this keeps us from
                // later trying to ingest a block with the same number again
                warn!(&logger,
                    "Provider went backwards - ignoring this latest block";
                    "current_block_head" => head_block.number,
                    "latest_block_head" => latest_block.number);
                return Ok(());
            }
        }

        // Compare latest block with head ptr, alert user if far behind
        match head_block_ptr_opt {
            None => {
                info!(
                    &logger,
                    "Downloading latest blocks from Ethereum, this may take a few minutes..."
                );
            }
            Some(head_block_ptr) => {
                let latest_number = latest_block.number;
                let head_number = head_block_ptr.number;
                let distance = latest_number - head_number;
                let blocks_needed = (distance).min(self.ancestor_count);
                let code = if distance >= 15 {
                    LogCode::BlockIngestionLagging
                } else {
                    LogCode::BlockIngestionStatus
                };
                if distance > 0 {
                    info!(
                    &logger,
                        "Syncing {} blocks from Ethereum",
                        blocks_needed;
                        "current_block_head" => head_number,
                        "latest_block_head" => latest_number,
                        "blocks_behind" => distance,
                        "blocks_needed" => blocks_needed,
                        "code" => code,
                    );
                }
            }
        }

        // Store latest block in block store.
        // Might be a no-op if latest block is one that we have seen.
        // ingest_blocks will return a (potentially incomplete) list of blocks that are
        // missing.
        let mut missing_block_hash = self
            .ingest_block(logger, &eth_adapter, &latest_block.hash)
            .await?;

        // Repeatedly fetch missing parent blocks, and ingest them.
        // ingest_blocks will continue to tell us about more missing parent
        // blocks until we have filled in all missing pieces of the
        // blockchain in the block number range we care about.
        //
        // Loop will terminate because:
        // - The number of blocks in the ChainStore in the block number
        //   range [latest - ancestor_count, latest] is finite.
        // - The missing parents in the first iteration have at most block
        //   number latest-1.
        // - Each iteration loads parents of all blocks in the range whose
        //   parent blocks are not already in the ChainStore, so blocks
        //   with missing parents in one iteration will not have missing
        //   parents in the next.
        // - Therefore, if the missing parents in one iteration have at
        //   most block number N, then the missing parents in the next
        //   iteration will have at most block number N-1.
        // - Therefore, the loop will iterate at most ancestor_count times.
        while let Some(hash) = missing_block_hash {
            missing_block_hash = self.ingest_block(logger, &eth_adapter, &hash).await?;
        }
        Ok(())
    }

    async fn ingest_block(
        &self,
        logger: &Logger,
        eth_adapter: &Arc<EthereumAdapter>,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, IngestorError> {
        let block_hash = B256::from_slice(block_hash.as_slice());

        // Get the fully populated block
        let block = eth_adapter
            .block_by_hash(logger, block_hash)
            .await?
            .ok_or(IngestorError::BlockUnavailable(block_hash))?;
        let ethereum_block = eth_adapter.load_full_block(logger, block).await?;

        // We need something that implements `Block` to store the block; the
        // store does not care whether the block is final or not
        let ethereum_block = BlockFinality::NonFinal(EthereumBlockWithCalls {
            ethereum_block,
            calls: None,
        });

        // Store it in the database and try to advance the chain head pointer
        self.chain_store
            .upsert_block(Arc::new(ethereum_block))
            .await?;

        self.chain_store
            .cheap_clone()
            .attempt_chain_head_update(self.ancestor_count)
            .await
            .map(|missing| missing.map(|h256| h256.into()))
            .map_err(|e| {
                error!(logger, "failed to update chain head");
                IngestorError::Unknown(e)
            })
    }

    async fn latest_block(
        &self,
        logger: &Logger,
        eth_adapter: &Arc<EthereumAdapter>,
    ) -> Result<BlockPtr, IngestorError> {
        eth_adapter.latest_block_ptr(logger).await
    }

    /// Executes one polling iteration. On failure delegates to `on_poll_failure` for
    /// probe+switch logic.
    async fn poll_once(
        &self,
        providers: &[Arc<EthereumAdapter>],
        current_provider: &mut Option<String>,
    ) {
        // Resolve by name; resets to first provider if the tracked one left the list.
        let eth_adapter = resolve_provider(providers, current_provider, &self.logger).clone();
        // Pin the name so the next iteration knows which provider is active.
        let provider_name = eth_adapter.provider().to_string();
        let logger = self.logger.new(o!("provider" => provider_name.clone()));
        *current_provider = Some(provider_name);

        if let Err(err) = self.do_poll(&logger, eth_adapter).await {
            error!(logger, "Trying again after block polling failed: {}", err);
            on_poll_failure(providers, current_provider, &self.logger).await;
        }
    }
}

/// Returns the currently-tracked provider from `providers`.
///
/// If the tracked provider is no longer in the list (it became invalid and was removed by
/// `ProviderManager`), logs a warning, resets the state, and returns the first available
/// provider.
fn resolve_provider<'a, A: crate::EthereumAdapterTrait>(
    providers: &'a [Arc<A>],
    current_provider: &mut Option<String>,
    logger: &Logger,
) -> &'a Arc<A> {
    if let Some(name) = current_provider.as_ref() {
        if let Some(found) = providers.iter().find(|p| p.provider() == name) {
            return found;
        }
        warn!(
            logger,
            "Current RPC provider is no longer available, resetting to first provider";
            "provider" => name,
        );
        *current_provider = None;
    }
    &providers[0]
}

async fn on_poll_failure<A: crate::EthereumAdapterTrait>(
    providers: &[Arc<A>],
    current_provider: &mut Option<String>,
    logger: &Logger,
) {
    if providers.len() <= 1 {
        return;
    }

    let current_name = match current_provider.as_ref() {
        Some(name) => name.clone(),
        None => return,
    };

    // Probe the current provider before trying alternatives. do_poll() can fail for
    // reasons unrelated to RPC availability (e.g. DB errors from chain_head_ptr or
    // attempt_chain_head_update, or a BlockUnavailable from a chain reorg). All of
    // these surface as IngestorError::Unknown, indistinguishable from an RPC failure
    // at the match level. If the current provider responds to eth_blockNumber, the
    // failure was not caused by provider unavailability — switching cannot help.
    let current = providers.iter().find(|p| p.provider() == current_name);
    if let Some(current) = current
        && current.is_reachable().await
    {
        return;
    }

    // Probe all alternatives in parallel; switch to the first that responds.
    let futs = providers
        .iter()
        .filter(|p| p.provider() != current_name)
        .map(|p| {
            let name = p.provider().to_string();
            debug!(logger, "Trying RPC provider"; "provider" => &name);
            Box::pin(async move {
                if p.is_reachable().await {
                    Ok(name)
                } else {
                    Err(())
                }
            })
        });

    match select_ok(futs).await {
        Ok((next_name, _)) => {
            warn!(
                logger,
                "Switching RPC provider for block ingestor";
                "from" => &current_name,
                "to" => &next_name,
            );
            *current_provider = Some(next_name);
        }
        Err(_) => {
            warn!(
                logger,
                "All RPC providers unreachable, continuing on current provider";
                "provider" => &current_name,
            );
        }
    }
}

#[async_trait]
impl BlockIngestor for PollingBlockIngestor {
    async fn run(self: Box<Self>) {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        // Name of the provider currently in use. `None` until the first poll.
        let mut current_provider: Option<String> = None;

        loop {
            let providers = self
                .chain_client
                .rpc()
                .expect("PollingBlockIngestor is only created for RPC chains")
                .all_cheapest()
                .await;

            if providers.is_empty() {
                error!(self.logger, "No RPC providers available for block ingestor");
                backoff.sleep_async().await;
                continue;
            }
            backoff.reset();

            self.poll_once(&providers, &mut current_provider).await;

            if ENV_VARS.cleanup_blocks {
                self.cleanup_cached_blocks().await;
            }

            tokio::time::sleep(self.polling_interval).await;
        }
    }

    fn network_name(&self) -> ChainName {
        self.network_name.clone()
    }

    fn kind(&self) -> BlockchainKind {
        BlockchainKind::Ethereum
    }
}

#[cfg(test)]
mod tests {
    use super::on_poll_failure;
    use super::*;
    use crate::adapter::{
        ContractCallError, EthereumAdapter as EthereumAdapterTrait, EthereumRpcError,
    };
    use async_trait::async_trait;
    use graph::blockchain::{BlockPtr, ChainIdentifier};
    use graph::components::ethereum::AnyBlock;
    use graph::components::ethereum::LightEthereumBlock;
    use graph::data::store::ethereum::call;
    use graph::data_source::common::ContractCall;
    use graph::prelude::alloy::primitives::{Address, B256, Bytes, U256};
    use graph::prelude::{BlockNumber, Error, EthereumCallCache, Logger};
    use graph::slog::Discard;
    use std::collections::HashSet;
    use std::sync::Arc;

    struct MockEthAdapter {
        provider_name: String,
        reachable: bool,
    }

    impl MockEthAdapter {
        fn new(name: &str, reachable: bool) -> Arc<Self> {
            Arc::new(Self {
                provider_name: name.to_string(),
                reachable,
            })
        }
    }

    #[async_trait]
    impl EthereumAdapterTrait for MockEthAdapter {
        fn provider(&self) -> &str {
            &self.provider_name
        }

        async fn is_reachable(&self) -> bool {
            self.reachable
        }

        async fn net_identifiers(&self) -> Result<ChainIdentifier, Error> {
            unimplemented!()
        }
        async fn latest_block_ptr(
            &self,
            _: &Logger,
        ) -> Result<BlockPtr, graph::blockchain::IngestorError> {
            unimplemented!()
        }
        async fn load_blocks(
            &self,
            _: Logger,
            _: Arc<dyn graph::prelude::ChainStore>,
            _: HashSet<B256>,
        ) -> Result<Vec<Arc<LightEthereumBlock>>, Error> {
            unimplemented!()
        }
        async fn block_by_hash(&self, _: &Logger, _: B256) -> Result<Option<AnyBlock>, Error> {
            unimplemented!()
        }
        async fn block_by_number(
            &self,
            _: &Logger,
            _: BlockNumber,
        ) -> Result<Option<AnyBlock>, Error> {
            unimplemented!()
        }
        async fn load_full_block(
            &self,
            _: &Logger,
            _: AnyBlock,
        ) -> Result<graph::prelude::EthereumBlock, graph::blockchain::IngestorError> {
            unimplemented!()
        }
        async fn next_existing_ptr_to_number(
            &self,
            _: &Logger,
            _: BlockNumber,
        ) -> Result<BlockPtr, Error> {
            unimplemented!()
        }
        async fn contract_call(
            &self,
            _: &Logger,
            _: &ContractCall,
            _: Arc<dyn EthereumCallCache>,
        ) -> Result<(Option<Vec<graph::abi::DynSolValue>>, call::Source), ContractCallError>
        {
            unimplemented!()
        }
        async fn contract_calls(
            &self,
            _: &Logger,
            _: &[&ContractCall],
            _: Arc<dyn EthereumCallCache>,
        ) -> Result<Vec<(Option<Vec<graph::abi::DynSolValue>>, call::Source)>, ContractCallError>
        {
            unimplemented!()
        }
        async fn get_balance(
            &self,
            _: &Logger,
            _: Address,
            _: BlockPtr,
        ) -> Result<U256, EthereumRpcError> {
            unimplemented!()
        }
        async fn get_code(
            &self,
            _: &Logger,
            _: Address,
            _: BlockPtr,
        ) -> Result<Bytes, EthereumRpcError> {
            unimplemented!()
        }
    }

    fn discard_logger() -> Logger {
        Logger::root(Discard, o!())
    }

    #[test]
    fn test_current_provider_unavailable_resets_to_first() {
        // p0 left the validated list; only p1 and p2 remain (p1 is now at index 0).
        let providers: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p1", true),
            MockEthAdapter::new("p2", true),
        ];
        let mut current_provider = Some("p0".to_string());
        let resolved = resolve_provider(&providers, &mut current_provider, &discard_logger());
        assert_eq!(resolved.provider(), "p1");
        assert_eq!(current_provider, None);
    }

    #[tokio::test]
    async fn test_current_reachable_no_switch() {
        // Current provider is reachable: on_poll_failure should return early without
        // switching, even if alternatives are also reachable.
        let providers: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", true),
            MockEthAdapter::new("p1", true),
        ];
        let mut current_provider = Some("p0".to_string());
        on_poll_failure(&providers, &mut current_provider, &discard_logger()).await;
        assert_eq!(current_provider, Some("p0".to_string()));
    }

    #[tokio::test]
    async fn test_successful_probe_switches_provider() {
        let providers: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", false),
            MockEthAdapter::new("p1", true),
        ];
        let mut current_provider = Some("p0".to_string());
        on_poll_failure(&providers, &mut current_provider, &discard_logger()).await;
        assert_eq!(current_provider, Some("p1".to_string()));
    }

    #[tokio::test]
    async fn test_all_unreachable_stays_on_current() {
        let providers: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", false),
            MockEthAdapter::new("p1", false),
        ];
        let mut current_provider = Some("p0".to_string());
        on_poll_failure(&providers, &mut current_provider, &discard_logger()).await;
        assert_eq!(current_provider, Some("p0".to_string()));
    }

    #[tokio::test]
    async fn test_single_provider_no_switch() {
        let providers: Vec<Arc<MockEthAdapter>> = vec![MockEthAdapter::new("p0", true)];
        let mut current_provider = Some("p0".to_string());
        on_poll_failure(&providers, &mut current_provider, &discard_logger()).await;
        assert_eq!(current_provider, Some("p0".to_string()));
    }

    // --- Provider-list churn tests ---
    //
    // These tests simulate the state carried across loop iterations where
    // all_cheapest() returns a different subset each time.

    /// Current provider survives a list shrink; its index shifts but identity is preserved.
    ///
    /// Before: [p0, p1, p2], active = p1 (index 1)
    /// After:  [p1, p2],     active = p1 (index 0)
    ///
    /// With index-based tracking this would silently move to p2. Name-based tracking
    /// must keep us on p1.
    #[test]
    fn test_list_shrinks_current_provider_index_remaps() {
        let providers: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p1", true),
            MockEthAdapter::new("p2", true),
        ];
        let mut current_provider = Some("p1".to_string());
        let resolved = resolve_provider(&providers, &mut current_provider, &discard_logger());
        // p1 is now at index 0 — must not drift to p2
        assert_eq!(resolved.provider(), "p1");
        assert_eq!(current_provider, Some("p1".to_string()));
    }

    /// After failing over to p1, p1 itself disappears from the validated list in the
    /// next iteration. The ingestor must fall back to the first available provider (p0).
    ///
    /// Iteration 1: [p0(unreachable), p1(reachable)] — p0 fails, switch to p1
    /// Iteration 2: [p0(reachable), p2(reachable)]   — p1 gone, reset to p0
    #[tokio::test]
    async fn test_failover_target_then_disappears_resets_to_first() {
        let logger = discard_logger();

        // Iteration 1: p0 fails, switch to p1.
        let providers_iter1: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", false),
            MockEthAdapter::new("p1", true),
        ];
        let mut current_provider = Some("p0".to_string());
        on_poll_failure(&providers_iter1, &mut current_provider, &logger).await;
        assert_eq!(current_provider, Some("p1".to_string()));

        // Iteration 2: p1 has dropped from the validated list.
        let providers_iter2: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", true),
            MockEthAdapter::new("p2", true),
        ];
        let resolved = resolve_provider(&providers_iter2, &mut current_provider, &logger);
        assert_eq!(resolved.provider(), "p0"); // reset to first
        assert_eq!(current_provider, None);
    }

    /// After failing over to p1, the original provider p0 reappears in the list.
    /// The ingestor must stay on p1 — there is no proactive return to p0.
    ///
    /// Iteration 1: [p0(unreachable), p1(reachable)] — switch to p1
    /// Iteration 2: [p0(reachable), p1(reachable)]   — p0 is back; must stay on p1
    #[tokio::test]
    async fn test_original_provider_reappears_no_involuntary_return() {
        let logger = discard_logger();

        // Iteration 1: switch to p1.
        let providers_iter1: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", false),
            MockEthAdapter::new("p1", true),
        ];
        let mut current_provider = Some("p0".to_string());
        on_poll_failure(&providers_iter1, &mut current_provider, &logger).await;
        assert_eq!(current_provider, Some("p1".to_string()));

        // Iteration 2: p0 is back in the validated list alongside p1.
        let providers_iter2: Vec<Arc<MockEthAdapter>> = vec![
            MockEthAdapter::new("p0", true),
            MockEthAdapter::new("p1", true),
        ];
        let resolved = resolve_provider(&providers_iter2, &mut current_provider, &logger);
        // Must resolve to p1, not drift back to p0.
        assert_eq!(resolved.provider(), "p1");
        assert_eq!(current_provider, Some("p1".to_string()));
    }
}

use core::ops::Deref;
use envconfig::Envconfig;
use graph::env::EnvVarBoolean;
use graph::parking_lot;
use graph::prelude::{envconfig, lazy_static, BlockNumber};
use parking_lot::{RwLock, RwLockReadGuard};
use std::time::Duration;

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

struct Inner {
    env_vars: EnvVarsEthereum,
    geth_eth_call_errors: Vec<String>,
}

impl Inner {
    fn from_env() -> Result<Self, envconfig::Error> {
        let env_vars = EnvVarsEthereum::init_from_env()?;

        let geth_eth_call_errors = env_vars
            .geth_eth_call_errors
            .split(';')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();

        Ok(Self {
            env_vars,
            geth_eth_call_errors,
        })
    }
}

pub struct EnvVars {
    inner: RwLock<Inner>,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        Ok(Self {
            inner: RwLock::new(Inner::from_env()?),
        })
    }

    /// Refreshes all internally stored environment variables to reflect the
    /// current environment variable values.
    ///
    /// Only available in debug builds.
    #[cfg(debug_assertions)]
    pub fn refresh(&self) -> Result<(), envconfig::Error> {
        *self.inner.write() = Inner::from_env()?;
        Ok(())
    }

    fn inner(&self) -> RwLockReadGuard<Inner> {
        self.inner.read()
    }

    /// Set by the environment variable `ETHEREUM_REORG_THRESHOLD`. The default
    /// value is 250 blocks.
    pub fn reorg_threshold(&self) -> BlockNumber {
        self.inner().env_vars.reorg_threshold
    }

    /// Controls if firehose should be preferred over RPC if Firehose endpoints
    /// are present, if not set, the default behavior is is kept which is to
    /// automatically favor Firehose.
    ///
    /// Set by the flag `GRAPH_ETHEREUM_IS_FIREHOSE_PREFERRED`. On by default.
    pub fn is_firehose_preferred(&self) -> bool {
        self.inner().env_vars.is_firehose_preferred.0
    }

    /// Ideal number of triggers in a range. The range size will adapt to try to
    /// meet this.
    ///
    /// Set by the environment variable
    /// `GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE`. The default value is
    /// 100.
    pub fn target_triggers_per_block_range(&self) -> u64 {
        self.inner().env_vars.target_triggers_per_block_range
    }

    /// Maximum number of blocks to request in each chunk.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE`.
    /// The default value is 2000 blocks.
    pub fn max_block_range_size(&self) -> BlockNumber {
        self.inner().env_vars.max_block_range_size
    }

    /// Set by the environment variable `ETHEREUM_TRACE_STREAM_STEP_SIZE`. The
    /// default value is 50 blocks.
    pub fn trace_stream_step_size(&self) -> BlockNumber {
        self.inner().env_vars.trace_stream_step_size
    }

    /// Maximum range size for `eth.getLogs` requests that don't filter on
    /// contract address, only event signature, and are therefore expensive.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE`. The
    /// default value is 500 blocks, which is reasonable according to Ethereum
    /// node operators.
    pub fn max_event_only_range(&self) -> BlockNumber {
        self.inner().env_vars.max_event_only_range
    }

    /// Set by the environment variable `ETHEREUM_BLOCK_BATCH_SIZE`. The
    /// default value is 10 blocks.
    pub fn block_batch_size(&self) -> usize {
        self.inner().env_vars.block_batch_size
    }

    /// This should not be too large that it causes requests to timeout without
    /// us catching it, nor too small that it causes us to timeout requests that
    /// would've succeeded. We've seen successful `eth_getLogs` requests take
    /// over 120 seconds.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_JSON_RPC_TIMEOUT`
    /// (expressed in seconds). The default value is 180s.
    pub fn json_rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.inner().env_vars.json_rpc_timeout_in_secs)
    }

    /// This is used for requests that will not fail the subgraph if the limit
    /// is reached, but will simply restart the syncing step, so it can be low.
    /// This limit guards against scenarios such as requesting a block hash that
    /// has been reorged.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_REQUEST_RETRIES`. The
    /// default value is 10.
    pub fn request_retries(&self) -> usize {
        self.inner().env_vars.request_retries
    }

    /// Additional deterministic errors that have not yet been hardcoded.
    ///
    /// Set by the environment variable `GRAPH_GETH_ETH_CALL_ERRORS`, separated
    /// by `;`.
    pub fn geth_eth_call_errors(&self) -> impl Deref<Target = [String]> + '_ {
        RwLockReadGuard::map(self.inner(), |x| &x.geth_eth_call_errors[..])
    }

    /// Set by the environment variable `GRAPH_ETH_GET_LOGS_MAX_CONTRACTS`. The
    /// default value is 2000.
    pub fn get_logs_max_contracts(&self) -> usize {
        self.inner().env_vars.get_logs_max_contracts
    }

    /// Set by the environment variable
    /// `GRAPH_ETHEREUM_BLOCK_INGESTOR_MAX_CONCURRENT_JSON_RPC_CALLS_FOR_TXN_RECEIPTS`.
    /// The default value is 1000.
    pub fn block_ingestor_max_concurrent_json_rpc_calls(&self) -> usize {
        self.inner()
            .env_vars
            .block_ingestor_max_concurrent_json_rpc_calls
    }

    /// Set by the flag `GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES`. Enabled
    /// by default on macOS (to avoid DNS issues) and disabled by default on all
    /// other systems.
    pub fn fetch_receipts_in_batches(&self) -> bool {
        let default = cfg!(target_os = "macos");

        self.inner()
            .env_vars
            .fetch_receipts_in_batches
            .map(|x| x.0)
            .unwrap_or(default)
    }

    /// `graph_node::config` disallows setting this in a store with multiple
    /// shards. See 8b6ad0c64e244023ac20ced7897fe666 for the reason.
    ///
    /// Set by the flag `GRAPH_ETHEREUM_CLEANUP_BLOCKS`. Off by default.
    pub fn cleanup_blocks(&self) -> bool {
        self.inner().env_vars.cleanup_blocks.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct EnvVarsEthereum {
    #[envconfig(from = "GRAPH_ETHEREUM_IS_FIREHOSE_PREFERRED", default = "true")]
    is_firehose_preferred: EnvVarBoolean,
    #[envconfig(from = "GRAPH_GETH_ETH_CALL_ERRORS", default = "")]
    geth_eth_call_errors: String,
    #[envconfig(from = "GRAPH_ETH_GET_LOGS_MAX_CONTRACTS", default = "2000")]
    get_logs_max_contracts: usize,

    // JSON-RPC specific.
    #[envconfig(from = "ETHEREUM_REORG_THRESHOLD", default = "250")]
    reorg_threshold: BlockNumber,
    #[envconfig(from = "ETHEREUM_TRACE_STREAM_STEP_SIZE", default = "50")]
    trace_stream_step_size: BlockNumber,
    #[envconfig(from = "GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE", default = "500")]
    max_event_only_range: BlockNumber,
    #[envconfig(from = "ETHEREUM_BLOCK_BATCH_SIZE", default = "10")]
    block_batch_size: usize,
    #[envconfig(from = "GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE", default = "2000")]
    max_block_range_size: BlockNumber,
    #[envconfig(from = "GRAPH_ETHEREUM_JSON_RPC_TIMEOUT", default = "180")]
    json_rpc_timeout_in_secs: u64,
    #[envconfig(from = "GRAPH_ETHEREUM_REQUEST_RETRIES", default = "10")]
    request_retries: usize,
    #[envconfig(
        from = "GRAPH_ETHEREUM_BLOCK_INGESTOR_MAX_CONCURRENT_JSON_RPC_CALLS_FOR_TXN_RECEIPTS",
        default = "1000"
    )]
    block_ingestor_max_concurrent_json_rpc_calls: usize,
    #[envconfig(from = "GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES")]
    fetch_receipts_in_batches: Option<EnvVarBoolean>,
    #[envconfig(from = "GRAPH_ETHEREUM_CLEANUP_BLOCKS", default = "false")]
    cleanup_blocks: EnvVarBoolean,
    #[envconfig(
        from = "GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE",
        default = "100"
    )]
    target_triggers_per_block_range: u64,
}

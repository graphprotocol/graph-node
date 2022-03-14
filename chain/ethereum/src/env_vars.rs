use envconfig::Envconfig;
use graph::prelude::{lazy_static, BlockNumber};
use std::{str::FromStr, time::Duration};

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

#[derive(Clone, Debug)]
pub struct EnvVars {
    inner: Inner,
    geth_eth_call_errors: Vec<String>,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;
        let geth_eth_call_errors = inner
            .geth_eth_call_errors
            .split(';')
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect();

        Ok(Self {
            inner,
            geth_eth_call_errors,
        })
    }

    pub fn trace_stream_step_size(&self) -> BlockNumber {
        self.inner.trace_stream_step_size
    }

    /// Maximum range size for `eth.getLogs` requests that dont filter on
    /// contract address, only event signature, and are therefore expensive.
    ///
    /// According to Ethereum node operators, size 500 is reasonable here.
    pub fn max_event_only_range(&self) -> BlockNumber {
        self.inner.max_event_only_range
    }

    pub fn block_batch_size(&self) -> usize {
        self.inner.block_batch_size
    }

    /// This should not be too large that it causes requests to timeout without us catching it, nor
    /// too small that it causes us to timeout requests that would've succeeded. We've seen
    /// successful `eth_getLogs` requests take over 120 seconds.
    pub fn json_rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.json_rpc_timeout_in_sec)
    }

    /// This is used for requests that will not fail the subgraph if the limit is reached, but will
    /// simply restart the syncing step, so it can be low. This limit guards against scenarios such
    /// as requesting a block hash that has been reorged.
    pub fn request_retries(&self) -> usize {
        self.inner.request_retries
    }

    /// Additional deterministic errors that have not yet been hardcoded. Separated by `;`.
    pub fn geth_eth_call_errors(&self) -> &[String] {
        &self.geth_eth_call_errors
    }

    pub fn max_concurrent_json_rpc_calls(&self) -> usize {
        self.inner.max_concurrent_json_rpc_calls
    }

    /// Set to true by default in MacOS to avoid DNS issues.
    pub fn fetch_receipts_in_batches(&self) -> bool {
        self.inner.fetch_receipts_in_batches.0
    }

    /// graph_node::config disallows setting this in a store with multiple
    /// shards. See 8b6ad0c64e244023ac20ced7897fe666 for the reason
    pub fn cleanup_blocks(&self) -> bool {
        self.inner.cleanup_blocks.0
    }

    pub fn eth_get_logs_max_contracts(&self) -> usize {
        self.inner.eth_get_logs_max_contracts
    }

    /// Maximum number of blocks to request in each chunk.
    pub fn max_block_range_size(&self) -> BlockNumber {
        self.inner.max_block_range_size
    }

    /// Ideal number of triggers in a range. The range size will adapt to try to meet this.
    pub fn target_triggers_per_block_range(&self) -> u64 {
        self.inner.target_triggers_per_block_range
    }

    /// Controls if firehose should be preferred over RPC if Firehose endpoints
    /// are present, if not set, the default behavior is kept which is to
    /// automatically favor Firehose.
    pub fn is_firehose_preferred(&self) -> bool {
        self.inner.is_firehose_preferred.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "ETHEREUM_TRACE_STREAM_STEP_SIZE", default = "50")]
    trace_stream_step_size: BlockNumber,
    #[envconfig(from = "GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE", default = "500")]
    max_event_only_range: BlockNumber,
    #[envconfig(from = "ETHEREUM_BLOCK_BATCH_SIZE", default = "10")]
    block_batch_size: usize,
    #[envconfig(from = "GRAPH_ETHEREUM_JSON_RPC_TIMEOUT", default = "180")]
    json_rpc_timeout_in_sec: u64,
    #[envconfig(from = "GRAPH_ETHEREUM_REQUEST_RETRIES", default = "10")]
    request_retries: usize,
    #[envconfig(from = "GRAPH_GETH_ETH_CALL_ERRORS", default = "")]
    geth_eth_call_errors: String,
    #[envconfig(
        from = "GRAPH_ETHEREUM_BLOCK_INGESTOR_MAX_CONCURRENT_JSON_RPC_CALLS_FOR_TXN_RECEIPTS",
        default = "1000"
    )]
    max_concurrent_json_rpc_calls: usize,
    #[cfg(not(target_os = "macos"))]
    #[envconfig(
        from = "GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES",
        default = "false"
    )]
    fetch_receipts_in_batches: EnvVarBoolean,
    // Set to true by default in MacOS to avoid DNS issues.
    #[cfg(target_os = "macos")]
    #[envconfig(
        from = "GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES",
        default = "true"
    )]
    fetch_receipts_in_batches: EnvVarBoolean,
    #[envconfig(from = "GRAPH_ETHEREUM_CLEANUP_BLOCKS", default = "false")]
    cleanup_blocks: EnvVarBoolean,
    #[envconfig(from = "GRAPH_ETH_GET_LOGS_MAX_CONTRACTS", default = "2000")]
    eth_get_logs_max_contracts: usize,
    #[envconfig(from = "GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE", default = "2000")]
    max_block_range_size: BlockNumber,
    #[envconfig(
        from = "GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE",
        default = "100"
    )]
    target_triggers_per_block_range: u64,
    #[envconfig(from = "GRAPH_ETHEREUM_IS_FIREHOSE_PREFERRED", default = "true")]
    is_firehose_preferred: EnvVarBoolean,
}

#[derive(Copy, Clone, Debug)]
struct EnvVarBoolean(pub bool);

impl FromStr for EnvVarBoolean {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "true" | "1" => Ok(Self(true)),
            "false" | "0" => Ok(Self(false)),
            _ => Err("Invalid env. var. flag, expected true / false / 1 / 0".to_string()),
        }
    }
}

use envconfig::Envconfig;
use lazy_static::lazy_static;
use std::{collections::HashSet, str::FromStr, time::Duration};

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

#[derive(Clone, Debug)]
pub struct EnvVars {
    inner: Inner,
    log_query_timing: HashSet<String>,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;
        let log_query_timing = inner
            .log_query_timing
            .split(',')
            .map(ToOwned::to_owned)
            .collect();

        Ok(Self {
            inner,
            log_query_timing,
        })
    }

    /// Size limit of the entity LFU cache in bytes.
    pub fn entity_cache_size(&self) -> usize {
        self.inner.entity_cache_sizein_kb * 1000
    }

    pub fn subscription_throttle_interval(&self) -> Duration {
        Duration::from_millis(self.inner.subscription_throttle_interval_in_msec)
    }

    pub fn log_poi_events(&self) -> bool {
        self.inner.log_poi_events.0
    }

    // Load management can be disabled by setting the threshold to 0. This
    // makes sure in particular that we never take any of the locks
    // associated with it
    pub fn load_threshold(&self) -> Duration {
        Duration::from_millis(self.inner.load_threshold_in_msec)
    }

    pub fn load_management_is_disabled(&self) -> bool {
        self.load_threshold() == Duration::ZERO
    }

    pub fn jail_queries(&self) -> bool {
        self.inner.jail_queries.0
    }

    pub fn jail_threshold(&self) -> f64 {
        self.inner.jail_threshold
    }

    pub fn load_simulate(&self) -> bool {
        self.inner.load_simulate.0
    }

    pub fn allow_non_deterministic_fulltext_search(&self) -> bool {
        cfg!(debug_assertions) || self.inner.allow_non_deterministic_fulltext_search.0
    }

    pub fn max_spec_version(&self) -> Version {
        self.inner.max_spec_version.clone()
    }

    pub fn max_api_version(&self) -> Version {
        self.inner.max_api_version.clone()
    }

    pub fn disable_grafts(&self) -> bool {
        self.inner.disable_grafts.0
    }

    pub fn es_flush_interval(&self) -> Duration {
        Duration::from_secs(self.inner.es_flush_interval_in_sec)
    }

    pub fn es_max_retries(&self) -> usize {
        self.inner.es_max_retries
    }

    pub fn log_sql_timing(&self) -> bool {
        self.log_query_timing.contains("sql")
    }

    pub fn log_gql_timing(&self) -> bool {
        self.log_query_timing.contains("gql")
    }

    pub fn log_gql_cache_timing(&self) -> bool {
        self.log_gql_timing() && self.log_query_timing.contains("cache")
    }

    pub fn max_gas_per_handler(&self) -> u64 {
        self.inner.max_gas_per_handler.0
    }

    pub fn load_window_size(&self) -> Duration {
        Duration::from_secs(self.inner.load_window_size_in_sec)
    }

    pub fn load_bin_size(&self) -> Duration {
        Duration::from_secs(self.inner.load_bin_size_in_sec)
    }

    pub fn lock_contention_log_threshold(&self) -> Duration {
        Duration::from_millis(self.inner.lock_contention_log_threshold_in_msec)
    }

    /// The default file size limit for the IPFS cache is 1MiB.
    pub fn max_ipfs_cache_file_size(&self) -> u64 {
        self.inner.max_ipfs_cache_file_size
    }

    /// The default size limit for the IPFS cache is 50 items.
    pub fn max_ipfs_cache_size(&self) -> u64 {
        self.inner.max_ipfs_cache_size
    }

    /// The timeout for IPFS requests in seconds
    pub fn ipfs_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.ipfs_timeout_in_sec)
    }

    pub fn max_data_sources(&self) -> Option<usize> {
        self.inner.max_data_sources
    }

    // Keep deterministic errors non-fatal even if the subgraph is pending.
    // Used for testing Graph Node itself.
    pub fn disable_fail_fast(&self) -> bool {
        self.inner.disable_fail_fast.0
    }

    /// Ceiling for the backoff retry of non-deterministic errors, in seconds.
    pub fn subgraph_error_retry_ceil(&self) -> Duration {
        Duration::from_secs(self.inner.subgraph_error_retry_ceil_in_sec)
    }

    /// Environment variable for limiting the `ipfs.cat` file size limit.
    pub fn max_ipfs_file_size(&self) -> Option<usize> {
        self.inner.max_ipfs_file_size
    }

    /// Environment variable for limiting the `ipfs.map` file size limit.
    pub fn max_ipfs_map_file_size(&self) -> usize {
        self.inner.max_ipfs_map_file_size
    }

    pub fn mapping_handler_timeout(&self) -> Option<Duration> {
        self.inner
            .mapping_handler_timeout_in_sec
            .map(Duration::from_secs)
    }

    pub fn allow_non_deterministic_ipfs(&self) -> bool {
        self.inner.allow_non_deterministic_ipfs.0
    }

    /// Verbose logging of mapping inputs
    pub fn log_trigger_data(&self) -> bool {
        self.inner.log_trigger_data.0
    }

    /// Maximum stack size for the WASM runtime
    pub fn runtime_max_stack_size(&self) -> usize {
        self.inner.runtime_max_stack_size
    }
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "GRAPH_ENTITY_CACHE_SIZE", default = "10000")]
    entity_cache_sizein_kb: usize,
    #[envconfig(from = "SUBSCRIPTION_THROTTLE_INTERVAL", default = "1000")]
    subscription_throttle_interval_in_msec: u64,
    #[envconfig(from = "GRAPH_LOG_POI_EVENTS", default = "false")]
    log_poi_events: EnvVarBoolean,
    #[envconfig(from = "GRAPH_LOAD_THRESHOLD", default = "0")]
    load_threshold_in_msec: u64,
    #[envconfig(from = "GRAPH_LOAD_JAIL_THRESHOLD", default = "false")]
    jail_queries: EnvVarBoolean,
    #[envconfig(from = "GRAPH_LOAD_JAIL_THRESHOLD", default = "1e9")]
    jail_threshold: f64,
    #[envconfig(from = "GRAPH_LOAD_SIMULATE", default = "false")]
    load_simulate: EnvVarBoolean,
    #[envconfig(
        from = "GRAPH_ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH",
        default = "false"
    )]
    allow_non_deterministic_fulltext_search: EnvVarBoolean,
    #[envconfig(from = "GRAPH_MAX_SPEC_VERSION", default = "0.0.4")]
    max_spec_version: Version,
    #[envconfig(from = "GRAPH_MAX_API_VERSION", default = "0.0.6")]
    max_api_version: Version,
    #[envconfig(from = "GRAPH_DISABLE_GRAFTS", default = "false")]
    disable_grafts: EnvVarBoolean,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_FLUSH_INTERVAL_SECS", default = "5")]
    es_flush_interval_in_sec: u64,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_MAX_RETRIES", default = "5")]
    es_max_retries: usize,
    #[envconfig(from = "GRAPH_LOG_QUERY_TIMING", default = "")]
    log_query_timing: String,
    // Set max gas to 1000 seconds worth of gas per handler. The intent here is to have the determinism
    // cutoff be very high, while still allowing more reasonable timer based cutoffs. Having a unit
    // like 10 gas for ~1ns allows us to be granular in instructions which are aggregated into metered
    // blocks via https://docs.rs/pwasm-utils/0.16.0/pwasm_utils/fn.inject_gas_counter.html But we can
    // still charge very high numbers for other things.
    #[envconfig(from = "GRAPH_MAX_GAS_PER_HANDLER", default = "10_000_000_000_000")]
    max_gas_per_handler: WithoutUnderscores<u64>,
    #[envconfig(from = "GRAPH_LOAD_WINDOW_SIZE", default = "300")]
    load_window_size_in_sec: u64,
    #[envconfig(from = "GRAPH_LOAD_BIN_SIZE", default = "1")]
    load_bin_size_in_sec: u64,
    #[envconfig(from = "GRAPH_LOCK_CONTENTION_LOG_THRESHOLD_MS", default = "100")]
    lock_contention_log_threshold_in_msec: u64,

    // 1048576 = 1024 * 1024
    #[envconfig(from = "GRAPH_MAX_IPFS_CACHE_FILE_SIZE", default = "1048576")]
    max_ipfs_cache_file_size: u64,
    #[envconfig(from = "GRAPH_MAX_IPFS_CACHE_SIZE", default = "50")]
    max_ipfs_cache_size: u64,
    #[envconfig(from = "GRAPH_IPFS_TIMEOUT", default = "30")]
    ipfs_timeout_in_sec: u64,
    #[envconfig(from = "GRAPH_SUBGRAPH_MAX_DATA_SOURCES")]
    max_data_sources: Option<usize>,
    #[envconfig(from = "GRAPH_DISABLE_FAIL_FAST", default = "false")]
    disable_fail_fast: EnvVarBoolean,
    #[envconfig(from = "GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS", default = "30")]
    subgraph_error_retry_ceil_in_sec: u64,
    #[envconfig(from = "GRAPH_MAX_IPFS_FILE_BYTES")]
    max_ipfs_file_size: Option<usize>,
    // 268435456 = 256 * 1024 * 1024
    #[envconfig(from = "GRAPH_MAX_IPFS_MAP_FILE_SIZE", default = "268435456")]
    max_ipfs_map_file_size: usize,

    #[envconfig(from = "GRAPH_MAPPING_HANDLER_TIMEOUT")]
    mapping_handler_timeout_in_sec: Option<u64>,
    #[envconfig(from = "GRAPH_ALLOW_NON_DETERMINISTIC_IPFS", default = "true")]
    allow_non_deterministic_ipfs: EnvVarBoolean,
    #[envconfig(from = "GRAPH_LOG_TRIGGER_DATA", default = "true")]
    log_trigger_data: EnvVarBoolean,
    // Default is half of a MiB. 1048576 / 2 = 524288
    #[envconfig(from = "GRAPH_RUNTIME_MAX_STACK_SIZE", default = "524288")]
    runtime_max_stack_size: usize,
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

#[derive(Copy, Clone, Debug)]
struct WithoutUnderscores<T>(pub T);

impl<T> FromStr for WithoutUnderscores<T>
where
    T: FromStr,
    T::Err: ToString,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match T::from_str(s.replace('_', "").as_str()) {
            Ok(x) => Ok(Self(x)),
            Err(e) => Err(e.to_string()),
        }
    }
}

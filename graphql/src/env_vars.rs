use envconfig::Envconfig;
use lazy_static::lazy_static;
use std::{str::FromStr, time::Duration};

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

#[derive(Clone, Debug)]
pub struct EnvVars {
    inner: Inner,
    graphql_cached_subgraph_ids: Option<Vec<String>>,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;
        let graphql_cached_subgraph_ids = if inner.graphql_cached_subgraph_ids == "*" {
            None
        } else {
            Some(
                inner
                    .graphql_cached_subgraph_ids
                    .split(',')
                    .map(ToOwned::to_owned)
                    .collect(),
            )
        };

        Ok(Self {
            inner,
            graphql_cached_subgraph_ids,
        })
    }

    pub fn graphql_query_timeout(&self) -> Option<Duration> {
        self.inner
            .graphql_query_timeout_in_sec
            .map(Duration::from_secs)
    }

    pub fn graphql_max_complexity(&self) -> Option<u64> {
        self.inner.graphql_max_complexity
    }

    pub fn graphql_max_depth(&self) -> u8 {
        self.inner.graphql_max_depth
    }

    pub fn graphql_max_first(&self) -> u32 {
        self.inner.graphql_max_first
    }

    pub fn graphql_max_skip(&self) -> u32 {
        self.inner.graphql_max_skip
    }

    /// Allow skipping the check whether a deployment has changed while
    /// we were running a query. Once we are sure that the check mechanism
    /// is reliable, this variable should be removed
    pub fn graphql_allow_deployment_changes(&self) -> bool {
        self.inner.graphql_allow_deployment_changes.0
    }

    /// Returns [`None`] if queries are cached for all subgraphs (i.e. the
    /// environment variable is set to `*`).
    pub fn graphql_cached_subgraph_ids(&self) -> Option<&[String]> {
        self.graphql_cached_subgraph_ids.as_deref()
    }

    /// Returns `true` iff queries are cached for all subgraphs (i.e.
    /// [`EnvVar::graphql_cached_subgraph_ids`] is [`None`]).
    pub fn graphql_cache_all(&self) -> bool {
        self.graphql_cached_subgraph_ids.is_none()
    }

    /// How many blocks per network should be kept in the query cache. When the limit is reached,
    /// older blocks are evicted. This should be kept small since a lookup to the cache is O(n) on
    /// this value, and the cache memory usage also increases with larger number. Set to 0 to disable
    /// the cache. Defaults to 2.
    pub fn query_cache_blocks(&self) -> usize {
        self.inner.query_cache_blocks
    }

    /// Maximum total memory to be used by the cache. Each block has a max size of
    /// `QUERY_CACHE_MAX_MEM` / (`QUERY_CACHE_BLOCKS` * `GRAPH_QUERY_BLOCK_CACHE_SHARDS`).
    /// The env var is in MB.
    pub fn query_cache_max_memory(&self) -> usize {
        self.inner.query_cache_max_memory_in_mbytes * 1_000_000
    }

    pub fn query_cache_stale_period(&self) -> u64 {
        self.inner.query_cache_stale_period
    }

    /// In how many shards (mutexes) the query block cache is split.
    /// Ideally this should divide 256 so that the distribution of queries to
    /// shards is even.
    pub fn query_block_cache_shards(&self) -> u8 {
        self.inner.query_block_cache_shards
    }

    pub fn query_lfu_cache_shards(&self) -> u8 {
        self.inner
            .query_lfu_cache_shards
            .unwrap_or_else(|| self.query_block_cache_shards())
    }

    pub fn disable_experimental_feature_select_by_specific_attribute_names(&self) -> bool {
        self.inner
            .disable_experimental_feature_select_by_specific_attribute_names
            .0
    }

    pub fn graphql_warn_result_size(&self) -> usize {
        self.inner.graphql_warn_result_size
    }

    pub fn graphql_error_result_size(&self) -> usize {
        self.inner.graphql_error_result_size
    }

    pub fn enable_graphql_validations(&self) -> bool {
        self.inner.enable_graphql_validations.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "GRAPH_GRAPHQL_QUERY_TIMEOUT")]
    graphql_query_timeout_in_sec: Option<u64>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_COMPLEXITY")]
    graphql_max_complexity: Option<u64>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_DEPTH", default = "255")]
    graphql_max_depth: u8,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_FIRST", default = "1000")]
    graphql_max_first: u32,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_SKIP", default = "4294967295")]
    graphql_max_skip: u32,
    #[envconfig(from = "GRAPH_GRAPHQL_ALLOW_DEPLOYMENT_CHANGE", default = "false")]
    graphql_allow_deployment_changes: EnvVarBoolean,
    // Comma separated subgraph ids to cache queries for.
    // If `*` is present in the list, queries are cached for all subgraphs.
    // Defaults to "*".
    #[envconfig(from = "GRAPH_CACHED_SUBGRAPH_IDS", default = "*")]
    graphql_cached_subgraph_ids: String,
    #[envconfig(from = "GRAPH_QUERY_CACHE_BLOCKS", default = "2")]
    query_cache_blocks: usize,
    #[envconfig(from = "GRAPH_QUERY_CACHE_MAX_MEM", default = "1000")]
    query_cache_max_memory_in_mbytes: usize,
    #[envconfig(from = "GRAPH_QUERY_CACHE_STALE_PERIOD", default = "100")]
    query_cache_stale_period: u64,
    #[envconfig(from = "GRAPH_QUERY_BLOCK_CACHE_SHARDS", default = "128")]
    query_block_cache_shards: u8,
    #[envconfig(from = "QUERY_LFU_CACHE_SHARDS")]
    query_lfu_cache_shards: Option<u8>,
    // Setting this environment variable to any value will enable the experimental feature "Select
    // by Specific Attributes".
    #[envconfig(from = "GRAPH_ENABLE_SELECT_BY_SPECIFIC_ATTRIBUTES", default = "false")]
    disable_experimental_feature_select_by_specific_attribute_names: EnvVarBoolean,
    #[envconfig(
        from = "GRAPH_GRAPHQL_WARN_RESULT_SIZE",
        default = "18446744073709551615" // usize::MAX
    )]
    graphql_warn_result_size: usize,
    #[envconfig(
        from = "GRAPH_GRAPHQL_ERROR_RESULT_SIZE",
        default = "18446744073709551615" // usize::MAX
    )]
    graphql_error_result_size: usize,
    #[envconfig(from = "ENABLE_GRAPHQL_VALIDATIONS", default = "false")]
    enable_graphql_validations: EnvVarBoolean,
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

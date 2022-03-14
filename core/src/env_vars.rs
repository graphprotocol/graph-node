use envconfig::Envconfig;
use graph::prelude::lazy_static;
use std::{str::FromStr, time::Duration};

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

#[derive(Clone, Debug)]
pub struct EnvVars {
    inner: Inner,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;

        Ok(Self { inner })
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
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
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

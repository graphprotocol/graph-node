use envconfig::Envconfig;
use lazy_static::lazy_static;
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

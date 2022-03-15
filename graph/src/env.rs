use envconfig::Envconfig;
use lazy_static::lazy_static;
use std::{
    env::VarError,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

pub static UNSAFE_CONFIG: AtomicBool = AtomicBool::new(false);

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

// This is currently unusued but is kept as a potentially useful mechanism.
/// Panics if:
/// - The value is not UTF8.
/// - The value cannot be parsed as T.
/// - The value differs from the default, and `--unsafe-config` flag is not set.
pub fn unsafe_env_var<E: std::error::Error + Send + Sync, T: FromStr<Err = E> + Eq>(
    name: &'static str,
    default_value: T,
) -> T {
    let var = match std::env::var(name) {
        Ok(var) => var,
        Err(VarError::NotPresent) => return default_value,
        Err(VarError::NotUnicode(_)) => panic!("environment variable {} is not UTF8", name),
    };

    let value = var
        .parse::<T>()
        .unwrap_or_else(|e| panic!("failed to parse environment variable {}: {}", name, e));

    if !UNSAFE_CONFIG.load(Ordering::SeqCst) && value != default_value {
        panic!(
            "unsafe environment variable {} is set. The recommended action is to unset it. \
             If this is not an indexer on the network, \
             you may provide the `--unsafe-config` to allow setting this variable.",
            name
        )
    }

    value
}

/// Panics if:
/// - The value is not UTF8.
/// - The value cannot be parsed as T..
pub fn env_var<E: std::error::Error + Send + Sync, T: FromStr<Err = E> + Eq>(
    name: &'static str,
    default_value: T,
) -> T {
    let var = match std::env::var(name) {
        Ok(var) => var,
        Err(VarError::NotPresent) => return default_value,
        Err(VarError::NotUnicode(_)) => panic!("environment variable {} is not UTF8", name),
    };

    var.parse::<T>()
        .unwrap_or_else(|e| panic!("failed to parse environment variable {}: {}", name, e))
}

pub struct EnvVars {
    inner: Inner,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;

        Ok(Self { inner })
    }

    /// Size limit of the entity LFU cache.
    ///
    /// Set by the environment variable `GRAPH_ENTITY_CACHE_SIZE` (expressed in
    /// kilobytes). The default value is 10 megabytes.
    pub fn entity_cache_size(&self) -> usize {
        self.inner.entity_cache_size_in_kb * 1000
    }

    pub fn subscription_throttle_interval(&self) -> Duration {
        Duration::from_millis(self.inner.subscription_throttle_interval_in_ms)
    }

    pub fn load_threshold(&self) -> Duration {
        Duration::from_millis(self.inner.load_threshold_in_ms)
    }

    /// When the system is overloaded, any query that causes more than this
    /// fraction of the effort will be rejected for as long as the process is
    /// running (i.e. even after the overload situation is resolved).
    ///
    /// Set by the environment variable `GRAPH_LOAD_THRESHOLD`
    /// (expressed as a number). No default value is provided. When *not* set,
    /// no queries will ever be jailed, even though they will still be subject
    /// to normal load management when the system is overloaded.
    pub fn load_jail_threshold(&self) -> Option<f64> {
        self.inner.load_jail_threshold
    }
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "GRAPH_ENTITY_CACHE_SIZE", default = "10000")]
    entity_cache_size_in_kb: usize,
    #[envconfig(from = "SUBSCRIPTION_THROTTLE_INTERVAL", default = "1000")]
    subscription_throttle_interval_in_ms: u64,
    #[envconfig(from = "GRAPH_LOAD_THRESHOLD", default = "0")]
    load_threshold_in_ms: u64,
    #[envconfig(from = "GRAPH_LOAD_JAIL_THRESHOLD")]
    load_jail_threshold: Option<f64>,
}

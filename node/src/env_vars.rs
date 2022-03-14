use envconfig::Envconfig;
use graph::prelude::{BlockNumber, SubgraphVersionSwitchingMode};
use lazy_static::lazy_static;
use std::str::FromStr;

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

    /// Defaults to 250.
    pub fn reorg_threshold(&self) -> BlockNumber {
        self.inner.reorg_threshold
    }

    pub fn experimental_static_filter(&self) -> bool {
        self.inner.experimental_static_filter.0
    }

    pub fn disable_firehose_filters(&self) -> bool {
        self.inner.disable_firehose_filters.0
    }

    pub fn experimental_subgraph_version_switching_mode(&self) -> SubgraphVersionSwitchingMode {
        self.inner.experimental_subgraph_version_switching_mode
    }

    pub fn kill_node_if_unresponsive(&self) -> bool {
        self.inner.kill_node_if_unresponsive.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "ETHEREUM_REORG_THRESHOLD", default = "250")]
    reorg_threshold: BlockNumber,
    #[envconfig(from = "EXPERIMENTAL_STATIC_FILTERS", default = "false")]
    experimental_static_filter: EnvVarBoolean,
    #[envconfig(from = "DISABLE_FIREHOSE_FILTERS", default = "false")]
    disable_firehose_filters: EnvVarBoolean,
    #[envconfig(
        from = "EXPERIMENTAL_SUBGRAPH_VERSION_SWITCHING_MODE",
        default = "instant"
    )]
    experimental_subgraph_version_switching_mode: SubgraphVersionSwitchingMode,
    #[envconfig(from = "GRAPH_KILL_IF_UNRESPONSIVE", default = "false")]
    kill_node_if_unresponsive: EnvVarBoolean,
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

use crate::prelude::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt, str::FromStr};

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum SubgraphFeature {
    NonFatalErrors,
    Grafting,
    FullTextSearch,
    IpfsOnEthereumContracts,
}

impl fmt::Display for SubgraphFeature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        serde_plain::to_string(self)
            .map_err(|_| fmt::Error)
            .and_then(|x| write!(f, "{}", x))
    }
}

impl FromStr for SubgraphFeature {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        serde_plain::from_str(value)
            .map_err(|_error| anyhow::anyhow!("Invalid subgraph feature: {}", value))
    }
}

}

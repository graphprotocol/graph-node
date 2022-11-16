use graph::{anyhow::Error, impl_slog_value};
use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::str::FromStr;

use crate::data_source::DataSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapabilities {}

impl PartialOrd for NodeCapabilities {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl FromStr for NodeCapabilities {
    type Err = Error;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Ok(NodeCapabilities {})
    }
}

impl fmt::Display for NodeCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("near")
    }
}

impl_slog_value!(NodeCapabilities, "{}");

impl graph::blockchain::NodeCapabilities<crate::Chain> for NodeCapabilities {
    fn from_data_sources(_data_sources: &[DataSource]) -> Self {
        NodeCapabilities {}
    }
}

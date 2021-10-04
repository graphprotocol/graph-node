use anyhow::Error;
use graph::impl_slog_value;
use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::str::FromStr;

use crate::DataSource;

// FIXME (NEAR): Hard-coded all stuff related to capabilities, what capabilities means for a NEAR support?
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapabilities {}

impl PartialOrd for NodeCapabilities {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        // FIXME (NEAR): Does a None ordering makes sense?
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

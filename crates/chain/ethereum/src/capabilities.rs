use anyhow::Error;
use graph::impl_slog_value;
use std::fmt;
use std::str::FromStr;
use std::{
    cmp::{Ord, Ordering, PartialOrd},
    collections::BTreeSet,
};

use crate::DataSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapabilities {
    pub archive: bool,
    pub traces: bool,
}

// Take all NodeCapabilities fields into account when ordering
// A NodeCapabilities instance is considered equal or greater than another
// if all of its fields are equal or greater than the other
impl Ord for NodeCapabilities {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.archive.cmp(&other.archive),
            self.traces.cmp(&other.traces),
        ) {
            (Ordering::Greater, Ordering::Greater) => Ordering::Greater,
            (Ordering::Greater, Ordering::Equal) => Ordering::Greater,
            (Ordering::Equal, Ordering::Greater) => Ordering::Greater,
            (Ordering::Equal, Ordering::Equal) => Ordering::Equal,
            (Ordering::Less, _) => Ordering::Less,
            (_, Ordering::Less) => Ordering::Less,
        }
    }
}

impl PartialOrd for NodeCapabilities {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for NodeCapabilities {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let capabilities: BTreeSet<&str> = s.split(',').collect();
        Ok(NodeCapabilities {
            archive: capabilities.contains("archive"),
            traces: capabilities.contains("traces"),
        })
    }
}

impl fmt::Display for NodeCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let NodeCapabilities { archive, traces } = self;

        let mut capabilities = vec![];
        if *archive {
            capabilities.push("archive");
        }
        if *traces {
            capabilities.push("traces");
        }

        f.write_str(&capabilities.join(", "))
    }
}

impl_slog_value!(NodeCapabilities, "{}");

impl graph::blockchain::NodeCapabilities<crate::Chain> for NodeCapabilities {
    fn from_data_sources(data_sources: &[DataSource]) -> Self {
        NodeCapabilities {
            archive: data_sources.iter().any(|ds| {
                ds.mapping
                    .requires_archive()
                    .expect("failed to parse mappings")
            }),
            traces: data_sources.into_iter().any(|ds| {
                ds.mapping.has_call_handler() || ds.mapping.has_block_handler_with_call_filter()
            }),
        }
    }
}

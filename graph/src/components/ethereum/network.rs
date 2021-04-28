use std::fmt;
use std::{
    cmp::{Ord, Ordering, PartialOrd},
    collections::BTreeSet,
};

pub use crate::impl_slog_value;
use crate::prelude::Error;
use std::str::FromStr;

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
        let capabilities: BTreeSet<&str> = s.split(",").collect();
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

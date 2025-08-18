use super::ProofOfIndexingEvent;
use crate::util::stable_hash_glue::impl_stable_hash;

pub struct PoICausalityRegion<'a> {
    pub blocks: Vec<Block<'a>>,
}

impl_stable_hash!(PoICausalityRegion<'_> {blocks});

impl PoICausalityRegion<'_> {
    pub fn from_network(network: &str) -> String {
        format!("ethereum/{}", network)
    }
}

#[derive(Default)]
pub struct Block<'a> {
    pub events: Vec<ProofOfIndexingEvent<'a>>,
}

impl_stable_hash!(Block<'_> {events});

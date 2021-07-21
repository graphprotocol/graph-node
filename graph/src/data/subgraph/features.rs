use crate::{
    blockchain::Blockchain,
    data::{schema::Schema, subgraph::SubgraphManifest},
    prelude::{Deserialize, Serialize},
};
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
// TODO: How can we access the mapping source code? (schema and manifest are ok)
pub fn detect_features<C: Blockchain>(manifest: &SubgraphManifest<C>) -> BTreeSet<SubgraphFeature> {
    vec![
        detect_non_fatal_errors(&manifest),
        detect_grafting(&manifest),
        detect_full_text_search(&manifest.schema),
        detect_ipfs_on_ethereum_contracts(&manifest),
    ]
    .into_iter()
    .filter_map(|x| x)
    .collect()
}

fn detect_non_fatal_errors<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> Option<SubgraphFeature> {
    todo!()
}
fn detect_grafting<C: Blockchain>(manifest: &SubgraphManifest<C>) -> Option<SubgraphFeature> {
    todo!()
}
fn detect_full_text_search(schema: &Schema) -> Option<SubgraphFeature> {
    todo!()
}
fn detect_ipfs_on_ethereum_contracts<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> Option<SubgraphFeature> {
    todo!()
}

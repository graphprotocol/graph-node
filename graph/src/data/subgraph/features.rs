//! Functions to detect subgraph features.
//!
//! The rationale of this module revolves around the concept of feature declaration and detection.
//!
//! Features are declared in the `subgraph.yml` file, also known as the subgraph's manifest, and are
//! validated by a graph-node instance during the deploy phase or by direct request.
//!
//! A feature validation error will be triggered in either one of the following cases:
//! 1. Using undeclared features.
//! 2. Declaring a nonexistent feature name.
//!
//! Feature validation is performed by the [`validate_subgraph_features`] function.

use crate::{
    blockchain::Blockchain,
    data::{graphql::DocumentExt, schema::Schema, subgraph::SubgraphManifest},
    prelude::{Deserialize, Serialize},
};
use itertools::Itertools;
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Serialize, thiserror::Error, Debug)]
pub enum SubgraphFeatureValidationError {
    /// A feature is used by the subgraph but it is not declared in the `features` section of the manifest file.
    #[error("The feature `{}` is used by the subgraph  but it is not declared in the manifest.", fmt_subgraph_features(.0))]
    Undeclared(BTreeSet<SubgraphFeature>),

    /// TODO: Variant for errors propagated from whithin detection functions. It is still unclear
    /// why they happen, but,hopefully, they would receive their own variant in this enum.
    #[error("{0}")]
    Other(String),
}

fn fmt_subgraph_features(subgraph_features: &BTreeSet<SubgraphFeature>) -> String {
    subgraph_features.iter().join(", ")
}

impl From<anyhow::Error> for SubgraphFeatureValidationError {
    fn from(error: anyhow::Error) -> Self {
        SubgraphFeatureValidationError::Other(error.to_string())
    }
}

pub fn validate_subgraph_features<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> Result<BTreeSet<SubgraphFeature>, SubgraphFeatureValidationError> {
    let declared: &BTreeSet<SubgraphFeature> = &manifest.features;
    let used = detect_features(&manifest)?;
    let undeclared: BTreeSet<SubgraphFeature> = used.difference(&declared).cloned().collect();
    if !undeclared.is_empty() {
        Err(SubgraphFeatureValidationError::Undeclared(undeclared))
    } else {
        Ok(used)
    }
}

// TODO: How can we access the mapping source code? (schema and manifest are ok)
pub fn detect_features<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> anyhow::Result<BTreeSet<SubgraphFeature>> {
    let collected_features: BTreeSet<SubgraphFeature> = vec![
        detect_non_fatal_errors(&manifest),
        detect_grafting(&manifest),
        detect_full_text_search(&manifest.schema)?,
        detect_ipfs_on_ethereum_contracts(&manifest),
    ]
    .into_iter()
    .filter_map(|x| x)
    .collect();
    Ok(collected_features)
}

fn detect_non_fatal_errors<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> Option<SubgraphFeature> {
    if manifest.features.contains(&SubgraphFeature::NonFatalErrors) {
        Some(SubgraphFeature::NonFatalErrors)
    } else {
        None
    }
}

fn detect_grafting<C: Blockchain>(manifest: &SubgraphManifest<C>) -> Option<SubgraphFeature> {
    if manifest.graft.is_some() {
        Some(SubgraphFeature::Grafting)
    } else {
        None
    }
}

fn detect_full_text_search(schema: &Schema) -> anyhow::Result<Option<SubgraphFeature>> {
    if schema.document.get_fulltext_directives()?.is_empty() {
        Ok(Some(SubgraphFeature::FullTextSearch))
    } else {
        Ok(None)
    }
}
fn detect_ipfs_on_ethereum_contracts<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> Option<SubgraphFeature> {
    for mapping in manifest.mappings() {
        if mapping.calls_host_fn("ipfs.map") || mapping.calls_host_fn("ipfs.cat") {
            return Some(SubgraphFeature::IpfsOnEthereumContracts);
        }
    }
    None
}

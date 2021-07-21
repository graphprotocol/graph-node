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
    data::{schema::Schema, subgraph::SubgraphManifest},
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
    #[error("The feature `{0}` is used by the subgraph  but it is not declared in the manifest.")]
    Undeclared(SubgraphFeature),

    /// A name for a feature that doesn't exist was declared.
    #[error("The name `{0}` is not a known feature.")]
    NonExistent(String),

    /// A feature is declared but is not used by the subgraph.
    #[error("The feature `{0}` is declared but is not used by the subgraph.")]
    Unused(SubgraphFeature),
    /// A feature name was declared multiple times in the manifest file.

    #[error("The feature `{0}` was declared more than once in the manifest file.")]
    MultipleDeclarations(String),
}

#[derive(Serialize)]
pub struct FeatureValidationResults {
    used: BTreeSet<SubgraphFeature>,
    errors: BTreeSet<SubgraphFeatureValidationError>,
}

pub fn validate_subgraph_features<C: Blockchain>(
    manifest: &SubgraphManifest<C>,
) -> FeatureValidationResults {
    let mut errors: BTreeSet<SubgraphFeatureValidationError> = BTreeSet::new();
    let mut declared: BTreeSet<SubgraphFeature> = BTreeSet::new();

    let declared_feature_names: Vec<&str> =
        todo!("just list the names inside the features section of the manifest");
    let used = detect_features(&manifest);

    // check if any feature name were declared more than once
    declared_feature_names
        .iter()
        .counts()
        .into_iter()
        .filter(|(_, count)| *count != 1)
        .for_each(|(feature_name, _)| {
            errors.insert(SubgraphFeatureValidationError::MultipleDeclarations(
                feature_name.to_string(),
            ));
        });

    for declared_feature_name in &declared_feature_names {
        match SubgraphFeature::from_str(declared_feature_name) {
            Ok(feature) => declared.insert(feature),
            Err(_) => errors.insert(SubgraphFeatureValidationError::NonExistent(
                declared_feature_name.to_string(),
            )),
        };
    }

    let undeclared = used.difference(&declared);
    for feature in undeclared {
        errors.insert(SubgraphFeatureValidationError::Undeclared(feature.clone()));
    }
    FeatureValidationResults { used, errors }
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

use itertools::Itertools;
use lazy_static::lazy_static;
use semver::Version;
use std::collections::BTreeSet;
use thiserror::Error;

use super::SubgraphManifestValidationError;

/// This version adds a new subgraph validation step that rejects manifests whose mappings have
/// different API versions if at least one of them is equal to or higher than `0.0.5`.
pub const API_VERSION_0_0_5: Version = Version::new(0, 0, 5);

/// Before this check was introduced, there were already subgraphs in the wild with spec version
/// 0.0.3, due to confusion with the api version. To avoid breaking those, we accept 0.0.3 though it
/// doesn't exist.
pub const SPEC_VERSION_0_0_3: Version = Version::new(0, 0, 3);

/// This version supports subgraph feature management.
pub const SPEC_VERSION_0_0_4: Version = Version::new(0, 0, 4);

pub const MIN_SPEC_VERSION: Version = Version::new(0, 0, 2);

lazy_static! {
    pub static ref MAX_SPEC_VERSION: Version = std::env::var("GRAPH_MAX_SPEC_VERSION")
        .ok()
        .and_then(|api_version_str| Version::parse(&api_version_str).ok())
        .unwrap_or(SPEC_VERSION_0_0_4);
    pub(super) static ref MAX_API_VERSION: semver::Version = std::env::var("GRAPH_MAX_API_VERSION")
        .ok()
        .and_then(|api_version_str| semver::Version::parse(&api_version_str).ok())
        .unwrap_or(semver::Version::new(0, 0, 6));
}

#[derive(Clone, PartialEq, Debug)]
pub struct UnifiedMappingApiVersion(Option<Version>);

impl UnifiedMappingApiVersion {
    pub fn equal_or_greater_than(&self, other_version: &Version) -> bool {
        assert!(
            other_version >= &API_VERSION_0_0_5,
            "api versions before 0.0.5 should not be used for comparison"
        );
        match &self.0 {
            Some(version) => version >= other_version,
            None => false,
        }
    }

    pub(super) fn try_from_versions(
        versions: impl Iterator<Item = Version>,
    ) -> Result<Self, DifferentMappingApiVersions> {
        let unique_versions: BTreeSet<Version> = versions.collect();

        let all_below_referential_version = unique_versions.iter().all(|v| *v < API_VERSION_0_0_5);
        let all_the_same = unique_versions.len() == 1;

        let unified_version: Option<Version> = match (all_below_referential_version, all_the_same) {
            (false, false) => return Err(DifferentMappingApiVersions(unique_versions)),
            (false, true) => Some(unique_versions.iter().nth(0).unwrap().clone()),
            (true, _) => None,
        };

        Ok(UnifiedMappingApiVersion(unified_version))
    }
}

pub(super) fn format_versions(versions: &BTreeSet<Version>) -> String {
    versions.iter().map(ToString::to_string).join(", ")
}

#[derive(Error, Debug, PartialEq)]
#[error("Expected a single apiVersion for mappings. Found: {}.", format_versions(.0))]
pub struct DifferentMappingApiVersions(BTreeSet<Version>);

impl From<DifferentMappingApiVersions> for SubgraphManifestValidationError {
    fn from(versions: DifferentMappingApiVersions) -> Self {
        SubgraphManifestValidationError::DifferentApiVersions(versions.0)
    }
}

#[test]
fn unified_mapping_api_version_from_iterator() {
    let input = [
        vec![Version::new(0, 0, 5), Version::new(0, 0, 5)], // Ok(Some(0.0.5))
        vec![Version::new(0, 0, 6), Version::new(0, 0, 6)], // Ok(Some(0.0.6))
        vec![Version::new(0, 0, 3), Version::new(0, 0, 4)], // Ok(None)
        vec![Version::new(0, 0, 4), Version::new(0, 0, 4)], // Ok(None)
        vec![Version::new(0, 0, 3), Version::new(0, 0, 5)], // Err({0.0.3, 0.0.5})
        vec![Version::new(0, 0, 6), Version::new(0, 0, 5)], // Err({0.0.5, 0.0.6})
    ];
    let output: [Result<UnifiedMappingApiVersion, DifferentMappingApiVersions>; 6] = [
        Ok(UnifiedMappingApiVersion(Some(Version::new(0, 0, 5)))),
        Ok(UnifiedMappingApiVersion(Some(Version::new(0, 0, 6)))),
        Ok(UnifiedMappingApiVersion(None)),
        Ok(UnifiedMappingApiVersion(None)),
        Err(DifferentMappingApiVersions(
            input[4].iter().cloned().collect::<BTreeSet<Version>>(),
        )),
        Err(DifferentMappingApiVersions(
            input[5].iter().cloned().collect::<BTreeSet<Version>>(),
        )),
    ];
    for (version_vec, expected_unified_version) in input.iter().zip(output.iter()) {
        let unified = UnifiedMappingApiVersion::try_from_versions(version_vec.iter().cloned());
        match (unified, expected_unified_version) {
            (Ok(a), Ok(b)) => assert_eq!(a, *b),
            (Err(a), Err(b)) => assert_eq!(a, *b),
            _ => panic!(),
        }
    }
}

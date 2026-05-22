//! Pure manifest validation functions.
//!
//! This module provides pure validation functions that can be used by both
//! graph-node and external tools (like gnd) without requiring database access
//! or other heavy dependencies.

use semver::Version;
use std::collections::BTreeSet;

use super::api_version::API_VERSION_0_0_5;
use super::{DifferentMappingApiVersions, SubgraphManifestValidationError};

/// Validate that the manifest has at least one data source.
pub fn validate_has_data_sources(count: usize) -> Result<(), SubgraphManifestValidationError> {
    if count == 0 {
        Err(SubgraphManifestValidationError::NoDataSources)
    } else {
        Ok(())
    }
}

/// Validate that all data sources use the same network.
///
/// Returns Ok(()) if:
/// - All data sources use the same network, or
/// - No data sources specify a network (for non-chain data sources)
///
/// Returns an error if data sources use different networks.
pub fn validate_single_network(
    networks: &[Option<&str>],
) -> Result<(), SubgraphManifestValidationError> {
    let unique_networks: BTreeSet<&str> = networks.iter().filter_map(|n| *n).collect();

    match unique_networks.len() {
        0 => Err(SubgraphManifestValidationError::EthereumNetworkRequired),
        1 => Ok(()),
        _ => Err(SubgraphManifestValidationError::MultipleEthereumNetworks),
    }
}

/// Validate that all data sources use the same API version when any version >= 0.0.5.
///
/// For API versions < 0.0.5, different versions are allowed.
/// For API versions >= 0.0.5, all data sources must use the same version.
pub fn validate_api_versions(versions: &[Version]) -> Result<(), SubgraphManifestValidationError> {
    let unique_versions: BTreeSet<Version> = versions.iter().cloned().collect();

    let all_below_005 = unique_versions.iter().all(|v| *v < API_VERSION_0_0_5);
    let all_the_same = unique_versions.len() <= 1;

    if all_below_005 || all_the_same {
        Ok(())
    } else {
        Err(SubgraphManifestValidationError::DifferentApiVersions(
            DifferentMappingApiVersions(unique_versions),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_has_data_sources() {
        assert!(validate_has_data_sources(0).is_err());
        assert!(validate_has_data_sources(1).is_ok());
        assert!(validate_has_data_sources(5).is_ok());
    }

    #[test]
    fn test_validate_single_network() {
        // All same network - ok
        assert!(validate_single_network(&[Some("mainnet"), Some("mainnet")]).is_ok());

        // Single network - ok
        assert!(validate_single_network(&[Some("mainnet")]).is_ok());

        // No network specified - error
        assert!(matches!(
            validate_single_network(&[]),
            Err(SubgraphManifestValidationError::EthereumNetworkRequired)
        ));

        // All None - error (no network)
        assert!(matches!(
            validate_single_network(&[None, None]),
            Err(SubgraphManifestValidationError::EthereumNetworkRequired)
        ));

        // Different networks - error
        let result = validate_single_network(&[Some("mainnet"), Some("ropsten")]);
        assert!(matches!(
            result,
            Err(SubgraphManifestValidationError::MultipleEthereumNetworks)
        ));

        // Mixed with None - ok if all specified networks are the same
        assert!(validate_single_network(&[Some("mainnet"), None, Some("mainnet")]).is_ok());
    }

    #[test]
    fn test_validate_api_versions() {
        // All same version >= 0.0.5 - ok
        assert!(validate_api_versions(&[Version::new(0, 0, 5), Version::new(0, 0, 5)]).is_ok());

        // All same version >= 0.0.6 - ok
        assert!(validate_api_versions(&[Version::new(0, 0, 6), Version::new(0, 0, 6)]).is_ok());

        // All below 0.0.5 - ok (even if different)
        assert!(validate_api_versions(&[Version::new(0, 0, 3), Version::new(0, 0, 4)]).is_ok());

        // Mixed with version >= 0.0.5 and different - error
        assert!(validate_api_versions(&[Version::new(0, 0, 3), Version::new(0, 0, 5)]).is_err());

        // Different versions both >= 0.0.5 - error
        assert!(validate_api_versions(&[Version::new(0, 0, 5), Version::new(0, 0, 6)]).is_err());

        // Empty - ok
        assert!(validate_api_versions(&[]).is_ok());

        // Single version - ok
        assert!(validate_api_versions(&[Version::new(0, 0, 5)]).is_ok());
    }
}

mod chain_identifier_validator;
mod extended_blocks_check;
mod genesis_hash_check;
mod network_details;
mod provider_check;
mod provider_manager;

pub use self::chain_identifier_validator::chain_id_validator;
pub use self::chain_identifier_validator::ChainIdentifierValidationError;
pub use self::chain_identifier_validator::ChainIdentifierValidator;
pub use self::extended_blocks_check::ExtendedBlocksCheck;
pub use self::genesis_hash_check::GenesisHashCheck;
pub use self::network_details::NetworkDetails;
pub use self::provider_check::ProviderCheck;
pub use self::provider_check::ProviderCheckStatus;
pub use self::provider_manager::ProviderCheckStrategy;
pub use self::provider_manager::ProviderManager;

use crate::http::Uri;
use std::collections::HashMap;

// Used to increase memory efficiency.
// Currently, there is no need to create a separate type for this.
pub type ChainName = crate::data::value::Word;

// Used to increase memory efficiency.
// Currently, there is no need to create a separate type for this.
pub type ProviderName = crate::data::value::Word;

/// Resolved per-chain Amp configuration, with the address parsed as a `Uri`.
///
/// This struct is the *runtime* counterpart of the TOML-level `AmpConfig`
/// (which stores the address as a plain `String`). The `Config::amp_chain_configs()`
/// method bridges the two by parsing each address string into a `Uri`.
#[derive(Clone, Debug)]
pub struct AmpChainConfig {
    pub address: Uri,
    pub token: Option<String>,
    pub context_dataset: String,
    pub context_table: String,
    pub network: Option<String>,
}

/// Maps Amp network names to internal graph-node chain names.
///
/// Amp-powered subgraphs may use different network names than graph-node
/// (e.g., Amp uses `"ethereum-mainnet"` while graph-node uses `"mainnet"`).
/// This type provides a config-driven translation layer.
#[derive(Clone, Debug, Default)]
pub struct AmpChainNames(HashMap<ChainName, ChainName>);

impl AmpChainNames {
    pub fn new(mapping: HashMap<ChainName, ChainName>) -> Self {
        AmpChainNames(mapping)
    }

    /// Returns the internal chain name for an AMP alias, or the input
    /// unchanged if no alias matches.
    pub fn resolve(&self, name: &ChainName) -> ChainName {
        self.0.get(name).cloned().unwrap_or_else(|| name.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amp_chain_names_resolve_known_alias() {
        let mut map = HashMap::new();
        map.insert(
            ChainName::from("ethereum-mainnet"),
            ChainName::from("mainnet"),
        );
        let names = AmpChainNames::new(map);
        assert_eq!(
            names.resolve(&ChainName::from("ethereum-mainnet")),
            ChainName::from("mainnet")
        );
    }

    #[test]
    fn amp_chain_names_resolve_unknown_passthrough() {
        let names = AmpChainNames::default();
        assert_eq!(
            names.resolve(&ChainName::from("mainnet")),
            ChainName::from("mainnet")
        );
    }
}

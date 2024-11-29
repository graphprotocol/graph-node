mod chain_identifier_store;
mod extended_blocks_check;
mod genesis_hash_check;
mod network_details;
mod provider_check;
mod provider_manager;

pub use self::chain_identifier_store::ChainIdentifierStore;
pub use self::chain_identifier_store::ChainIdentifierStoreError;
pub use self::extended_blocks_check::ExtendedBlocksCheck;
pub use self::genesis_hash_check::GenesisHashCheck;
pub use self::network_details::NetworkDetails;
pub use self::provider_check::ProviderCheck;
pub use self::provider_check::ProviderCheckStatus;
pub use self::provider_manager::ProviderCheckStrategy;
pub use self::provider_manager::ProviderManager;

// Used to increase memory efficiency.
// Currently, there is no need to create a separate type for this.
pub type ChainName = crate::data::value::Word;

// Used to increase memory efficiency.
// Currently, there is no need to create a separate type for this.
pub type ProviderName = crate::data::value::Word;

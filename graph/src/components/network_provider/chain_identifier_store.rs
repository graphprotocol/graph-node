use anyhow::anyhow;
use thiserror::Error;

use crate::blockchain::BlockHash;
use crate::blockchain::ChainIdentifier;
use crate::components::network_provider::ChainName;
use crate::components::store::BlockStore;
use crate::components::store::ChainStore;

/// Additional requirements for stores that are necessary for provider checks.
pub trait ChainIdentifierStore: Send + Sync + 'static {
    /// Verifies that the chain identifier returned by the network provider
    /// matches the previously stored value.
    ///
    /// Fails if the identifiers do not match or if something goes wrong.
    fn validate_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierStoreError>;

    /// Saves the provided identifier that will be used as the source of truth
    /// for future validations.
    fn update_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierStoreError>;
}

#[derive(Debug, Error)]
pub enum ChainIdentifierStoreError {
    #[error("identifier not set for chain '{0}'")]
    IdentifierNotSet(ChainName),

    #[error("net version mismatch on chain '{chain_name}'; expected '{store_net_version}', found '{chain_net_version}'")]
    NetVersionMismatch {
        chain_name: ChainName,
        store_net_version: String,
        chain_net_version: String,
    },

    #[error("genesis block hash mismatch on chain '{chain_name}'; expected '{store_genesis_block_hash}', found '{chain_genesis_block_hash}'")]
    GenesisBlockHashMismatch {
        chain_name: ChainName,
        store_genesis_block_hash: BlockHash,
        chain_genesis_block_hash: BlockHash,
    },

    #[error("store error: {0:#}")]
    Store(#[source] anyhow::Error),
}

impl<C, B> ChainIdentifierStore for B
where
    C: ChainStore,
    B: BlockStore<ChainStore = C>,
{
    fn validate_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierStoreError> {
        let chain_store = self.chain_store(&chain_name).ok_or_else(|| {
            ChainIdentifierStoreError::Store(anyhow!(
                "unable to get store for chain '{chain_name}'"
            ))
        })?;

        let store_identifier = chain_store
            .chain_identifier()
            .map_err(|err| ChainIdentifierStoreError::Store(err))?;

        if store_identifier.is_default() {
            return Err(ChainIdentifierStoreError::IdentifierNotSet(
                chain_name.clone(),
            ));
        }

        if store_identifier.net_version != chain_identifier.net_version {
            // This behavior is carried over from the previous implementation.
            // Firehose does not provide a `net_version`, so switching to and from Firehose will
            // cause this value to be different. We prioritize RPC when creating the chain,
            // but it's possible that it will be created by Firehose. Firehose always returns "0"
            // for `net_version`, so we need to allow switching between the two.
            if store_identifier.net_version != "0" && chain_identifier.net_version != "0" {
                return Err(ChainIdentifierStoreError::NetVersionMismatch {
                    chain_name: chain_name.clone(),
                    store_net_version: store_identifier.net_version,
                    chain_net_version: chain_identifier.net_version.clone(),
                });
            }
        }

        if store_identifier.genesis_block_hash != chain_identifier.genesis_block_hash {
            return Err(ChainIdentifierStoreError::GenesisBlockHashMismatch {
                chain_name: chain_name.clone(),
                store_genesis_block_hash: store_identifier.genesis_block_hash,
                chain_genesis_block_hash: chain_identifier.genesis_block_hash.clone(),
            });
        }

        Ok(())
    }

    fn update_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierStoreError> {
        let chain_store = self.chain_store(&chain_name).ok_or_else(|| {
            ChainIdentifierStoreError::Store(anyhow!(
                "unable to get store for chain '{chain_name}'"
            ))
        })?;

        chain_store
            .set_chain_identifier(chain_identifier)
            .map_err(|err| ChainIdentifierStoreError::Store(err))
    }
}

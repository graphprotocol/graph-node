use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::blockchain::BlockHash;
use crate::blockchain::ChainIdentifier;
use crate::components::network_provider::ChainName;
use crate::components::store::ChainIdStore;

/// Additional requirements for stores that are necessary for provider checks.
#[async_trait]
pub trait ChainIdentifierValidator: Send + Sync + 'static {
    /// Verifies that the chain identifier returned by the network provider
    /// matches the previously stored value.
    ///
    /// Fails if the identifiers do not match or if something goes wrong.
    async fn validate_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierValidationError>;

    /// Saves the provided identifier that will be used as the source of truth
    /// for future validations.
    async fn update_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierValidationError>;
}

#[derive(Debug, Error)]
pub enum ChainIdentifierValidationError {
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

pub fn chain_id_validator(store: Box<dyn ChainIdStore>) -> Arc<dyn ChainIdentifierValidator> {
    Arc::new(ChainIdentifierStore::new(store))
}

pub(crate) struct ChainIdentifierStore {
    store: Box<dyn ChainIdStore>,
}

impl ChainIdentifierStore {
    pub fn new(store: Box<dyn ChainIdStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ChainIdentifierValidator for ChainIdentifierStore {
    async fn validate_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierValidationError> {
        let store_identifier = self
            .store
            .chain_identifier(chain_name)
            .await
            .map_err(|err| ChainIdentifierValidationError::Store(err))?;

        if store_identifier.is_default() {
            return Err(ChainIdentifierValidationError::IdentifierNotSet(
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
                return Err(ChainIdentifierValidationError::NetVersionMismatch {
                    chain_name: chain_name.clone(),
                    store_net_version: store_identifier.net_version,
                    chain_net_version: chain_identifier.net_version.clone(),
                });
            }
        }

        if store_identifier.genesis_block_hash != chain_identifier.genesis_block_hash {
            return Err(ChainIdentifierValidationError::GenesisBlockHashMismatch {
                chain_name: chain_name.clone(),
                store_genesis_block_hash: store_identifier.genesis_block_hash,
                chain_genesis_block_hash: chain_identifier.genesis_block_hash.clone(),
            });
        }

        Ok(())
    }

    async fn update_identifier(
        &self,
        chain_name: &ChainName,
        chain_identifier: &ChainIdentifier,
    ) -> Result<(), ChainIdentifierValidationError> {
        self.store
            .set_chain_identifier(chain_name, chain_identifier)
            .await
            .map_err(|err| ChainIdentifierValidationError::Store(err))
    }
}

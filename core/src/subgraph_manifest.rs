use graph::{
    cheap_clone::CheapClone as _,
    components::{
        link_resolver::{LinkResolver, LinkResolverContext},
        store::{StoreError, SubgraphStore},
    },
    data::subgraph::DeploymentHash,
};
use slog::{debug, Logger};

pub(super) async fn load_raw_subgraph_manifest(
    logger: &Logger,
    subgraph_store: &dyn SubgraphStore,
    link_resolver: &dyn LinkResolver,
    hash: &DeploymentHash,
) -> Result<serde_yaml::Mapping, Error> {
    if let Some(raw_manifest) =
        subgraph_store
            .raw_manifest(hash)
            .await
            .map_err(|e| Error::LoadManifest {
                hash: hash.cheap_clone(),
                source: anyhow::Error::from(e),
            })?
    {
        debug!(logger, "Loaded raw manifest from the subgraph store");
        return Ok(raw_manifest);
    }

    debug!(logger, "Loading raw manifest using link resolver");

    let link_resolver =
        link_resolver
            .for_manifest(&hash.to_string())
            .map_err(|e| Error::CreateLinkResolver {
                hash: hash.cheap_clone(),
                source: e,
            })?;

    let file_bytes = link_resolver
        .cat(
            &LinkResolverContext::new(hash, logger),
            &hash.to_ipfs_link(),
        )
        .await
        .map_err(|e| Error::LoadManifest {
            hash: hash.cheap_clone(),
            source: e,
        })?;

    let raw_manifest: serde_yaml::Mapping =
        serde_yaml::from_slice(&file_bytes).map_err(|e| Error::ParseManifest {
            hash: hash.cheap_clone(),
            source: e,
        })?;

    subgraph_store
        .set_raw_manifest_once(hash, &raw_manifest)
        .await
        .map_err(|e| Error::StoreManifest {
            hash: hash.cheap_clone(),
            source: e,
        })?;

    Ok(raw_manifest)
}

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("failed to create link resolver for '{hash}': {source:#}")]
    CreateLinkResolver {
        hash: DeploymentHash,
        source: anyhow::Error,
    },

    #[error("failed to load manifest for '{hash}': {source:#}")]
    LoadManifest {
        hash: DeploymentHash,
        source: anyhow::Error,
    },

    #[error("failed to parse manifest for '{hash}': {source:#}")]
    ParseManifest {
        hash: DeploymentHash,
        source: serde_yaml::Error,
    },

    #[error("failed to store manifest for '{hash}': {source:#}")]
    StoreManifest {
        hash: DeploymentHash,
        source: StoreError,
    },
}

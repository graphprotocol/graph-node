pub mod data_source;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use itertools::Itertools;
use semver::Version;
use slog::Logger;

use crate::{
    amp::Client,
    blockchain::Blockchain,
    cheap_clone::CheapClone as _,
    components::link_resolver::LinkResolver,
    data::subgraph::{BaseSubgraphManifest, DeploymentHash, UnresolvedSubgraphManifest},
    data_source::DataSource as GenericDataSource,
    schema::InputSchema,
};

pub use self::data_source::DataSource;

/// Represents a valid Amp subgraph manifest.
///
/// This manifest contains parsed, formatted, and resolved data.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// The schema of the subgraph.
    ///
    /// Contains all the entities, aggregations, and relationships between them.
    pub schema: InputSchema,

    /// The Amp data sources of the subgraph.
    ///
    /// An Amp subgraph can only contain Amp data sources.
    pub data_sources: Vec<DataSource>,
}

impl Manifest {
    /// Resolves and returns a valid Amp subgraph manifest.
    pub async fn resolve<C: Blockchain, AC: Client>(
        logger: &Logger,
        link_resolver: Arc<dyn LinkResolver>,
        amp_client: Arc<AC>,
        max_spec_version: Version,
        deployment: DeploymentHash,
        raw_manifest: serde_yaml::Mapping,
    ) -> Result<Self> {
        let unresolved_manifest =
            UnresolvedSubgraphManifest::<C>::parse(deployment.cheap_clone(), raw_manifest)
                .context("failed to parse subgraph manifest")?;

        let resolved_manifest = unresolved_manifest
            .resolve(
                &deployment,
                &link_resolver,
                Some(amp_client),
                logger,
                max_spec_version,
            )
            .await
            .context("failed to resolve subgraph manifest")?;

        let BaseSubgraphManifest {
            id: _,
            spec_version: _,
            features: _,
            description: _,
            repository: _,
            schema,
            data_sources,
            graft: _,
            templates: _,
            chain: _,
            indexer_hints: _,
        } = resolved_manifest;

        let data_sources_count = data_sources.len();
        let amp_data_sources = data_sources
            .into_iter()
            .filter_map(|data_source| match data_source {
                GenericDataSource::Amp(amp_data_source) => Some(amp_data_source),
                _ => None,
            })
            .collect_vec();

        if amp_data_sources.is_empty() {
            bail!("invalid subgraph manifest: failed to find Amp data sources");
        }

        if amp_data_sources.len() != data_sources_count {
            bail!("invalid subgraph manifest: only Amp data sources are allowed");
        }

        Ok(Self {
            schema,
            data_sources: amp_data_sources,
        })
    }
}

use crate::{
    blockchain::{block_stream::EntitySourceOperation, Block, Blockchain},
    components::{link_resolver::LinkResolver, store::BlockNumber},
    data::{
        subgraph::{
            calls_host_fn, SubgraphManifest, UnresolvedSubgraphManifest, LATEST_VERSION,
            SPEC_VERSION_1_3_0,
        },
        value::Word,
    },
    data_source::{self, common::DeclaredCall},
    ensure,
    prelude::{CheapClone, DataSourceContext, DeploymentHash, Link},
    schema::TypeKind,
};
use anyhow::{anyhow, Context, Error, Result};
use futures03::{stream::FuturesOrdered, TryStreamExt};
use serde::Deserialize;
use slog::{info, Logger};
use std::{fmt, sync::Arc};

use super::{
    common::{CallDecls, FindMappingABI, MappingABI, UnresolvedMappingABI},
    DataSourceTemplateInfo, TriggerWithHandler,
};

pub const SUBGRAPH_DS_KIND: &str = "subgraph";

const ENTITY_HANDLER_KINDS: &str = "entity";

#[derive(Debug, Clone)]
pub struct DataSource {
    pub kind: String,
    pub name: String,
    pub network: String,
    pub manifest_idx: u32,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
}

impl DataSource {
    pub fn new(
        kind: String,
        name: String,
        network: String,
        manifest_idx: u32,
        source: Source,
        mapping: Mapping,
        context: Arc<Option<DataSourceContext>>,
        creation_block: Option<BlockNumber>,
    ) -> Self {
        Self {
            kind,
            name,
            network,
            manifest_idx,
            source,
            mapping,
            context,
            creation_block,
        }
    }

    pub fn min_spec_version(&self) -> semver::Version {
        SPEC_VERSION_1_3_0
    }

    pub fn handler_kind(&self) -> &str {
        ENTITY_HANDLER_KINDS
    }

    pub fn network(&self) -> Option<&str> {
        Some(&self.network)
    }

    pub fn match_and_decode<C: Blockchain>(
        &self,
        block: &Arc<C::Block>,
        trigger: &TriggerData,
    ) -> Result<Option<TriggerWithHandler<super::MappingTrigger<C>>>> {
        if self.source.address != trigger.source {
            return Ok(None);
        }

        let mut matching_handlers: Vec<_> = self
            .mapping
            .handlers
            .iter()
            .filter(|handler| handler.entity == trigger.entity_type())
            .collect();

        // Get the matching handler if any
        let handler = match matching_handlers.pop() {
            Some(handler) => handler,
            None => return Ok(None),
        };

        ensure!(
            matching_handlers.is_empty(),
            format!(
                "Multiple handlers defined for entity `{}`, only one is supported",
                trigger.entity_type()
            )
        );

        let calls =
            DeclaredCall::from_entity_trigger(&self.mapping, &handler.calls, &trigger.entity)?;
        let mapping_trigger = MappingEntityTrigger {
            data: trigger.clone(),
            calls,
        };

        Ok(Some(TriggerWithHandler::new(
            data_source::MappingTrigger::Subgraph(mapping_trigger),
            handler.handler.clone(),
            block.ptr(),
            block.timestamp(),
        )))
    }

    pub fn address(&self) -> Option<Vec<u8>> {
        Some(self.source.address().to_bytes())
    }

    pub fn source_subgraph(&self) -> DeploymentHash {
        self.source.address()
    }
}

pub type Base64 = Word;

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    pub address: DeploymentHash,
    #[serde(default)]
    pub start_block: BlockNumber,
}

impl Source {
    /// The concept of an address may or not make sense for a subgraph data source, but graph node
    /// will use this in a few places where some sort of not necessarily unique id is useful:
    /// 1. This is used as the value to be returned to mappings from the `dataSource.address()` host
    ///    function, so changing this is a breaking change.
    /// 2. This is used to match with triggers with hosts in `fn hosts_for_trigger`, so make sure
    ///    the `source` of the data source is equal the `source` of the `TriggerData`.
    pub fn address(&self) -> DeploymentHash {
        self.address.clone()
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub language: String,
    pub api_version: semver::Version,
    pub abis: Vec<Arc<MappingABI>>,
    pub entities: Vec<String>,
    pub handlers: Vec<EntityHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

impl Mapping {
    pub fn requires_archive(&self) -> anyhow::Result<bool> {
        calls_host_fn(&self.runtime, "ethereum.call")
    }
}

impl FindMappingABI for Mapping {
    fn find_abi(&self, abi_name: &str) -> Result<Arc<MappingABI>, Error> {
        Ok(self
            .abis
            .iter()
            .find(|abi| abi.name == abi_name)
            .ok_or_else(|| anyhow!("No ABI entry with name `{}` found", abi_name))?
            .cheap_clone())
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct EntityHandler {
    pub handler: String,
    pub entity: String,
    #[serde(default)]
    pub calls: CallDecls,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub name: String,
    pub network: String,
    pub source: UnresolvedSource,
    pub mapping: UnresolvedMapping,
    pub context: Option<DataSourceContext>,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedSource {
    address: DeploymentHash,
    #[serde(default)]
    start_block: BlockNumber,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub file: Link,
    pub handlers: Vec<EntityHandler>,
    pub abis: Option<Vec<UnresolvedMappingABI>>,
    pub entities: Vec<String>,
}

impl UnresolvedDataSource {
    fn validate_mapping_entities<C: Blockchain>(
        mapping_entities: &[String],
        source_manifest: &SubgraphManifest<C>,
    ) -> Result<(), Error> {
        for entity in mapping_entities {
            let type_kind = source_manifest.schema.kind_of_declared_type(&entity);

            match type_kind {
                Some(TypeKind::Interface) => {
                    return Err(anyhow!(
                        "Entity {} is an interface and cannot be used as a mapping entity",
                        entity
                    ));
                }
                Some(TypeKind::Aggregation) => {
                    return Err(anyhow!(
                        "Entity {} is an aggregation and cannot be used as a mapping entity",
                        entity
                    ));
                }
                None => {
                    return Err(anyhow!("Entity {} not found in source manifest", entity));
                }
                Some(TypeKind::Object) => {
                    // Check if the entity is immutable
                    let entity_type = source_manifest.schema.entity_type(entity)?;
                    if !entity_type.is_immutable() {
                        return Err(anyhow!(
                            "Entity {} is not immutable and cannot be used as a mapping entity",
                            entity
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn resolve_source_manifest<C: Blockchain>(
        &self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Arc<SubgraphManifest<C>>, Error> {
        let resolver: Arc<dyn LinkResolver> =
            Arc::from(resolver.for_manifest(&self.source.address.to_string())?);
        let source_raw = resolver
            .cat(logger, &self.source.address.to_ipfs_link())
            .await
            .context(format!(
                "Failed to resolve source subgraph [{}] manifest",
                self.source.address,
            ))?;

        let source_raw: serde_yaml::Mapping =
            serde_yaml::from_slice(&source_raw).context(format!(
                "Failed to parse source subgraph [{}] manifest as YAML",
                self.source.address
            ))?;

        let deployment_hash = self.source.address.clone();

        let source_manifest = UnresolvedSubgraphManifest::<C>::parse(deployment_hash, source_raw)
            .context(format!(
            "Failed to parse source subgraph [{}] manifest",
            self.source.address
        ))?;

        let resolver: Arc<dyn LinkResolver> =
            Arc::from(resolver.for_manifest(&self.source.address.to_string())?);
        source_manifest
            .resolve(&resolver, logger, LATEST_VERSION.clone())
            .await
            .context(format!(
                "Failed to resolve source subgraph [{}] manifest",
                self.source.address
            ))
            .map(Arc::new)
    }

    /// Recursively verifies that all grafts in the chain meet the minimum spec version requirement for a subgraph source
    async fn verify_graft_chain_sourcable<C: Blockchain>(
        manifest: Arc<SubgraphManifest<C>>,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        graft_chain: &mut Vec<String>,
    ) -> Result<(), Error> {
        // Add current manifest to graft chain
        graft_chain.push(manifest.id.to_string());

        // Check if current manifest meets spec version requirement
        if manifest.spec_version < SPEC_VERSION_1_3_0 {
            return Err(anyhow!(
                "Subgraph with a spec version {} is not supported for a subgraph source, minimum supported version is {}. Graft chain: {}",
                manifest.spec_version,
                SPEC_VERSION_1_3_0,
                graft_chain.join(" -> ")
            ));
        }

        // If there's a graft, recursively verify it
        if let Some(graft) = &manifest.graft {
            let graft_raw = resolver
                .cat(logger, &graft.base.to_ipfs_link())
                .await
                .context("Failed to resolve graft base manifest")?;

            let graft_raw: serde_yaml::Mapping = serde_yaml::from_slice(&graft_raw)
                .context("Failed to parse graft base manifest as YAML")?;

            let graft_manifest =
                UnresolvedSubgraphManifest::<C>::parse(graft.base.clone(), graft_raw)
                    .context("Failed to parse graft base manifest")?
                    .resolve(resolver, logger, LATEST_VERSION.clone())
                    .await
                    .context("Failed to resolve graft base manifest")?;

            Box::pin(Self::verify_graft_chain_sourcable(
                Arc::new(graft_manifest),
                resolver,
                logger,
                graft_chain,
            ))
            .await?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub(super) async fn resolve<C: Blockchain>(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSource, Error> {
        info!(logger, "Resolve subgraph data source";
            "name" => &self.name,
            "kind" => &self.kind,
            "source" => format_args!("{:?}", &self.source),
        );

        let kind = self.kind.clone();
        let source_manifest = self.resolve_source_manifest::<C>(resolver, logger).await?;
        let source_spec_version = &source_manifest.spec_version;
        if source_spec_version < &SPEC_VERSION_1_3_0 {
            return Err(anyhow!(
                "Source subgraph [{}] manifest spec version {} is not supported, minimum supported version is {}",
                self.source.address,
                source_spec_version,
                SPEC_VERSION_1_3_0
            ));
        }

        // Verify the entire graft chain meets spec version requirements
        let mut graft_chain = Vec::new();
        Self::verify_graft_chain_sourcable(
            source_manifest.clone(),
            resolver,
            logger,
            &mut graft_chain,
        )
        .await?;

        if source_manifest
            .data_sources
            .iter()
            .any(|ds| matches!(ds, crate::data_source::DataSource::Subgraph(_)))
        {
            return Err(anyhow!(
                "Nested subgraph data sources [{}] are not supported.",
                self.name
            ));
        }

        let mapping_entities: Vec<String> = self
            .mapping
            .handlers
            .iter()
            .map(|handler| handler.entity.clone())
            .collect();

        Self::validate_mapping_entities(&mapping_entities, &source_manifest)?;

        let source = Source {
            address: self.source.address,
            start_block: self.source.start_block,
        };

        Ok(DataSource {
            manifest_idx,
            kind,
            name: self.name,
            network: self.network,
            source,
            mapping: self.mapping.resolve(resolver, logger).await?,
            context: Arc::new(self.context),
            creation_block: None,
        })
    }
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, Error> {
        info!(logger, "Resolve subgraph ds mapping"; "link" => &self.file.link);

        // Resolve each ABI and collect the results
        let abis = match self.abis {
            Some(abis) => {
                abis.into_iter()
                    .map(|unresolved_abi| {
                        let resolver = Arc::clone(resolver);
                        let logger = logger.clone();
                        async move {
                            let resolved_abi = unresolved_abi.resolve(&resolver, &logger).await?;
                            Ok::<_, Error>(Arc::new(resolved_abi))
                        }
                    })
                    .collect::<FuturesOrdered<_>>()
                    .try_collect::<Vec<_>>()
                    .await?
            }
            None => Vec::new(),
        };

        Ok(Mapping {
            language: self.language,
            api_version: semver::Version::parse(&self.api_version)?,
            entities: self.entities,
            handlers: self.handlers,
            abis,
            runtime: Arc::new(resolver.cat(logger, &self.file).await?),
            link: self.file,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct UnresolvedDataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug)]
pub struct DataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub manifest_idx: u32,
    pub mapping: Mapping,
}

impl Into<DataSourceTemplateInfo> for DataSourceTemplate {
    fn into(self) -> DataSourceTemplateInfo {
        let DataSourceTemplate {
            kind,
            network: _,
            name,
            manifest_idx,
            mapping,
        } = self;

        DataSourceTemplateInfo {
            api_version: mapping.api_version.clone(),
            runtime: Some(mapping.runtime),
            name,
            manifest_idx: Some(manifest_idx),
            kind: kind.to_string(),
        }
    }
}

impl UnresolvedDataSourceTemplate {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSourceTemplate, Error> {
        let kind = self.kind;

        let mapping = self
            .mapping
            .resolve(resolver, logger)
            .await
            .with_context(|| format!("failed to resolve data source template {}", self.name))?;

        Ok(DataSourceTemplate {
            kind,
            network: self.network,
            name: self.name,
            manifest_idx,
            mapping,
        })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct MappingEntityTrigger {
    pub data: TriggerData,
    pub calls: Vec<DeclaredCall>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TriggerData {
    pub source: DeploymentHash,
    pub entity: EntitySourceOperation,
    pub source_idx: u32,
}

impl TriggerData {
    pub fn new(source: DeploymentHash, entity: EntitySourceOperation, source_idx: u32) -> Self {
        Self {
            source,
            entity,
            source_idx,
        }
    }

    pub fn entity_type(&self) -> &str {
        self.entity.entity_type.as_str()
    }
}

impl Ord for TriggerData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.source_idx.cmp(&other.source_idx) {
            std::cmp::Ordering::Equal => self.entity.vid.cmp(&other.entity.vid),
            ord => ord,
        }
    }
}

impl PartialOrd for TriggerData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for TriggerData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TriggerData {{ source: {:?}, entity: {:?} }}",
            self.source, self.entity,
        )
    }
}

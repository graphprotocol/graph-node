use std::collections::BTreeMap;
use std::convert::TryInto;

use either::Either;
use graph::data::query::Trace;
use web3::types::Address;

use graph::blockchain::{Blockchain, BlockchainKind, BlockchainMap};
use graph::components::store::{BlockStore, EntityType, Store};
use graph::components::versions::VERSIONS;
use graph::data::graphql::{object, IntoValue, ObjectOrInterface, ValueMap};
use graph::data::subgraph::features::detect_features;
use graph::data::subgraph::status;
use graph::data::value::{Object, Word};
use graph::prelude::*;
use graph_graphql::prelude::{a, ExecutionContext, Resolver};

use crate::auth::PoiProtection;

#[derive(Clone, Debug)]
struct PublicProofOfIndexingRequest {
    pub deployment: DeploymentHash,
    pub block_number: BlockNumber,
}

impl TryFromValue for PublicProofOfIndexingRequest {
    fn try_from_value(value: &r::Value) -> Result<Self, Error> {
        match value {
            r::Value::Object(o) => Ok(Self {
                deployment: DeploymentHash::new(o.get_required::<String>("deployment")?).unwrap(),
                block_number: o.get_required::<BlockNumber>("blockNumber")?,
            }),
            _ => Err(anyhow!(
                "Cannot parse non-object value as PublicProofOfIndexingRequest: {:?}",
                value
            )),
        }
    }
}

#[derive(Debug)]
struct PublicProofOfIndexingResult {
    pub deployment: DeploymentHash,
    pub block: PartialBlockPtr,
    pub proof_of_indexing: Option<[u8; 32]>,
}

impl IntoValue for PublicProofOfIndexingResult {
    fn into_value(self) -> r::Value {
        object! {
            __typename: "ProofOfIndexingResult",
            deployment: self.deployment.to_string(),
            block: object! {
                number: self.block.number,
                hash: self.block.hash.map(|hash| hash.hash_hex()),
            },
            proofOfIndexing: self.proof_of_indexing.map(|poi| format!("0x{}", hex::encode(poi))),
        }
    }
}

/// Resolver for the index node GraphQL API.
#[derive(Clone)]
pub struct IndexNodeResolver<S: Store> {
    logger: Logger,
    blockchain_map: Arc<BlockchainMap>,
    store: Arc<S>,
    link_resolver: Arc<dyn LinkResolver>,
    bearer_token: Option<String>,
}

impl<S: Store> IndexNodeResolver<S> {
    pub fn new(
        logger: &Logger,
        store: Arc<S>,
        link_resolver: Arc<dyn LinkResolver>,
        bearer_token: Option<String>,
        blockchain_map: Arc<BlockchainMap>,
    ) -> Self {
        let logger = logger.new(o!("component" => "IndexNodeResolver"));

        Self {
            logger,
            blockchain_map,
            store,
            link_resolver,
            bearer_token,
        }
    }

    fn resolve_indexing_statuses(&self, field: &a::Field) -> Result<r::Value, QueryExecutionError> {
        let deployments = field
            .argument_value("subgraphs")
            .map(|value| match value {
                r::Value::List(ids) => ids
                    .iter()
                    .map(|id| match id {
                        r::Value::String(s) => s.clone(),
                        _ => unreachable!(),
                    })
                    .collect(),
                _ => unreachable!(),
            })
            .unwrap_or_else(Vec::new);

        let infos = self
            .store
            .status(status::Filter::Deployments(deployments))?;
        Ok(infos.into_value())
    }

    fn resolve_indexing_statuses_for_subgraph_name(
        &self,
        field: &a::Field,
    ) -> Result<r::Value, QueryExecutionError> {
        // Get the subgraph name from the arguments; we can safely use `expect` here
        // because the argument will already have been validated prior to the resolver
        // being called
        let subgraph_name = field
            .get_required::<String>("subgraphName")
            .expect("subgraphName not provided");

        debug!(
            self.logger,
            "Resolve indexing statuses for subgraph name";
            "name" => &subgraph_name
        );

        let infos = self
            .store
            .status(status::Filter::SubgraphName(subgraph_name))?;

        Ok(infos.into_value())
    }

    fn resolve_entity_changes_in_block(
        &self,
        field: &a::Field,
    ) -> Result<r::Value, QueryExecutionError> {
        let subgraph_id = field
            .get_required::<DeploymentHash>("subgraphId")
            .expect("Valid subgraphId required");

        let block_number = field
            .get_required::<BlockNumber>("blockNumber")
            .expect("Valid blockNumber required");

        let entity_changes = self
            .store
            .subgraph_store()
            .entity_changes_in_block(&subgraph_id, block_number)?;

        Ok(entity_changes_to_graphql(entity_changes))
    }

    fn resolve_block_data(&self, field: &a::Field) -> Result<r::Value, QueryExecutionError> {
        let network = field
            .get_required::<String>("network")
            .expect("Valid network required");

        let block_hash = field
            .get_required::<BlockHash>("blockHash")
            .expect("Valid blockHash required");

        let chain_store = if let Some(cs) = self.store.block_store().chain_store(&network) {
            cs
        } else {
            error!(
                self.logger,
                "Failed to fetch block data; nonexistent network";
                "network" => network,
                "block_hash" => format!("{}", block_hash),
            );
            return Ok(r::Value::Null);
        };

        let blocks_res = chain_store.blocks(&[block_hash.cheap_clone()]);
        Ok(match blocks_res {
            Ok(blocks) if blocks.is_empty() => {
                error!(
                    self.logger,
                    "Failed to fetch block data; block not found";
                    "network" => network,
                    "block_hash" => format!("{}", block_hash),
                );
                r::Value::Null
            }
            Ok(mut blocks) => {
                assert!(blocks.len() == 1, "Multiple blocks with the same hash");
                blocks.pop().unwrap().into()
            }
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to fetch block data; storage error";
                    "network" => network.as_str(),
                    "block_hash" => format!("{}", block_hash),
                    "error" => e.to_string(),
                );
                r::Value::Null
            }
        })
    }

    async fn resolve_block_hash_from_number(
        &self,
        field: &a::Field,
    ) -> Result<r::Value, QueryExecutionError> {
        let network = field
            .get_required::<String>("network")
            .expect("Valid network required");
        let block_number = field
            .get_required::<BlockNumber>("blockNumber")
            .expect("Valid blockNumber required");

        macro_rules! try_resolve_for_chain {
            ( $typ:path ) => {
                let blockchain = self.blockchain_map.get::<$typ>(network.to_string()).ok();

                if let Some(blockchain) = blockchain {
                    debug!(
                        self.logger,
                        "Fetching block hash from number";
                        "network" => &network,
                        "block_number" => block_number,
                    );

                    let block_ptr_res = blockchain
                        .block_pointer_from_number(&self.logger, block_number)
                        .await;

                        if let Err(e) = block_ptr_res {
                            warn!(
                                self.logger,
                                "Failed to fetch block hash from number";
                                "network" => &network,
                                "chain" => <$typ as Blockchain>::KIND.to_string(),
                                "block_number" => block_number,
                                "error" => e.to_string(),
                            );
                            return Ok(r::Value::Null);
                        }

                    let block_ptr = block_ptr_res.unwrap();
                    return Ok(r::Value::String(block_ptr.hash_hex()));
                }
            };
        }

        // Ugly, but we can't get back an object trait from the `BlockchainMap`,
        // so this seems like the next best thing.
        try_resolve_for_chain!(graph_chain_ethereum::Chain);
        try_resolve_for_chain!(graph_chain_arweave::Chain);
        try_resolve_for_chain!(graph_chain_cosmos::Chain);
        try_resolve_for_chain!(graph_chain_near::Chain);

        // If you're adding support for a new chain and this `match` clause just
        // gave you a compiler error, then this message is for you! You need to
        // add a new `try_resolve!` macro invocation above for your new chain
        // type.
        match BlockchainKind::Ethereum {
            // Note: we don't actually care about substreams here.
            BlockchainKind::Substreams
            | BlockchainKind::Arweave
            | BlockchainKind::Ethereum
            | BlockchainKind::Cosmos
            | BlockchainKind::Near => (),
        }

        // The given network does not exist.
        Ok(r::Value::Null)
    }

    async fn resolve_cached_ethereum_calls(
        &self,
        field: &a::Field,
    ) -> Result<r::Value, QueryExecutionError> {
        let network = field
            .get_required::<String>("network")
            .expect("Valid network required");

        let block_hash = field
            .get_required::<BlockHash>("blockHash")
            .expect("Valid blockHash required");

        let chain = if let Ok(c) = self
            .blockchain_map
            .get::<graph_chain_ethereum::Chain>(network.clone())
        {
            c
        } else {
            error!(
                self.logger,
                "Failed to fetch cached Ethereum calls; nonexistent network";
                "network" => network,
                "block_hash" => format!("{}", block_hash),
            );
            return Ok(r::Value::Null);
        };
        let chain_store = chain.chain_store();
        let call_cache = chain.call_cache();

        let (block_number, timestamp) = match chain_store.block_number(&block_hash).await {
            Ok(Some((_, n, timestamp))) => (n, timestamp),
            Ok(None) => {
                error!(
                    self.logger,
                    "Failed to fetch cached Ethereum calls; block not found";
                    "network" => network,
                    "block_hash" => format!("{}", block_hash),
                );
                return Ok(r::Value::Null);
            }
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to fetch cached Ethereum calls; storage error";
                    "network" => network.as_str(),
                    "block_hash" => format!("{}", block_hash),
                    "error" => e.to_string(),
                );
                return Ok(r::Value::Null);
            }
        };
        let block_ptr = BlockPtr::new(block_hash.cheap_clone(), block_number);

        let calls = match call_cache.get_calls_in_block(block_ptr) {
            Ok(c) => c,
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to fetch cached Ethereum calls; storage error";
                    "network" => network.as_str(),
                    "block_hash" => format!("{}", block_hash),
                    "error" => e.to_string(),
                );
                return Err(QueryExecutionError::StoreError(e.into()));
            }
        };

        Ok(r::Value::List(
            calls
                .into_iter()
                .map(|cached_call| {
                    object! {
                        idHash: &cached_call.blake3_id[..],
                        block: object! {
                            hash: cached_call.block_ptr.hash.hash_hex(),
                            number: cached_call.block_ptr.number,
                            timestamp: timestamp,
                        },
                        contractAddress: &cached_call.contract_address[..],
                        returnValue: &cached_call.return_value[..],
                    }
                })
                .collect::<Vec<r::Value>>(),
        ))
    }

    fn resolve_proof_of_indexing(&self, field: &a::Field) -> Result<r::Value, QueryExecutionError> {
        let deployment_id = field
            .get_required::<DeploymentHash>("subgraph")
            .expect("Valid subgraphId required");

        let block_number: i32 = field
            .get_required::<i32>("blockNumber")
            .expect("Valid blockNumber required")
            .try_into()
            .unwrap();

        let block_hash = field
            .get_required::<BlockHash>("blockHash")
            .expect("Valid blockHash required");

        let block = BlockPtr::new(block_hash, block_number);

        let mut indexer = field
            .get_optional::<Address>("indexer")
            .expect("Invalid indexer");

        let poi_protection = PoiProtection::from_env(&ENV_VARS);
        if !poi_protection.validate_access_token(self.bearer_token.as_deref()) {
            // Let's sign the POI with a zero'd address when the access token is
            // invalid.
            indexer = Some(Address::zero());
        }

        let poi_fut = self
            .store
            .get_proof_of_indexing(&deployment_id, &indexer, block.clone());
        let poi = match futures::executor::block_on(poi_fut) {
            Ok(Some(poi)) => r::Value::String(format!("0x{}", hex::encode(poi))),
            Ok(None) => r::Value::Null,
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to query proof of indexing";
                    "subgraph" => deployment_id,
                    "block" => format!("{}", block),
                    "error" => format!("{:?}", e)
                );
                r::Value::Null
            }
        };

        Ok(poi)
    }

    fn resolve_public_proofs_of_indexing(
        &self,
        field: &a::Field,
    ) -> Result<r::Value, QueryExecutionError> {
        let requests = field
            .get_required::<Vec<PublicProofOfIndexingRequest>>("requests")
            .expect("valid requests required, validation should have caught this");

        // Only 10 requests are allowed at a time to avoid generating too many SQL queries;
        // NOTE: Indexers should rate limit the status API anyway, but this adds some soft
        // extra protection
        if requests.len() > 10 {
            return Err(QueryExecutionError::TooExpensive);
        }

        Ok(r::Value::List(
            requests
                .into_iter()
                .map(|request| {
                    match futures::executor::block_on(
                        self.store.get_public_proof_of_indexing(
                            &request.deployment,
                            request.block_number,
                        ),
                    ) {
                        Ok(Some(poi)) => (Some(poi), request),
                        Ok(None) => (None, request),
                        Err(e) => {
                            error!(
                                self.logger,
                                "Failed to query public proof of indexing";
                                "subgraph" => &request.deployment,
                                "block" => format!("{}", request.block_number),
                                "error" => format!("{:?}", e)
                            );
                            (None, request)
                        }
                    }
                })
                .map(|(poi_result, request)| PublicProofOfIndexingResult {
                    deployment: request.deployment,
                    block: match poi_result {
                        Some((ref block, _)) => block.clone(),
                        None => PartialBlockPtr::from(request.block_number),
                    },
                    proof_of_indexing: poi_result.map(|(_, poi)| poi),
                })
                .map(IntoValue::into_value)
                .collect(),
        ))
    }

    fn resolve_indexing_status_for_version(
        &self,
        field: &a::Field,

        // If `true` return the current version, if `false` return the pending version.
        current_version: bool,
    ) -> Result<r::Value, QueryExecutionError> {
        // We can safely unwrap because the argument is non-nullable and has been validated.
        let subgraph_name = field.get_required::<String>("subgraphName").unwrap();

        debug!(
            self.logger,
            "Resolve indexing status for subgraph name";
            "name" => &subgraph_name,
            "current_version" => current_version,
        );

        let infos = self.store.status(status::Filter::SubgraphVersion(
            subgraph_name,
            current_version,
        ))?;

        Ok(infos
            .into_iter()
            .next()
            .map(|info| info.into_value())
            .unwrap_or(r::Value::Null))
    }

    async fn resolve_subgraph_features(
        &self,
        field: &a::Field,
    ) -> Result<r::Value, QueryExecutionError> {
        // We can safely unwrap because the argument is non-nullable and has been validated.
        let subgraph_id = field.get_required::<String>("subgraphId").unwrap();

        // TODO:
        //
        // An interesting optimization would involve trying to get the subgraph manifest from the
        // SubgraphStore before hitting IPFS, but we must fix a dependency cycle between the `graph`
        // and `server` crates first.
        //
        // 1. implement a new method in subgraph store to retrieve the SubgraphManifest of a given deployment id
        // 2. try to fetch this subgraph from our SubgraphStore before hitting IPFS

        // Try to build a deployment hash with the input string
        let deployment_hash = DeploymentHash::new(subgraph_id).map_err(|invalid_qm_hash| {
            QueryExecutionError::SubgraphDeploymentIdError(invalid_qm_hash)
        })?;

        let ValidationPostProcessResult {
            features,
            errors,
            network,
        } = {
            let raw: serde_yaml::Mapping = {
                let file_bytes = self
                    .link_resolver
                    .cat(&self.logger, &deployment_hash.to_ipfs_link())
                    .await
                    .map_err(SubgraphManifestResolveError::ResolveError)?;

                serde_yaml::from_slice(&file_bytes)
                    .map_err(SubgraphManifestResolveError::ParseError)?
            };

            let kind = BlockchainKind::from_manifest(&raw)
                .map_err(SubgraphManifestResolveError::ResolveError)?;
            match kind {
                BlockchainKind::Ethereum => {
                    let unvalidated_subgraph_manifest =
                        UnvalidatedSubgraphManifest::<graph_chain_ethereum::Chain>::resolve(
                            deployment_hash,
                            raw,
                            &self.link_resolver,
                            &self.logger,
                            ENV_VARS.max_spec_version.clone(),
                        )
                        .await?;

                    validate_and_extract_features(
                        &self.store.subgraph_store(),
                        unvalidated_subgraph_manifest,
                    )
                    .await?
                }

                BlockchainKind::Cosmos => {
                    let unvalidated_subgraph_manifest =
                        UnvalidatedSubgraphManifest::<graph_chain_cosmos::Chain>::resolve(
                            deployment_hash,
                            raw,
                            &self.link_resolver,
                            &self.logger,
                            ENV_VARS.max_spec_version.clone(),
                        )
                        .await?;

                    validate_and_extract_features(
                        &self.store.subgraph_store(),
                        unvalidated_subgraph_manifest,
                    )
                    .await?
                }

                BlockchainKind::Near => {
                    let unvalidated_subgraph_manifest =
                        UnvalidatedSubgraphManifest::<graph_chain_near::Chain>::resolve(
                            deployment_hash,
                            raw,
                            &self.link_resolver,
                            &self.logger,
                            ENV_VARS.max_spec_version.clone(),
                        )
                        .await?;

                    validate_and_extract_features(
                        &self.store.subgraph_store(),
                        unvalidated_subgraph_manifest,
                    )
                    .await?
                }

                BlockchainKind::Arweave => {
                    let unvalidated_subgraph_manifest =
                        UnvalidatedSubgraphManifest::<graph_chain_arweave::Chain>::resolve(
                            deployment_hash,
                            raw,
                            &self.link_resolver,
                            &self.logger,
                            ENV_VARS.max_spec_version.clone(),
                        )
                        .await?;

                    validate_and_extract_features(
                        &self.store.subgraph_store(),
                        unvalidated_subgraph_manifest,
                    )
                    .await?
                }
                // TODO(filipe): Kick this can down the road!
                BlockchainKind::Substreams => unimplemented!(),
            }
        };

        // We then bulid a GraphqQL `Object` value that contains the feature detection and
        // validation results and send it back as a response.
        let response = [
            ("features".into(), features),
            ("errors".into(), errors),
            ("network".into(), network),
        ];
        let response = Object::from_iter(response);

        Ok(r::Value::Object(response))
    }

    fn resolve_api_versions(&self, _field: &a::Field) -> Result<r::Value, QueryExecutionError> {
        Ok(r::Value::List(
            VERSIONS
                .iter()
                .map(|version| {
                    r::Value::Object(Object::from_iter(vec![(
                        "version".into(),
                        r::Value::String(version.to_string()),
                    )]))
                })
                .collect(),
        ))
    }
}

struct ValidationPostProcessResult {
    features: r::Value,
    errors: r::Value,
    network: r::Value,
}

async fn validate_and_extract_features<C, SgStore>(
    subgraph_store: &Arc<SgStore>,
    unvalidated_subgraph_manifest: UnvalidatedSubgraphManifest<C>,
) -> Result<ValidationPostProcessResult, QueryExecutionError>
where
    C: Blockchain,
    SgStore: SubgraphStore,
{
    // Validate the subgraph we've just obtained.
    //
    // Note that feature valiadation errors will be inside the error variant vector (because
    // `validate` also validates subgraph features), so we must filter them out to build our
    // response.
    let subgraph_validation: Either<_, _> = match unvalidated_subgraph_manifest
        .validate(subgraph_store.clone(), false)
        .await
    {
        Ok(subgraph_manifest) => Either::Left(subgraph_manifest),
        Err(validation_errors) => {
            // We must ensure that all the errors are of the `FeatureValidationError`
            // variant and that there is at least one error of that kind.
            let feature_validation_errors: Vec<_> = validation_errors
                .into_iter()
                .filter(|error| {
                    matches!(
                        error,
                        SubgraphManifestValidationError::FeatureValidationError(_)
                    )
                })
                .collect();

            if !feature_validation_errors.is_empty() {
                Either::Right(feature_validation_errors)
            } else {
                // If other error variants are present or there are no feature validation
                // errors, we must return early with an error.
                //
                // It might be useful to return a more thoughtful error, but that is not the
                // purpose of this endpoint.
                return Err(QueryExecutionError::InvalidSubgraphManifest);
            }
        }
    };

    // At this point, we have either:
    // 1. A valid subgraph manifest with no errors.
    // 2. No subgraph manifest and a set of feature validation errors.
    //
    // For this step we must collect whichever results we have into GraphQL `Value` types.
    match subgraph_validation {
        Either::Left(subgraph_manifest) => {
            let features = r::Value::List(
                detect_features(&subgraph_manifest)
                    .map_err(|_| QueryExecutionError::InvalidSubgraphManifest)?
                    .iter()
                    .map(ToString::to_string)
                    .map(r::Value::String)
                    .collect(),
            );
            let errors = r::Value::List(vec![]);
            let network = r::Value::String(subgraph_manifest.network_name());

            Ok(ValidationPostProcessResult {
                features,
                errors,
                network,
            })
        }
        Either::Right(errors) => {
            let features = r::Value::List(vec![]);
            let errors = r::Value::List(
                errors
                    .iter()
                    .map(ToString::to_string)
                    .map(r::Value::String)
                    .collect(),
            );
            let network = r::Value::Null;
            Ok(ValidationPostProcessResult {
                features,
                errors,
                network,
            })
        }
    }
}

fn entity_changes_to_graphql(entity_changes: Vec<EntityOperation>) -> r::Value {
    // Results are sorted first alphabetically by entity type, then by entity
    // ID, and then aphabetically by field name.

    // First, we isolate updates and deletions with the same entity type.
    let mut updates: BTreeMap<EntityType, Vec<Entity>> = BTreeMap::new();
    let mut deletions: BTreeMap<EntityType, Vec<Word>> = BTreeMap::new();

    for change in entity_changes {
        match change {
            EntityOperation::Remove { key } => {
                deletions
                    .entry(key.entity_type)
                    .or_default()
                    .push(key.entity_id);
            }
            EntityOperation::Set { key, data } => {
                updates.entry(key.entity_type).or_default().push(data);
            }
        }
    }

    // Now we're ready for GraphQL type conversions.
    let mut updates_graphql: Vec<r::Value> = Vec::with_capacity(updates.len());
    let mut deletions_graphql: Vec<r::Value> = Vec::with_capacity(deletions.len());

    for (entity_type, mut entities) in updates {
        entities.sort_unstable_by_key(|e| e.id());
        updates_graphql.push(object! {
            type: entity_type.to_string(),
            entities:
                entities
                    .into_iter()
                    .map(|e| {
                        r::Value::object(
                            e.sorted()
                                .into_iter()
                                .map(|(name, value)| (name.into(), value.into()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<r::Value>>(),
        });
    }

    for (entity_type, mut ids) in deletions {
        ids.sort_unstable();
        deletions_graphql.push(object! {
            type: entity_type.to_string(),
            entities:
                ids.into_iter().map(|s| s.to_string()).map(r::Value::String).collect::<Vec<r::Value>>(),
        });
    }

    object! {
        updates: updates_graphql,
        deletions: deletions_graphql,
    }
}

#[async_trait]
impl<S: Store> Resolver for IndexNodeResolver<S> {
    const CACHEABLE: bool = false;

    async fn query_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QueryExecutionError> {
        self.store.query_permit().await.map_err(Into::into)
    }

    fn prefetch(
        &self,
        _: &ExecutionContext<Self>,
        _: &a::SelectionSet,
    ) -> Result<(Option<r::Value>, Trace), Vec<QueryExecutionError>> {
        Ok((None, Trace::None))
    }

    /// Resolves a scalar value for a given scalar type.
    async fn resolve_scalar_value(
        &self,
        parent_object_type: &s::ObjectType,
        field: &a::Field,
        scalar_type: &s::ScalarType,
        value: Option<r::Value>,
    ) -> Result<r::Value, QueryExecutionError> {
        match (
            parent_object_type.name.as_str(),
            field.name.as_str(),
            scalar_type.name.as_str(),
        ) {
            ("Query", "proofOfIndexing", "Bytes") => self.resolve_proof_of_indexing(field),
            ("Query", "blockData", "JSONObject") => self.resolve_block_data(field),
            ("Query", "blockHashFromNumber", "Bytes") => {
                self.resolve_block_hash_from_number(field).await
            }

            // Fallback to the same as is in the default trait implementation. There
            // is no way to call back into the default implementation for the trait.
            // So, note that this is duplicated.
            // See also c2112309-44fd-4a84-92a0-5a651e6ed548
            _ => Ok(value.unwrap_or(r::Value::Null)),
        }
    }

    async fn resolve_objects(
        &self,
        prefetched_objects: Option<r::Value>,
        field: &a::Field,
        _field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
    ) -> Result<r::Value, QueryExecutionError> {
        // Resolves the `field.name` top-level field.
        match (prefetched_objects, object_type.name(), field.name.as_str()) {
            (None, "SubgraphIndexingStatus", "indexingStatuses") => {
                self.resolve_indexing_statuses(field)
            }
            (None, "SubgraphIndexingStatus", "indexingStatusesForSubgraphName") => {
                self.resolve_indexing_statuses_for_subgraph_name(field)
            }
            (None, "CachedEthereumCall", "cachedEthereumCalls") => {
                self.resolve_cached_ethereum_calls(field).await
            }

            // The top-level `publicProofsOfIndexing` field
            (None, "PublicProofOfIndexingResult", "publicProofsOfIndexing") => {
                self.resolve_public_proofs_of_indexing(field)
            }

            // Resolve fields of `Object` values (e.g. the `chains` field of `ChainIndexingStatus`)
            (value, _, _) => Ok(value.unwrap_or(r::Value::Null)),
        }
    }

    async fn resolve_object(
        &self,
        prefetched_object: Option<r::Value>,
        field: &a::Field,
        _field_definition: &s::Field,
        _object_type: ObjectOrInterface<'_>,
    ) -> Result<r::Value, QueryExecutionError> {
        // Resolves the `field.name` top-level field.
        match (prefetched_object, field.name.as_str()) {
            (None, "indexingStatusForCurrentVersion") => {
                self.resolve_indexing_status_for_version(field, true)
            }
            (None, "indexingStatusForPendingVersion") => {
                self.resolve_indexing_status_for_version(field, false)
            }
            (None, "subgraphFeatures") => self.resolve_subgraph_features(field).await,
            (None, "entityChangesInBlock") => self.resolve_entity_changes_in_block(field),
            // The top-level `subgraphVersions` field
            (None, "apiVersions") => self.resolve_api_versions(field),

            // Resolve fields of `Object` values (e.g. the `latestBlock` field of `EthereumBlock`)
            (value, _) => Ok(value.unwrap_or(r::Value::Null)),
        }
    }
}

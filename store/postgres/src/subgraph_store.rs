use diesel::{
    pg::Pg,
    serialize::Output,
    sql_types::Text,
    types::{FromSql, ToSql},
};
use std::iter::FromIterator;
use std::sync::RwLock;
use std::{collections::BTreeMap, collections::HashMap, sync::Arc};
use std::{fmt, io::Write};

use graph::{
    components::{
        server::index_node::VersionInfo,
        store::{self, EntityType},
    },
    constraint_violation,
    data::query::QueryTarget,
    data::subgraph::schema::MetadataType,
    data::subgraph::schema::SubgraphError,
    data::subgraph::status,
    prelude::StoreEvent,
    prelude::SubgraphDeploymentEntity,
    prelude::{
        lazy_static, o, web3::types::Address, ApiSchema, CheapClone, DeploymentState, DynTryFuture,
        Entity, EntityKey, EntityModification, EntityQuery, Error, EthereumBlockPointer, Logger,
        MetadataOperation, MetricsRegistry, NodeId, QueryExecutionError, Schema, StopwatchMetrics,
        StoreError, SubgraphDeploymentId, SubgraphName, SubgraphStore as SubgraphStoreTrait,
        SubgraphVersionSwitchingMode,
    },
};
use store::StoredDynamicDataSource;

use crate::{connection_pool::ConnectionPool, primary, primary::Site};
use crate::{
    deployment_store::{DeploymentStore, ReplicaId},
    detail::DeploymentDetail,
    primary::UnusedDeployment,
};

/// The name of a database shard; valid names must match `[a-z0-9_]+`
#[derive(Clone, Debug, Eq, PartialEq, Hash, AsExpression, FromSqlRow)]
pub struct Shard(String);

lazy_static! {
    /// The name of the primary shard that contains all instance-wide data
    pub static ref PRIMARY_SHARD: Shard = Shard("primary".to_string());
}

impl Shard {
    pub fn new(name: String) -> Result<Self, StoreError> {
        if name.is_empty() {
            return Err(StoreError::InvalidIdentifier(format!(
                "shard names must not be empty"
            )));
        }
        if name.len() > 30 {
            return Err(StoreError::InvalidIdentifier(format!(
                "shard names can be at most 30 characters, but `{}` has {} characters",
                name,
                name.len()
            )));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(StoreError::InvalidIdentifier(format!(
                "shard names must only contain lowercase alphanumeric characters or '_'"
            )));
        }
        Ok(Shard(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromSql<Text, Pg> for Shard {
    fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        Shard::new(s).map_err(Into::into)
    }
}

impl ToSql<Text, Pg> for Shard {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> diesel::serialize::Result {
        <String as ToSql<Text, Pg>>::to_sql(&self.0, out)
    }
}

/// Decide where a new deployment should be placed based on the subgraph name
/// and the network it is indexing. If the deployment can be placed, returns
/// the name of the database shard for the deployment and the names of the
/// indexers that should index it. The deployment should then be assigned to
/// one of the returned indexers.
pub trait DeploymentPlacer {
    fn place(&self, name: &str, network: &str) -> Result<Option<(Shard, Vec<NodeId>)>, String>;
}

/// Tools for managing unused deployments
pub mod unused {
    pub enum Filter {
        /// List all unused deployments
        All,
        /// List only deployments that are unused but have not been removed yet
        New,
    }
}

/// Multiplex store operations on subgraphs and deployments between a
/// primary and any number of additional storage shards. The primary
/// contains information about named subgraphs, and how the underlying
/// deployments are spread across shards, while the actual deployment data
/// and metadata is stored in the shards.  Depending on the configuration,
/// the database for the primary and for the shards can be the same
/// database, in which case they are all backed by one connection pool, or
/// separate databases in the same Postgres cluster, or entirely separate
/// clusters. Details of how to configure shards can be found in [this
/// document](https://github.com/graphprotocol/graph-node/blob/master/docs/sharding.md)
///
/// The primary uses the following database tables:
/// - `public.deployment_schemas`: immutable data about deployments, including
///   the shard that stores the deployment data and metadata, the namespace in
///   the shard that contains the deployment data, and the network/chain that
///   the  deployment is indexing
/// - `subgraphs.subgraph` and `subgraphs.subgraph_version`: information about
///   named subgraphs and how they map to deployments
/// - `subgraphs.subgraph_deployment_assignment`: which index node is indexing
///   what deployment
///
/// For each deployment, the corresponding shard contains a namespace for
/// the deployment data; the schema in that namespace is generated from the
/// deployment's GraphQL schema by the [crate::relational::Layout], which
/// is also responsible for modifying and querying subgraph
/// data. Deployment metadata is stored in tables in the `subgraphs`
/// namespace in the same shard as the deployment data. The most important
/// of these tables are
///
/// - `subgraphs.subgraph_deployment`: the main table for deployment metadata;
///   most importantly, it stores the pointer to the current subgraph head, i.e.,
///   the block up to which the subgraph has indexed the chain, together with
///   other things like whether the subgraph has synced, whether it has failed
///   and whether it encountered any errors
/// - `subgraphs.subgraph_manifest`: immutable information derived from the YAML
///   manifest for the deployment
/// - `subgraphs.dynamic_ethereum_contract_data_source`: the data sources that
///   the subgraph has created from templates in the manifest.
/// - `subgraphs.subgraph_error`: details about errors that the deployment has
///   encountered
///
/// There are more metadata tables, but they are rarely if ever read, only
/// written to when a deployment is created and should be removed in a
/// future version of `graph-node`
///
/// The `SubgraphStore` mostly orchestrates access to the primary and the
/// shards.  The actual work is done by code in the `primary` module for
/// queries against the primary store, and by the `DeploymentStore` for
/// access to deployment data and metadata.
pub struct SubgraphStore {
    logger: Logger,
    primary: ConnectionPool,
    stores: HashMap<Shard, Arc<DeploymentStore>>,
    /// Cache for the mapping from deployment id to shard/namespace/id
    sites: RwLock<HashMap<SubgraphDeploymentId, Arc<Site>>>,
    placer: Arc<dyn DeploymentPlacer + Send + Sync + 'static>,
}

impl SubgraphStore {
    /// Create a new store for subgraphs that distributes deployments across
    /// multiple databases
    ///
    /// `stores` is a list of the shards. The tuple contains the shard name, the main
    /// connection pool for the database, a list of read-only connections
    /// for the same database, and a list of weights determining how often
    /// to use the main pool and the read replicas for queries. The list
    /// of weights must be one longer than the list of read replicas, and
    /// `weights[0]` is used for the main pool.
    ///
    /// All write operations for a shard are performed against the main
    /// pool. One of the shards must be named `primary`
    ///
    /// The `placer` determines where `create_subgraph_deployment` puts a new deployment
    pub fn new(
        logger: &Logger,
        stores: Vec<(Shard, ConnectionPool, Vec<ConnectionPool>, Vec<usize>)>,
        placer: Arc<dyn DeploymentPlacer + Send + Sync + 'static>,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let primary = stores
            .iter()
            .find(|(name, _, _, _)| name == &*PRIMARY_SHARD)
            .map(|(_, pool, _, _)| pool.clone())
            .expect("we always have a primary shard");
        let stores = HashMap::from_iter(stores.into_iter().map(
            |(name, main_pool, read_only_pools, weights)| {
                let logger = logger.new(o!("shard" => name.to_string()));

                (
                    name,
                    Arc::new(DeploymentStore::new(
                        &logger,
                        main_pool,
                        read_only_pools,
                        weights,
                        registry.cheap_clone(),
                    )),
                )
            },
        ));
        let sites = RwLock::new(HashMap::new());
        let logger = logger.new(o!("shard" => PRIMARY_SHARD.to_string()));
        Self {
            logger,
            primary,
            stores,
            sites,
            placer,
        }
    }

    // Only needed for tests
    #[cfg(debug_assertions)]
    pub(crate) fn clear_caches(&self) {
        for store in self.stores.values() {
            store.layout_cache.lock().unwrap().clear();
        }
        self.sites.write().unwrap().clear();
    }

    fn site(&self, id: &SubgraphDeploymentId) -> Result<Arc<Site>, StoreError> {
        if let Some(site) = self.sites.read().unwrap().get(id) {
            return Ok(site.clone());
        }

        let conn = self.primary_conn()?;
        let site = conn
            .find_site(id)?
            .ok_or_else(|| StoreError::DeploymentNotFound(id.to_string()))?;
        let site = Arc::new(site);

        self.sites.write().unwrap().insert(id.clone(), site.clone());
        Ok(site)
    }

    /// Look up the sites for the given ids in bulk and cache them
    fn cache_sites(&self, ids: &Vec<SubgraphDeploymentId>) -> Result<(), StoreError> {
        let sites = self
            .primary_conn()?
            .find_sites(ids)?
            .into_iter()
            .map(|site| (site.deployment.clone(), Arc::new(site)));
        self.sites.write().unwrap().extend(sites);
        Ok(())
    }

    fn store(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<(&Arc<DeploymentStore>, Arc<Site>), StoreError> {
        let site = self.site(id)?;
        let store = self
            .stores
            .get(&site.shard)
            .ok_or(StoreError::UnknownShard(site.shard.as_str().to_string()))?;
        Ok((store, site))
    }

    fn place(
        &self,
        name: &SubgraphName,
        network_name: &str,
        default_node: NodeId,
    ) -> Result<(Shard, NodeId), StoreError> {
        // We try to place the deployment according to the configured rules.
        // If they don't yield a match, place into the primary and have
        // `default_node` index the deployment. The latter can only happen
        // when `graph-node` is not using a configuration file, but
        // uses the legacy command-line options as configuration
        let placement = self
            .placer
            .place(name.as_str(), network_name)
            .map_err(|msg| {
                constraint_violation!("illegal indexer name in deployment rule: {}", msg)
            })?;

        match placement {
            None => Ok((PRIMARY_SHARD.clone(), default_node)),
            Some((_, nodes)) if nodes.is_empty() => {
                // This is really a configuration error
                Ok((PRIMARY_SHARD.clone(), default_node))
            }
            Some((shard, mut nodes)) if nodes.len() == 1 => Ok((shard, nodes.pop().unwrap())),
            Some((shard, nodes)) => {
                let conn = self.primary_conn()?;

                // unwrap is fine since nodes is not empty
                let node = conn.least_assigned_node(&nodes)?.unwrap();
                Ok((shard, node))
            }
        }
    }

    fn create_deployment_internal(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        network_name: String,
        mode: SubgraphVersionSwitchingMode,
        // replace == true is only used in tests; for non-test code, it must
        // be 'false'
        replace: bool,
    ) -> Result<(), StoreError> {
        #[cfg(not(debug_assertions))]
        assert!(!replace);

        let (shard, node_id) = self.place(&name, &network_name, node_id)?;

        // TODO: Check this for behavior on failure
        let site = self
            .primary_conn()?
            .allocate_site(shard.clone(), &schema.id, network_name)?;

        let graft_site = deployment
            .graft_base
            .as_ref()
            .map(|base| self.primary_conn()?.find_existing_site(&base))
            .transpose()?;
        if let Some(ref graft_site) = graft_site {
            if &graft_site.shard != &shard {
                return Err(constraint_violation!("Can not graft across shards. {} is in shard {}, and the base {} is in shard {}", site.deployment, site.shard, graft_site.deployment, graft_site.shard));
            }
        }

        // Create the actual databases schema and metadata entries
        let deployment_store = self
            .stores
            .get(&shard)
            .ok_or_else(|| StoreError::UnknownShard(shard.to_string()))?;
        deployment_store.create_deployment(schema, deployment, &site, graft_site, replace)?;

        let exists_and_synced = |id: &SubgraphDeploymentId| {
            let (store, _) = self.store(id)?;
            store.deployment_exists_and_synced(id)
        };

        // FIXME: This simultaneously holds a `primary_conn` and a shard connection, which can
        // potentially deadlock.
        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            // Create subgraph, subgraph version, and assignment
            let changes = pconn.create_subgraph_version(
                name,
                &schema.id,
                node_id,
                mode,
                exists_and_synced,
            )?;
            let event = StoreEvent::new(changes);
            pconn.send_store_event(&event)?;
            Ok(())
        })
    }

    // Only for tests to simplify their handling of test fixtures, so that
    // tests can reset the block pointer of a subgraph by recreating it
    #[cfg(debug_assertions)]
    pub fn create_deployment_replace(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        network_name: String,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        self.create_deployment_internal(name, schema, deployment, node_id, network_name, mode, true)
    }

    pub(crate) fn send_store_event(&self, event: &StoreEvent) -> Result<(), StoreError> {
        let conn = self.primary_conn()?;
        conn.send_store_event(event)
    }

    /// Get a connection to the primary shard. Code must never hold one of these
    /// connections while also accessing a `DeploymentStore`, since both
    /// might draw connections from the same pool, and trying to get two
    /// connections can deadlock the entire process if the pool runs out
    /// of connections in between getting the first one and trying to get the
    /// second one.
    fn primary_conn(&self) -> Result<primary::Connection, StoreError> {
        let conn = self.primary.get_with_timeout_warning(&self.logger)?;
        Ok(primary::Connection::new(conn))
    }

    pub(crate) async fn with_primary_conn<T: Send + 'static>(
        &self,
        f: impl 'static + Send + FnOnce(primary::Connection) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.primary
            .with_conn(|conn, _| f(primary::Connection::new(conn)).map_err(|e| e.into()))
            .await
    }

    pub(crate) fn replica_for_query(
        &self,
        target: QueryTarget,
        for_subscription: bool,
    ) -> Result<(Arc<DeploymentStore>, Arc<Site>, ReplicaId), StoreError> {
        let id = match target {
            QueryTarget::Name(name) => {
                let conn = self.primary_conn()?;
                conn.transaction(|| conn.current_deployment_for_subgraph(name))?
            }
            QueryTarget::Deployment(id) => id,
        };

        let (store, site) = self.store(&id)?;
        let replica = store.replica_for_query(for_subscription)?;

        Ok((store.clone(), site.clone(), replica))
    }

    /// Delete all entities. This function exists solely for integration tests
    /// and should never be called from any other code. Unfortunately, Rust makes
    /// it very hard to export items just for testing
    #[cfg(debug_assertions)]
    pub fn delete_all_entities_for_test_use_only(&self) -> Result<(), StoreError> {
        let pconn = self.primary_conn()?;
        let schemas = pconn.sites()?;

        // Delete all subgraph schemas
        for schema in schemas {
            let (store, _) = self.store(&schema.deployment)?;
            store.drop_deployment_schema(&schema.namespace)?;
        }

        for store in self.stores.values() {
            store.drop_all_metadata()?;
        }
        self.clear_caches();
        Ok(())
    }

    /// Partition the list of deployments by the shard they belong to. If
    /// deployments is empty, return a partition of all deployments
    fn deployments_by_shard(
        &self,
        deployments: Vec<String>,
    ) -> Result<HashMap<Shard, Vec<Arc<Site>>>, StoreError> {
        let sites: Vec<_> = if deployments.is_empty() {
            self.primary_conn()?
                .sites()?
                .into_iter()
                .map(|site| Arc::new(site))
                .collect()
        } else {
            // Ignore invalid subgraph ids
            let deployments: Vec<SubgraphDeploymentId> = deployments
                .iter()
                .filter_map(|d| SubgraphDeploymentId::new(d).ok())
                .collect();

            self.cache_sites(&deployments)?;

            // For each deployment, find the shard it lives in, but ignore
            // deployments that do not exist
            deployments
                .into_iter()
                .map(|id| self.site(&id))
                .filter(|res| !matches!(res, Err(StoreError::DeploymentNotFound(_))))
                .collect::<Result<Vec<_>, StoreError>>()?
        };

        // Partition the list of deployments by shard
        let by_shard: HashMap<Shard, Vec<Arc<Site>>> =
            sites.into_iter().fold(HashMap::new(), |mut map, site| {
                map.entry(site.shard.clone()).or_default().push(site);
                map
            });
        Ok(by_shard)
    }

    /// Look for new unused deployments and add them to the `unused_deployments`
    /// table
    pub fn record_unused_deployments(&self) -> Result<Vec<DeploymentDetail>, StoreError> {
        let deployments = self.primary_conn()?.detect_unused_deployments()?;

        // deployments_by_shard takes an empty vec to mean 'give me everything',
        // so we short-circuit that here
        if deployments.is_empty() {
            return Ok(vec![]);
        }

        let by_shard = self.deployments_by_shard(deployments)?;
        // Go shard-by-shard to look up deployment statuses
        let mut details = Vec::new();
        for (shard, ids) in by_shard.into_iter() {
            let store = self
                .stores
                .get(&shard)
                .ok_or(StoreError::UnknownShard(shard.to_string()))?;
            let ids = ids
                .into_iter()
                .map(|site| site.deployment.to_string())
                .collect();
            details.extend(store.deployment_details(ids)?);
        }

        self.primary_conn()?.update_unused_deployments(&details)?;
        Ok(details)
    }

    pub fn list_unused_deployments(
        &self,
        filter: unused::Filter,
    ) -> Result<Vec<UnusedDeployment>, StoreError> {
        self.primary_conn()?.list_unused_deployments(filter)
    }

    /// Remove a deployment, i.e., all its data and metadata. This is only permissible
    /// if the deployment is unused in the sense that it is neither the current nor
    /// pending version of any subgraph, and is not currently assigned to any node
    pub fn remove_deployment(&self, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
        let (store, site) = self.store(id)?;

        // Check that deployment is not assigned
        match self.primary_conn()?.assigned_node(id)? {
            Some(node) => {
                return Err(constraint_violation!(
                    "deployment {} can not be removed since it is assigned to node {}",
                    id.as_str(),
                    node.as_str()
                ));
            }
            None => { /* ok */ }
        }

        // Check that it is not current/pending for any subgraph
        let versions = self.primary_conn()?.subgraphs_using_deployment(id)?;
        if versions.len() > 0 {
            return Err(constraint_violation!(
                "deployment {} can not be removed \
                since it is the current or pending version for the subgraph(s) {}",
                id.as_str(),
                versions.join(", "),
            ));
        }

        store.drop_deployment(&site)?;

        self.primary_conn()?.drop_site(&site.deployment)?;

        Ok(())
    }

    pub(crate) fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        let deployments = match filter {
            status::Filter::SubgraphName(name) => {
                let deployments = self.primary_conn()?.deployments_for_subgraph(name)?;
                if deployments.is_empty() {
                    return Ok(Vec::new());
                }
                deployments
            }
            status::Filter::SubgraphVersion(name, use_current) => {
                let deployment = self.primary_conn()?.subgraph_version(name, use_current)?;
                match deployment {
                    Some(deployment) => vec![deployment],
                    None => {
                        return Ok(Vec::new());
                    }
                }
            }
            status::Filter::Deployments(deployments) => deployments,
        };

        let by_shard: HashMap<Shard, Vec<Arc<Site>>> = self.deployments_by_shard(deployments)?;

        // Go shard-by-shard to look up deployment statuses
        let mut infos = Vec::new();
        for (shard, sites) in by_shard.into_iter() {
            let store = self
                .stores
                .get(&shard)
                .ok_or(StoreError::UnknownShard(shard.to_string()))?;
            infos.extend(store.deployment_statuses(&sites)?);
        }
        let infos = self.primary_conn()?.fill_assignments(infos)?;
        Ok(infos)
    }

    pub(crate) fn version_info(&self, version: &str) -> Result<VersionInfo, StoreError> {
        if let Some((deployment_id, created_at)) = self.primary_conn()?.version_info(version)? {
            let id = SubgraphDeploymentId::new(deployment_id.clone())
                .map_err(|id| constraint_violation!("illegal deployment id {}", id))?;
            let (store, site) = self.store(&id)?;
            let statuses = store.deployment_statuses(&vec![site])?;
            let status = statuses
                .first()
                .ok_or_else(|| StoreError::DeploymentNotFound(deployment_id.clone()))?;
            let chain = status
                .chains
                .first()
                .ok_or_else(|| constraint_violation!("no chain info for {}", deployment_id))?;
            let latest_ethereum_block_number =
                chain.latest_block.as_ref().map(|ref block| block.number());
            let subgraph_info = store.subgraph_info(&id)?;
            let network = self.network_name(&id)?;

            let info = VersionInfo {
                created_at,
                deployment_id,
                latest_ethereum_block_number,
                total_ethereum_blocks_count: None,
                synced: status.synced,
                failed: status.health.is_failed(),
                description: subgraph_info.description,
                repository: subgraph_info.repository,
                schema: subgraph_info.input,
                network: network.to_string(),
            };
            Ok(info)
        } else {
            Err(StoreError::DeploymentNotFound(version.to_string()))
        }
    }

    pub(crate) fn versions_for_subgraph_id(
        &self,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        let primary = self.primary_conn()?;

        primary.versions_for_subgraph_id(subgraph_id)
    }

    #[cfg(debug_assertions)]
    pub fn error_count(&self, id: &SubgraphDeploymentId) -> Result<usize, StoreError> {
        let (store, _) = self.store(id)?;
        store.error_count(id)
    }
}

#[async_trait::async_trait]
impl SubgraphStoreTrait for SubgraphStore {
    fn block_ptr(&self, id: &SubgraphDeploymentId) -> Result<Option<EthereumBlockPointer>, Error> {
        let (store, site) = self.store(id)?;
        store.block_ptr(site.as_ref())
    }

    fn supports_proof_of_indexing<'a>(
        self: Arc<Self>,
        id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool> {
        let (store, site) = self.store(&id).unwrap();
        store.clone().supports_proof_of_indexing(site)
    }

    fn get_proof_of_indexing<'a>(
        self: Arc<Self>,
        id: &'a SubgraphDeploymentId,
        indexer: &'a Option<Address>,
        block: EthereumBlockPointer,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        let (store, site) = self.store(&id).unwrap();
        store.clone().get_proof_of_indexing(site, indexer, block)
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let (store, site) = self.store(&key.subgraph_id)?;
        store.get(site.as_ref(), key)
    }

    fn get_many(
        &self,
        id: &SubgraphDeploymentId,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        let (store, site) = self.store(&id)?;
        store.get_many(site.as_ref(), ids_for_type)
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        let (store, site) = self.store(&query.subgraph_id)?;
        store.find(site.as_ref(), query)
    }

    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        let (store, site) = self.store(&query.subgraph_id)?;
        store.find_one(site.as_ref(), query)
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        Ok(self.primary_conn()?.find_ens_name(hash)?)
    }

    fn transact_block_operations(
        &self,
        id: SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        assert!(
            mods.in_shard(&id),
            "can only transact operations within one shard"
        );
        let (store, site) = self.store(&id)?;
        let event = store.transact_block_operations(
            site.as_ref(),
            block_ptr_to,
            mods,
            stopwatch,
            deterministic_errors,
        )?;
        self.send_store_event(&event)
    }

    fn revert_block_operations(
        &self,
        id: SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        let (store, site) = self.store(&id)?;
        let event = store.revert_block_operations(site.as_ref(), block_ptr_to)?;
        self.send_store_event(&event)
    }

    async fn deployment_state_from_name(
        &self,
        name: SubgraphName,
    ) -> Result<DeploymentState, StoreError> {
        let id = self
            .with_primary_conn(|conn| conn.current_deployment_for_subgraph(name))
            .await?;
        self.deployment_state_from_id(id).await
    }

    async fn deployment_state_from_id(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<DeploymentState, StoreError> {
        let (store, _) = self.store(&id)?;
        store.deployment_state_from_id(id).await
    }

    fn start_subgraph_deployment(
        &self,
        logger: &Logger,
        id: &SubgraphDeploymentId,
    ) -> Result<(), StoreError> {
        let (store, site) = self.store(id)?;

        let graft_base = match store.graft_pending(id)? {
            Some((base_id, base_ptr)) => {
                let site = self.primary_conn()?.find_existing_site(&base_id)?;
                Some((site, base_ptr))
            }
            None => None,
        };
        store.start_subgraph(logger, site, graft_base)
    }

    fn unfail(&self, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
        let (store, site) = self.store(id)?;
        store.unfail(site)
    }

    fn is_deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<bool, Error> {
        let (store, _) = self.store(&id)?;
        Ok(store.exists_and_synced(&id)?)
    }

    fn deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<(), Error> {
        let event = {
            // Make sure we drop `pconn` before we call into the deployment
            // store so that we do not hold two database connections which
            // might come from the same pool and could therefore deadlock
            let pconn = self.primary_conn()?;
            pconn.transaction(|| -> Result<_, Error> {
                let changes = pconn.promote_deployment(id)?;
                Ok(StoreEvent::new(changes))
            })?
        };

        let (dstore, _) = self.store(id)?;
        dstore.deployment_synced(id)?;

        Ok(self.primary_conn()?.send_store_event(&event)?)
    }

    // FIXME: This method should not get a node_id
    fn create_subgraph_deployment(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        network_name: String,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        self.create_deployment_internal(
            name,
            schema,
            deployment,
            node_id,
            network_name,
            mode,
            false,
        )
    }

    fn create_subgraph(&self, name: SubgraphName) -> Result<String, StoreError> {
        let pconn = self.primary_conn()?;
        pconn.transaction(|| pconn.create_subgraph(&name))
    }

    fn remove_subgraph(&self, name: SubgraphName) -> Result<(), StoreError> {
        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            let changes = pconn.remove_subgraph(name)?;
            pconn.send_store_event(&StoreEvent::new(changes))
        })
    }

    fn reassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
        node_id: &NodeId,
    ) -> Result<(), StoreError> {
        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            let changes = pconn.reassign_subgraph(id, node_id)?;
            pconn.send_store_event(&StoreEvent::new(changes))
        })
    }

    fn unassign_subgraph(&self, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            let changes = pconn.unassign_subgraph(id)?;
            pconn.send_store_event(&StoreEvent::new(changes))
        })
    }

    async fn load_dynamic_data_sources(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        let (store, _) = self.store(&id)?;
        store.load_dynamic_data_sources(id).await
    }

    async fn fail_subgraph(
        &self,
        id: SubgraphDeploymentId,
        error: SubgraphError,
    ) -> Result<(), StoreError> {
        let (store, _) = self.store(&id)?;
        store.fail_subgraph(id, error).await
    }

    fn assigned_node(&self, id: &SubgraphDeploymentId) -> Result<Option<NodeId>, StoreError> {
        let primary = self.primary_conn()?;
        primary.assigned_node(id)
    }

    fn assignments(&self, node: &NodeId) -> Result<Vec<SubgraphDeploymentId>, StoreError> {
        let primary = self.primary_conn()?;
        primary.assignments(node)
    }

    fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError> {
        let primary = self.primary_conn()?;
        primary.subgraph_exists(name)
    }

    fn input_schema(&self, id: &SubgraphDeploymentId) -> Result<Arc<Schema>, StoreError> {
        let (store, _) = self.store(&id)?;
        let info = store.subgraph_info(id)?;
        Ok(info.input)
    }

    fn api_schema(&self, id: &SubgraphDeploymentId) -> Result<Arc<ApiSchema>, StoreError> {
        let (store, _) = self.store(&id)?;
        let info = store.subgraph_info(id)?;
        Ok(info.api)
    }

    fn network_name(&self, id: &SubgraphDeploymentId) -> Result<String, StoreError> {
        let (_, site) = self.store(&id)?;
        Ok(site.network.to_string())
    }
}

trait ShardData {
    // Return `true` if this object resides in the shard for the
    // data for the given deployment
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool;
}

impl ShardData for MetadataType {
    fn in_shard(&self, _: &SubgraphDeploymentId) -> bool {
        use MetadataType::*;

        match self {
            SubgraphDeploymentAssignment => false,
            SubgraphDeployment
            | SubgraphManifest
            | DynamicEthereumContractDataSource
            | SubgraphError => true,
        }
    }
}

impl ShardData for MetadataOperation {
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool {
        use MetadataOperation::*;
        match self {
            Set { key, .. } | Remove { key, .. } => {
                &key.subgraph_id == id && key.entity_type.in_shard(id)
            }
        }
    }
}

impl<T> ShardData for Vec<T>
where
    T: ShardData,
{
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool {
        self.iter().all(|op| op.in_shard(id))
    }
}

impl ShardData for EntityModification {
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool {
        let key = self.entity_key();

        match &key.entity_type {
            EntityType::Data(_) => &key.subgraph_id == id,
            EntityType::Metadata(typ) => &key.subgraph_id == id && typ.in_shard(id),
        }
    }
}

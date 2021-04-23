use diesel::{
    pg::Pg,
    serialize::Output,
    sql_types::Text,
    types::{FromSql, ToSql},
};
use std::{collections::BTreeMap, collections::HashMap, sync::Arc};
use std::{fmt, io::Write};
use std::{iter::FromIterator, time::Duration};

use graph::{
    components::{
        server::index_node::VersionInfo,
        store::{self, DeploymentLocator, EntityType, WritableStore as WritableStoreTrait},
    },
    constraint_violation,
    data::query::QueryTarget,
    data::subgraph::schema::SubgraphError,
    data::subgraph::status,
    prelude::StoreEvent,
    prelude::SubgraphDeploymentEntity,
    prelude::{
        anyhow, futures03::future::join_all, lazy_static, o, web3::types::Address, ApiSchema,
        BlockPtr, DeploymentHash, DynTryFuture, Entity, EntityKey, EntityModification, Error,
        Logger, NodeId, QueryExecutionError, Schema, StopwatchMetrics, StoreError, SubgraphName,
        SubgraphStore as SubgraphStoreTrait, SubgraphVersionSwitchingMode,
    },
    util::timed_cache::TimedCache,
};
use store::StoredDynamicDataSource;

use crate::{
    connection_pool::ConnectionPool,
    primary,
    primary::{DeploymentId, Site},
    relational::Layout,
};
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

/// How long to cache information about a deployment site
const SITES_CACHE_TTL: Duration = Duration::from_secs(120);

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
/// - `public.deployment_schemas`: immutable data about deployments,
///   including the shard that stores the deployment data and metadata, the
///   namespace in the shard that contains the deployment data, and the
///   network/chain that the  deployment is indexing
/// - `subgraphs.subgraph` and `subgraphs.subgraph_version`: information
///   about named subgraphs and how they map to deployments
/// - `subgraphs.subgraph_deployment_assignment`: which index node is
///   indexing what deployment
///
/// The primary is also the database that is used to send and receive
/// notifications through Postgres' `LISTEN`/`NOTIFY` mechanism. That is
/// used to send notifications about new blocks that a block ingestor has
/// discovered, and to send `StoreEvents`, which are used to broadcast
/// changes in deployment assignments and changes in subgraph data to
/// trigger updates on GraphQL subscriptions.
///
/// For each deployment, the corresponding shard contains a namespace for
/// the deployment data; the schema in that namespace is generated from the
/// deployment's GraphQL schema by the [crate::relational::Layout], which is
/// also responsible for modifying and querying subgraph data. Deployment
/// metadata is stored in tables in the `subgraphs` namespace in the same
/// shard as the deployment data. The most important of these tables are
///
/// - `subgraphs.subgraph_deployment`: the main table for deployment
///   metadata; most importantly, it stores the pointer to the current
///   subgraph head, i.e., the block up to which the subgraph has indexed
///   the chain, together with other things like whether the subgraph has
///   synced, whether it has failed and whether it encountered any errors
/// - `subgraphs.subgraph_manifest`: immutable information derived from the
///   YAML manifest for the deployment
/// - `subgraphs.dynamic_ethereum_contract_data_source`: the data sources
///   that the subgraph has created from templates in the manifest.
/// - `subgraphs.subgraph_error`: details about errors that the deployment
///   has encountered
///
/// The `SubgraphStore` mostly orchestrates access to the primary and the
/// shards.  The actual work is done by code in the `primary` module for
/// queries against the primary store, and by the `DeploymentStore` for
/// access to deployment data and metadata.
#[derive(Clone)]
pub struct SubgraphStore {
    inner: Arc<SubgraphStoreInner>,
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
    ) -> Self {
        Self {
            inner: Arc::new(SubgraphStoreInner::new(logger, stores, placer)),
        }
    }

    pub(crate) fn get_proof_of_indexing<'a>(
        &self,
        id: &'a DeploymentHash,
        indexer: &'a Option<Address>,
        block: BlockPtr,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        self.inner.clone().get_proof_of_indexing(id, indexer, block)
    }
}

impl std::ops::Deref for SubgraphStore {
    type Target = SubgraphStoreInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct SubgraphStoreInner {
    logger: Logger,
    primary: ConnectionPool,
    stores: HashMap<Shard, Arc<DeploymentStore>>,
    /// Cache for the mapping from deployment id to shard/namespace/id. Only
    /// active sites are cached here to ensure we have a unique mapping from
    /// `SubgraphDeploymentId` to `Site`. The cache keeps entry only for
    /// `SITES_CACHE_TTL` so that changes, in particular, activation of a
    /// different deployment for the same hash propagate across different
    /// graph-node processes over time.
    sites: TimedCache<DeploymentHash, Site>,
    placer: Arc<dyn DeploymentPlacer + Send + Sync + 'static>,
}

impl SubgraphStoreInner {
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
                    )),
                )
            },
        ));
        let sites = TimedCache::new(SITES_CACHE_TTL);
        let logger = logger.new(o!("shard" => PRIMARY_SHARD.to_string()));
        SubgraphStoreInner {
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
        self.sites.clear();
    }

    // Only needed for tests
    #[cfg(debug_assertions)]
    pub fn shard(&self, deployment: &DeploymentLocator) -> Result<Shard, StoreError> {
        self.find_site(deployment.id.into())
            .map(|site| site.shard.clone())
    }

    fn cache_active(&self, site: &Arc<Site>) {
        if site.active {
            self.sites.set(site.deployment.clone(), site.clone());
        }
    }

    /// Return the active `Site` for this deployment hash
    fn site(&self, id: &DeploymentHash) -> Result<Arc<Site>, StoreError> {
        if let Some(site) = self.sites.get(id) {
            return Ok(site);
        }

        let conn = self.primary_conn()?;
        let site = conn
            .find_active_site(id)?
            .ok_or_else(|| StoreError::DeploymentNotFound(id.to_string()))?;
        let site = Arc::new(site);

        self.cache_active(&site);
        Ok(site)
    }

    fn find_site(&self, id: DeploymentId) -> Result<Arc<Site>, StoreError> {
        if let Some(site) = self.sites.find(|site| site.id == id) {
            return Ok(site);
        }

        let conn = self.primary_conn()?;
        let site = conn
            .find_site_by_ref(id)?
            .ok_or_else(|| StoreError::DeploymentNotFound(id.to_string()))?;
        let site = Arc::new(site);

        self.cache_active(&site);
        Ok(site)
    }

    /// Return the store and site for the active deployment of this
    /// deployment hash
    fn store(&self, id: &DeploymentHash) -> Result<(&Arc<DeploymentStore>, Arc<Site>), StoreError> {
        let site = self.site(id)?;
        let store = self
            .stores
            .get(&site.shard)
            .ok_or(StoreError::UnknownShard(site.shard.as_str().to_string()))?;
        Ok((store, site))
    }

    fn for_site(&self, site: &Site) -> Result<&Arc<DeploymentStore>, StoreError> {
        self.stores
            .get(&site.shard)
            .ok_or(StoreError::UnknownShard(site.shard.as_str().to_string()))
    }

    fn layout(&self, id: &DeploymentHash) -> Result<Arc<Layout>, StoreError> {
        let (store, site) = self.store(id)?;
        store.find_layout(site)
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
    ) -> Result<DeploymentLocator, StoreError> {
        #[cfg(not(debug_assertions))]
        assert!(!replace);

        let (shard, node_id) = self.place(&name, &network_name, node_id)?;

        // TODO: Check this for behavior on failure
        let site = self
            .primary_conn()?
            .allocate_site(shard.clone(), &schema.id, network_name)?;
        let site = Arc::new(site);

        let graft_base = deployment
            .graft_base
            .as_ref()
            .map(|base| self.layout(base))
            .transpose()?;

        if let Some(graft_base) = &graft_base {
            self.primary_conn()?
                .record_active_copy(graft_base.site.as_ref(), site.as_ref())?;
        }

        // Create the actual databases schema and metadata entries
        let deployment_store = self
            .stores
            .get(&shard)
            .ok_or_else(|| StoreError::UnknownShard(shard.to_string()))?;
        deployment_store.create_deployment(
            schema,
            deployment,
            site.clone(),
            graft_base,
            replace,
        )?;

        let exists_and_synced = |id: &DeploymentHash| {
            let (store, _) = self.store(id)?;
            store.deployment_exists_and_synced(id)
        };

        // FIXME: This simultaneously holds a `primary_conn` and a shard connection, which can
        // potentially deadlock.
        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            // Create subgraph, subgraph version, and assignment
            let changes =
                pconn.create_subgraph_version(name, &site, node_id, mode, exists_and_synced)?;
            let event = StoreEvent::new(changes);
            pconn.send_store_event(&event)?;
            Ok(())
        })?;
        Ok(site.as_ref().into())
    }

    pub fn copy_deployment(
        &self,
        src: &DeploymentLocator,
        shard: Shard,
        node: NodeId,
        block: BlockPtr,
    ) -> Result<DeploymentLocator, StoreError> {
        let src = self.find_site(src.id.into())?;
        let src_store = self.for_site(src.as_ref())?;
        let src_info = src_store.subgraph_info(src.as_ref())?;
        let src_loc = DeploymentLocator::from(src.as_ref());

        let dst = Arc::new(self.primary_conn()?.copy_site(&src, shard.clone())?);
        let dst_loc = DeploymentLocator::from(dst.as_ref());

        if src.id == dst.id {
            return Err(StoreError::Unknown(anyhow!(
                "can not copy deployment {} onto itself",
                src_loc
            )));
        }
        // The very last thing we do when we set up a copy here is assign it
        // to a node. Therefore, if `dst` is already assigned, this function
        // should not have been called.
        if let Some(node) = self.primary_conn()?.assigned_node(dst.as_ref())? {
            return Err(StoreError::Unknown(anyhow!(
                "can not copy into deployment {} since it is already assigned to node `{}`",
                dst_loc,
                node
            )));
        }
        let deployment = src_store.load_deployment(src.as_ref())?;
        if deployment.failed {
            return Err(StoreError::Unknown(anyhow!(
                "can not copy deployment {} because it has failed",
                src_loc
            )));
        }

        // Transmogrify the deployment into a new one
        let deployment = SubgraphDeploymentEntity {
            manifest: deployment.manifest,
            failed: false,
            health: deployment.health,
            synced: false,
            fatal_error: None,
            non_fatal_errors: vec![],
            earliest_block: deployment.earliest_block.clone(),
            latest_block: deployment.earliest_block,
            graft_base: Some(src.deployment.clone()),
            graft_block: Some(block),
            reorg_count: 0,
            current_reorg_depth: 0,
            max_reorg_depth: 0,
        };

        let graft_base = self.layout(&src.deployment)?;

        self.primary_conn()?
            .record_active_copy(src.as_ref(), dst.as_ref())?;

        // Create the actual databases schema and metadata entries
        let deployment_store = self
            .stores
            .get(&shard)
            .ok_or_else(|| StoreError::UnknownShard(shard.to_string()))?;

        deployment_store.create_deployment(
            &src_info.input,
            deployment,
            dst.clone(),
            Some(graft_base),
            false,
        )?;

        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            // Create subgraph, subgraph version, and assignment. We use the
            // existence of an assignment as a signal that we already set up
            // the copy
            let changes = pconn.assign_subgraph(dst.as_ref(), &node)?;
            let event = StoreEvent::new(changes);
            pconn.send_store_event(&event)?;
            Ok(())
        })?;
        Ok(dst.as_ref().into())
    }

    /// Mark `deployment` as the only active deployment amongst all sites
    /// with the same deployment hash. Activating this specific deployment
    /// will make queries use that instead of whatever was active before
    pub fn activate(&self, deployment: &DeploymentLocator) -> Result<(), StoreError> {
        self.primary_conn()?.activate(deployment)?;
        // As a side-effect, this will update the `self.sites` cache with
        // the new active site
        self.find_site(deployment.id.into())?;
        Ok(())
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
    ) -> Result<DeploymentLocator, StoreError> {
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

    /// Partition the list of deployments by the shard they belong to. As a
    /// side-effect, add all `sites` to the cache
    fn deployments_by_shard(
        &self,
        sites: Vec<Site>,
    ) -> Result<HashMap<Shard, Vec<Arc<Site>>>, StoreError> {
        let sites: Vec<_> = sites.into_iter().map(|site| Arc::new(site)).collect();
        for site in &sites {
            self.cache_active(site);
        }

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
    pub fn remove_deployment(&self, id: DeploymentId) -> Result<(), StoreError> {
        let site = self.find_site(id)?;
        let store = self.for_site(site.as_ref())?;

        // Check that deployment is not assigned
        match self.primary_conn()?.assigned_node(site.as_ref())? {
            Some(node) => {
                return Err(constraint_violation!(
                    "deployment {} can not be removed since it is assigned to node {}",
                    site.deployment.as_str(),
                    node.as_str()
                ));
            }
            None => { /* ok */ }
        }

        // Check that it is not current/pending for any subgraph
        let versions = self
            .primary_conn()?
            .subgraphs_using_deployment(site.as_ref())?;
        if versions.len() > 0 {
            return Err(constraint_violation!(
                "deployment {} can not be removed \
                since it is the current or pending version for the subgraph(s) {}",
                site.deployment.as_str(),
                versions.join(", "),
            ));
        }

        store.drop_deployment(&site)?;

        self.primary_conn()?.drop_site(site.as_ref())?;

        Ok(())
    }

    pub(crate) fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        let sites = match filter {
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
            status::Filter::Deployments(deployments) => {
                self.primary_conn()?.find_sites(deployments, true)?
            }
        };

        let by_shard: HashMap<Shard, Vec<Arc<Site>>> = self.deployments_by_shard(sites)?;

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
            let id = DeploymentHash::new(deployment_id.clone())
                .map_err(|id| constraint_violation!("illegal deployment id {}", id))?;
            let (store, site) = self.store(&id)?;
            let statuses = store.deployment_statuses(&vec![site.clone()])?;
            let status = statuses
                .first()
                .ok_or_else(|| StoreError::DeploymentNotFound(deployment_id.clone()))?;
            let chain = status
                .chains
                .first()
                .ok_or_else(|| constraint_violation!("no chain info for {}", deployment_id))?;
            let latest_ethereum_block_number =
                chain.latest_block.as_ref().map(|ref block| block.number());
            let subgraph_info = store.subgraph_info(site.as_ref())?;
            let network = site.network.clone();

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
    pub fn error_count(&self, id: &DeploymentHash) -> Result<usize, StoreError> {
        let (store, _) = self.store(id)?;
        store.error_count(id)
    }

    /// Vacuum the `subgraph_deployment` table in each shard
    pub(crate) async fn vacuum(&self) -> Vec<Result<(), StoreError>> {
        join_all(self.stores.values().map(|store| store.vacuum())).await
    }

    pub fn rewind(&self, id: DeploymentHash, block_ptr_to: BlockPtr) -> Result<(), StoreError> {
        let (store, site) = self.store(&id)?;
        let event = store.rewind(site, block_ptr_to)?;
        self.send_store_event(&event)
    }

    pub(crate) fn get_proof_of_indexing<'a>(
        self: Arc<Self>,
        id: &'a DeploymentHash,
        indexer: &'a Option<Address>,
        block: BlockPtr,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        let (store, site) = self.store(&id).unwrap();
        store.clone().get_proof_of_indexing(site, indexer, block)
    }

    // Only used by tests
    #[cfg(debug_assertions)]
    pub fn find(
        &self,
        query: graph::prelude::EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let (store, site) = self.store(&query.subgraph_id)?;
        store.find(site, query)
    }

    pub fn locate_in_shard(
        &self,
        hash: &DeploymentHash,
        shard: Shard,
    ) -> Result<Option<DeploymentLocator>, StoreError> {
        Ok(self
            .primary_conn()?
            .find_site_in_shard(hash, &shard)?
            .as_ref()
            .map(|site| site.into()))
    }
}

#[async_trait::async_trait]
impl SubgraphStoreTrait for SubgraphStore {
    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        Ok(self.primary_conn()?.find_ens_name(hash)?)
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
    ) -> Result<DeploymentLocator, StoreError> {
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
        deployment: &DeploymentLocator,
        node_id: &NodeId,
    ) -> Result<(), StoreError> {
        let site = self.find_site(deployment.id.into())?;
        let pconn = self.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            let changes = pconn.reassign_subgraph(site.as_ref(), node_id)?;
            pconn.send_store_event(&StoreEvent::new(changes))
        })
    }

    fn assigned_node(&self, deployment: &DeploymentLocator) -> Result<Option<NodeId>, StoreError> {
        let site = self.find_site(deployment.id.into())?;
        let primary = self.primary_conn()?;
        primary.assigned_node(site.as_ref())
    }

    fn assignments(&self, node: &NodeId) -> Result<Vec<DeploymentLocator>, StoreError> {
        let primary = self.primary_conn()?;
        primary
            .assignments(node)
            .map(|sites| sites.iter().map(|site| site.into()).collect())
    }

    fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError> {
        let primary = self.primary_conn()?;
        primary.subgraph_exists(name)
    }

    fn input_schema(&self, id: &DeploymentHash) -> Result<Arc<Schema>, StoreError> {
        let (store, site) = self.store(&id)?;
        let info = store.subgraph_info(site.as_ref())?;
        Ok(info.input)
    }

    fn api_schema(&self, id: &DeploymentHash) -> Result<Arc<ApiSchema>, StoreError> {
        let (store, site) = self.store(&id)?;
        let info = store.subgraph_info(&site)?;
        Ok(info.api)
    }

    fn writable(
        &self,
        deployment: &DeploymentLocator,
    ) -> Result<Arc<dyn store::WritableStore>, StoreError> {
        let site = self.find_site(deployment.id.into())?;
        Ok(Arc::new(WritableStore::new(self.clone(), site)?))
    }

    fn writable_for_network_indexer(
        &self,
        id: &DeploymentHash,
    ) -> Result<Arc<dyn WritableStoreTrait>, StoreError> {
        let site = self.site(id)?;
        Ok(Arc::new(WritableStore::new(self.clone(), site)?))
    }

    fn is_deployed(&self, id: &DeploymentHash) -> Result<bool, Error> {
        match self.site(id) {
            Ok(_) => Ok(true),
            Err(StoreError::DeploymentNotFound(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn least_block_ptr(&self, id: &DeploymentHash) -> Result<Option<BlockPtr>, Error> {
        let (store, site) = self.store(id)?;
        store.block_ptr(site.as_ref())
    }

    /// Find the deployment locators for the subgraph with the given hash
    fn locators(&self, hash: &str) -> Result<Vec<DeploymentLocator>, StoreError> {
        Ok(self
            .primary_conn()?
            .find_sites(vec![hash.to_string()], false)?
            .iter()
            .map(|site| site.into())
            .collect())
    }
}

/// A wrapper around `SubgraphStore` that only exposes functions that are
/// safe to call from `WritableStore`, i.e., functions that either do not
/// deal with anything that depends on a specific deployment
/// location/instance, or where the result is independent of the deployment
/// instance
struct WritableSubgraphStore(SubgraphStore);

impl WritableSubgraphStore {
    fn primary_conn(&self) -> Result<primary::Connection, StoreError> {
        self.0.primary_conn()
    }

    pub(crate) fn send_store_event(&self, event: &StoreEvent) -> Result<(), StoreError> {
        self.0.send_store_event(event)
    }

    fn layout(&self, id: &DeploymentHash) -> Result<Arc<Layout>, StoreError> {
        self.0.layout(id)
    }
}

struct WritableStore {
    store: WritableSubgraphStore,
    writable: Arc<DeploymentStore>,
    site: Arc<Site>,
}

impl WritableStore {
    fn new(subgraph_store: SubgraphStore, site: Arc<Site>) -> Result<Self, StoreError> {
        let store = WritableSubgraphStore(subgraph_store.clone());
        let writable = subgraph_store.for_site(site.as_ref())?.clone();
        Ok(Self {
            store,
            writable,
            site,
        })
    }
}

#[async_trait::async_trait]
impl WritableStoreTrait for WritableStore {
    fn block_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        self.writable.block_ptr(self.site.as_ref())
    }

    fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError> {
        let store = &self.writable;

        let graft_base = match store.graft_pending(&self.site.deployment)? {
            Some((base_id, base_ptr)) => {
                let src = self.store.layout(&base_id)?;
                Some((src, base_ptr))
            }
            None => None,
        };
        store.start_subgraph(logger, self.site.clone(), graft_base)?;
        self.store.primary_conn()?.copy_finished(self.site.as_ref())
    }

    fn revert_block_operations(&self, block_ptr_to: BlockPtr) -> Result<(), StoreError> {
        let event = self
            .writable
            .revert_block_operations(self.site.clone(), block_ptr_to)?;
        self.store.send_store_event(&event)
    }

    fn unfail(&self) -> Result<(), StoreError> {
        self.writable.unfail(self.site.clone())
    }

    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError> {
        self.writable
            .fail_subgraph(self.site.deployment.clone(), error)
            .await
    }

    fn supports_proof_of_indexing<'a>(self: Arc<Self>) -> DynTryFuture<'a, bool> {
        self.writable
            .clone()
            .supports_proof_of_indexing(self.site.clone())
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        self.writable.get(self.site.clone(), key)
    }

    fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        assert!(
            same_subgraph(&mods, &self.site.deployment),
            "can only transact operations within one shard"
        );
        let event = self.writable.transact_block_operations(
            self.site.clone(),
            block_ptr_to,
            mods,
            stopwatch,
            data_sources,
            deterministic_errors,
        )?;
        self.store.send_store_event(&event)
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.writable.get_many(self.site.clone(), ids_for_type)
    }

    fn is_deployment_synced(&self) -> Result<bool, Error> {
        Ok(self.writable.exists_and_synced(&self.site.deployment)?)
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        let pconn = self.store.primary_conn()?;
        pconn.transaction(|| -> Result<_, StoreError> {
            let changes = pconn.unassign_subgraph(self.site.as_ref())?;
            pconn.send_store_event(&StoreEvent::new(changes))
        })
    }

    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.writable
            .load_dynamic_data_sources(self.site.deployment.clone())
            .await
    }

    fn deployment_synced(&self) -> Result<(), Error> {
        let event = {
            // Make sure we drop `pconn` before we call into the deployment
            // store so that we do not hold two database connections which
            // might come from the same pool and could therefore deadlock
            let pconn = self.store.primary_conn()?;
            pconn.transaction(|| -> Result<_, Error> {
                let changes = pconn.promote_deployment(&self.site.deployment)?;
                Ok(StoreEvent::new(changes))
            })?
        };

        self.writable.deployment_synced(&self.site.deployment)?;

        Ok(self.store.send_store_event(&event)?)
    }
}

fn same_subgraph(mods: &Vec<EntityModification>, id: &DeploymentHash) -> bool {
    mods.iter().all(|md| &md.entity_key().subgraph_id == id)
}

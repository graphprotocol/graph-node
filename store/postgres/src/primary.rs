//! Utilities for dealing with subgraph metadata that resides in the primary
//! shard. Anything in this module can only be used with a database connection
//! for the primary shard.
use crate::{
    block_range::UNVERSIONED_RANGE,
    detail::DeploymentDetail,
    pool::{PermittedConnection, PRIMARY_PUBLIC},
    subgraph_store::{unused, Shard, PRIMARY_SHARD},
    AsyncPgConnection, ConnectionPool, ForeignServer, NotificationSender,
};
use diesel::dsl::{delete, insert_into, sql, update};
use diesel::prelude::{
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl,
};
use diesel::{
    data_types::PgTimestamp,
    deserialize::FromSql,
    dsl::{exists, not, select},
    pg::Pg,
    serialize::{Output, ToSql},
    sql_types::{Array, BigInt, Bool, Integer, Text},
};
use diesel_async::{
    scoped_futures::{ScopedBoxFuture, ScopedFutureExt},
    RunQueryDsl, SimpleAsyncConnection as _, TransactionManager,
};
use graph::{
    components::store::DeploymentLocator,
    data::{
        store::scalar::ToPrimitive,
        subgraph::{status, DeploymentFeatures},
    },
    derive::CheapClone,
    futures03::{future::BoxFuture, FutureExt},
    internal_error,
    prelude::{
        anyhow,
        chrono::{DateTime, Utc},
        serde_json, AssignmentChange, DeploymentHash, NodeId, StoreError, SubgraphName,
        SubgraphVersionSwitchingMode,
    },
};
use graph::{
    components::store::{DeploymentId as GraphDeploymentId, DeploymentSchemaVersion},
    prelude::chrono,
};
use graph::{data::subgraph::schema::generate_entity_id, prelude::StoreEvent};
use itertools::Itertools;
use std::{
    borrow::Borrow,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(debug_assertions)]
use std::sync::Mutex;
#[cfg(debug_assertions)]
lazy_static::lazy_static! {
    /// Tests set this to true so that `send_store_event` will store a copy
    /// of each event sent in `EVENT_TAP`
    pub static ref EVENT_TAP_ENABLED: Mutex<bool> = Mutex::new(false);
    pub static ref EVENT_TAP: Mutex<Vec<StoreEvent>> = Mutex::new(Vec::new());
}

table! {
    subgraphs.subgraph (vid) {
        vid -> BigInt,
        id -> Text,
        name -> Text,
        current_version -> Nullable<Text>,
        pending_version -> Nullable<Text>,
        created_at -> Numeric,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.subgraph_features (id) {
        id -> Text,
        spec_version -> Text,
        api_version -> Nullable<Text>,
        features -> Array<Text>,
        data_sources -> Array<Text>,
        handlers -> Array<Text>,
        network -> Text,
        has_declared_calls -> Bool,
        has_bytes_as_ids -> Bool,
        has_aggregations -> Bool,
        immutable_entities -> Array<Text>
    }
}

table! {
    subgraphs.subgraph_version (vid) {
        vid -> BigInt,
        id -> Text,
        subgraph -> Text,
        deployment -> Text,
        created_at -> Numeric,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.subgraph_deployment_assignment {
        id -> Integer,
        node_id -> Text,
        paused_at -> Nullable<Timestamptz>,
        assigned_at -> Nullable<Timestamptz>,
    }
}

table! {
    active_copies(dst) {
        src -> Integer,
        dst -> Integer,
        queued_at -> Timestamptz,
        // Setting this column to a value signals to a running copy process
        // that a cancel has been requested. The copy process checks this
        // periodically and stops as soon as this is not null anymore
        cancelled_at -> Nullable<Timestamptz>,
    }
}

table! {
    public.ens_names(hash) {
        hash -> Varchar,
        name -> Varchar,
    }
}

table! {
    deployment_schemas(id) {
        id -> Integer,
        created_at -> Timestamptz,
        subgraph -> Text,
        name -> Text,
        shard -> Text,
        /// The subgraph layout scheme used for this subgraph
        version -> Integer,
        network -> Text,
        /// If there are multiple entries for the same IPFS hash (`subgraph`)
        /// only one of them will be active. That's the one we use for
        /// querying
        active -> Bool,
    }
}

table! {
    /// A table to track deployments that are no longer used. Once an unused
    /// deployment has been removed, the entry in this table is the only
    /// trace in the system that it ever existed
    unused_deployments(id) {
        // This is the same as what deployment_schemas.id was when the
        // deployment was still around
        id -> Integer,
        // The IPFS hash of the deployment
        deployment -> Text,
        // When we first detected that the deployment was unused
        unused_at -> Timestamptz,
        // When we actually deleted the deployment
        removed_at -> Nullable<Timestamptz>,
        // When the deployment was created
        created_at -> Timestamptz,
        /// Data that we get from the primary
        subgraphs -> Nullable<Array<Text>>,
        namespace -> Text,
        shard -> Text,

        /// Data we fill in from the deployment's shard
        entity_count -> Integer,
        latest_ethereum_block_hash -> Nullable<Binary>,
        latest_ethereum_block_number -> Nullable<Integer>,
        failed -> Bool,
        synced_at -> Nullable<Timestamptz>,
        synced_at_block_number -> Nullable<Int4>,
    }
}

table! {
    public.db_version(version) {
        #[sql_name = "db_version"]
        version -> BigInt,
    }
}

allow_tables_to_appear_in_same_query!(
    subgraph,
    subgraph_version,
    subgraph_deployment_assignment,
    deployment_schemas,
    unused_deployments,
    active_copies,
);

/// Information about the database schema that stores the entities for a
/// subgraph.
#[derive(Clone, Queryable, QueryableByName, Debug)]
#[diesel(table_name = deployment_schemas)]
struct Schema {
    id: DeploymentId,
    #[allow(dead_code)]
    pub created_at: PgTimestamp,
    pub subgraph: String,
    pub name: String,
    pub shard: String,
    version: i32,
    pub network: String,
    pub(crate) active: bool,
}

#[derive(Clone, Queryable, QueryableByName, Debug)]
#[diesel(table_name = unused_deployments)]
pub struct UnusedDeployment {
    pub id: DeploymentId,
    pub deployment: String,
    pub unused_at: PgTimestamp,
    pub removed_at: Option<PgTimestamp>,
    pub created_at: PgTimestamp,
    pub subgraphs: Option<Vec<String>>,
    pub namespace: String,
    pub shard: String,

    /// Data we fill in from the deployment's shard
    pub entity_count: i32,
    pub latest_ethereum_block_hash: Option<Vec<u8>>,
    pub latest_ethereum_block_number: Option<i32>,
    pub failed: bool,
    pub synced_at: Option<DateTime<Utc>>,
    pub synced_at_block_number: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, AsExpression, FromSqlRow)]
#[diesel(sql_type = Text)]
/// A namespace (schema) in the database
pub struct Namespace(String);

/// The name of the `public` schema in Postgres
pub const NAMESPACE_PUBLIC: &str = "public";
/// The name of the `subgraphs` schema in Postgres
pub const NAMESPACE_SUBGRAPHS: &str = "subgraphs";

impl Namespace {
    pub fn new(s: String) -> Result<Self, String> {
        // Normal database namespaces must be of the form `sgd[0-9]+`
        if !s.starts_with("sgd") || s.len() <= 3 {
            return Err(s);
        }
        for c in s.chars().skip(3) {
            if !c.is_numeric() {
                return Err(s);
            }
        }

        Ok(Namespace(s))
    }

    pub fn prune(id: DeploymentId) -> Self {
        Namespace(format!("prune{id}"))
    }

    /// A namespace that is not a deployment namespace. This is used for
    /// special namespaces we use. No checking is done on `s` and the caller
    /// must ensure it's a valid namespace name
    pub fn special(s: impl Into<String>) -> Self {
        Namespace(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromSql<Text, Pg> for Namespace {
    fn from_sql(bytes: diesel::pg::PgValue) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        Namespace::new(s).map_err(Into::into)
    }
}

impl ToSql<Text, Pg> for Namespace {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        <String as ToSql<Text, Pg>>::to_sql(&self.0, out)
    }
}

impl Borrow<str> for Namespace {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for &Namespace {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// A marker that an `i32` references a deployment. Values of this type hold
/// the primary key from the `deployment_schemas` table
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, AsExpression, FromSqlRow)]
#[diesel(sql_type = Integer)]
pub struct DeploymentId(i32);

impl fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<DeploymentId> for GraphDeploymentId {
    fn from(id: DeploymentId) -> Self {
        GraphDeploymentId::new(id.0)
    }
}

impl From<GraphDeploymentId> for DeploymentId {
    fn from(id: GraphDeploymentId) -> Self {
        DeploymentId(id.0)
    }
}

impl From<DeploymentLocator> for DeploymentId {
    fn from(loc: DeploymentLocator) -> Self {
        Self::from(loc.id)
    }
}

impl FromSql<Integer, Pg> for DeploymentId {
    fn from_sql(bytes: diesel::pg::PgValue) -> diesel::deserialize::Result<Self> {
        let id = <i32 as FromSql<Integer, Pg>>::from_sql(bytes)?;
        Ok(DeploymentId(id))
    }
}

impl ToSql<Integer, Pg> for DeploymentId {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        <i32 as ToSql<Integer, Pg>>::to_sql(&self.0, out)
    }
}

#[derive(Debug, PartialEq)]
/// Details about a deployment and the shard in which it is stored. We need
/// the database namespace for the deployment as that information is only
/// stored in the primary database.
///
/// Any instance of this struct must originate in the database
pub struct Site {
    pub id: DeploymentId,
    /// The subgraph deployment
    pub deployment: DeploymentHash,
    /// The name of the database shard
    pub shard: Shard,
    /// The database namespace (schema) that holds the data for the deployment
    pub namespace: Namespace,
    /// The name of the network to which this deployment belongs
    pub network: String,
    /// Whether this is the site that should be used for queries. There's
    /// exactly one for each `deployment`, i.e., other entries for that
    /// deployment have `active = false`
    pub active: bool,

    pub(crate) schema_version: DeploymentSchemaVersion,
    /// Only the store and tests can create Sites
    _creation_disallowed: (),
}

impl TryFrom<Schema> for Site {
    type Error = StoreError;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        let deployment = DeploymentHash::new(&schema.subgraph)
            .map_err(|s| internal_error!("Invalid deployment id {}", s))?;
        let namespace = Namespace::new(schema.name.clone()).map_err(|nsp| {
            internal_error!(
                "Invalid schema name {} for deployment {}",
                nsp,
                &schema.subgraph
            )
        })?;
        let shard = Shard::new(schema.shard)?;
        let schema_version = DeploymentSchemaVersion::try_from(schema.version)?;
        Ok(Self {
            id: schema.id,
            deployment,
            namespace,
            shard,
            network: schema.network,
            active: schema.active,
            schema_version,
            _creation_disallowed: (),
        })
    }
}

impl std::fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[sgd{}]", self.deployment, self.id)
    }
}

impl From<&Site> for DeploymentLocator {
    fn from(site: &Site) -> Self {
        DeploymentLocator::new(site.id.into(), site.deployment.clone())
    }
}

/// This is only used for tests to allow them to create a `Site` that does
/// not originate in the database
#[cfg(debug_assertions)]
pub fn make_dummy_site(deployment: DeploymentHash, namespace: Namespace, network: String) -> Site {
    Site {
        id: DeploymentId(-7),
        deployment,
        shard: PRIMARY_SHARD.clone(),
        namespace,
        network,
        active: true,
        schema_version: DeploymentSchemaVersion::V0,
        _creation_disallowed: (),
    }
}

/// Queries that we need for both the `Connection` and the `Mirror`. Since
/// they will also be used by `Mirror`, they can only use tables that are
/// mirrored through `Mirror::refresh_tables` and must be queries, i.e.,
/// read-only
mod queries {
    use diesel::data_types::PgTimestamp;
    use diesel::dsl::{exists, sql};
    use diesel::prelude::{
        BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
        OptionalExtension, QueryDsl,
    };
    use diesel::sql_types::Text;
    use diesel_async::RunQueryDsl;
    use graph::prelude::NodeId;
    use graph::{
        components::store::DeploymentId as GraphDeploymentId,
        data::subgraph::status,
        internal_error,
        prelude::{DeploymentHash, StoreError, SubgraphName},
    };
    use std::{collections::HashMap, convert::TryFrom, convert::TryInto};

    use crate::{AsyncPgConnection, Shard};

    use super::{DeploymentId, Schema, Site};

    // These are the only tables that functions in this module may use. If
    // additional tables are needed, they need to be set up for mirroring
    // first
    use super::deployment_schemas as ds;
    use super::subgraph as s;
    use super::subgraph_deployment_assignment as a;
    use super::subgraph_version as v;

    pub(super) async fn find_active_site(
        conn: &mut AsyncPgConnection,
        subgraph: &DeploymentHash,
    ) -> Result<Option<Site>, StoreError> {
        let schema = ds::table
            .filter(ds::subgraph.eq(subgraph.to_string()))
            .filter(ds::active.eq(true))
            .first::<Schema>(conn)
            .await
            .optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub(super) async fn find_site_by_ref(
        conn: &mut AsyncPgConnection,
        id: DeploymentId,
    ) -> Result<Option<Site>, StoreError> {
        let schema = ds::table.find(id).first::<Schema>(conn).await.optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub(super) async fn subgraph_exists(
        conn: &mut AsyncPgConnection,
        name: &SubgraphName,
    ) -> Result<bool, StoreError> {
        Ok(
            diesel::select(exists(s::table.filter(s::name.eq(name.as_str()))))
                .get_result::<bool>(conn)
                .await?,
        )
    }

    pub(super) async fn current_deployment_for_subgraph(
        conn: &mut AsyncPgConnection,
        name: &SubgraphName,
    ) -> Result<DeploymentHash, StoreError> {
        let id = v::table
            .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
            .filter(s::name.eq(name.as_str()))
            .select(v::deployment)
            .first::<String>(conn)
            .await
            .optional()?;
        match id {
            Some(id) => DeploymentHash::new(id)
                .map_err(|id| internal_error!("illegal deployment id: {}", id)),
            None => Err(StoreError::DeploymentNotFound(name.to_string())),
        }
    }

    pub(super) async fn deployments_for_subgraph(
        conn: &mut AsyncPgConnection,
        name: &str,
    ) -> Result<Vec<Site>, StoreError> {
        ds::table
            .inner_join(v::table.on(ds::subgraph.eq(v::deployment)))
            .inner_join(s::table.on(v::subgraph.eq(s::id)))
            .filter(s::name.eq(name))
            .filter(ds::active)
            .order_by(v::created_at.asc())
            .select(ds::all_columns)
            .load::<Schema>(conn)
            .await?
            .into_iter()
            .map(Site::try_from)
            .collect::<Result<Vec<Site>, _>>()
    }

    pub(super) async fn subgraph_version(
        conn: &mut AsyncPgConnection,
        name: &str,
        use_current: bool,
    ) -> Result<Option<Site>, StoreError> {
        let deployment = if use_current {
            ds::table
                .select(ds::all_columns)
                .inner_join(v::table.on(ds::subgraph.eq(v::deployment)))
                .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
                .filter(s::name.eq(&name))
                .filter(ds::active)
                .first::<Schema>(conn)
                .await
        } else {
            ds::table
                .select(ds::all_columns)
                .inner_join(v::table.on(ds::subgraph.eq(v::deployment)))
                .inner_join(s::table.on(s::pending_version.eq(v::id.nullable())))
                .filter(s::name.eq(&name))
                .filter(ds::active)
                .first::<Schema>(conn)
                .await
        };
        deployment.optional()?.map(Site::try_from).transpose()
    }

    /// Find sites by their subgraph deployment hashes. If `ids` is empty,
    /// return all sites
    pub(super) async fn find_sites(
        conn: &mut AsyncPgConnection,
        ids: &[String],
        only_active: bool,
    ) -> Result<Vec<Site>, StoreError> {
        let schemas = if ids.is_empty() {
            if only_active {
                ds::table.filter(ds::active).load::<Schema>(conn).await?
            } else {
                ds::table.load::<Schema>(conn).await?
            }
        } else if only_active {
            ds::table
                .filter(ds::active)
                .filter(ds::subgraph.eq_any(ids))
                .load::<Schema>(conn)
                .await?
        } else {
            ds::table
                .filter(ds::subgraph.eq_any(ids))
                .load::<Schema>(conn)
                .await?
        };
        schemas
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
    }

    /// Find sites by their subgraph deployment ids. If `ids` is empty,
    /// return no sites
    pub(super) async fn find_sites_by_id(
        conn: &mut AsyncPgConnection,
        ids: &[DeploymentId],
    ) -> Result<Vec<Site>, StoreError> {
        let schemas = ds::table
            .filter(ds::id.eq_any(ids))
            .load::<Schema>(conn)
            .await?;
        schemas
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
    }

    pub(super) async fn find_site_in_shard(
        conn: &mut AsyncPgConnection,
        subgraph: &DeploymentHash,
        shard: &Shard,
    ) -> Result<Option<Site>, StoreError> {
        let schema = ds::table
            .filter(ds::subgraph.eq(subgraph.as_str()))
            .filter(ds::shard.eq(shard.as_str()))
            .first::<Schema>(conn)
            .await
            .optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub(super) async fn assignments(
        conn: &mut AsyncPgConnection,
        node: &NodeId,
    ) -> Result<Vec<Site>, StoreError> {
        ds::table
            .inner_join(a::table.on(a::id.eq(ds::id)))
            .filter(a::node_id.eq(node.as_str()))
            .select(ds::all_columns)
            .load::<Schema>(conn)
            .await?
            .into_iter()
            .map(Site::try_from)
            .collect::<Result<Vec<Site>, _>>()
    }

    // All assignments for a node that are currently not paused
    pub(super) async fn active_assignments(
        conn: &mut AsyncPgConnection,
        node: &NodeId,
    ) -> Result<Vec<Site>, StoreError> {
        ds::table
            .inner_join(a::table.on(a::id.eq(ds::id)))
            .filter(a::node_id.eq(node.as_str()))
            .filter(a::paused_at.is_null())
            .select(ds::all_columns)
            .load::<Schema>(conn)
            .await?
            .into_iter()
            .map(Site::try_from)
            .collect::<Result<Vec<Site>, _>>()
    }

    pub(super) async fn fill_assignments(
        conn: &mut AsyncPgConnection,
        infos: &[status::Info],
    ) -> Result<HashMap<GraphDeploymentId, (String, bool)>, StoreError> {
        let ids: Vec<_> = infos.iter().map(|info| &info.id).collect();
        let nodes: HashMap<_, _> = a::table
            .inner_join(ds::table.on(ds::id.eq(a::id)))
            .filter(ds::id.eq_any(ids))
            .select((ds::id, a::node_id, a::paused_at.is_not_null()))
            .load::<(GraphDeploymentId, String, bool)>(conn)
            .await?
            .into_iter()
            .map(|(id, node, paused)| (id, (node, paused)))
            .collect();
        Ok(nodes)
    }

    pub(super) async fn assigned_node(
        conn: &mut AsyncPgConnection,
        site: &Site,
    ) -> Result<Option<NodeId>, StoreError> {
        a::table
            .filter(a::id.eq(site.id))
            .select(a::node_id)
            .first::<String>(conn)
            .await
            .optional()?
            .map(|node| {
                NodeId::new(node).map_err(|node| {
                    internal_error!(
                        "invalid node id `{}` in assignment for `{}`",
                        node,
                        site.deployment
                    )
                })
            })
            .transpose()
    }

    /// Returns Option<(node_id,is_paused)> where `node_id` is the node that
    /// the subgraph is assigned to, and `is_paused` is true if the
    /// subgraph is paused.
    /// Returns None if the deployment does not exist.
    pub(super) async fn assignment_status(
        conn: &mut AsyncPgConnection,
        site: &Site,
    ) -> Result<Option<(NodeId, bool)>, StoreError> {
        a::table
            .filter(a::id.eq(site.id))
            .select((a::node_id, a::paused_at))
            .first::<(String, Option<PgTimestamp>)>(conn)
            .await
            .optional()?
            .map(|(node, ts)| {
                let node_id = NodeId::new(node).map_err(|node| {
                    internal_error!(
                        "invalid node id `{}` in assignment for `{}`",
                        node,
                        site.deployment
                    )
                })?;

                match ts {
                    Some(_) => Ok((node_id, true)),
                    None => Ok((node_id, false)),
                }
            })
            .transpose()
    }

    pub(super) async fn version_info(
        conn: &mut AsyncPgConnection,
        version: &str,
    ) -> Result<Option<(String, String)>, StoreError> {
        Ok(v::table
            .select((v::deployment, sql::<Text>("created_at::text")))
            .filter(v::id.eq(version))
            .first::<(String, String)>(conn)
            .await
            .optional()?)
    }

    pub(super) async fn versions_for_subgraph_id(
        conn: &mut AsyncPgConnection,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        Ok(s::table
            .select((s::current_version.nullable(), s::pending_version.nullable()))
            .filter(s::id.eq(subgraph_id))
            .first::<(Option<String>, Option<String>)>(conn)
            .await
            .optional()?
            .unwrap_or((None, None)))
    }

    /// Returns all (subgraph_name, version) pairs for a given deployment hash.
    pub async fn subgraphs_by_deployment_hash(
        conn: &mut AsyncPgConnection,
        deployment_hash: &str,
    ) -> Result<Vec<(String, String)>, StoreError> {
        v::table
                .inner_join(
                    s::table.on(v::id
                        .nullable()
                        .eq(s::current_version)
                        .or(v::id.nullable().eq(s::pending_version))),
                )
                .filter(v::deployment.eq(&deployment_hash))
                .select((
                    s::name,
                    sql::<Text>(
                        "(case when subgraphs.subgraph.pending_version = subgraphs.subgraph_version.id then 'pending'
                               when subgraphs.subgraph.current_version = subgraphs.subgraph_version.id then 'current'
                               else 'unused'
                         end) as version",
                    ),
                ))
                .get_results(conn)
                .await
                .map_err(Into::into)
    }
}

/// How to handle conflicts when restoring a deployment.
/// Constructed from CLI flags --replace, --add, --force.
pub enum RestoreMode {
    /// No conflict flags given — error if deployment exists anywhere
    Default,
    /// --replace: drop and recreate in the target shard
    Replace,
    /// --add: create a copy in a shard that doesn't have the deployment
    Add,
    /// --force: restore no matter what (replace if in shard, add if not)
    Force,
}

/// The action `plan_restore` decided on based on `RestoreMode` and current
/// state.
pub enum RestoreAction {
    /// Create a new site (active=true if fresh, active=false if copy
    /// exists elsewhere)
    Create { active: bool },
    /// Drop existing site in target shard, then recreate
    Replace { existing: Site },
}

/// A wrapper for a database connection that provides access to functionality
/// that works only on the primary database
pub struct Connection {
    conn: PermittedConnection,
}

impl Connection {
    pub fn new(conn: PermittedConnection) -> Self {
        Self { conn }
    }

    /// Run an async `callback` inside a database transaction on the
    /// connection that `self` contains. If `callback` returns `Ok(T)`, the
    /// transaction is committed and `Ok(T)` is returned. If `callback`
    /// returns `Err(E)`, the transaction is rolled back and `Err(E)` is
    /// returned. If committing or rolling back the transaction fails,
    /// return an error
    pub(crate) fn transaction<'a, 'conn, R, F>(
        &'conn mut self,
        callback: F,
    ) -> BoxFuture<'conn, Result<R, StoreError>>
    where
        F: for<'r> FnOnce(&'r mut Self) -> ScopedBoxFuture<'a, 'r, Result<R, StoreError>>
            + Send
            + 'a,
        R: Send + 'a,
        'a: 'conn,
    {
        type TM = <AsyncPgConnection as diesel_async::AsyncConnection>::TransactionManager;

        async move {
            TM::begin_transaction(&mut *self.conn).await?;
            match callback(self).await {
                Ok(value) => {
                    TM::commit_transaction(&mut *self.conn).await?;
                    Ok(value)
                }
                Err(user_error) => match TM::rollback_transaction(&mut *self.conn).await {
                    Ok(()) => Err(user_error),
                    Err(diesel::result::Error::BrokenTransactionManager) => {
                        // In this case we are probably more interested by the
                        // original error, which likely caused this
                        Err(user_error)
                    }
                    Err(rollback_error) => Err(rollback_error.into()),
                },
            }
        }
        .boxed()
    }

    /// Signal any copy process that might be copying into one of these
    /// deployments that it should stop. Copying is cancelled whenever we
    /// remove the assignment for a deployment
    async fn cancel_copies(&mut self, ids: Vec<DeploymentId>) -> Result<(), StoreError> {
        use active_copies as ac;

        update(ac::table.filter(ac::dst.eq_any(ids)))
            .set(ac::cancelled_at.eq(sql("now()")))
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Delete all assignments for deployments that are neither the current nor the
    /// pending version of a subgraph and return the deployment id's
    async fn remove_unused_assignments(&mut self) -> Result<Vec<AssignmentChange>, StoreError> {
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;

        let conn = &mut self.conn;
        let named = v::table
            .inner_join(
                s::table.on(v::id
                    .nullable()
                    .eq(s::current_version)
                    .or(v::id.nullable().eq(s::pending_version))),
            )
            .inner_join(ds::table.on(v::deployment.eq(ds::subgraph)))
            .filter(a::id.eq(ds::id))
            .select(ds::id);

        let removed = delete(a::table.filter(not(exists(named))))
            .returning(a::id)
            .load::<i32>(conn)
            .await?;

        let removed: Vec<_> = ds::table
            .filter(ds::id.eq_any(removed))
            .select((ds::id, ds::subgraph))
            .load::<(DeploymentId, String)>(conn)
            .await?
            .into_iter()
            .collect();

        // Stop ongoing copies
        let removed_ids: Vec<_> = removed.iter().map(|(id, _)| *id).collect();
        self.cancel_copies(removed_ids).await?;

        let events = removed
            .into_iter()
            .map(|(id, hash)| {
                DeploymentHash::new(hash)
                    .map(|hash| AssignmentChange::removed(DeploymentLocator::new(id.into(), hash)))
                    .map_err(|id| {
                        StoreError::InternalError(format!(
                            "invalid id `{}` for deployment assignment",
                            id
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events)
    }

    /// Promote the deployment `id` to the current version everywhere where it was
    /// the pending version so far, and remove any assignments that are not needed
    /// any longer as a result. Return the changes that were made to assignments
    /// in the process
    pub async fn promote_deployment(
        &mut self,
        id: &DeploymentHash,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let conn = &mut self.conn;

        // Subgraphs where we need to promote the version
        let pending_subgraph_versions: Vec<(String, String)> = s::table
            .inner_join(v::table.on(s::pending_version.eq(v::id.nullable())))
            .filter(v::deployment.eq(id.as_str()))
            .select((s::id, v::id))
            .for_update()
            .load(conn)
            .await?;

        // Switch the pending version to the current version
        for (subgraph, version) in &pending_subgraph_versions {
            update(s::table.filter(s::id.eq(subgraph)))
                .set((
                    s::current_version.eq(version),
                    s::pending_version.eq::<Option<&str>>(None),
                ))
                .execute(conn)
                .await?;
        }

        // Clean up assignments if we could possibly have changed any
        // subgraph versions
        let changes = if pending_subgraph_versions.is_empty() {
            vec![]
        } else {
            self.remove_unused_assignments().await?
        };
        Ok(changes)
    }

    /// Create a new subgraph with the given name. If one already exists, use
    /// the existing one. Return the `id` of the newly created or existing
    /// subgraph
    pub async fn create_subgraph(&mut self, name: &SubgraphName) -> Result<String, StoreError> {
        use subgraph as s;

        let conn = &mut self.conn;
        let id = generate_entity_id();
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let inserted = insert_into(s::table)
            .values((
                s::id.eq(&id),
                s::name.eq(name.as_str()),
                // using BigDecimal::from(created_at) produced a scale error
                s::created_at.eq(sql(&format!("{}", created_at))),
                s::block_range.eq(UNVERSIONED_RANGE),
            ))
            .on_conflict(s::name)
            .do_nothing()
            .execute(conn)
            .await?;
        if inserted == 0 {
            let existing_id = s::table
                .filter(s::name.eq(name.as_str()))
                .select(s::id)
                .first::<String>(conn)
                .await?;
            Ok(existing_id)
        } else {
            Ok(id)
        }
    }

    pub async fn create_subgraph_version<F>(
        &mut self,
        name: SubgraphName,
        site: &Site,
        node_id: NodeId,
        mode: SubgraphVersionSwitchingMode,
        exists_and_synced: F,
    ) -> Result<Vec<AssignmentChange>, StoreError>
    where
        F: AsyncFn(&DeploymentHash) -> Result<bool, StoreError>,
    {
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;
        use SubgraphVersionSwitchingMode::*;

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Check the current state of the the subgraph. If no subgraph with the
        // name exists, create one
        let info = s::table
            .left_outer_join(v::table.on(s::current_version.eq(v::id.nullable())))
            .filter(s::name.eq(name.as_str()))
            .select((s::id, v::deployment.nullable()))
            .first::<(String, Option<String>)>(&mut self.conn)
            .await
            .optional()?;
        let (subgraph_id, current_deployment) = match info {
            Some((subgraph_id, current_deployment)) => {
                let current_deployment = current_deployment
                    .map(|d| DeploymentHash::new(d).map_err(StoreError::DeploymentNotFound))
                    .transpose()?;
                (subgraph_id, current_deployment)
            }
            None => (self.create_subgraph(&name).await?, None),
        };
        let pending_deployment = s::table
            .left_outer_join(v::table.on(s::pending_version.eq(v::id.nullable())))
            .filter(s::id.eq(&subgraph_id))
            .select(v::deployment.nullable())
            .first::<Option<String>>(&mut self.conn)
            .await?;

        // See if the current version of that subgraph is synced. If the subgraph
        // has no current version, we treat it the same as if it were not synced
        // The `optional` below only comes into play if data is corrupted/missing;
        // ignoring that via `optional` makes it possible to fix a missing version
        // or deployment by deploying over it.
        let current_exists_and_synced = match current_deployment {
            None => false,
            Some(ref id) => exists_and_synced(id).await?,
        };

        // Check if we even need to make any changes
        let change_needed = match (mode, current_exists_and_synced) {
            (Instant, _) | (Synced, false) => current_deployment.as_ref() != Some(&site.deployment),
            (Synced, true) => pending_deployment.as_deref() != Some(site.deployment.as_str()),
        };
        if !change_needed {
            return Ok(vec![]);
        }

        // Create the actual subgraph version
        let version_id = generate_entity_id();
        insert_into(v::table)
            .values((
                v::id.eq(&version_id),
                v::subgraph.eq(&subgraph_id),
                v::deployment.eq(site.deployment.as_str()),
                // using BigDecimal::from(created_at) produced a scale error
                v::created_at.eq(sql(&format!("{}", created_at))),
                v::block_range.eq(UNVERSIONED_RANGE),
            ))
            .execute(&mut self.conn)
            .await?;

        // Create a subgraph assignment if there isn't one already
        let new_assignment = a::table
            .filter(a::id.eq(site.id))
            .select(a::id)
            .first::<i32>(&mut self.conn)
            .await
            .optional()?
            .is_none();
        if new_assignment {
            insert_into(a::table)
                .values((a::id.eq(site.id), a::node_id.eq(node_id.as_str())))
                .execute(&mut self.conn)
                .await?;
        }

        // See if we should make this the current or pending version
        let subgraph_row = update(s::table.filter(s::id.eq(&subgraph_id)));
        // When the new deployment is also synced already, we always want to
        // overwrite the current version
        let new_exists_and_synced = exists_and_synced(&site.deployment).await?;
        match (mode, current_exists_and_synced, new_exists_and_synced) {
            (Instant, _, _) | (Synced, false, _) | (Synced, true, true) => {
                subgraph_row
                    .set((
                        s::current_version.eq(&version_id),
                        s::pending_version.eq::<Option<&str>>(None),
                    ))
                    .execute(&mut self.conn)
                    .await?;
            }
            (Synced, true, false) => {
                subgraph_row
                    .set(s::pending_version.eq(&version_id))
                    .execute(&mut self.conn)
                    .await?;
            }
        }

        // Clean up any assignments we might have displaced
        let mut changes = self.remove_unused_assignments().await?;
        if new_assignment {
            let change = AssignmentChange::set(site.into());
            changes.push(change);
        }
        Ok(changes)
    }

    pub async fn remove_subgraph(
        &mut self,
        name: SubgraphName,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let conn = &mut self.conn;

        // Get the id of the given subgraph. If no subgraph with the
        // name exists, there is nothing to do
        let subgraph: Option<String> = s::table
            .filter(s::name.eq(name.as_str()))
            .select(s::id)
            .first(conn)
            .await
            .optional()?;
        if let Some(subgraph) = subgraph {
            delete(v::table.filter(v::subgraph.eq(&subgraph)))
                .execute(conn)
                .await?;
            delete(s::table.filter(s::id.eq(subgraph)))
                .execute(conn)
                .await?;
            self.remove_unused_assignments().await
        } else {
            Ok(vec![])
        }
    }

    pub async fn pause_subgraph(
        &mut self,
        site: &Site,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = &mut self.conn;

        let updates = update(a::table.filter(a::id.eq(site.id)))
            .set(a::paused_at.eq(sql("now()")))
            .execute(conn)
            .await?;
        match updates {
            0 => Err(StoreError::DeploymentNotFound(site.deployment.to_string())),
            1 => {
                let change = AssignmentChange::removed(site.into());
                Ok(vec![change])
            }
            _ => {
                // `id` is the primary key of the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    pub async fn resume_subgraph(
        &mut self,
        site: &Site,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = &mut self.conn;

        let updates = update(a::table.filter(a::id.eq(site.id)))
            .set(a::paused_at.eq(sql("null")))
            .execute(conn)
            .await?;
        match updates {
            0 => Err(StoreError::DeploymentNotFound(site.deployment.to_string())),
            1 => {
                let change = AssignmentChange::set(site.into());
                Ok(vec![change])
            }
            _ => {
                // `id` is the primary key of the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    pub async fn reassign_subgraph(
        &mut self,
        site: &Site,
        node: &NodeId,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = &mut self.conn;
        let updates = update(a::table.filter(a::id.eq(site.id)))
            .set(a::node_id.eq(node.as_str()))
            .execute(conn)
            .await?;
        match updates {
            0 => Err(StoreError::DeploymentNotFound(site.deployment.to_string())),
            1 => {
                let change = AssignmentChange::set(site.into());
                Ok(vec![change])
            }
            _ => {
                // `id` is the primary key of the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    pub async fn get_subgraph_features(
        &mut self,
        id: String,
    ) -> Result<Option<DeploymentFeatures>, StoreError> {
        use subgraph_features as f;

        let conn = &mut self.conn;
        let features = f::table
            .filter(f::id.eq(id))
            .select((
                f::id,
                f::spec_version,
                f::api_version,
                f::features,
                f::data_sources,
                f::handlers,
                f::network,
                f::has_declared_calls,
                f::has_bytes_as_ids,
                f::has_aggregations,
                f::immutable_entities,
            ))
            .first::<(
                String,
                String,
                Option<String>,
                Vec<String>,
                Vec<String>,
                Vec<String>,
                String,
                bool,
                bool,
                bool,
                Vec<String>,
            )>(conn)
            .await
            .optional()?;

        let features = features.map(
            |(
                id,
                spec_version,
                api_version,
                features,
                data_sources,
                handlers,
                network,
                has_declared_calls,
                has_bytes_as_ids,
                has_aggregations,
                immutable_entities,
            )| {
                DeploymentFeatures {
                    id,
                    spec_version,
                    api_version,
                    features,
                    data_source_kinds: data_sources,
                    handler_kinds: handlers,
                    network,
                    has_declared_calls,
                    has_bytes_as_ids,
                    has_aggregations,
                    immutable_entities,
                }
            },
        );

        Ok(features)
    }

    pub async fn create_subgraph_features(
        &mut self,
        features: DeploymentFeatures,
    ) -> Result<(), StoreError> {
        use subgraph_features as f;

        let DeploymentFeatures {
            id,
            spec_version,
            api_version,
            features,
            data_source_kinds,
            handler_kinds,
            network,
            has_declared_calls,
            has_bytes_as_ids,
            immutable_entities,
            has_aggregations,
        } = features;

        let conn = &mut self.conn;
        let changes = (
            f::id.eq(id),
            f::spec_version.eq(spec_version),
            f::api_version.eq(api_version),
            f::features.eq(features),
            f::data_sources.eq(data_source_kinds),
            f::handlers.eq(handler_kinds),
            f::network.eq(network),
            f::has_declared_calls.eq(has_declared_calls),
            f::has_bytes_as_ids.eq(has_bytes_as_ids),
            f::immutable_entities.eq(immutable_entities),
            f::has_aggregations.eq(has_aggregations),
        );

        insert_into(f::table)
            .values(changes.clone())
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn assign_subgraph(
        &mut self,
        site: &Site,
        node: &NodeId,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = &mut self.conn;
        insert_into(a::table)
            .values((a::id.eq(site.id), a::node_id.eq(node.as_str())))
            .execute(conn)
            .await?;

        let change = AssignmentChange::set(site.into());
        Ok(vec![change])
    }

    pub async fn unassign_subgraph(
        &mut self,
        site: &Site,
    ) -> Result<Vec<AssignmentChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = &mut self.conn;
        let delete_count = delete(a::table.filter(a::id.eq(site.id)))
            .execute(conn)
            .await?;

        self.cancel_copies(vec![site.id]).await?;

        match delete_count {
            0 => Ok(vec![]),
            1 => {
                let change = AssignmentChange::removed(site.into());
                Ok(vec![change])
            }
            _ => {
                // `id` is the unique in the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    /// Create a new site and possibly set it to the active site. This
    /// function only performs the basic operations for creation, and the
    /// caller must check that other conditions (like whether there already
    /// is an active site for the deployment) are met
    pub(crate) async fn create_site(
        &mut self,
        shard: Shard,
        deployment: DeploymentHash,
        network: String,
        schema_version: DeploymentSchemaVersion,
        active: bool,
    ) -> Result<Site, StoreError> {
        use deployment_schemas as ds;

        let conn = &mut self.conn;

        let schemas: Vec<(DeploymentId, String)> = diesel::insert_into(ds::table)
            .values((
                ds::subgraph.eq(deployment.as_str()),
                ds::shard.eq(shard.as_str()),
                ds::version.eq(schema_version as i32),
                ds::network.eq(network.as_str()),
                ds::active.eq(active),
            ))
            .returning((ds::id, ds::name))
            .get_results(conn)
            .await?;
        let (id, namespace) = schemas
            .as_slice()
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("failed to read schema name for {} back", deployment))?;
        let namespace = Namespace::new(namespace).map_err(|name| {
            internal_error!("Generated database schema name {} is invalid", name)
        })?;

        Ok(Site {
            id,
            deployment,
            shard,
            namespace,
            network,
            active,
            schema_version,
            _creation_disallowed: (),
        })
    }

    /// Create a site for a brand new deployment.
    /// If it already exists, return the existing site
    /// and a boolean indicating whether a new site was created.
    /// `false` means the site already existed.
    pub async fn allocate_site(
        &mut self,
        shard: Shard,
        subgraph: &DeploymentHash,
        network: String,
        graft_base: Option<&DeploymentHash>,
    ) -> Result<(Site, bool), StoreError> {
        let conn = &mut self.conn;
        if let Some(site) = queries::find_active_site(conn, subgraph).await? {
            return Ok((site, false));
        }

        let site_was_created = true;
        let schema_version = match graft_base {
            Some(graft_base) => {
                let site = queries::find_active_site(conn, graft_base).await?;
                site.map(|site| site.schema_version).ok_or_else(|| {
                    StoreError::DeploymentNotFound("graft_base not found".to_string())
                })
            }
            None => Ok(DeploymentSchemaVersion::LATEST),
        }?;

        self.create_site(shard, subgraph.clone(), network, schema_version, true)
            .await
            .map(|site| (site, site_was_created))
    }

    pub async fn find_active_site(
        &mut self,
        subgraph: &DeploymentHash,
    ) -> Result<Option<Site>, StoreError> {
        queries::find_active_site(&mut self.conn, subgraph).await
    }

    pub async fn assigned_node(&mut self, site: &Site) -> Result<Option<NodeId>, StoreError> {
        queries::assigned_node(&mut self.conn, site).await
    }

    /// Returns Option<(node_id,is_paused)> where `node_id` is the node that
    /// the subgraph is assigned to, and `is_paused` is true if the
    /// subgraph is paused.
    /// Returns None if the deployment does not exist.
    pub async fn assignment_status(
        &mut self,
        site: &Site,
    ) -> Result<Option<(NodeId, bool)>, StoreError> {
        queries::assignment_status(&mut self.conn, site).await
    }

    /// Create a copy of the site `src` in the shard `shard`, but mark it as
    /// not active. If there already is a site in `shard`, return that
    /// instead.
    pub async fn copy_site(&mut self, src: &Site, shard: Shard) -> Result<Site, StoreError> {
        if let Some(site) =
            queries::find_site_in_shard(&mut self.conn, &src.deployment, &shard).await?
        {
            return Ok(site);
        }

        self.create_site(
            shard,
            src.deployment.clone(),
            src.network.clone(),
            src.schema_version,
            false,
        )
        .await
    }

    pub(crate) async fn activate(
        &mut self,
        deployment: &DeploymentLocator,
    ) -> Result<(), StoreError> {
        use deployment_schemas as ds;
        let conn = &mut self.conn;

        // We need to tread lightly so we do not violate the unique constraint on
        // `subgraph where active`
        update(ds::table.filter(ds::subgraph.eq(deployment.hash.as_str())))
            .set(ds::active.eq(false))
            .execute(conn)
            .await?;

        update(ds::table.filter(ds::id.eq(DeploymentId::from(deployment.id))))
            .set(ds::active.eq(true))
            .execute(conn)
            .await
            .map_err(|e| e.into())
            .map(|_| ())
    }

    /// Remove all subgraph versions, the entry in `deployment_schemas` and the entry in
    /// `subgraph_features` for subgraph `id` in a transaction
    pub async fn drop_site(&mut self, site: &Site) -> Result<(), StoreError> {
        use deployment_schemas as ds;
        use subgraph_features as f;
        use subgraph_version as v;
        use unused_deployments as u;

        self.transaction(|pconn| {
            async {
                let conn = &mut pconn.conn;

                delete(ds::table.filter(ds::id.eq(site.id)))
                    .execute(conn)
                    .await?;

                // If there is no site for this deployment any more, we can get
                // rid of versions pointing to it
                let exists = select(exists(
                    ds::table.filter(ds::subgraph.eq(site.deployment.as_str())),
                ))
                .get_result::<bool>(conn)
                .await?;
                if !exists {
                    delete(v::table.filter(v::deployment.eq(site.deployment.as_str())))
                        .execute(conn)
                        .await?;

                    // Remove the entry in `subgraph_features`
                    delete(f::table.filter(f::id.eq(site.deployment.as_str())))
                        .execute(conn)
                        .await?;
                }

                update(u::table.filter(u::id.eq(site.id)))
                    .set(u::removed_at.eq(sql("now()")))
                    .execute(conn)
                    .await?;
                Ok(())
            }
            .scope_boxed()
        })
        .await
    }

    /// Determine what action to take when restoring `subgraph` into `shard`
    /// based on the `mode` and the current state of the deployment.
    pub async fn plan_restore(
        &mut self,
        shard: &Shard,
        subgraph: &DeploymentHash,
        mode: &RestoreMode,
    ) -> Result<RestoreAction, StoreError> {
        let conn = &mut self.conn;
        let in_shard = queries::find_site_in_shard(conn, subgraph, shard).await?;
        let active = queries::find_active_site(conn, subgraph).await?;

        match (in_shard, active, mode) {
            // Deployment exists in target shard
            (Some(existing), _, RestoreMode::Replace | RestoreMode::Force) => {
                Ok(RestoreAction::Replace { existing })
            }
            (Some(_), _, RestoreMode::Default | RestoreMode::Add) => {
                Err(StoreError::Input(format!(
                    "deployment {} already exists in shard {}; use --replace or --force",
                    subgraph,
                    shard.as_str()
                )))
            }
            // Deployment does not exist in target shard but exists elsewhere
            (None, Some(ref active_site), RestoreMode::Add | RestoreMode::Force) => {
                let _ = active_site;
                Ok(RestoreAction::Create { active: false })
            }
            (None, Some(active_site), RestoreMode::Default) => Err(StoreError::Input(format!(
                "deployment {} already exists in shard {}; use --add --shard {} or --force",
                subgraph,
                active_site.shard.as_str(),
                shard.as_str()
            ))),
            (None, Some(_), RestoreMode::Replace) => Err(StoreError::Input(format!(
                "deployment {} is not in shard {}; nothing to replace",
                subgraph,
                shard.as_str()
            ))),
            // Deployment does not exist anywhere
            (None, None, RestoreMode::Replace) => Err(StoreError::Input(format!(
                "deployment {} does not exist; nothing to replace",
                subgraph
            ))),
            (None, None, _) => Ok(RestoreAction::Create { active: true }),
        }
    }

    pub async fn locate_site(
        &mut self,
        locator: DeploymentLocator,
    ) -> Result<Option<Site>, StoreError> {
        let schema = deployment_schemas::table
            .filter(deployment_schemas::id.eq::<DeploymentId>(locator.into()))
            .first::<Schema>(&mut self.conn)
            .await
            .optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub async fn find_sites_for_network(&mut self, network: &str) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;

        ds::table
            .filter(ds::network.eq(network))
            .load::<Schema>(&mut self.conn)
            .await?
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
    }

    pub async fn sites(&mut self) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;

        ds::table
            .filter(ds::name.ne("subgraphs"))
            .load::<Schema>(&mut self.conn)
            .await?
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
    }

    pub async fn send_store_event(
        &mut self,
        sender: &NotificationSender,
        event: &StoreEvent,
    ) -> Result<(), StoreError> {
        // Performance: Don't bog down the db with many empty changelists.
        if event.changes.is_empty() {
            return Ok(());
        }
        let v = serde_json::to_value(event)?;
        #[cfg(debug_assertions)]
        {
            if *EVENT_TAP_ENABLED.lock().unwrap() {
                EVENT_TAP.lock().unwrap().push(event.clone());
            }
        }
        sender
            .notify(&mut self.conn, "store_events", None, &v)
            .await
    }

    /// Return the name of the node that has the fewest assignments out of the
    /// given `nodes`. If `nodes` is empty, return `None`
    pub async fn least_assigned_node(
        &mut self,
        nodes: &[NodeId],
    ) -> Result<Option<NodeId>, StoreError> {
        use subgraph_deployment_assignment as a;

        let nodes: Vec<_> = nodes.iter().map(|n| n.as_str()).collect();

        let conn = &mut self.conn;

        let assigned = a::table
            .filter(a::node_id.eq_any(&nodes))
            .select((a::node_id, sql::<BigInt>("count(*)")))
            .group_by(a::node_id)
            .order_by(sql::<BigInt>("count(*)"))
            .load::<(String, i64)>(conn)
            .await?;

        // Any nodes without assignments will be missing from `assigned`
        let missing = nodes
            .into_iter()
            .filter(|node| !assigned.iter().any(|(a, _)| a == node))
            .map(|node| (node, 0));

        assigned
            .iter()
            .map(|(node, count)| (node.as_str(), *count))
            .chain(missing)
            .min_by_key(|(_, count)| *count)
            .map(|(node, _)| NodeId::new(node))
            .transpose()
            // This can't really happen since we filtered by valid NodeId's
            .map_err(|node| {
                internal_error!("database has assignment for illegal node name {:?}", node)
            })
    }

    /// Return the shard that has the fewest deployments out of the given
    /// `shards`. If `shards` is empty, return `None`
    ///
    /// Usage of a shard is taken to be the number of assigned deployments
    /// that are stored in it. Unassigned deployments are ignored; in
    /// particular, that ignores deployments that are going to be removed
    /// soon.
    pub async fn least_used_shard(
        &mut self,
        shards: &[Shard],
    ) -> Result<Option<Shard>, StoreError> {
        use deployment_schemas as ds;
        use subgraph_deployment_assignment as a;

        let conn = &mut self.conn;

        let used = ds::table
            .inner_join(a::table.on(a::id.eq(ds::id)))
            .filter(ds::shard.eq_any(shards))
            .select((ds::shard, sql::<BigInt>("count(*)")))
            .group_by(ds::shard)
            .order_by(sql::<BigInt>("count(*)"))
            .load::<(String, i64)>(conn)
            .await?;

        // Any shards that have no deployments in them will not be in
        // 'used'; add them in with a count of 0
        let missing = shards
            .iter()
            .filter(|shard| !used.iter().any(|(s, _)| s == shard.as_str()))
            .map(|shard| (shard.as_str(), 0));

        used.iter()
            .map(|(shard, count)| (shard.as_str(), *count))
            .chain(missing)
            .min_by_key(|(_, count)| *count)
            .map(|(shard, _)| Shard::new(shard.to_string()))
            .transpose()
            // This can't really happen since we filtered by valid shards
            .map_err(|e| internal_error!("database has illegal shard name: {}", e))
    }

    #[cfg(debug_assertions)]
    pub async fn versions_for_subgraph(
        &mut self,
        name: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        use subgraph as s;

        Ok(s::table
            .select((s::current_version.nullable(), s::pending_version.nullable()))
            .filter(s::name.eq(&name))
            .first::<(Option<String>, Option<String>)>(&mut self.conn)
            .await
            .optional()?
            .unwrap_or((None, None)))
    }

    #[cfg(debug_assertions)]
    pub async fn deployment_for_version(
        &mut self,
        name: &str,
    ) -> Result<Option<String>, StoreError> {
        use subgraph_version as v;

        Ok(v::table
            .select(v::deployment)
            .filter(v::id.eq(name))
            .first::<String>(&mut self.conn)
            .await
            .optional()?)
    }

    /// Find all deployments that are not in use and add them to the
    /// `unused_deployments` table. Only values that are available in the
    /// primary will be filled in `unused_deployments`
    pub async fn detect_unused_deployments(&mut self) -> Result<Vec<Site>, StoreError> {
        use active_copies as cp;
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;
        use unused_deployments as u;

        let conn = &mut self.conn;
        // Deployment is assigned
        let assigned = a::table.filter(a::id.eq(ds::id));
        // Deployment is current or pending version
        let current_or_pending = v::table
            .inner_join(
                s::table.on(v::id
                    .nullable()
                    .eq(s::current_version)
                    .or(v::id.nullable().eq(s::pending_version))),
            )
            .filter(v::deployment.eq(ds::subgraph));
        // Deployment is the source of an in-progress copy
        let copy_src = cp::table.filter(cp::src.eq(ds::id));

        // Subgraphs that used a deployment
        let used_by = s::table
            .inner_join(v::table.on(s::id.eq(v::subgraph)))
            .filter(v::deployment.eq(ds::subgraph))
            .select(sql::<Array<Text>>("array_agg(distinct name)"))
            .single_value();

        // A deployment is unused if it fulfills all of these criteria:
        // 1. It is not assigned to a node
        // 2. It is either not marked as active or is neither the current or
        //    pending version of a subgraph. The rest of the system makes
        //    sure that there is always one active copy of a deployment
        // 3. It is not the source of a currently running copy operation
        let unused = ds::table
            .filter(not(exists(assigned)))
            .filter(not(ds::active).or(not(exists(current_or_pending))))
            .filter(not(exists(copy_src)))
            .select((
                ds::id,
                ds::created_at,
                ds::subgraph,
                ds::name,
                ds::shard,
                used_by,
            ));

        let ids = insert_into(u::table)
            .values(unused)
            .into_columns((
                u::id,
                u::created_at,
                u::deployment,
                u::namespace,
                u::shard,
                u::subgraphs,
            ))
            .on_conflict(u::id)
            .do_nothing()
            .returning(u::id)
            .get_results::<DeploymentId>(conn)
            .await?;

        // We need to load again since we do not record the network in
        // unused_deployments
        ds::table
            .filter(ds::id.eq_any(ids))
            .select(ds::all_columns)
            .load::<Schema>(conn)
            .await?
            .into_iter()
            .map(Site::try_from)
            .collect()
    }

    /// Add details from the deployment shard to unused deployments
    pub async fn update_unused_deployments(
        &mut self,
        details: &[DeploymentDetail],
    ) -> Result<(), StoreError> {
        use crate::detail::block;
        use unused_deployments as u;

        for detail in details {
            let (latest_hash, latest_number) = block(
                &detail.subgraph,
                "latest_ethereum_block",
                detail.block_hash.clone(),
                detail.block_number,
            )?
            .map(|b| b.to_ptr())
            .map(|ptr| (Some(Vec::from(ptr.hash_slice())), Some(ptr.number)))
            .unwrap_or((None, None));
            let entity_count = detail.entity_count.to_u64().unwrap_or(0) as i32;

            update(u::table.filter(u::id.eq(&detail.id)))
                .set((
                    u::entity_count.eq(entity_count),
                    u::latest_ethereum_block_hash.eq(latest_hash),
                    u::latest_ethereum_block_number.eq(latest_number),
                    u::failed.eq(detail.failed),
                    u::synced_at.eq(detail.synced_at),
                    u::synced_at_block_number.eq(detail.synced_at_block_number),
                ))
                .execute(&mut self.conn)
                .await?;
        }
        Ok(())
    }

    /// The deployment `site` that we marked as unused previously is in fact
    /// now used again, e.g., because it was redeployed in between recording
    /// it as unused and now. Remove it from the `unused_deployments` table
    pub async fn unused_deployment_is_used(&mut self, site: &Site) -> Result<(), StoreError> {
        use unused_deployments as u;
        delete(u::table.filter(u::id.eq(site.id)))
            .execute(&mut self.conn)
            .await
            .map(|_| ())
            .map_err(StoreError::from)
    }

    pub async fn list_unused_deployments(
        &mut self,
        filter: unused::Filter,
    ) -> Result<Vec<UnusedDeployment>, StoreError> {
        use unused::Filter::*;
        use unused_deployments as u;

        let conn = &mut self.conn;
        match filter {
            All => Ok(u::table.order_by(u::unused_at.desc()).load(conn).await?),
            New => Ok(u::table
                .filter(u::removed_at.is_null())
                .order_by(u::entity_count)
                .load(conn)
                .await?),
            UnusedLongerThan(duration) => {
                let ts = chrono::offset::Local::now()
                    .checked_sub_signed(duration)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!("duration {} is too large", duration))
                    })?;
                Ok(u::table
                    .filter(u::removed_at.is_null())
                    .filter(u::unused_at.lt(ts))
                    .order_by(u::entity_count)
                    .load(conn)
                    .await?)
            }

            Name(name) => Ok(u::table
                .filter(u::subgraphs.is_not_null())
                .filter(
                    sql::<Bool>("ARRAY[")
                        .bind::<Text, _>(name)
                        .sql("] <@ subgraphs"),
                )
                .order_by(u::entity_count)
                .load(conn)
                .await?),

            Hash(hash) => Ok(u::table
                .filter(u::deployment.eq(hash))
                .order_by(u::entity_count)
                .load(conn)
                .await?),

            Deployment(id) => Ok(u::table
                .filter(u::namespace.eq(id))
                .order_by(u::entity_count)
                .load(conn)
                .await?),
        }
    }

    pub async fn subgraphs_using_deployment(
        &mut self,
        site: &Site,
    ) -> Result<Vec<String>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        Ok(s::table
            .inner_join(
                v::table.on(v::id
                    .nullable()
                    .eq(s::current_version)
                    .or(v::id.nullable().eq(s::pending_version))),
            )
            .filter(v::deployment.eq(site.deployment.as_str()))
            .select(s::name)
            .distinct()
            .load(&mut self.conn)
            .await?)
    }

    pub async fn find_ens_name(&mut self, hash: &str) -> Result<Option<String>, StoreError> {
        use ens_names as dsl;

        dsl::table
            .select(dsl::name)
            .find(hash)
            .get_result::<String>(&mut self.conn)
            .await
            .optional()
            .map_err(|e| anyhow!("error looking up ens_name for hash {}: {}", hash, e).into())
    }

    pub async fn is_ens_table_empty(&mut self) -> Result<bool, StoreError> {
        use ens_names as dsl;

        dsl::table
            .select(dsl::name)
            .limit(1)
            .get_result::<String>(&mut self.conn)
            .await
            .optional()
            .map(|r| r.is_none())
            .map_err(|e| anyhow!("error if ens table is empty: {}", e).into())
    }

    pub async fn record_active_copy(&mut self, src: &Site, dst: &Site) -> Result<(), StoreError> {
        use active_copies as cp;

        insert_into(cp::table)
            .values((
                cp::src.eq(src.id),
                cp::dst.eq(dst.id),
                cp::queued_at.eq(sql("now()")),
            ))
            .on_conflict_do_nothing()
            .execute(&mut self.conn)
            .await?;

        Ok(())
    }

    pub async fn copy_finished(&mut self, dst: &Site) -> Result<(), StoreError> {
        use active_copies as cp;

        delete(cp::table.filter(cp::dst.eq(dst.id)))
            .execute(&mut self.conn)
            .await?;

        Ok(())
    }
}

/// A limited interface to query the primary database.
#[derive(Clone, CheapClone)]
pub struct Primary {
    pool: Arc<ConnectionPool>,
}

impl Primary {
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        // This really indicates a programming error
        if pool.shard != *PRIMARY_SHARD {
            panic!("Primary pool must be the primary shard");
        }

        Primary { pool }
    }

    /// Return `true` if the site is the source of a copy operation. The copy
    /// operation might be just queued or in progress already. This method will
    /// block until a fdw connection becomes available.
    pub async fn is_source(&self, site: &Site) -> Result<bool, StoreError> {
        use active_copies as ac;

        let mut conn = self.pool.get_permitted().await?;

        select(diesel::dsl::exists(
            ac::table
                .filter(ac::src.eq(site.id))
                .filter(ac::cancelled_at.is_null()),
        ))
        .get_result::<bool>(&mut conn)
        .await
        .map_err(StoreError::from)
    }

    pub async fn is_copy_cancelled(&self, dst: &Site) -> Result<bool, StoreError> {
        use active_copies as ac;

        let mut conn = self.pool.get_permitted().await?;

        ac::table
            .filter(ac::dst.eq(dst.id))
            .select(ac::cancelled_at.is_not_null())
            .get_result::<bool>(&mut conn)
            .await
            .map_err(StoreError::from)
    }
}

/// Return `true` if we deem this installation to be empty, defined as
/// having no deployments and no subgraph names in the database
pub async fn is_empty(conn: &mut AsyncPgConnection) -> Result<bool, StoreError> {
    use deployment_schemas as ds;
    use subgraph as s;

    let empty = ds::table.count().get_result::<i64>(conn).await? == 0
        && s::table.count().get_result::<i64>(conn).await? == 0;
    Ok(empty)
}

/// A struct that reads from pools in order, trying each pool in turn until
/// a query returns either success or anything but a
/// `Err(StoreError::DatabaseUnavailable)`. This only works for tables that
/// are mirrored through `refresh_tables`
#[derive(Clone, CheapClone)]
pub struct Mirror {
    pools: Arc<Vec<ConnectionPool>>,
}

impl Mirror {
    // The tables that we mirror
    //
    // `chains` needs to be mirrored before `deployment_schemas` because
    // of the fk constraint on `deployment_schemas.network`. We don't
    // care much about mirroring `active_copies` but it has a fk
    // constraint on `deployment_schemas` and is tiny, therefore it's
    // easiest to just mirror it
    pub(crate) const PUBLIC_TABLES: [&str; 3] = ["chains", "deployment_schemas", "active_copies"];
    pub(crate) const SUBGRAPHS_TABLES: [&str; 3] = [
        "subgraph_deployment_assignment",
        "subgraph",
        "subgraph_version",
    ];

    pub fn new(pools: &HashMap<Shard, ConnectionPool>) -> Mirror {
        let primary = pools
            .get(&PRIMARY_SHARD)
            .expect("we always have a primary pool")
            .clone();
        let pools = pools
            .iter()
            .filter(|(shard, _)| *shard != &*PRIMARY_SHARD)
            .fold(vec![primary], |mut pools, (_, pool)| {
                pools.push(pool.clone());
                pools
            });
        let pools = Arc::new(pools);
        Mirror { pools }
    }

    /// Create a mirror that only uses the primary. Such a mirror will not
    /// be able to do anything if the primary is down, and should only be
    /// used for non-critical uses like command line tools
    pub fn primary_only(primary: ConnectionPool) -> Mirror {
        Mirror {
            pools: Arc::new(vec![primary]),
        }
    }

    /// Execute the `callback` with connections from each of our pools in
    /// order until for one of them we get any result other than
    /// `Err(StoreError::DatabaseUnavailable)`. In other words, we try to
    /// execute `callback` against our pools in order until we can be sure
    /// that we talked to a database that is up. The function `callback`
    /// must only access tables that are mirrored through `refresh_tables`
    ///
    /// The function `callback` must not do any blocking work itself
    pub(crate) fn read_async<'a, 's, R, F>(
        &'s self,
        callback: F,
    ) -> BoxFuture<'s, Result<R, StoreError>>
    where
        F: for<'r> Fn(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<R, StoreError>>
            + Send
            + 'a,
        R: Send + 'a,
        'a: 's,
    {
        async move {
            for pool in self.pools.as_ref() {
                let mut conn = match pool.get_permitted().await {
                    Ok(conn) => conn,
                    Err(StoreError::DatabaseUnavailable) => continue,
                    Err(e) => return Err(e),
                };
                match callback(&mut conn).await {
                    Ok(v) => return Ok(v),
                    Err(StoreError::DatabaseUnavailable) => continue,
                    Err(e) => return Err(e),
                }
            }
            Err(StoreError::DatabaseUnavailable)
        }
        .boxed()
    }

    /// Refresh the contents of mirrored tables from the primary (through
    /// the fdw mapping that `ForeignServer` establishes)
    pub(crate) async fn refresh_tables(conn: &mut AsyncPgConnection) -> Result<(), StoreError> {
        async fn run_query(conn: &mut AsyncPgConnection, query: String) -> Result<(), StoreError> {
            conn.batch_execute(&query).await.map_err(StoreError::from)
        }

        async fn copy_table(
            conn: &mut AsyncPgConnection,
            src_nsp: &str,
            dst_nsp: &str,
            table_name: &str,
        ) -> Result<(), StoreError> {
            run_query(
                conn,
                format!(
                    "insert into {dst_nsp}.{table_name} select * from {src_nsp}.{table_name};",
                    src_nsp = src_nsp,
                    dst_nsp = dst_nsp,
                    table_name = table_name
                ),
            )
            .await
        }

        // Truncate all tables at once, otherwise truncation can fail
        // because of foreign key constraints
        let tables = Self::PUBLIC_TABLES
            .iter()
            .map(|name| (NAMESPACE_PUBLIC, name))
            .chain(
                Self::SUBGRAPHS_TABLES
                    .iter()
                    .map(|name| (NAMESPACE_SUBGRAPHS, name)),
            )
            .map(|(nsp, name)| format!("{}.{}", nsp, name))
            .join(", ");
        let query = format!("truncate table {};", tables);
        conn.batch_execute(&query).await?;

        // Repopulate `PUBLIC_TABLES` by copying their data wholesale
        for table_name in Self::PUBLIC_TABLES {
            copy_table(conn, PRIMARY_PUBLIC, NAMESPACE_PUBLIC, table_name).await?;
        }

        // Repopulate `SUBGRAPHS_TABLES` but only copy the data we actually
        // need to respond to queries when the primary is down
        let src_nsp = ForeignServer::metadata_schema(&PRIMARY_SHARD);
        let dst_nsp = NAMESPACE_SUBGRAPHS;

        run_query(
            conn,
            format!(
                "insert into {dst_nsp}.subgraph \
                     select * from {src_nsp}.subgraph
                     where current_version is not null;"
            ),
        )
        .await?;
        run_query(
            conn,
            format!(
                "insert into {dst_nsp}.subgraph_version \
                 select v.* from {src_nsp}.subgraph_version v, {src_nsp}.subgraph s
                  where v.id = s.current_version;"
            ),
        )
        .await?;
        copy_table(conn, &src_nsp, dst_nsp, "subgraph_deployment_assignment").await?;

        Ok(())
    }

    /// Return the actual primary connection pool; all write access to the
    /// primary should go through this pool
    pub(crate) fn primary(&self) -> &ConnectionPool {
        // Will not panic since the constructor ensures we always have a
        // primary
        &self.pools[0]
    }

    pub async fn assignments(&self, node: &NodeId) -> Result<Vec<Site>, StoreError> {
        self.read_async(|conn| queries::assignments(conn, node).scope_boxed())
            .await
    }

    pub async fn active_assignments(&self, node: &NodeId) -> Result<Vec<Site>, StoreError> {
        self.read_async(|conn| queries::active_assignments(conn, node).scope_boxed())
            .await
    }

    pub async fn assigned_node(&self, site: &Site) -> Result<Option<NodeId>, StoreError> {
        self.read_async(|conn| queries::assigned_node(conn, site).scope_boxed())
            .await
    }

    /// Returns Option<(node_id,is_paused)> where `node_id` is the node that
    /// the subgraph is assigned to, and `is_paused` is true if the
    /// subgraph is paused.
    /// Returns None if the deployment does not exist.
    pub async fn assignment_status(
        &self,
        site: Arc<Site>,
    ) -> Result<Option<(NodeId, bool)>, StoreError> {
        self.read_async(|conn| queries::assignment_status(conn, &site).scope_boxed())
            .await
    }

    pub async fn find_active_site(
        &self,
        subgraph: &DeploymentHash,
    ) -> Result<Option<Site>, StoreError> {
        self.read_async(|conn| queries::find_active_site(conn, subgraph).scope_boxed())
            .await
    }

    pub async fn find_site_by_ref(&self, id: DeploymentId) -> Result<Option<Site>, StoreError> {
        self.read_async(|conn| queries::find_site_by_ref(conn, id).scope_boxed())
            .await
    }

    pub async fn current_deployment_for_subgraph(
        &self,
        name: &SubgraphName,
    ) -> Result<DeploymentHash, StoreError> {
        self.read_async(|conn| queries::current_deployment_for_subgraph(conn, name).scope_boxed())
            .await
    }

    pub async fn deployments_for_subgraph(&self, name: &str) -> Result<Vec<Site>, StoreError> {
        self.read_async(|conn| queries::deployments_for_subgraph(conn, name).scope_boxed())
            .await
    }

    pub async fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError> {
        self.read_async(|conn| queries::subgraph_exists(conn, name).scope_boxed())
            .await
    }

    pub async fn subgraph_version(
        &self,
        name: &str,
        use_current: bool,
    ) -> Result<Option<Site>, StoreError> {
        self.read_async(|conn| queries::subgraph_version(conn, name, use_current).scope_boxed())
            .await
    }

    /// Find sites by their subgraph deployment hashes. If `ids` is empty,
    /// return all sites
    pub async fn find_sites(
        &self,
        ids: &[String],
        only_active: bool,
    ) -> Result<Vec<Site>, StoreError> {
        self.read_async(|conn| queries::find_sites(conn, ids, only_active).scope_boxed())
            .await
    }

    /// Find sites by their subgraph deployment ids. If `ids` is empty,
    /// return no sites
    pub async fn find_sites_by_id(&self, ids: &[DeploymentId]) -> Result<Vec<Site>, StoreError> {
        self.read_async(|conn| queries::find_sites_by_id(conn, ids).scope_boxed())
            .await
    }

    pub async fn fill_assignments(
        &self,
        infos: &[status::Info],
    ) -> Result<HashMap<GraphDeploymentId, (String, bool)>, StoreError> {
        self.read_async(|conn| queries::fill_assignments(conn, infos).scope_boxed())
            .await
    }

    pub async fn version_info(
        &self,
        version: &str,
    ) -> Result<Option<(String, String)>, StoreError> {
        self.read_async(|conn| queries::version_info(conn, version).scope_boxed())
            .await
    }

    pub async fn versions_for_subgraph_id(
        &self,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        self.read_async(|conn| queries::versions_for_subgraph_id(conn, subgraph_id).scope_boxed())
            .await
    }

    /// Returns all (subgraph_name, version) pairs for a given deployment hash.
    pub async fn subgraphs_by_deployment_hash(
        &self,
        deployment_hash: &str,
    ) -> Result<Vec<(String, String)>, StoreError> {
        self.read_async(|conn| {
            queries::subgraphs_by_deployment_hash(conn, deployment_hash).scope_boxed()
        })
        .await
    }

    pub async fn find_site_in_shard(
        &self,
        subgraph: &DeploymentHash,
        shard: &Shard,
    ) -> Result<Option<Site>, StoreError> {
        self.read_async(|conn| queries::find_site_in_shard(conn, subgraph, shard).scope_boxed())
            .await
    }
}

//! Utilities for dealing with subgraph metadata that resides in the primary
//! shard. Anything in this module can only be used with a database connection
//! for the primary shard.
use diesel::{
    data_types::PgTimestamp,
    dsl::{any, exists, not},
    pg::Pg,
    serialize::Output,
    sql_types::{Array, Text},
    types::{FromSql, ToSql},
};
use diesel::{
    dsl::{delete, insert_into, sql, update},
    r2d2::PooledConnection,
};
use diesel::{pg::PgConnection, r2d2::ConnectionManager};
use diesel::{
    prelude::{
        BoolExpressionMethods, ExpressionMethods, GroupByDsl, JoinOnDsl, NullableExpressionMethods,
        OptionalExtension, QueryDsl, RunQueryDsl,
    },
    Connection as _,
};
use graph::{
    constraint_violation,
    data::subgraph::schema::MetadataType,
    data::subgraph::status,
    prelude::{
        anyhow, bigdecimal::ToPrimitive, entity, lazy_static, serde_json, EntityChange,
        EntityChangeOperation, MetadataOperation, NodeId, StoreError, SubgraphDeploymentId,
        SubgraphName, SubgraphVersionSwitchingMode,
    },
};
use graph::{data::subgraph::schema::generate_entity_id, prelude::StoreEvent};
use maybe_owned::MaybeOwned;
use std::{
    collections::HashMap,
    convert::TryFrom,
    convert::TryInto,
    fmt,
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    block_range::UNVERSIONED_RANGE,
    detail::DeploymentDetail,
    notification_listener::JsonNotification,
    subgraph_store::{unused, Shard},
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

// Diesel tables for some of the metadata
// See also: ed42d219c6704a4aab57ce1ea66698e7
// Changes to the GraphQL schema might require changes to these tables.
// The definitions of the tables can be generated with
//    cargo run -p graph-store-postgres --example layout -- \
//      -g diesel store/postgres/src/subgraphs.graphql subgraphs
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
    subgraphs.subgraph_deployment_assignment (vid) {
        vid -> BigInt,
        id -> Text,
        node_id -> Text,
        cost -> Numeric,
        block_range -> Range<Integer>,
    }
}

table! {
    public.ens_names(hash) {
        hash -> Varchar,
        name -> Varchar,
    }
}

/// We used to support different layout schemes. The old 'Split' scheme
/// which used JSONB layout has been removed, and we will only deal
/// with relational layout. Trying to do anything with a 'Split' subgraph
/// will result in an error.
#[derive(DbEnum, Debug, Clone, Copy)]
pub enum DeploymentSchemaVersion {
    Split,
    Relational,
}

table! {
    deployment_schemas(id) {
        id -> Integer,
        subgraph -> Text,
        name -> Text,
        shard -> Text,
        /// The subgraph layout scheme used for this subgraph
        version -> crate::primary::DeploymentSchemaVersionMapping,
        network -> Text,
    }
}

table! {
    /// A table to track deployments that are no longer used. Once an unused
    /// deployment has been removed, the entry in this table is the only
    /// trace in the system that it ever existed
    unused_deployments(id) {
        // The deployment id
        id -> Text,
        // When we first detected that the deployment was unsued
        unused_at -> Timestamptz,
        // When we actually deleted the deployment
        removed_at -> Nullable<Timestamptz>,

        /// Data that we get from the primary
        subgraphs -> Nullable<Array<Text>>,
        namespace -> Text,
        shard -> Text,

        /// Data we fill in from the deployment's shard
        entity_count -> Integer,
        latest_ethereum_block_hash -> Nullable<Binary>,
        latest_ethereum_block_number -> Nullable<Integer>,
        failed -> Bool,
        synced -> Bool,
    }
}

allow_tables_to_appear_in_same_query!(
    subgraph,
    subgraph_version,
    subgraph_deployment_assignment,
    deployment_schemas,
    unused_deployments,
);

/// Information about the database schema that stores the entities for a
/// subgraph.
#[derive(Clone, Queryable, QueryableByName, Debug)]
#[table_name = "deployment_schemas"]
struct Schema {
    id: i32,
    pub subgraph: String,
    pub name: String,
    pub shard: String,
    /// The version currently in use. Always `Relational`, attempts to load
    /// schemas from the database with `Split` produce an error
    version: DeploymentSchemaVersion,
    pub network: String,
}

#[derive(Clone, Queryable, QueryableByName, Debug)]
#[table_name = "unused_deployments"]
pub struct UnusedDeployment {
    pub id: String,
    pub unused_at: PgTimestamp,
    pub removed_at: Option<PgTimestamp>,
    pub subgraphs: Option<Vec<String>>,
    pub namespace: String,
    pub shard: String,

    /// Data we fill in from the deployment's shard
    pub entity_count: i32,
    pub latest_ethereum_block_hash: Option<Vec<u8>>,
    pub latest_ethereum_block_number: Option<i32>,
    pub failed: bool,
    pub synced: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, AsExpression, FromSqlRow)]
#[sql_type = "diesel::sql_types::Text"]
/// A namespace (schema) in the database
pub struct Namespace(String);

lazy_static! {
    pub static ref METADATA_NAMESPACE: Namespace = Namespace(String::from("subgraphs"));
}

impl Namespace {
    pub fn new(s: String) -> Result<Self, String> {
        if s.as_str() == METADATA_NAMESPACE.as_str() {
            return Ok(Namespace(s));
        }

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

    pub fn is_metadata(&self) -> bool {
        self == &*METADATA_NAMESPACE
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
    fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        Namespace::new(s).map_err(Into::into)
    }
}

impl ToSql<Text, Pg> for Namespace {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> diesel::serialize::Result {
        <String as ToSql<Text, Pg>>::to_sql(&self.0, out)
    }
}

#[derive(Debug)]
/// Details about a deployment and the shard in which it is stored. We need
/// the database namespace for the deployment as that information is only
/// stored in the primary database
pub struct Site {
    /// The subgraph deployment
    pub deployment: SubgraphDeploymentId,
    /// The name of the database shard
    pub shard: Shard,
    /// The database namespace (schema) that holds the data for the deployment
    pub namespace: Namespace,
    /// The name of the network to which this deployment belongs
    pub network: String,
}

impl TryFrom<Schema> for Site {
    type Error = StoreError;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        let deployment = SubgraphDeploymentId::new(&schema.subgraph)
            .map_err(|s| constraint_violation!("Invalid deployment id {}", s))?;
        let namespace = Namespace::new(schema.name.clone()).map_err(|nsp| {
            constraint_violation!(
                "Invalid schema name {} for deployment {}",
                nsp,
                &schema.subgraph
            )
        })?;
        let shard = Shard::new(schema.shard)?;
        Ok(Self {
            deployment,
            namespace,
            shard,
            network: schema.network,
        })
    }
}

/// A wrapper for a database connection that provides access to functionality
/// that works only on the primary database
pub struct Connection<'a>(MaybeOwned<'a, PooledConnection<ConnectionManager<PgConnection>>>);

impl<'a> Connection<'a> {
    pub fn new(
        conn: impl Into<MaybeOwned<'a, PooledConnection<ConnectionManager<PgConnection>>>>,
    ) -> Self {
        Self(conn.into())
    }

    pub(crate) fn transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        self.0.transaction(f)
    }

    pub fn current_deployment_for_subgraph(
        &self,
        name: SubgraphName,
    ) -> Result<SubgraphDeploymentId, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let id = v::table
            .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
            .filter(s::name.eq(name.as_str()))
            .select(v::deployment)
            .first::<String>(self.0.as_ref())
            .optional()?;
        match id {
            Some(id) => SubgraphDeploymentId::new(id)
                .map_err(|id| constraint_violation!("illegal deployment id: {}", id)),
            None => Err(StoreError::QueryExecutionError(format!(
                "Subgraph `{}` not found",
                name.as_str()
            ))),
        }
    }

    /// Delete all assignments for deployments that are neither the current nor the
    /// pending version of a subgraph and return the deployment id's
    fn remove_unused_assignments(&self) -> Result<Vec<EntityChange>, StoreError> {
        const QUERY: &str = "
    delete from subgraphs.subgraph_deployment_assignment a
    where not exists (select 1
                        from subgraphs.subgraph s, subgraphs.subgraph_version v
                       where v.id in (s.current_version, s.pending_version)
                         and v.deployment = a.id)
    returning a.id
    ";
        #[derive(QueryableByName)]
        struct Removed {
            #[sql_type = "Text"]
            id: String,
        }

        Ok(diesel::sql_query(QUERY)
            .load::<Removed>(self.0.as_ref())?
            .into_iter()
            .map(|r| {
                SubgraphDeploymentId::new(r.id.clone())
                    .map(|id| {
                        let key = MetadataType::SubgraphDeploymentAssignment.key(id, r.id);
                        MetadataOperation::Remove { key }.into()
                    })
                    .map_err(|id| {
                        StoreError::ConstraintViolation(format!(
                            "invalid id `{}` for deployment assignment",
                            id
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Promote the deployment `id` to the current version everywhere where it was
    /// the pending version so far, and remove any assignments that are not needed
    /// any longer as a result. Return the changes that were made to assignments
    /// in the process
    pub fn promote_deployment(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let conn = self.0.as_ref();

        // Subgraphs where we need to promote the version
        let pending_subgraph_versions: Vec<(String, String)> = s::table
            .inner_join(v::table.on(s::pending_version.eq(v::id.nullable())))
            .filter(v::deployment.eq(id.as_str()))
            .select((s::id, v::id))
            .for_update()
            .load(conn)?;

        // Switch the pending version to the current version
        for (subgraph, version) in &pending_subgraph_versions {
            update(s::table.filter(s::id.eq(subgraph)))
                .set((
                    s::current_version.eq(version),
                    s::pending_version.eq::<Option<&str>>(None),
                ))
                .execute(conn)?;
        }

        // Clean up assignments if we could possibly have changed any
        // subgraph versions
        let changes = if pending_subgraph_versions.is_empty() {
            vec![]
        } else {
            self.remove_unused_assignments()?
        };
        Ok(changes)
    }

    /// Create a new subgraph with the given name. If one already exists, use
    /// the existing one. Return the `id` of the newly created or existing
    /// subgraph
    pub fn create_subgraph(&self, name: &SubgraphName) -> Result<String, StoreError> {
        use subgraph as s;

        let conn = self.0.as_ref();
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
            .execute(conn)?;
        if inserted == 0 {
            let existing_id = s::table
                .filter(s::name.eq(name.as_str()))
                .select(s::id)
                .first::<String>(conn)?;
            Ok(existing_id)
        } else {
            Ok(id)
        }
    }

    pub fn create_subgraph_version<F>(
        &self,
        name: SubgraphName,
        id: &SubgraphDeploymentId,
        node_id: NodeId,
        mode: SubgraphVersionSwitchingMode,
        exists_and_synced: F,
    ) -> Result<Vec<EntityChange>, StoreError>
    where
        F: FnOnce(&SubgraphDeploymentId) -> Result<bool, StoreError>,
    {
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;
        use SubgraphVersionSwitchingMode::*;

        let conn = self.0.as_ref();

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
            .first::<(String, Option<String>)>(conn)
            .optional()?;
        let (subgraph_id, current_deployment) = match info {
            Some((subgraph_id, current_deployment)) => (subgraph_id, current_deployment),
            None => (self.create_subgraph(&name)?, None),
        };
        let pending_deployment = s::table
            .left_outer_join(v::table.on(s::pending_version.eq(v::id.nullable())))
            .filter(s::id.eq(&subgraph_id))
            .select(v::deployment.nullable())
            .first::<Option<String>>(conn)?;

        // See if the current version of that subgraph is synced. If the subgraph
        // has no current version, we treat it the same as if it were not synced
        // The `optional` below only comes into play if data is corrupted/missing;
        // ignoring that via `optional` makes it possible to fix a missing version
        // or deployment by deploying over it.
        let current_exists_and_synced = current_deployment
            .as_deref()
            .map(|id| {
                SubgraphDeploymentId::new(id)
                    .map_err(|e| StoreError::DeploymentNotFound(e))
                    .and_then(|id| exists_and_synced(&id))
            })
            .transpose()?
            .unwrap_or(false);

        // Check if we even need to make any changes
        let change_needed = match (mode, current_exists_and_synced) {
            (Instant, _) | (Synced, false) => current_deployment.as_deref() != Some(id.as_str()),
            (Synced, true) => pending_deployment.as_deref() != Some(id.as_str()),
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
                v::deployment.eq(id.as_str()),
                // using BigDecimal::from(created_at) produced a scale error
                v::created_at.eq(sql(&format!("{}", created_at))),
                v::block_range.eq(UNVERSIONED_RANGE),
            ))
            .execute(conn)?;

        // Create a subgraph assignment if there isn't one already
        let new_assignment = a::table
            .filter(a::id.eq(id.as_str()))
            .select(a::id)
            .first::<String>(conn)
            .optional()?
            .is_none();
        if new_assignment {
            insert_into(a::table)
                .values((
                    a::id.eq(id.as_str()),
                    a::node_id.eq(node_id.as_str()),
                    a::block_range.eq(UNVERSIONED_RANGE),
                    a::cost.eq(sql("1")),
                ))
                .execute(conn)?;
        }

        // See if we should make this the current or pending version
        let subgraph_row = update(s::table.filter(s::id.eq(&subgraph_id)));
        match (mode, current_exists_and_synced) {
            (Instant, _) | (Synced, false) => {
                subgraph_row
                    .set((
                        s::current_version.eq(&version_id),
                        s::pending_version.eq::<Option<&str>>(None),
                    ))
                    .execute(conn)?;
            }
            (Synced, true) => {
                subgraph_row
                    .set(s::pending_version.eq(&version_id))
                    .execute(conn)?;
            }
        }

        // Clean up any assignments we might have displaced
        let mut changes = self.remove_unused_assignments()?;
        if new_assignment {
            let change = EntityChange::from_key(
                MetadataType::SubgraphDeploymentAssignment
                    .key(id.clone(), id.to_string())
                    .into(),
                EntityChangeOperation::Set,
            );
            changes.push(change);
        }
        Ok(changes)
    }

    pub fn remove_subgraph(&self, name: SubgraphName) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let conn = self.0.as_ref();

        // Get the id of the given subgraph. If no subgraph with the
        // name exists, there is nothing to do
        let subgraph: Option<String> = s::table
            .filter(s::name.eq(name.as_str()))
            .select(s::id)
            .first(conn)
            .optional()?;
        if let Some(subgraph) = subgraph {
            delete(v::table.filter(v::subgraph.eq(&subgraph))).execute(conn)?;
            delete(s::table.filter(s::id.eq(subgraph))).execute(conn)?;
            self.remove_unused_assignments()
        } else {
            Ok(vec![])
        }
    }

    pub fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError> {
        use subgraph as s;

        Ok(
            diesel::select(exists(s::table.filter(s::name.eq(name.as_str()))))
                .get_result::<bool>(self.0.as_ref())?,
        )
    }

    pub fn reassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
        node: &NodeId,
    ) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = self.0.as_ref();
        let updates = update(a::table.filter(a::id.eq(id.as_str())))
            .set(a::node_id.eq(node.as_str()))
            .execute(conn)?;
        match updates {
            0 => Err(StoreError::DeploymentNotFound(id.to_string())),
            1 => {
                let key =
                    MetadataType::SubgraphDeploymentAssignment.key(id.clone(), id.to_string());
                let op = MetadataOperation::Set {
                    key,
                    data: entity! { node_id: node.to_string() },
                };
                Ok(vec![op.into()])
            }
            _ => {
                // `id` is the primary key of the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    pub fn unassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = self.0.as_ref();
        let delete_count = delete(a::table.filter(a::id.eq(id.as_str()))).execute(conn)?;

        match delete_count {
            0 => Ok(vec![]),
            1 => {
                let key =
                    MetadataType::SubgraphDeploymentAssignment.key(id.clone(), id.to_string());
                let op = MetadataOperation::Remove { key };
                Ok(vec![op.into()])
            }
            _ => {
                // `id` is the unique in the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    pub fn allocate_site(
        &self,
        shard: Shard,
        subgraph: &SubgraphDeploymentId,
        network: String,
    ) -> Result<Site, StoreError> {
        use deployment_schemas as ds;
        use DeploymentSchemaVersion as v;

        let conn = self.0.as_ref();

        if let Some(schema) = self.find_site(subgraph)? {
            return Ok(schema);
        }

        // Create a schema for the deployment.
        let schemas: Vec<String> = diesel::insert_into(ds::table)
            .values((
                ds::subgraph.eq(subgraph.as_str()),
                ds::shard.eq(shard.as_str()),
                ds::version.eq(v::Relational),
                ds::network.eq(network.as_str()),
            ))
            .returning(ds::name)
            .get_results(conn)?;
        let namespace = schemas
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("failed to read schema name for {} back", subgraph))?;
        let namespace = Namespace::new(namespace).map_err(|name| {
            constraint_violation!("Generated database schema name {} is invalid", name)
        })?;

        Ok(Site {
            deployment: subgraph.clone(),
            namespace,
            shard,
            network,
        })
    }

    /// Remove all subgraph versions and the entry in `deployment_schemas` for
    /// subgraph `id` in a transaction
    pub fn drop_site(&self, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
        use deployment_schemas as ds;
        use subgraph_version as v;
        use unused_deployments as u;

        self.transaction(|| {
            delete(v::table.filter(v::deployment.eq(id.as_str()))).execute(self.0.as_ref())?;
            delete(ds::table.filter(ds::subgraph.eq(id.as_str()))).execute(self.0.as_ref())?;
            update(u::table.filter(u::id.eq(id.as_str())))
                .set(u::removed_at.eq(sql("now()")))
                .execute(self.0.as_ref())?;
            Ok(())
        })
    }

    pub fn find_site(&self, subgraph: &SubgraphDeploymentId) -> Result<Option<Site>, StoreError> {
        let schema = deployment_schemas::table
            .filter(deployment_schemas::subgraph.eq(subgraph.to_string()))
            .first::<Schema>(self.0.as_ref())
            .optional()?;
        if let Some(Schema { version, .. }) = schema {
            if matches!(version, DeploymentSchemaVersion::Split) {
                return Err(constraint_violation!(
                    "the subgraph {} uses JSONB layout which is not supported any longer",
                    subgraph.as_str()
                ));
            }
        }
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub fn find_sites(&self, ids: &Vec<SubgraphDeploymentId>) -> Result<Vec<Site>, StoreError> {
        let ids: Vec<_> = ids.iter().map(|id| id.to_string()).collect();
        let schemas = deployment_schemas::table
            .filter(deployment_schemas::subgraph.eq_any(ids))
            .load::<Schema>(self.0.as_ref())?;
        schemas
            .into_iter()
            .map(|schema| {
                let Schema {
                    version, subgraph, ..
                } = &schema;
                if matches!(version, DeploymentSchemaVersion::Split) {
                    Err(constraint_violation!(
                        "the subgraph {} uses JSONB layout which is not supported any longer",
                        subgraph.as_str()
                    ))
                } else {
                    schema.try_into()
                }
            })
            .collect()
    }

    pub fn find_existing_site(&self, subgraph: &SubgraphDeploymentId) -> Result<Site, StoreError> {
        self.find_site(subgraph)?
            .ok_or_else(|| StoreError::DeploymentNotFound(subgraph.to_string()))
    }

    pub fn sites(&self) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;

        ds::table
            .filter(ds::name.ne("subgraphs"))
            .load::<Schema>(self.0.as_ref())?
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
    }

    pub fn send_store_event(&self, event: &StoreEvent) -> Result<(), StoreError> {
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
        JsonNotification::send("store_events", &v, self.0.as_ref())
    }

    /// Return the name of the node that has the fewest assignments out of the
    /// given `nodes`. If `nodes` is empty, return `None`
    pub fn least_assigned_node(&self, nodes: &Vec<NodeId>) -> Result<Option<NodeId>, StoreError> {
        use subgraph_deployment_assignment as a;

        let nodes: Vec<_> = nodes.iter().map(|n| n.as_str()).collect();

        let assigned = a::table
            .filter(a::node_id.eq(any(&nodes)))
            .select((a::node_id, sql("count(*)")))
            .group_by(a::node_id)
            .order_by(sql::<i64>("count(*)"))
            .load::<(String, i64)>(self.0.as_ref())?;

        // Any nodes without assignments will be missing from `assigned`
        let missing = nodes
            .into_iter()
            .filter(|node| !assigned.iter().any(|(a, _)| a == node))
            .map(|node| (node, 0));

        assigned
            .iter()
            .map(|(node, count)| (node.as_str(), count.clone()))
            .chain(missing)
            .min_by(|(_, a), (_, b)| a.cmp(b))
            .map(|(node, _)| NodeId::new(node).map_err(|()| node))
            .transpose()
            // This can't really happen since we filtered by valid NodeId's
            .map_err(|node| {
                constraint_violation!("database has assignment for illegal node name {:?}", node)
            })
    }

    pub fn assigned_node(&self, id: &SubgraphDeploymentId) -> Result<Option<NodeId>, StoreError> {
        use subgraph_deployment_assignment as a;

        a::table
            .filter(a::id.eq(id.as_str()))
            .select(a::node_id)
            .first::<String>(self.0.as_ref())
            .optional()?
            .map(|node| {
                NodeId::new(&node).map_err(|()| {
                    constraint_violation!("invalid node id `{}` in assignment for `{}`", node, id)
                })
            })
            .transpose()
    }

    pub fn assignments(&self, node: &NodeId) -> Result<Vec<SubgraphDeploymentId>, StoreError> {
        use subgraph_deployment_assignment as a;

        a::table
            .filter(a::node_id.eq(node.as_str()))
            .select(a::id)
            .load::<String>(self.0.as_ref())?
            .into_iter()
            .map(|id| {
                SubgraphDeploymentId::new(id).map_err(|id| {
                    constraint_violation!(
                        "invalid deployment id `{}` assigned to node `{}`",
                        id,
                        node
                    )
                })
            })
            .collect()
    }

    pub fn fill_assignments(
        &self,
        mut infos: Vec<status::Info>,
    ) -> Result<Vec<status::Info>, StoreError> {
        use subgraph_deployment_assignment as a;

        let ids: Vec<_> = infos.iter().map(|info| &info.subgraph).collect();
        let nodes: HashMap<_, _> = a::table
            .filter(a::id.eq(any(ids)))
            .select((a::id, a::node_id))
            .load::<(String, String)>(self.0.as_ref())?
            .into_iter()
            .collect();
        for mut info in &mut infos {
            info.node = nodes.get(&info.subgraph).map(|s| s.clone());
        }
        Ok(infos)
    }

    pub(crate) fn deployments_for_subgraph(&self, name: String) -> Result<Vec<String>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        Ok(v::table
            .inner_join(s::table.on(v::subgraph.eq(s::id)))
            .filter(s::name.eq(&name))
            .order_by(v::created_at.asc())
            .select(v::deployment)
            .load(self.0.as_ref())?)
    }

    pub fn subgraph_version(
        &self,
        name: String,
        use_current: bool,
    ) -> Result<Option<String>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let deployment = if use_current {
            v::table
                .select(v::deployment.nullable())
                .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
                .filter(s::name.eq(&name))
                .first::<Option<String>>(self.0.as_ref())
        } else {
            v::table
                .select(v::deployment.nullable())
                .inner_join(s::table.on(s::pending_version.eq(v::id.nullable())))
                .filter(s::name.eq(&name))
                .first::<Option<String>>(self.0.as_ref())
        };
        Ok(deployment.optional()?.flatten())
    }

    #[cfg(debug_assertions)]
    pub fn versions_for_subgraph(
        &self,
        name: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        use subgraph as s;

        Ok(s::table
            .select((s::current_version.nullable(), s::pending_version.nullable()))
            .filter(s::name.eq(&name))
            .first::<(Option<String>, Option<String>)>(self.0.as_ref())
            .optional()?
            .unwrap_or((None, None)))
    }

    #[cfg(debug_assertions)]
    pub fn deployment_for_version(&self, name: &str) -> Result<Option<String>, StoreError> {
        use subgraph_version as v;

        Ok(v::table
            .select(v::deployment)
            .filter(v::id.eq(name))
            .first::<String>(self.0.as_ref())
            .optional()?)
    }

    pub fn version_info(&self, version: &str) -> Result<Option<(String, String)>, StoreError> {
        use subgraph_version as v;

        Ok(v::table
            .select((v::deployment, sql("created_at::text")))
            .filter(v::id.eq(version))
            .first::<(String, String)>(self.0.as_ref())
            .optional()?)
    }

    pub fn versions_for_subgraph_id(
        &self,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        use subgraph as s;

        Ok(s::table
            .select((s::current_version.nullable(), s::pending_version.nullable()))
            .filter(s::id.eq(subgraph_id))
            .first::<(Option<String>, Option<String>)>(self.0.as_ref())
            .optional()?
            .unwrap_or((None, None)))
    }

    /// Find all deployments that are not in use and add them to the
    /// `unused_deployments` table. Only values that are available in the
    /// primary will be filled in `unused_deployments`
    pub fn detect_unused_deployments(&self) -> Result<Vec<String>, StoreError> {
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;
        use unused_deployments as u;

        // Deployment is assigned
        let assigned = a::table.filter(a::id.eq(ds::subgraph));
        // Deployment is current or pending version
        let active = v::table
            .inner_join(
                s::table.on(v::id
                    .nullable()
                    .eq(s::current_version)
                    .or(v::id.nullable().eq(s::pending_version))),
            )
            .filter(v::deployment.eq(ds::subgraph));
        // Subgraphs that used a deployment
        let used_by = s::table
            .inner_join(v::table.on(s::id.eq(v::subgraph)))
            .filter(v::deployment.eq(ds::subgraph))
            .select(sql::<Array<Text>>("array_agg(name)"))
            .single_value();

        let unused = ds::table
            .filter(not(exists(assigned)))
            .filter(not(exists(active)))
            .select((ds::subgraph, ds::name, ds::shard, used_by));

        Ok(insert_into(u::table)
            .values(unused)
            .into_columns((u::id, u::namespace, u::shard, u::subgraphs))
            .on_conflict(u::id)
            .do_nothing()
            .returning(u::id)
            .get_results::<String>(self.0.as_ref())?)
    }

    /// Add details from the deployment shard to unuseddeployments
    pub fn update_unused_deployments(
        &self,
        details: &Vec<DeploymentDetail>,
    ) -> Result<(), StoreError> {
        use crate::detail::block;
        use unused_deployments as u;

        for detail in details {
            let (latest_hash, latest_number) = block(
                &detail.id,
                "latest_ethereum_block",
                detail.latest_ethereum_block_hash.clone(),
                detail.latest_ethereum_block_number.clone(),
            )?
            .map(|b| b.to_ptr())
            .map(|ptr| {
                (
                    Some(Vec::from(ptr.hash.as_bytes())),
                    Some(ptr.number as i32),
                )
            })
            .unwrap_or((None, None));
            let entity_count = detail.entity_count.to_u64().unwrap_or(0) as i32;

            update(u::table.filter(u::id.eq(&detail.id)))
                .set((
                    u::entity_count.eq(entity_count),
                    u::latest_ethereum_block_hash.eq(latest_hash),
                    u::latest_ethereum_block_number.eq(latest_number),
                    u::failed.eq(detail.failed),
                    u::synced.eq(detail.synced),
                ))
                .execute(self.0.as_ref())?;
        }
        Ok(())
    }

    pub fn list_unused_deployments(
        &self,
        filter: unused::Filter,
    ) -> Result<Vec<UnusedDeployment>, StoreError> {
        use unused::Filter::*;
        use unused_deployments as u;

        match filter {
            All => Ok(u::table
                .order_by(u::unused_at.desc())
                .load(self.0.as_ref())?),
            New => Ok(u::table
                .filter(u::removed_at.is_null())
                .order_by(u::entity_count)
                .load(self.0.as_ref())?),
        }
    }

    pub fn subgraphs_using_deployment(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<Vec<String>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        Ok(s::table
            .inner_join(
                v::table.on(v::subgraph
                    .nullable()
                    .eq(s::current_version)
                    .or(v::subgraph.nullable().eq(s::pending_version))),
            )
            .filter(v::deployment.eq(id.as_str()))
            .select(s::name)
            .distinct()
            .load(self.0.as_ref())?)
    }

    pub fn find_ens_name(&self, hash: &str) -> Result<Option<String>, StoreError> {
        use ens_names as dsl;

        dsl::table
            .select(dsl::name)
            .find(hash)
            .get_result::<String>(self.0.as_ref())
            .optional()
            .map_err(|e| anyhow!("error looking up ens_name for hash {}: {}", hash, e).into())
    }
}

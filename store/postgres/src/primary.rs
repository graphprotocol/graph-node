//! Utilities for dealing with subgraph metadata that resides in the primary
//! shard. Anything in this module can only be used with a database connection
//! for the primary shard.
use diesel::{
    data_types::PgTimestamp,
    dsl::{any, exists, not, select},
    pg::Pg,
    serialize::Output,
    sql_types::{Array, Integer, Text},
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
use graph::components::store::DeploymentId as GraphDeploymentId;
use graph::{
    components::store::DeploymentLocator,
    constraint_violation,
    data::subgraph::status,
    prelude::{
        anyhow, bigdecimal::ToPrimitive, serde_json, DeploymentHash, EntityChange,
        EntityChangeOperation, NodeId, StoreError, SubgraphName, SubgraphVersionSwitchingMode,
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
    subgraphs.subgraph_deployment_assignment {
        id -> Integer,
        node_id -> Text,
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
    active_copies,
);

/// Information about the database schema that stores the entities for a
/// subgraph.
#[derive(Clone, Queryable, QueryableByName, Debug)]
#[table_name = "deployment_schemas"]
struct Schema {
    id: DeploymentId,
    pub subgraph: String,
    pub name: String,
    pub shard: String,
    /// The version currently in use. Always `Relational`, attempts to load
    /// schemas from the database with `Split` produce an error
    version: DeploymentSchemaVersion,
    pub network: String,
    pub(crate) active: bool,
}

#[derive(Clone, Queryable, QueryableByName, Debug)]
#[table_name = "unused_deployments"]
pub struct UnusedDeployment {
    pub id: DeploymentId,
    pub deployment: String,
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

/// A marker that an `i32` references a deployment. Values of this type hold
/// the primary key from the `deployment_schemas` table
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, AsExpression, FromSqlRow)]
#[sql_type = "diesel::sql_types::Integer"]
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

impl FromSql<Integer, Pg> for DeploymentId {
    fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
        let id = <i32 as FromSql<Integer, Pg>>::from_sql(bytes)?;
        Ok(DeploymentId(id))
    }
}

impl ToSql<Integer, Pg> for DeploymentId {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> diesel::serialize::Result {
        <i32 as ToSql<Integer, Pg>>::to_sql(&self.0, out)
    }
}

#[derive(Debug)]
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
    pub(crate) active: bool,
    /// Only the store and tests can create Sites
    _creation_disallowed: (),
}

impl TryFrom<Schema> for Site {
    type Error = StoreError;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        if matches!(schema.version, DeploymentSchemaVersion::Split) {
            return Err(constraint_violation!(
                "the subgraph {} uses JSONB layout which is not supported any longer",
                schema.subgraph.as_str()
            ));
        }
        let deployment = DeploymentHash::new(&schema.subgraph)
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
            id: schema.id,
            deployment,
            namespace,
            shard,
            network: schema.network,
            active: schema.active,
            _creation_disallowed: (),
        })
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
    use crate::PRIMARY_SHARD;

    Site {
        id: DeploymentId(-7),
        deployment,
        shard: PRIMARY_SHARD.clone(),
        namespace,
        network,
        active: true,
        _creation_disallowed: (),
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
    ) -> Result<DeploymentHash, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let id = v::table
            .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
            .filter(s::name.eq(name.as_str()))
            .select(v::deployment)
            .first::<String>(self.0.as_ref())
            .optional()?;
        match id {
            Some(id) => DeploymentHash::new(id)
                .map_err(|id| constraint_violation!("illegal deployment id: {}", id)),
            None => Err(StoreError::QueryExecutionError(format!(
                "Subgraph `{}` not found",
                name.as_str()
            ))),
        }
    }

    /// Signal any copy process that might be copying into one of these
    /// deployments that it should stop. Copying is cancelled whenever we
    /// remove the assignment for a deployment
    fn cancel_copies(&self, ids: Vec<DeploymentId>) -> Result<(), StoreError> {
        use active_copies as ac;

        update(ac::table.filter(ac::dst.eq_any(ids)))
            .set(ac::cancelled_at.eq(sql("now()")))
            .execute(self.0.as_ref())?;
        Ok(())
    }

    /// Delete all assignments for deployments that are neither the current nor the
    /// pending version of a subgraph and return the deployment id's
    fn remove_unused_assignments(&self) -> Result<Vec<EntityChange>, StoreError> {
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;

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
            .load::<i32>(self.0.as_ref())?;

        let removed: Vec<_> = ds::table
            .filter(ds::id.eq_any(removed))
            .select((ds::id, ds::subgraph))
            .load::<(DeploymentId, String)>(self.0.as_ref())?
            .into_iter()
            .collect();

        // Stop ongoing copies
        let removed_ids: Vec<_> = removed.iter().map(|(id, _)| id.clone()).collect();
        self.cancel_copies(removed_ids)?;

        let events = removed
            .into_iter()
            .map(|(id, hash)| {
                DeploymentHash::new(hash)
                    .map(|hash| {
                        EntityChange::for_assignment(
                            DeploymentLocator::new(id.into(), hash),
                            EntityChangeOperation::Removed,
                        )
                    })
                    .map_err(|id| {
                        StoreError::ConstraintViolation(format!(
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
    pub fn promote_deployment(&self, id: &DeploymentHash) -> Result<Vec<EntityChange>, StoreError> {
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
        site: &Site,
        node_id: NodeId,
        mode: SubgraphVersionSwitchingMode,
        exists_and_synced: F,
    ) -> Result<Vec<EntityChange>, StoreError>
    where
        F: FnOnce(&DeploymentHash) -> Result<bool, StoreError>,
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
                DeploymentHash::new(id)
                    .map_err(|e| StoreError::DeploymentNotFound(e))
                    .and_then(|id| exists_and_synced(&id))
            })
            .transpose()?
            .unwrap_or(false);

        // Check if we even need to make any changes
        let change_needed = match (mode, current_exists_and_synced) {
            (Instant, _) | (Synced, false) => {
                current_deployment.as_deref() != Some(site.deployment.as_str())
            }
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
            .execute(conn)?;

        // Create a subgraph assignment if there isn't one already
        let new_assignment = a::table
            .filter(a::id.eq(site.id))
            .select(a::id)
            .first::<i32>(conn)
            .optional()?
            .is_none();
        if new_assignment {
            insert_into(a::table)
                .values((a::id.eq(site.id), a::node_id.eq(node_id.as_str())))
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
            let change = EntityChange::for_assignment(site.into(), EntityChangeOperation::Set);
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
        site: &Site,
        node: &NodeId,
    ) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = self.0.as_ref();
        let updates = update(a::table.filter(a::id.eq(site.id)))
            .set(a::node_id.eq(node.as_str()))
            .execute(conn)?;
        match updates {
            0 => Err(StoreError::DeploymentNotFound(site.deployment.to_string())),
            1 => {
                let change = EntityChange::for_assignment(site.into(), EntityChangeOperation::Set);
                Ok(vec![change])
            }
            _ => {
                // `id` is the primary key of the subgraph_deployment_assignment table,
                // and we can therefore only update no or one entry
                unreachable!()
            }
        }
    }

    pub fn assign_subgraph(
        &self,
        site: &Site,
        node: &NodeId,
    ) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = self.0.as_ref();
        insert_into(a::table)
            .values((a::id.eq(site.id), a::node_id.eq(node.as_str())))
            .execute(conn)?;

        let change = EntityChange::for_assignment(site.into(), EntityChangeOperation::Set);
        Ok(vec![change])
    }

    pub fn unassign_subgraph(&self, site: &Site) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = self.0.as_ref();
        let delete_count = delete(a::table.filter(a::id.eq(site.id))).execute(conn)?;

        self.cancel_copies(vec![site.id])?;

        match delete_count {
            0 => Ok(vec![]),
            1 => {
                let change =
                    EntityChange::for_assignment(site.into(), EntityChangeOperation::Removed);
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
    fn create_site(
        &self,
        shard: Shard,
        deployment: DeploymentHash,
        network: String,
        active: bool,
    ) -> Result<Site, StoreError> {
        use deployment_schemas as ds;
        use DeploymentSchemaVersion as v;

        let conn = self.0.as_ref();

        let schemas: Vec<(DeploymentId, String)> = diesel::insert_into(ds::table)
            .values((
                ds::subgraph.eq(deployment.as_str()),
                ds::shard.eq(shard.as_str()),
                ds::version.eq(v::Relational),
                ds::network.eq(network.as_str()),
                ds::active.eq(active),
            ))
            .returning((ds::id, ds::name))
            .get_results(conn)?;
        let (id, namespace) = schemas
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("failed to read schema name for {} back", deployment))?;
        let namespace = Namespace::new(namespace).map_err(|name| {
            constraint_violation!("Generated database schema name {} is invalid", name)
        })?;

        Ok(Site {
            id,
            deployment,
            shard,
            namespace,
            network,
            active: true,
            _creation_disallowed: (),
        })
    }

    pub fn allocate_site(
        &self,
        shard: Shard,
        subgraph: &DeploymentHash,
        network: String,
    ) -> Result<Site, StoreError> {
        if let Some(site) = self.find_active_site(subgraph)? {
            return Ok(site);
        }

        self.create_site(shard, subgraph.clone(), network, true)
    }

    /// Create a copy of the site `src` in the shard `shard`, but mark it as
    /// not active. If there already is a site in `shard`, return that
    /// instead.
    pub fn copy_site(&self, src: &Site, shard: Shard) -> Result<Site, StoreError> {
        if let Some(site) = self.find_site_in_shard(&src.deployment, &shard)? {
            return Ok(site);
        }

        self.create_site(shard, src.deployment.clone(), src.network.clone(), false)
    }

    pub(crate) fn activate(&self, deployment: &DeploymentLocator) -> Result<(), StoreError> {
        use deployment_schemas as ds;

        // We need to tread lightly so we do not violate the unique constraint on
        // `subgraph where active`
        update(ds::table.filter(ds::subgraph.eq(deployment.hash.as_str())))
            .set(ds::active.eq(false))
            .execute(self.0.as_ref())?;

        update(ds::table.filter(ds::id.eq(DeploymentId::from(deployment.id))))
            .set(ds::active.eq(true))
            .execute(self.0.as_ref())
            .map_err(|e| e.into())
            .map(|_| ())
    }

    /// Remove all subgraph versions and the entry in `deployment_schemas` for
    /// subgraph `id` in a transaction
    pub fn drop_site(&self, site: &Site) -> Result<(), StoreError> {
        use deployment_schemas as ds;
        use subgraph_version as v;
        use unused_deployments as u;

        self.transaction(|| {
            let conn = self.0.as_ref();

            delete(ds::table.filter(ds::id.eq(site.id))).execute(conn)?;

            // If there is no site for this deployment any more, we can get
            // rid of versions pointing to it
            let exists = select(exists(
                ds::table.filter(ds::subgraph.eq(site.deployment.as_str())),
            ))
            .get_result::<bool>(conn)?;
            if !exists {
                delete(v::table.filter(v::deployment.eq(site.deployment.as_str())))
                    .execute(conn)?;
            }

            update(u::table.filter(u::id.eq(site.id)))
                .set(u::removed_at.eq(sql("now()")))
                .execute(self.0.as_ref())?;
            Ok(())
        })
    }

    pub fn find_active_site(&self, subgraph: &DeploymentHash) -> Result<Option<Site>, StoreError> {
        let schema = deployment_schemas::table
            .filter(deployment_schemas::subgraph.eq(subgraph.to_string()))
            .filter(deployment_schemas::active.eq(true))
            .first::<Schema>(self.0.as_ref())
            .optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub fn find_site_in_shard(
        &self,
        subgraph: &DeploymentHash,
        shard: &Shard,
    ) -> Result<Option<Site>, StoreError> {
        let schema = deployment_schemas::table
            .filter(deployment_schemas::subgraph.eq(subgraph.as_str()))
            .filter(deployment_schemas::shard.eq(shard.as_str()))
            .first::<Schema>(self.0.as_ref())
            .optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub fn find_site_by_ref(&self, id: DeploymentId) -> Result<Option<Site>, StoreError> {
        let schema = deployment_schemas::table
            .find(id)
            .first::<Schema>(self.0.as_ref())
            .optional()?;
        schema.map(|schema| schema.try_into()).transpose()
    }

    /// Find sites by their subgraph deployment ids. If `ids` is empty,
    /// return all sites
    pub fn find_sites(&self, ids: Vec<String>, only_active: bool) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;

        let schemas = if ids.is_empty() {
            if only_active {
                ds::table
                    .filter(ds::active)
                    .load::<Schema>(self.0.as_ref())?
            } else {
                ds::table.load::<Schema>(self.0.as_ref())?
            }
        } else {
            if only_active {
                ds::table
                    .filter(ds::active)
                    .filter(ds::subgraph.eq_any(ids))
                    .load::<Schema>(self.0.as_ref())?
            } else {
                ds::table
                    .filter(ds::subgraph.eq_any(ids))
                    .load::<Schema>(self.0.as_ref())?
            }
        };
        schemas
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
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

    pub fn assigned_node(&self, site: &Site) -> Result<Option<NodeId>, StoreError> {
        use subgraph_deployment_assignment as a;

        a::table
            .filter(a::id.eq(site.id))
            .select(a::node_id)
            .first::<String>(self.0.as_ref())
            .optional()?
            .map(|node| {
                NodeId::new(&node).map_err(|()| {
                    constraint_violation!(
                        "invalid node id `{}` in assignment for `{}`",
                        node,
                        site.deployment
                    )
                })
            })
            .transpose()
    }

    pub fn assignments(&self, node: &NodeId) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;
        use subgraph_deployment_assignment as a;

        ds::table
            .inner_join(a::table.on(a::id.eq(ds::id)))
            .filter(a::node_id.eq(node.as_str()))
            .select(ds::all_columns)
            .load::<Schema>(self.0.as_ref())?
            .into_iter()
            .map(|schema| Site::try_from(schema))
            .collect::<Result<Vec<Site>, _>>()
    }

    pub fn fill_assignments(
        &self,
        mut infos: Vec<status::Info>,
    ) -> Result<Vec<status::Info>, StoreError> {
        use deployment_schemas as ds;
        use subgraph_deployment_assignment as a;

        let ids: Vec<_> = infos.iter().map(|info| &info.subgraph).collect();
        let nodes: HashMap<_, _> = a::table
            .inner_join(ds::table.on(ds::id.eq(a::id)))
            .filter(ds::subgraph.eq(any(ids)))
            .select((ds::subgraph, a::node_id))
            .load::<(String, String)>(self.0.as_ref())?
            .into_iter()
            .collect();
        for mut info in &mut infos {
            info.node = nodes.get(&info.subgraph).map(|s| s.clone());
        }
        Ok(infos)
    }

    pub(crate) fn deployments_for_subgraph(&self, name: String) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_version as v;

        ds::table
            .inner_join(v::table.on(ds::subgraph.eq(v::deployment)))
            .inner_join(s::table.on(v::subgraph.eq(s::id)))
            .filter(s::name.eq(&name))
            .filter(ds::active)
            .order_by(v::created_at.asc())
            .select(ds::all_columns)
            .load::<Schema>(self.0.as_ref())?
            .into_iter()
            .map(|schema| Site::try_from(schema))
            .collect::<Result<Vec<Site>, _>>()
    }

    pub fn subgraph_version(
        &self,
        name: String,
        use_current: bool,
    ) -> Result<Option<Site>, StoreError> {
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_version as v;

        let deployment = if use_current {
            ds::table
                .select(ds::all_columns)
                .inner_join(v::table.on(ds::subgraph.eq(v::deployment)))
                .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
                .filter(s::name.eq(&name))
                .filter(ds::active)
                .first::<Schema>(self.0.as_ref())
        } else {
            ds::table
                .select(ds::all_columns)
                .inner_join(v::table.on(ds::subgraph.eq(v::deployment)))
                .inner_join(s::table.on(s::pending_version.eq(v::id.nullable())))
                .filter(s::name.eq(&name))
                .filter(ds::active)
                .first::<Schema>(self.0.as_ref())
        };
        deployment
            .optional()?
            .map(|schema| Site::try_from(schema))
            .transpose()
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
    pub fn detect_unused_deployments(&self) -> Result<Vec<Site>, StoreError> {
        use active_copies as cp;
        use deployment_schemas as ds;
        use subgraph as s;
        use subgraph_deployment_assignment as a;
        use subgraph_version as v;
        use unused_deployments as u;

        // Deployment is assigned
        let assigned = a::table.filter(a::id.eq(ds::id));
        // Deployment is current or pending version
        let active = v::table
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

        let unused = ds::table
            .filter(not(exists(assigned)))
            .filter(not(exists(active)))
            .filter(not(exists(copy_src)))
            .select((ds::id, ds::subgraph, ds::name, ds::shard, used_by));

        let ids = insert_into(u::table)
            .values(unused)
            .into_columns((u::id, u::deployment, u::namespace, u::shard, u::subgraphs))
            .on_conflict(u::id)
            .do_nothing()
            .returning(u::id)
            .get_results::<DeploymentId>(self.0.as_ref())?;

        // We need to load again since we do not record the network in
        // unused_deployments
        ds::table
            .filter(ds::id.eq_any(ids))
            .select(ds::all_columns)
            .load::<Schema>(self.0.as_ref())?
            .into_iter()
            .map(|schema| Site::try_from(schema))
            .collect()
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
                &detail.deployment,
                "latest_ethereum_block",
                detail.latest_ethereum_block_hash.clone(),
                detail.latest_ethereum_block_number.clone(),
            )?
            .map(|b| b.to_ptr())
            .map(|ptr| (Some(Vec::from(ptr.hash_slice())), Some(ptr.number as i32)))
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

    pub fn subgraphs_using_deployment(&self, site: &Site) -> Result<Vec<String>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        Ok(s::table
            .inner_join(
                v::table.on(v::subgraph
                    .nullable()
                    .eq(s::current_version)
                    .or(v::subgraph.nullable().eq(s::pending_version))),
            )
            .filter(v::deployment.eq(site.deployment.as_str()))
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

    pub fn record_active_copy(&self, src: &Site, dst: &Site) -> Result<(), StoreError> {
        use active_copies as cp;

        insert_into(cp::table)
            .values((
                cp::src.eq(src.id),
                cp::dst.eq(dst.id),
                cp::queued_at.eq(sql("now()")),
            ))
            .on_conflict_do_nothing()
            .execute(self.0.as_ref())?;

        Ok(())
    }

    pub fn copy_finished(&self, dst: &Site) -> Result<(), StoreError> {
        use active_copies as cp;

        delete(cp::table.filter(cp::dst.eq(dst.id))).execute(self.0.as_ref())?;

        Ok(())
    }
}

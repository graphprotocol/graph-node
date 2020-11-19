//! Utilities for dealing with subgraph metadata that resides in the primary
//! shard. Anything in this module can only be used with a database connection
//! for the primary shard.
use diesel::sql_types::Text;
use diesel::{
    dsl::{delete, insert_into, sql, update},
    r2d2::PooledConnection,
};
use diesel::{pg::PgConnection, r2d2::ConnectionManager};
use diesel::{
    prelude::{
        ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
        RunQueryDsl,
    },
    Connection as _,
};
use failure::format_err;
use graph::{
    data::subgraph::schema::SUBGRAPHS_ID,
    prelude::{
        entity, serde_json, EntityChange, EntityChangeOperation, MetadataOperation, NodeId,
        StoreError, SubgraphDeploymentId, SubgraphName, SubgraphVersionSwitchingMode, TypedEntity,
    },
};
use graph::{
    data::subgraph::schema::{generate_entity_id, SubgraphDeploymentAssignmentEntity},
    prelude::StoreEvent,
};
use std::{
    convert::TryFrom,
    convert::TryInto,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    block_range::UNVERSIONED_RANGE, notification_listener::JsonNotification, sharded_store::Shard,
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

/// We used to support different storage schemes. The old 'Split' scheme
/// which used JSONB storage has been removed, and we will only deal
/// with relational storage. Trying to do anything with a 'Split' subgraph
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
        /// The subgraph storage scheme used for this subgraph
        version -> crate::primary::DeploymentSchemaVersionMapping,
    }
}

allow_tables_to_appear_in_same_query!(subgraph, subgraph_version, deployment_schemas);

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
}

/// Details about a deployment and the shard in which it is stored. We need
/// the database namespace for the deployment as that information is only
/// stored in the primary database
pub struct Site {
    /// The subgraph deployment
    pub deployment: SubgraphDeploymentId,
    /// The name of the database shard
    pub shard: Shard,
    /// The database namespace (schema) that holds the data for the deployment
    pub namespace: String,
}

impl Site {
    /// A site that can be used to access the metadata subgraph in the given
    /// shard
    pub fn meta(shard: Shard) -> Self {
        Site {
            deployment: SUBGRAPHS_ID.clone(),
            namespace: SUBGRAPHS_ID.to_string(),
            shard,
        }
    }
}

impl TryFrom<Schema> for Site {
    type Error = StoreError;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        let deployment = SubgraphDeploymentId::new(schema.subgraph)
            .map_err(|s| StoreError::ConstraintViolation(format!("Invalid deployment id {}", s)))?;
        let shard = Shard::new(schema.shard)?;
        Ok(Self {
            deployment,
            namespace: schema.name,
            shard,
        })
    }
}

/// A wrapper for a database connection that provides access to functionality
/// that works only on the primary database
pub struct Connection(PooledConnection<ConnectionManager<PgConnection>>);

impl Connection {
    pub fn new(conn: PooledConnection<ConnectionManager<PgConnection>>) -> Self {
        Self(conn)
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
            .first::<String>(&self.0)
            .optional()?;
        match id {
            Some(id) => SubgraphDeploymentId::new(id).map_err(|id| {
                StoreError::ConstraintViolation(format!("illegal deployment id: {}", id))
            }),
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
            .load::<Removed>(&self.0)?
            .into_iter()
            .map(|r| {
                MetadataOperation::Remove {
                    entity: SubgraphDeploymentAssignmentEntity::TYPENAME,
                    id: r.id,
                }
                .into()
            })
            .collect::<Vec<_>>())
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

        let conn = &self.0;

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

        let conn = &self.0;
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

        let conn = &self.0;

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
                MetadataOperation::entity_key(
                    SubgraphDeploymentAssignmentEntity::TYPENAME,
                    id.to_string(),
                ),
                EntityChangeOperation::Set,
            );
            changes.push(change);
        }
        Ok(changes)
    }

    pub fn remove_subgraph(&self, name: SubgraphName) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph as s;
        use subgraph_version as v;

        let conn = &self.0;

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

    pub fn reassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
        node: &NodeId,
    ) -> Result<Vec<EntityChange>, StoreError> {
        use subgraph_deployment_assignment as a;

        let conn = &self.0;
        let updates = update(a::table.filter(a::id.eq(id.as_str())))
            .set(a::node_id.eq(node.as_str()))
            .execute(conn)?;
        match updates {
            0 => Err(StoreError::DeploymentNotFound(id.to_string())),
            1 => {
                let op = MetadataOperation::Set {
                    entity: SubgraphDeploymentAssignmentEntity::TYPENAME,
                    id: id.to_string(),
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

    pub fn allocate_site(
        &self,
        shard: Shard,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Site, StoreError> {
        use deployment_schemas as ds;
        use DeploymentSchemaVersion as v;

        let conn = &self.0;

        if let Some(schema) = self.find_site(subgraph)? {
            return Ok(schema);
        }

        // Create a schema for the deployment.
        let schemas: Vec<String> = diesel::insert_into(ds::table)
            .values((
                ds::subgraph.eq(subgraph.as_str()),
                ds::shard.eq(shard.as_str()),
                ds::version.eq(v::Relational),
            ))
            .returning(ds::name)
            .get_results(conn)?;
        let name = schemas
            .first()
            .cloned()
            .ok_or_else(|| format_err!("failed to read schema name for {} back", subgraph))?;

        Ok(Site {
            deployment: subgraph.clone(),
            namespace: name,
            shard,
        })
    }

    pub fn find_site(&self, subgraph: &SubgraphDeploymentId) -> Result<Option<Site>, StoreError> {
        let schema = deployment_schemas::table
            .filter(deployment_schemas::subgraph.eq(subgraph.to_string()))
            .first::<Schema>(&self.0)
            .optional()?;
        if let Some(Schema { version, .. }) = schema {
            if matches!(version, DeploymentSchemaVersion::Split) {
                return Err(StoreError::ConstraintViolation(format!(
                    "the subgraph {} uses JSONB storage which is not supported any longer",
                    subgraph.as_str()
                )));
            }
        }
        schema.map(|schema| schema.try_into()).transpose()
    }

    pub fn find_existing_site(&self, subgraph: &SubgraphDeploymentId) -> Result<Site, StoreError> {
        self.find_site(subgraph)?
            .ok_or_else(|| StoreError::DeploymentNotFound(subgraph.to_string()))
    }

    // Only restricted to tests because we don't need it anywhere else
    // Would be fine to expose in 'normal' code
    #[cfg(debug_assertions)]
    pub fn sites(&self) -> Result<Vec<Site>, StoreError> {
        use deployment_schemas as ds;

        ds::table
            .filter(ds::name.ne("subgraphs"))
            .load::<Schema>(&self.0)?
            .into_iter()
            .map(|schema| schema.try_into())
            .collect()
    }

    pub fn send_store_event(&self, event: &StoreEvent) -> Result<(), StoreError> {
        let v = serde_json::to_value(event)?;
        #[cfg(debug_assertions)]
        {
            if *EVENT_TAP_ENABLED.lock().unwrap() {
                EVENT_TAP.lock().unwrap().push(event.clone());
            }
        }
        JsonNotification::send("store_events", &v, &self.0)
    }
}

pub(crate) fn deployments_for_subgraph(
    conn: &PgConnection,
    name: String,
) -> Result<Vec<String>, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

    Ok(v::table
        .inner_join(s::table.on(v::subgraph.eq(s::id)))
        .filter(s::name.eq(&name))
        .order_by(v::created_at.asc())
        .select(v::deployment)
        .load(conn)?)
}

pub fn subgraph_version(
    conn: &PgConnection,
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
            .first::<Option<String>>(conn)
    } else {
        v::table
            .select(v::deployment.nullable())
            .inner_join(s::table.on(s::pending_version.eq(v::id.nullable())))
            .filter(s::name.eq(&name))
            .first::<Option<String>>(conn)
    };
    Ok(deployment.optional()?.flatten())
}

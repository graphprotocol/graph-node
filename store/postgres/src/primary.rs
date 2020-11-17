//! Utilities for dealing with subgraph metadata that resides in the primary
//! shard. Anything in this module can only be used with a database connection
//! for the primary shard.
use diesel::dsl::{delete, insert_into, sql, update};
use diesel::pg::PgConnection;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
    RunQueryDsl,
};
use diesel::sql_types::Text;
use graph::data::subgraph::schema::{generate_entity_id, SubgraphDeploymentAssignmentEntity};
use graph::prelude::{
    entity, EntityChange, EntityChangeOperation, MetadataOperation, NodeId, StoreError,
    SubgraphDeploymentId, SubgraphName, SubgraphVersionSwitchingMode, TypedEntity,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::block_range::UNVERSIONED_RANGE;

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

allow_tables_to_appear_in_same_query!(subgraph, subgraph_version);

pub fn current_deployment_for_subgraph(
    conn: &PgConnection,
    name: SubgraphName,
) -> Result<SubgraphDeploymentId, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

    let id = v::table
        .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
        .filter(s::name.eq(name.as_str()))
        .select(v::deployment)
        .first::<String>(conn)
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
fn remove_unused_assignments(conn: &PgConnection) -> Result<Vec<EntityChange>, StoreError> {
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
        .load::<Removed>(conn)?
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
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

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
        remove_unused_assignments(conn)?
    };
    Ok(changes)
}

/// Create a new subgraph with the given name. If one already exists, use
/// the existing one. Return the `id` of the newly created or existing
/// subgraph
pub fn create_subgraph(conn: &PgConnection, name: &SubgraphName) -> Result<String, StoreError> {
    use subgraph as s;

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
    conn: &PgConnection,
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
        None => (create_subgraph(conn, &name)?, None),
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
    let mut changes = remove_unused_assignments(conn)?;
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

pub fn remove_subgraph(
    conn: &PgConnection,
    name: SubgraphName,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

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
        remove_unused_assignments(conn)
    } else {
        Ok(vec![])
    }
}

pub fn reassign_subgraph(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    node: &NodeId,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph_deployment_assignment as a;

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

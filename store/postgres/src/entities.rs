use diesel::deserialize::QueryableByName;
use diesel::dsl::{any, sql};
use diesel::pg::{Pg, PgConnection};
use diesel::sql_types::{Jsonb, Nullable, Text};
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::{debug_query, insert_into};
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use diesel_dynamic_schema::{schema, Column, Table as DynamicTable};
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::{
    format_err, EntityChange, EntityChangeOperation, EntityFilter, EntityKey, Error, EventSource,
    QueryExecutionError, StoreError, StoreEvent, SubgraphDeploymentId, TransactionAbortError,
};
use graph::serde_json;

use crate::filter::{build_filter, store_filter};
use crate::functions::set_config;
use crate::jsonb::PgJsonbExpressionMethods as _;

/// Marker trait for tables that store entities
pub(crate) trait EntitySource {}

// The entities and related tables in the public schema. We put them in
// this module to make sure that nobody else gets access to them. All access
// to these tables must go through functions in this module.
mod public {
    table! {
        entities (id, subgraph, entity) {
            id -> Varchar,
            subgraph -> Varchar,
            entity -> Varchar,
            data -> Jsonb,
            event_source -> Varchar,
        }
    }

    table! {
        entity_history (id) {
            id -> Integer,
            // This is a BigInt in the database, but if we mark it that
            // diesel won't let us join event_meta_data and entity_history
            // Since event_meta_data.id is Integer, it shouldn't matter
            // that we call it Integer here
            event_id -> Integer,
            entity_id -> Varchar,
            subgraph -> Varchar,
            entity -> Varchar,
            data_before -> Nullable<Jsonb>,
            data_after -> Nullable<Jsonb>,
            op_id -> SmallInt,
            reversion -> Bool,
        }
    }

    table! {
        event_meta_data (id) {
            id -> Integer,
            db_transaction_id -> BigInt,
            db_transaction_time -> Timestamp,
            source -> Nullable<Varchar>,
        }
    }

    joinable!(entity_history -> event_meta_data (event_id));
    allow_tables_to_appear_in_same_query!(entity_history, event_meta_data);

    table! {
        deployment_schemas(id) {
            id -> Integer,
            subgraph -> Text,
            name -> Text,
        }
    }
}

// The entities table for the subgraph of subgraphs.
mod subgraphs {
    table! {
        subgraphs.entities (entity, id) {
            entity -> Varchar,
            id -> Varchar,
            data -> Jsonb,
            event_source -> Varchar,
        }
    }
}

impl EntitySource for self::public::entities::table {}

impl EntitySource for self::subgraphs::entities::table {}

// This is a bit weak, as any DynamicTable<String> is now an EntitySource
impl EntitySource for DynamicTable<String> {}

/// Support for the management of the schemas and tables we create in
/// the database for each deployment. The Postgres schemas for each
/// deployment/subgraph are tracked in the `deployment_schemas` table.
///
/// The functions in this module are very low-level and should only be used
/// directly by the Postgres store, and nowhere else. At the same time, all
/// manipulation of entities in the database should go through this module
/// to make it easier to handle future schema changes
// We use Diesel's dynamic table support for querying the entities and history
// tables of a subgraph. Unfortunately, this support is not good enough for
// modifying data, and we fall back to generating literal SQL queries for that.
// For the `entities` table of the subgraph of subgraphs, we do map the table
// statically and use it in some cases to bridge the gap between dynamic and
// static table support, in particular in the update operation for entities.
// Diesel deeply embeds the assumption that all schema is known at compile time;
// for example, the column for a dynamic table can not implement
// `diesel::query_source::Column` since that must carry the column name as a
// constant. As a consequence, a lot of Diesel functionality is not available
// for dynamic tables.
use public::deployment_schemas;

#[derive(Queryable, QueryableByName, Debug)]
#[table_name = "deployment_schemas"]
struct Schema {
    id: i32,
    subgraph: String,
    name: String,
}

type EntityColumn<ST> = Column<DynamicTable<String>, String, ST>;

/// A table representing a split entities table, i.e. a setup where
/// a subgraph deployment's entity are split into their own schema rather
/// than residing in the entities table in the `public` database schema
#[derive(Debug, Clone)]
pub(crate) struct SplitTable {
    /// The name of the database schema
    schema: String,
    /// The subgraph id
    subgraph: SubgraphDeploymentId,
    table: DynamicTable<String>,
    id: EntityColumn<diesel::sql_types::Text>,
    entity: EntityColumn<diesel::sql_types::Text>,
    data: EntityColumn<diesel::sql_types::Jsonb>,
    event_source: EntityColumn<diesel::sql_types::Text>,
}

#[derive(Queryable)]
struct RawHistory {
    id: i32,
    entity: String,
    entity_id: String,
    data: Option<serde_json::Value>,
    // The operation that lead to this history record
    // 0 = INSERT, 1 = UPDATE, 2 = DELETE
    op: i16,
}

impl QueryableByName<Pg> for RawHistory {
    // Extract one RawHistory entry from the database. The names of the columns
    // must follow exactly the names used in the queries in revert_block
    fn build<R: diesel::row::NamedRow<Pg>>(row: &R) -> diesel::deserialize::Result<Self> {
        Ok(RawHistory {
            id: row.get("id")?,
            entity: row.get("entity")?,
            entity_id: row.get("entity_id")?,
            data: row.get::<Nullable<Jsonb>, _>("data_before")?,
            op: row.get("op_id")?,
        })
    }
}

pub(crate) enum Table {
    Public(SubgraphDeploymentId),
    Split(SplitTable),
}

// Find the database schema for `subgraph`. If no explicit schema exists,
// return `None`.
fn find_schema(
    conn: &diesel::pg::PgConnection,
    subgraph: &SubgraphDeploymentId,
) -> Result<Option<Schema>, StoreError> {
    Ok(deployment_schemas::table
        .filter(deployment_schemas::subgraph.eq(subgraph.to_string()))
        .first::<Schema>(conn)
        .optional()?)
}

impl SplitTable {
    // Update for a split entities table, called from Table.update. It's lengthy,
    // so we split it into its own helper function
    fn update(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        data: &serde_json::Value,
        guard: Option<EntityFilter>,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        if !guard.is_none() && key.subgraph_id != *SUBGRAPHS_ID {
            // We can only make adding additional conditions to the update
            // operation work for a query that is fully generated with
            // Diesel's DSL. Trying to combine the result of build_filter
            // with a direct query is too cumbersome because of various type
            // system gyrations. For now, we also only need update guards for
            // the subgraph of subgraphs
            panic!("update guards are only possible for the 'subgraphs' subgraph");
        }
        if let Some(filter) = guard {
            // Update for subgraph of subgraphs with a guard
            use self::subgraphs::entities;

            let filter = build_filter(filter).map_err(|e| {
                TransactionAbortError::Other(format!(
                    "invalid filter '{}' for value '{}'",
                    e.filter, e.value
                ))
            })?;

            let target = entities::table
                .filter(entities::entity.eq(&key.entity_type))
                .filter(entities::id.eq(&key.entity_id));

            Ok(diesel::update(target)
                .set((
                    entities::data.eq(entities::data.merge(&data)),
                    entities::event_source.eq(event_source.to_string()),
                ))
                .filter(filter)
                .execute(conn)?)
        } else {
            // If there is no guard (which has to include all 'normal' subgraphs),
            // we need to use a direct query since diesel::update does not like
            // dynamic tables.
            let query = format!(
                "UPDATE {}.entities
                   SET data = data || $3, event_source = $4
                   WHERE entity = $1 AND id = $2",
                self.schema
            );
            let query = diesel::sql_query(query)
                .bind::<Text, _>(&key.entity_type)
                .bind::<Text, _>(&key.entity_id)
                .bind::<Jsonb, _>(data)
                .bind::<Text, _>(event_source.to_string());
            query.execute(conn).map_err(|e| {
                format_err!(
                    "Failed to update entity ({}, {}, {}): {}",
                    key.subgraph_id,
                    key.entity_type,
                    key.entity_id,
                    e
                )
                .into()
            })
        }
    }
}

impl Table {
    /// Look up the schema for `subgraph` and return its entity table.
    /// If `subgraph` does not have an entry in `deployment_schemas`, we assume
    /// it has not been migrated yet, and return the `entities` table
    /// in the `public` schema
    fn new(conn: &PgConnection, subgraph: &SubgraphDeploymentId) -> Result<Self, StoreError> {
        let table = match find_schema(conn, subgraph)? {
            Some(sc) => {
                let table = schema(sc.name.clone()).table("entities".to_owned());
                let id = table.column::<Text, _>("id".to_string());
                let entity = table.column::<Text, _>("entity".to_string());
                let data = table.column::<Jsonb, _>("data".to_string());
                let event_source = table.column::<Text, _>("event_source".to_string());

                Table::Split(SplitTable {
                    schema: sc.name,
                    subgraph: subgraph.clone(),
                    table,
                    id,
                    entity,
                    data,
                    event_source,
                })
            }
            None => Table::Public(subgraph.clone()),
        };
        Ok(table)
    }

    fn entity_key(&self, entity_type: String, entity_id: String) -> EntityKey {
        let subgraph_id = match self {
            Table::Public(subgraph) => subgraph.clone(),
            Table::Split(entities) => entities.subgraph.clone(),
        };
        EntityKey {
            subgraph_id,
            entity_type,
            entity_id,
        }
    }

    pub(crate) fn find(
        &self,
        conn: &PgConnection,
        entity: &String,
        id: &String,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        match self {
            Table::Public(subgraph) => Ok(public::entities::table
                .find((id, subgraph.to_string(), entity))
                .select(public::entities::data)
                .first::<serde_json::Value>(conn)
                .optional()?),
            Table::Split(entities) => {
                let entities = entities.clone();
                Ok(entities
                    .table
                    .filter(entities.entity.eq(entity).and(entities.id.eq(id)))
                    .select(entities.data)
                    .first::<serde_json::Value>(conn)
                    .optional()?)
            }
        }
    }

    /// order is a tuple (attribute, cast, direction)
    pub(crate) fn query(
        &self,
        conn: &PgConnection,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, &str, &str)>,
        first: Option<u32>,
        skip: u32,
    ) -> Result<Vec<(serde_json::Value, String)>, QueryExecutionError> {
        match self {
            Table::Public(subgraph) => {
                // Create base boxed query; this will be added to based on the
                // query parameters provided
                let mut query = public::entities::table
                    .filter(public::entities::entity.eq(any(entity_types)))
                    .filter(public::entities::subgraph.eq(subgraph.to_string()))
                    .select((public::entities::data, public::entities::entity))
                    .into_boxed::<Pg>();

                // Add specified filter to query
                if let Some(filter) = filter {
                    query =
                        store_filter::<public::entities::table, _>(query, filter).map_err(|e| {
                            QueryExecutionError::FilterNotSupportedError(
                                format!("{}", e.value),
                                e.filter,
                            )
                        })?;
                }

                // Add order by filters to query
                if let Some((attribute, cast, direction)) = order {
                    query = query.order(
                        sql::<Text>("(data ->")
                            .bind::<Text, _>(attribute)
                            .sql("->> 'data')")
                            .sql(cast)
                            .sql(" ")
                            .sql(direction)
                            .sql(" NULLS LAST"),
                    );
                }

                // Add range filter to query
                if let Some(first) = first {
                    query = query.limit(first as i64);
                }
                if skip > 0 {
                    query = query.offset(skip as i64);
                }

                let query_debug_info = debug_query(&query).to_string();

                // Process results; deserialize JSON data
                query
                    .load::<(serde_json::Value, String)>(conn)
                    .map_err(|e| {
                        QueryExecutionError::ResolveEntitiesError(format!(
                            "{}, query = {:?}",
                            e, query_debug_info
                        ))
                    })
            }
            Table::Split(entities) => {
                let entities = entities.clone();

                let mut query = entities
                    .table
                    .filter((&entities.entity).eq(any(entity_types)))
                    .select((&entities.data, &entities.entity))
                    .into_boxed::<Pg>();

                if let Some(filter) = filter {
                    query = store_filter(query, filter).map_err(|e| {
                        QueryExecutionError::FilterNotSupportedError(
                            format!("{}", e.value),
                            e.filter,
                        )
                    })?;
                }

                if let Some((attribute, cast, direction)) = order {
                    query = query.order(
                        sql::<Text>("(data ->")
                            .bind::<Text, _>(attribute)
                            .sql("->> 'data')")
                            .sql(cast)
                            .sql(" ")
                            .sql(direction)
                            .sql(" NULLS LAST"),
                    );
                }

                if let Some(first) = first {
                    query = query.limit(first as i64);
                }
                if skip > 0 {
                    query = query.offset(skip as i64);
                }

                let query_debug_info = debug_query(&query).to_string();

                query
                    .load::<(serde_json::Value, String)>(conn)
                    .map_err(|e| {
                        QueryExecutionError::ResolveEntitiesError(format!(
                            "{}, query = {:?}",
                            e, query_debug_info
                        ))
                    })
            }
        }
    }

    pub(crate) fn upsert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        data: &serde_json::Value,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        match self {
            Table::Public(subgraph) => Ok(insert_into(public::entities::table)
                .values((
                    public::entities::id.eq(&key.entity_id),
                    public::entities::entity.eq(&key.entity_type),
                    public::entities::subgraph.eq(subgraph.to_string()),
                    public::entities::data.eq(data),
                    public::entities::event_source.eq(&event_source.to_string()),
                ))
                .on_conflict((
                    public::entities::id,
                    public::entities::entity,
                    public::entities::subgraph,
                ))
                .do_update()
                .set((
                    public::entities::data.eq(data),
                    public::entities::event_source.eq(&event_source.to_string()),
                ))
                .execute(conn)?),
            Table::Split(entities) => {
                let query = format!(
                    "INSERT into {}.entities(entity, id, data, event_source)
                                VALUES($1, $2, $3, $4)
                                ON CONFLICT(entity, id)
                                DO UPDATE SET data = $3, event_source = $4",
                    entities.schema
                );
                let query = diesel::sql_query(query)
                    .bind::<Text, _>(&key.entity_type)
                    .bind::<Text, _>(&key.entity_id)
                    .bind::<Jsonb, _>(data)
                    .bind::<Text, _>(event_source.to_string());
                Ok(query.execute(conn)?)
            }
        }
    }

    pub(crate) fn update(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        data: &serde_json::Value,
        guard: Option<EntityFilter>,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        match self {
            Table::Public(subgraph) => {
                let target = public::entities::table
                    .filter(public::entities::subgraph.eq(subgraph.to_string()))
                    .filter(public::entities::entity.eq(&key.entity_type))
                    .filter(public::entities::id.eq(&key.entity_id));

                let query = diesel::update(target).set((
                    public::entities::data.eq(public::entities::data.merge(data)),
                    public::entities::event_source.eq(event_source.to_string()),
                ));

                match guard {
                    Some(filter) => {
                        let filter = build_filter(filter).map_err(|e| {
                            TransactionAbortError::Other(format!(
                                "invalid filter '{}' for value '{}'",
                                e.filter, e.value
                            ))
                        })?;
                        Ok(query.filter(filter).execute(conn)?)
                    }
                    None => Ok(query.execute(conn)?),
                }
            }
            Table::Split(entities) => entities.update(conn, key, data, guard, event_source),
        }
    }

    pub(crate) fn delete(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        diesel::select(set_config(
            "vars.current_event_source",
            event_source.to_string(),
            true,
        ))
        .execute(conn)
        .map_err(|e| format_err!("Failed to set event source for remove operation: {}", e))
        .map(|_| ())?;

        match self {
            Table::Public(subgraph) => Ok(diesel::delete(
                public::entities::table
                    .filter(public::entities::subgraph.eq(subgraph.to_string()))
                    .filter(public::entities::entity.eq(&key.entity_type))
                    .filter(public::entities::id.eq(&key.entity_id)),
            )
            .execute(conn)?),
            Table::Split(entities) => {
                let query = format!(
                    "DELETE FROM {}.entities
                    WHERE entity = $1 AND id = $2",
                    entities.schema
                );
                let query = diesel::sql_query(query)
                    .bind::<Text, _>(&key.entity_type)
                    .bind::<Text, _>(&key.entity_id);
                Ok(query.execute(conn)?)
            }
        }
    }

    pub(crate) fn count_entities(&self, conn: &PgConnection) -> Result<u64, Error> {
        match self {
            Table::Public(subgraph) => {
                let count: i64 = public::entities::table
                    .filter(public::entities::subgraph.eq(subgraph.to_string()))
                    .count()
                    .get_result(conn)?;
                Ok(count as u64)
            }
            Table::Split(entities) => {
                let table = entities.table.clone();
                let count: i64 = table.count().get_result(conn)?;
                Ok(count as u64)
            }
        }
    }

    pub(crate) fn conflicting_entity(
        &self,
        conn: &PgConnection,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        match self {
            Table::Public(subgraph) => Ok(public::entities::table
                .select(public::entities::entity)
                .filter(public::entities::subgraph.eq(subgraph.to_string()))
                .filter(public::entities::entity.eq(any(entities)))
                .filter(public::entities::id.eq(entity_id))
                .first(conn)
                .optional()?),
            Table::Split(ents) => {
                let ents = ents.clone();
                Ok(ents
                    .table
                    .select(ents.entity.clone())
                    .filter(ents.entity.eq(any(entities)))
                    .filter(ents.id.eq(entity_id))
                    .first(conn)
                    .optional()?)
            }
        }
    }

    /// Revert the block with the given `block_ptr` which must be the hash
    /// of the block to revert. The returned `StoreEvent` reflects the changes
    /// that were made during reversion
    pub(crate) fn revert_block(
        &self,
        conn: &PgConnection,
        block_ptr: String,
    ) -> Result<StoreEvent, StoreError> {
        let entries: Vec<RawHistory> = match self {
            Table::Public(subgraph) => {
                use self::public::entity_history::dsl as h;
                use self::public::event_meta_data as m;

                h::entity_history
                    .inner_join(m::table)
                    .select((h::id, h::entity, h::entity_id, h::data_before, h::op_id))
                    .filter(h::subgraph.eq(subgraph.to_string()))
                    .filter(m::source.eq(&block_ptr))
                    .order(h::event_id.desc())
                    .load(conn)?
            }
            Table::Split(entities) => {
                // We can't use Diesel's JoinOnDsl here because DynamicTable
                // does not implement AppearsInFromClause, so we have to run
                // a raw SQL query
                let query = format!(
                    "SELECT h.id, h.entity, h.entity_id, h.data_before, h.op_id
                    FROM {}.entity_history h, event_meta_data m
                    WHERE m.id = h.event_id
                      AND m.source = $1
                    ORDER BY h.event_id desc",
                    entities.schema
                );

                let query = diesel::sql_query(query).bind::<Text, _>(&block_ptr);

                query.get_results(conn)?
            }
        };

        let subgraph_id = match self {
            Table::Public(subgraph) => subgraph.clone(),
            Table::Split(entities) => entities.subgraph.clone(),
        };

        let mut changes = Vec::with_capacity(entries.len());
        for history in entries.into_iter() {
            // Perform the actual reversion
            let key = self.entity_key(history.entity.clone(), history.entity_id.clone());
            match history.op {
                0 => {
                    // Reverse an insert
                    self.delete(conn, &key, EventSource::None)?;
                }
                1 | 2 => {
                    // Reverse an update or delete
                    if let Some(data) = history.data {
                        self.upsert(conn, &key, &data, EventSource::None)?;
                    } else {
                        return Err(StoreError::Unknown(format_err!(
                            "History entry for update/delete has NULL data_before. id={}, op={}",
                            history.id,
                            history.op
                        )));
                    }
                }
                _ => {
                    return Err(StoreError::Unknown(format_err!(
                        "bad operation {}",
                        history.op
                    )))
                }
            }
            // Record the change that was just made
            let change = EntityChange {
                subgraph_id: subgraph_id.clone(),
                entity_type: history.entity,
                entity_id: history.entity_id,
                operation: match history.op {
                    0 => EntityChangeOperation::Removed,
                    1 | 2 => EntityChangeOperation::Set,
                    _ => {
                        return Err(StoreError::Unknown(format_err!(
                            "bad operation {}",
                            history.op
                        )))
                    }
                },
            };
            changes.push(change);
        }

        Ok(StoreEvent::new(changes))
    }
}

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
pub fn delete_all_entities_for_test_use_only(conn: &PgConnection) -> Result<usize, StoreError> {
    // Delete public entities and related data
    let mut rows = diesel::delete(public::entities::table).execute(conn)?;
    rows = rows + diesel::delete(public::entity_history::table).execute(conn)?;
    rows = rows + diesel::delete(public::event_meta_data::table).execute(conn)?;
    // Delete subgraphs entities
    rows = rows + diesel::delete(subgraphs::entities::table).execute(conn)?;
    Ok(rows)
}

/// Return a table for the subgraph
pub(crate) fn table(
    conn: &PgConnection,
    subgraph: &SubgraphDeploymentId,
) -> Result<Table, StoreError> {
    Table::new(conn, subgraph)
}

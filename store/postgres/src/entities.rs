use diesel::dsl::{any, sql};
use diesel::pg::{Pg, PgConnection};
use diesel::sql_types::Text;
use diesel::ExpressionMethods;
use diesel::{debug_query, insert_into};
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use graph::prelude::{
    format_err, EntityChange, EntityChangeOperation, EntityFilter, EntityKey, Error, EventSource,
    QueryExecutionError, StoreError, StoreEvent, SubgraphDeploymentId, TransactionAbortError,
};
use graph::serde_json;

use crate::filter::{build_filter, store_filter};
use crate::functions::{revert_block, set_config};
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
}

impl EntitySource for self::public::entities::table {}

//impl EntitySource for DynamicTable<String> {}

pub(crate) enum Table {
    Public(SubgraphDeploymentId),
}

impl Table {
    fn new(subgraph: &SubgraphDeploymentId) -> Self {
        Table::Public(subgraph.clone())
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
        match self {
            Table::Public(subgraph) => {
                use self::public::entity_history::dsl as h;
                use self::public::event_meta_data as m;

                let entries: Vec<(String, String, i16)> = h::entity_history
                    .inner_join(m::table)
                    .select((h::entity, h::entity_id, h::op_id))
                    .filter(h::subgraph.eq(&subgraph.to_string()))
                    .filter(m::source.eq(&block_ptr))
                    .order(h::event_id.desc())
                    .load(conn)?;

                let mut changes = Vec::with_capacity(entries.len());
                for (entity_type, entity_id, op) in entries.into_iter() {
                    let change = EntityChange {
                        subgraph_id: subgraph.clone(),
                        entity_type,
                        entity_id,
                        operation: match op {
                            0 => EntityChangeOperation::Removed,
                            1 | 2 => EntityChangeOperation::Set,
                            _ => {
                                return Err(StoreError::Unknown(format_err!(
                                    "bad operation {}",
                                    op
                                )))
                            }
                        },
                    };
                    changes.push(change);
                }

                public::entities::table
                    .select(revert_block(block_ptr, subgraph.to_string()))
                    .execute(conn)?;
                Ok(StoreEvent::new(changes))
            }
        }
    }
}

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
pub fn delete_all_entities_for_test_use_only(conn: &PgConnection) -> Result<usize, StoreError> {
    let mut rows = diesel::delete(public::entities::table).execute(conn)?;
    rows = rows + diesel::delete(public::entity_history::table).execute(conn)?;
    rows = rows + diesel::delete(public::event_meta_data::table).execute(conn)?;
    Ok(rows)
}

/// Return a table for the subgraph
pub(crate) fn table(subgraph: &SubgraphDeploymentId) -> Table {
    Table::new(subgraph)
}

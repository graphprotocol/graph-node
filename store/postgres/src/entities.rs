use diesel::connection::SimpleConnection;
use diesel::deserialize::QueryableByName;
use diesel::dsl::{any, sql};
use diesel::pg::{Pg, PgConnection};
use diesel::sql_types::{Bool, Integer, Jsonb, Nullable, Text};
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::{debug_query, insert_into};
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use diesel_dynamic_schema::{schema, Column, Table as DynamicTable};
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::{
    format_err, AttributeIndexDefinition, EntityChange, EntityChangeOperation, EntityFilter,
    EntityKey, Error, EventSource, HistoryEvent, QueryExecutionError, StoreError, StoreEvent,
    SubgraphDeploymentId, TransactionAbortError, ValueType,
};
use graph::serde_json;
use inflector::cases::snakecase::to_snake_case;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::filter::{build_filter, store_filter};
use crate::functions::set_config;
use crate::jsonb::PgJsonbExpressionMethods as _;

enum OperationType {
    Insert,
    Update,
    Delete,
}

impl Into<i32> for OperationType {
    fn into(self) -> i32 {
        match self {
            OperationType::Insert => 0,
            OperationType::Update => 1,
            OperationType::Delete => 2,
        }
    }
}

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

#[derive(Clone, Debug)]
pub(crate) enum Table {
    Public(SubgraphDeploymentId),
    Split(SplitTable),
}

/// A connection into the database to handle entities which caches the
/// mapping to actual database tables. Instances of this struct must not be
/// cached across transactions as we do not track possible changes to
/// entity storage, such as migrating a subgraph from the monolithic
/// entities table to a split entities table
pub(crate) struct Connection<'a> {
    conn: &'a PgConnection,
    tables: RefCell<HashMap<SubgraphDeploymentId, Table>>,
}

impl<'a> Connection<'a> {
    pub(crate) fn new(conn: &'a PgConnection) -> Connection<'a> {
        Connection {
            conn,
            tables: RefCell::new(HashMap::new()),
        }
    }

    /// Return a table for the subgraph
    fn table(&self, subgraph: &SubgraphDeploymentId) -> Result<Table, StoreError> {
        let mut tables = self.tables.borrow_mut();

        match tables.get(subgraph) {
            Some(table) => Ok(table.clone()),
            None => {
                let table = Table::new(self.conn, subgraph)?;
                tables.insert(subgraph.clone(), table.clone());
                Ok(table)
            }
        }
    }

    pub(crate) fn find(
        &self,
        subgraph: &SubgraphDeploymentId,
        entity: &String,
        id: &String,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        let table = self.table(subgraph)?;
        table.find(self.conn, entity, id)
    }

    pub(crate) fn query(
        &self,
        subgraph: &SubgraphDeploymentId,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, &str, &str)>,
        first: Option<u32>,
        skip: u32,
    ) -> Result<Vec<(serde_json::Value, String)>, QueryExecutionError> {
        let table = self.table(subgraph)?;
        table.query(self.conn, entity_types, filter, order, first, skip)
    }

    pub(crate) fn conflicting_entity(
        &self,
        subgraph: &SubgraphDeploymentId,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        let table = self.table(subgraph)?;
        table.conflicting_entity(self.conn, entity_id, entities)
    }

    pub(crate) fn insert(
        &self,
        key: &EntityKey,
        data: &serde_json::Value,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        let table = self.table(&key.subgraph_id)?;
        table.insert(self.conn, key, data, history_event)
    }

    #[allow(dead_code)]
    pub(crate) fn upsert(
        &self,
        key: &EntityKey,
        data: &serde_json::Value,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        let table = self.table(&key.subgraph_id)?;
        table.upsert(self.conn, key, data, event_source)
    }

    pub(crate) fn update(
        &self,
        key: &EntityKey,
        data: &serde_json::Value,
        guard: Option<EntityFilter>,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        let table = self.table(&key.subgraph_id)?;
        table.update(self.conn, key, data, guard, event_source)
    }

    pub(crate) fn delete(
        &self,
        key: &EntityKey,
        event_source: EventSource,
    ) -> Result<usize, StoreError> {
        let table = self.table(&key.subgraph_id)?;
        table.delete(self.conn, key, event_source)
    }

    pub(crate) fn build_attribute_index(
        &self,
        index: &AttributeIndexDefinition,
    ) -> Result<usize, StoreError> {
        let table = self.table(&index.subgraph_id)?;
        table.build_attribute_index(self.conn, index)
    }

    pub(crate) fn revert_block(
        &self,
        subgraph: &SubgraphDeploymentId,
        block_ptr: String,
    ) -> Result<(StoreEvent, i32), StoreError> {
        let table = self.table(subgraph)?;
        table.revert_block(self.conn, block_ptr)
    }

    pub(crate) fn update_entity_count(
        &self,
        subgraph: Option<SubgraphDeploymentId>,
        count: i32,
    ) -> Result<(), StoreError> {
        if count == 0 {
            return Ok(());
        }
        if let Some(subgraph) = subgraph {
            let table = self.table(&subgraph)?;
            table.update_entity_count(self.conn, subgraph, count)
        } else {
            Ok(())
        }
    }

    pub(crate) fn create_history_event(
        &self,
        subgraph: SubgraphDeploymentId,
        event_source: EventSource,
    ) -> Result<HistoryEvent, Error> {
        create_history_event(self.conn, subgraph, event_source)
    }
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
                "update {}.entities
                 set data = data || $3, event_source = $4
                 where entity = $1 and id = $2",
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

    fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
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
    fn query(
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

    fn insert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        data: &serde_json::Value,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        let event_source = HistoryEvent::to_event_source_string(&history_event);

        match self {
            Table::Public(subgraph) => Ok(insert_into(public::entities::table)
                .values((
                    public::entities::id.eq(&key.entity_id),
                    public::entities::entity.eq(&key.entity_type),
                    public::entities::subgraph.eq(subgraph.to_string()),
                    public::entities::data.eq(data),
                    public::entities::event_source.eq(&event_source),
                ))
                .execute(conn)?),
            Table::Split(entities) => {
                // Since we are not using a subgraphs.entities table trigger for
                // the history of the subgraph of subgraphs, write entity_history
                // data for the subgraph of subgraphs directly
                if history_event.is_some() && *key.subgraph_id == "subgraphs" {
                    let history_event = history_event.unwrap();
                    self.add_entity_history_record(
                        conn,
                        history_event,
                        false,
                        &key,
                        OperationType::Insert,
                    )?;
                }

                Ok(diesel::sql_query(format!(
                    "insert into {}.entities(entity, id, data, event_source)
                         values($1, $2, $3, $4)",
                    entities.schema
                ))
                .bind::<Text, _>(&key.entity_type)
                .bind::<Text, _>(&key.entity_id)
                .bind::<Jsonb, _>(&data)
                .bind::<Text, _>(&event_source)
                .execute(conn)?)
            }
        }
    }

    fn upsert(
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
                    "insert into {}.entities(entity, id, data, event_source)
                     values($1, $2, $3, $4)
                     on conflict(entity, id)
                     do update set data = $3, event_source = $4",
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

    fn update(
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

    fn delete(
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
                    "delete from {}.entities
                     where entity = $1
                       and id = $2",
                    entities.schema
                );
                let query = diesel::sql_query(query)
                    .bind::<Text, _>(&key.entity_type)
                    .bind::<Text, _>(&key.entity_id);
                Ok(query.execute(conn)?)
            }
        }
    }

    /// Adjust the `entityCount` property of the `SubgraphDeployment` for
    /// `subgraph` by `count`. This needs to be performed after the changes
    /// underlying `count` have been written to the store.
    pub(crate) fn update_entity_count(
        &self,
        conn: &PgConnection,
        subgraph: SubgraphDeploymentId,
        count: i32,
    ) -> Result<(), StoreError> {
        let count_query = match self {
            Table::Public(_) => format!(
                "select count(*) from public.entities where subgraph = '{}'",
                &subgraph
            ),
            Table::Split(entities) => format!("select count(*) from {}.entities", entities.schema),
        };
        // The big complication in this query is how to determine what the
        // new entityCount should be. We want to make sure that if the entityCount
        // is NULL or the special value `00`, it gets recomputed. Using `00` here
        // makes it possible to manually set the `entityCount` to that value
        // to force a recount; setting it to `NULL` is not desirable since
        // `entityCount` on the GraphQL level is not nullable, and so setting
        // `entityCount` to `NULL` could cause errors at that layer; temporarily
        // returning `0` is more palatable. To be exact, recounts have to be
        // done here, from the subgraph writer.
        //
        // The first argument of `coalesce` will be `NULL` if the entity count
        // is `NULL` or `00`, forcing `coalesce` to evaluate its second
        // argument, the query to count entities. In all other cases,
        // `coalesce` does not evaluate its second argument
        let current_count = "(nullif(data->'entityCount'->>'data', '00'))::numeric";
        let query = format!(
            "
            update subgraphs.entities
            set data = data || (format('{{\"entityCount\":
                                  {{ \"data\": \"%s\", 
                                    \"type\": \"BigInt\"}}}}',
                                  coalesce({current_count} + $1,
                                           ({count_query}))))::jsonb
            where entity='SubgraphDeployment'
              and id = $2
            ",
            current_count = current_count,
            count_query = count_query
        );
        Ok(diesel::sql_query(query)
            .bind::<Integer, _>(count)
            .bind::<Text, _>(subgraph.to_string())
            .execute(conn)
            .map(|_| ())?)
    }

    fn conflicting_entity(
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

    /// This takes a history event, a reversion flag, an entity key to create
    /// the history record for and an operation type (e.g. `OperationType::Insert`).
    /// It then creates an entry in the subgraph's `entity_history` table.
    ///
    /// Special casing is applied for the "public.entities" and "subgraphs.entities"
    /// tables, as their history records include the subgraph ID in the "subgraph"
    /// column.
    fn add_entity_history_record(
        &self,
        conn: &PgConnection,
        history_event: &HistoryEvent,
        reversion: bool,
        key: &EntityKey,
        operation: OperationType,
    ) -> Result<(), Error> {
        let schema = match self {
            Table::Public(_) => "public",
            Table::Split(split_table) => split_table.schema.as_str(),
        };

        if schema == "public" || schema == "subgraphs" {
            diesel::sql_query(format!(
                "insert into {}.entity_history(
                     event_id,
                     subgraph, entity, entity_id,
                     data_before, reversion, op_id
                 )
                 select
                     $1 as event_id,
                     $2 as subgraph,
                     $3 as entity,
                     $4 as entity_id,
                     (select data from {}.entities where entity = $3 and id = $4) as data_before,
                     $5 as reversion,
                     $6 as op_id",
                schema, schema,
            ))
            .bind::<Integer, _>(history_event.id)
            .bind::<Text, _>(&schema)
            .bind::<Text, _>(&key.entity_type)
            .bind::<Text, _>(&key.entity_id)
            .bind::<Bool, _>(&reversion)
            .bind::<Integer, i32>(operation.into())
            .execute(conn)?;
        } else {
            diesel::sql_query(format!(
                "insert into {}.entity_history(
                    event_id,
                    entity, entity_id,
                    data_before, reversion, op_id
                )
                select
                    $1 as event_id,
                    $2 as entity,
                    $3 as entity_id,
                    (select data from {}.entities where entity = $2 and id = $3) as data_before,
                    $4 as reversion,
                    $5 as op_id",
                schema, schema
            ))
            .bind::<Integer, _>(history_event.id)
            .bind::<Text, _>(&key.entity_type)
            .bind::<Text, _>(&key.entity_id)
            .bind::<Bool, _>(&reversion)
            .bind::<Integer, i32>(operation.into())
            .execute(conn)?;
        }

        Ok(())
    }
    /// Revert the block with the given `block_ptr` which must be the hash
    /// of the block to revert. The returned `StoreEvent` reflects the changes
    /// that were made during reversion
    fn revert_block(
        &self,
        conn: &PgConnection,
        block_ptr: String,
    ) -> Result<(StoreEvent, i32), StoreError> {
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
                    "select h.id, h.entity, h.entity_id, h.data_before, h.op_id
                     from {}.entity_history h, event_meta_data m
                     where m.id = h.event_id
                       and m.source = $1
                     order by h.event_id desc",
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
        let mut count = 0;
        for history in entries.into_iter() {
            // Perform the actual reversion
            let key = self.entity_key(history.entity.clone(), history.entity_id.clone());
            match history.op {
                0 => {
                    // Reverse an insert
                    self.delete(conn, &key, EventSource::None)?;
                    count -= 1;
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
                    if history.op == 2 {
                        count += 1;
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

        Ok((StoreEvent::new(changes), count))
    }

    fn build_attribute_index(
        &self,
        conn: &PgConnection,
        index: &AttributeIndexDefinition,
    ) -> Result<usize, StoreError> {
        let (index_type, index_operator, jsonb_operator) = match index.field_value_type {
            ValueType::Boolean
            | ValueType::BigInt
            | ValueType::Bytes
            | ValueType::BigDecimal
            | ValueType::ID
            | ValueType::Int => (String::from("btree"), String::from(""), "->>"),
            ValueType::String => (String::from("gin"), String::from("gin_trgm_ops"), "->>"),
            ValueType::List => (String::from("gin"), String::from("jsonb_path_ops"), "->"),
        };

        // It is not to be possible to use bind variables in this code,
        // and we have to interpolate everything into the query directly.
        // We also have to use conn.batch_execute to issue the `create index`
        // commands; using `sql_query(..).execute(conn)` will make the database
        // accept the commands, and log them as if they were successful, but
        // without any effect on the schema
        //
        // Note that this code depends on `index.entity_number` and
        // `index.attribute_number` to be stable, i.e., that we always get the
        // same numbers for the same `(entity, attribute)` combination. If it is
        // not stable, we will generate duplicate indexes
        match self {
            Table::Public(_) => {
                let index_name = format!(
                    "{}_{}_{}_idx",
                    &index.subgraph_id, index.entity_number, index.attribute_number,
                );
                let query = format!(
                    "create index if not exists {name}
                     on public.entities
                     using {index_type} (
                        (data->'{attribute_name}'{jsonb_operator}'data')
                        {index_operator}
                     )
                     where subgraph='{subgraph}'
                       and entity='{entity_name}'",
                    name = index_name,
                    index_type = index_type,
                    attribute_name = &index.attribute_name,
                    jsonb_operator = jsonb_operator,
                    index_operator = index_operator,
                    subgraph = index.subgraph_id.to_string(),
                    entity_name = &index.entity_name
                );
                conn.batch_execute(&*query)?;
                Ok(1)
            }
            Table::Split(entities) => {
                // It is possible that the user's `entity_name` and
                // `attribute_name` are so long that `name` becomes longer than
                // 63 characters which is Postgres' length limit on identifiers.
                // If we go over, Postgres will truncate the name to 63 characters;
                // because of that we include the `entity_number` and
                // `attribute_number` to ensure that a 63 character prefix
                // of the name is guaranteed to be unique
                let name = format!(
                    "attr_{}_{}_{}_{}",
                    index.entity_number,
                    index.attribute_number,
                    to_snake_case(&index.entity_name),
                    to_snake_case(&index.attribute_name)
                );
                let query = format!(
                    "create index if not exists {name}
                     on {subgraph}.entities
                     using {index_type} (
                         (data->'{attribute_name}'{jsonb_operator}'data')
                         {index_operator}
                     )
                     where entity='{entity_name}'",
                    name = name,
                    subgraph = entities.schema,
                    index_type = index_type,
                    attribute_name = &index.attribute_name,
                    jsonb_operator = jsonb_operator,
                    index_operator = index_operator,
                    entity_name = &index.entity_name
                );
                conn.batch_execute(&*query)?;
                Ok(1)
            }
        }
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

pub(crate) fn create_schema(
    conn: &PgConnection,
    subgraph_id: &SubgraphDeploymentId,
) -> Result<(), StoreError> {
    // Create a schema for the deployment
    let schemas: Vec<String> = diesel::insert_into(deployment_schemas::table)
        .values(deployment_schemas::subgraph.eq(subgraph_id.to_string()))
        .returning(deployment_schemas::name)
        .get_results(conn)?;
    let schema_name = schemas
        .first()
        .ok_or_else(|| format_err!("failed to read schema name for {} back", subgraph_id))?;

    // Note that we have to use conn.batch_execute to issue DDL commands; using
    // diesel::sql_query(..).execute(conn) can lead to cases where the command
    // is sent to the database, and shows up in the database logs, but has no
    // effect at all, but also does not result in an error.
    let query = format!("create schema {}", schema_name);
    conn.batch_execute(&*query)?;

    // The order of columns in the primary key matters a lot, since
    // we want the pk index to also support queries that do not have an id,
    // just an entity (like counting the number of entities of a certain type)
    let query = format!(
        "create table {}.entities(
            entity       varchar not null,
            id           varchar not null,
            data         jsonb,
            event_source varchar not null,
            primary key(entity, id)
        )",
        schema_name
    );
    conn.batch_execute(&*query)?;

    let query = format!(
        "create trigger entity_change_insert_trigger
         after insert on {schema}.entities
         for each row
         execute procedure subgraph_log_entity_event()",
        schema = schema_name
    );
    conn.batch_execute(&*query)?;

    let query = format!(
        "create trigger entity_change_update_trigger
         after update on {schema}.entities
         for each row
         when (old.data != new.data)
         execute procedure subgraph_log_entity_event()",
        schema = schema_name
    );
    conn.batch_execute(&*query)?;

    let query = format!(
        "create trigger entity_change_delete_trigger
            after delete on {schema}.entities
            for each row
            execute procedure subgraph_log_entity_event()",
        schema = schema_name
    );
    conn.batch_execute(&*query)?;

    let query = format!(
        "create table {}.entity_history (
            id           serial primary key,
	          event_id     integer references event_meta_data(id)
                             on update cascade on delete cascade,
            entity       varchar not null,
	          entity_id    varchar not null,
	          data_before  jsonb,
	          reversion    bool not null default false,
	          op_id        int2 NOT NULL
         )",
        schema_name
    );
    conn.batch_execute(&*query)?;

    let query = format!(
        "create index entity_history_event_id_btree_idx
         on {}.entity_history(event_id)",
        schema_name
    );
    conn.batch_execute(&*query)?;

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn drop_schema(
    conn: &diesel::pg::PgConnection,
    subgraph: &SubgraphDeploymentId,
) -> Result<usize, StoreError> {
    let info = find_schema(conn, subgraph)?;
    if let Some(schema) = info {
        let query = format!("drop schema if exists {} cascade", schema.name);
        conn.batch_execute(&*query)?;
        Ok(diesel::delete(deployment_schemas::table)
            .filter(deployment_schemas::subgraph.eq(schema.subgraph))
            .execute(conn)?)
    } else {
        Ok(0)
    }
}

/// Ensures a history event exists for the current transaction and returns its ID.
fn create_history_event(
    conn: &diesel::pg::PgConnection,
    subgraph: SubgraphDeploymentId,
    event_source: EventSource,
) -> Result<HistoryEvent, Error> {
    #[derive(Queryable, Debug)]
    struct Event {
        id: i32,
    };

    impl QueryableByName<Pg> for Event {
        fn build<R: diesel::row::NamedRow<Pg>>(row: &R) -> diesel::deserialize::Result<Self> {
            Ok(Event {
                id: row.get("event_id")?,
            })
        }
    }

    let result: Event = diesel::sql_query(
        "insert into event_meta_data (
             db_transaction_id, db_transaction_time, source
         )
         values (
             txid_current(), statement_timestamp(), $1
         )
         returning event_meta_data.id as event_id",
    )
    .bind::<Text, _>(event_source.to_string())
    .get_result(conn)?;

    Ok(HistoryEvent {
        id: result.id,
        subgraph,
        source: event_source,
    })
}

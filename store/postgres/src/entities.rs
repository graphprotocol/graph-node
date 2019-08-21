//! Support for the management of the schemas and tables we create in
//! the database for each deployment. The Postgres schemas for each
//! deployment/subgraph are tracked in the `deployment_schemas` table.
//!
//! The functions in this module are very low-level and should only be used
//! directly by the Postgres store, and nowhere else. At the same time, all
//! manipulation of entities in the database should go through this module
//! to make it easier to handle future schema changes

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

use diesel::connection::SimpleConnection;
use diesel::debug_query;
use diesel::deserialize::QueryableByName;
use diesel::dsl::{any, sql};
use diesel::pg::{Pg, PgConnection};
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::sql_types::{Integer, Jsonb, Nullable, Text};
use diesel::BoolExpressionMethods;
use diesel::Connection as _;
use diesel::ExpressionMethods;
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use inflector::cases::snakecase::to_snake_case;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use graph::data::schema::Schema as SubgraphSchema;
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::{
    debug, format_err, info, serde_json, warn, AttributeIndexDefinition, Entity, EntityChange,
    EntityChangeOperation, EntityFilter, EntityKey, Error, EthereumBlockPointer, EventSource,
    HistoryEvent, Logger, QueryExecutionError, StoreError, StoreEvent, SubgraphDeploymentId,
    TransactionAbortError, ValueType,
};

use crate::block_range::{block_number, BlockNumber};
use crate::filter::build_filter;
use crate::functions::set_config;
use crate::jsonb::PgJsonbExpressionMethods as _;
use crate::mapping::{IdType, Mapping};
use crate::store::Store;

/// The size of string prefixes that we index. This should be large enough
/// that we catch most strings, but small enough so that we can still insert
/// it into a Postgres BTree index
pub(crate) const STRING_PREFIX_SIZE: usize = 2048;

/// The type of operation that led to a history entry. When we revert a block,
/// we reverse the effects of that operation; e.g., an `Insert` entry in the
/// history will cause us to delete the underlying entity
enum OperationType {
    Insert,
    Update,
    Delete,
}

/// Translate from the integer that is stored in `entity_history.op_id` to
/// the symbolic `OperationType`
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

// Tables in the public schema that are shared across subgraphs. We put them
// in this module to make sure that nobody else gets access to them. All
// access to these tables must go through functions in this module.
mod public {
    table! {
        event_meta_data (id) {
            id -> Integer,
            db_transaction_id -> BigInt,
            db_transaction_time -> Timestamp,
            source -> Nullable<Varchar>,
        }
    }

    /// We support different storage schemes per subgraph. This enum is used
    /// to track which scheme a given subgraph uses and corresponds to the
    /// `deployment_schema_version` type in the database.
    ///
    /// The column `deployment_schemas.version` stores that information for
    /// each subgraph. Subgraphs that store their entities and history as
    /// JSONB blobs with a separate history table are marked with version
    /// `Split`. Subgraphs that use a relational schema for entities, and
    /// store their history in the same table are marked as 'Relational'
    ///
    /// Migrating a subgraph amounts to changing the storage scheme for that
    /// subgraph from one version to another. Whether a subgraph scheme needs
    /// migrating is determined by `Table::needs_migrating`, the migration
    /// machinery is kicked off with a call to `Connection::migrate`
    #[derive(DbEnum, Debug, Clone)]
    pub enum DeploymentSchemaVersion {
        Split,
        Relational,
    }

    /// Migrating a subgraph is broken into two steps: in the first step, the
    /// schema for the new storage scheme is put into place; in the second
    /// step data is moved from the old storage scheme to the new one. These
    /// two steps happen in separate database transactions, since the first
    /// step takes fairly strong locks, that can block other database work.
    /// The second step, moving data, only requires relatively weak locks
    /// that do not block write activity in other subgraphs.
    ///
    /// The `Ready` state indicates that the subgraph is ready to use the
    /// storage scheme indicated by `deployment_schemas.version`. After the
    /// first step of the migration has been done, the `version` field remains
    /// unchanged, but we indicate that we have put the new schema in place by
    /// setting the state to `Tables`. At the end of the second migration
    /// step, we change the `version` to the new version, and set the state to
    /// `Ready` to indicate that the subgraph can now be used with the new
    /// storage scheme.
    #[derive(DbEnum, Debug, Clone)]
    pub enum DeploymentSchemaState {
        Ready,
        Tables,
    }

    table! {
        deployment_schemas(id) {
            id -> Integer,
            subgraph -> Text,
            name -> Text,
            /// The subgraph storage scheme used for this subgraph
            version -> crate::entities::public::DeploymentSchemaVersionMapping,
            /// Whether this subgraph is in the process of being migrated to
            /// a new storage scheme. This column functions as a lock (or
            /// semaphore) and is used to limit the number of subgraphs that
            /// are being migrated at any given time. The details of handling
            /// this lock are in `Connection::should_migrate`
            migrating -> Bool,
            /// Track which step of a subgraph migration has been done
            state -> crate::entities::public::DeploymentSchemaStateMapping,
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

    table! {
        subgraphs.entity_history (id) {
            id -> Integer,
            // This is a BigInt in the database, but if we mark it that
            // diesel won't let us join event_meta_data and entity_history
            // Since event_meta_data.id is Integer, it shouldn't matter
            // that we call it Integer here
            event_id -> Integer,
            subgraph -> Varchar,
            entity -> Varchar,
            entity_id -> Varchar,
            data_before -> Nullable<Jsonb>,
            op_id -> SmallInt,
            reversion -> Bool,
        }
    }

    // NOTE: This is a duplicate of the `event_meta_data` in `public`. It exists
    // only so we can link from the subgraphs.entity_history table to
    // public.event_meta_data.
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

impl EntitySource for self::subgraphs::entities::table {}

pub(crate) type EntityTable = diesel_dynamic_schema::Table<String>;

pub(crate) type EntityColumn<ST> = diesel_dynamic_schema::Column<EntityTable, String, ST>;

// This is a bit weak, as any DynamicTable<String> is now an EntitySource
impl EntitySource for EntityTable {}

use public::deployment_schemas;

/// Information about the database schema that stores the entities for a
/// subgraph. The schemas are versioned by subgraph, which makes it possible
/// to migrate subgraphs one at a time to newer storage schemes. Migrations
/// are split into two stages to make sure that intrusive locks are
/// only held a very short amount of time. The overall goal is to pause
/// indexing (write activity) for a subgraph while we migrate, but keep it
/// possible to query the subgraph, and not affect other subgraph's operation.
///
/// When writing a migration, the following guidelines should be followed:
/// - each migration can only affect a single subgraph, and must not interfere
///   with the working of any other subgraph
/// - writing to the subgraph will be paused while the migration is running
/// - each migration step is run in its own database transaction
#[derive(Queryable, QueryableByName, Debug)]
#[table_name = "deployment_schemas"]
struct Schema {
    id: i32,
    subgraph: String,
    name: String,
    /// The version currently in use. While we are migrating, the version
    /// will remain at the old version until the new version is ready to use.
    /// Migrations should update this field as the very last operation they
    /// perform.
    version: public::DeploymentSchemaVersion,
    /// True if the subgraph is currently running a migration. The `migrating`
    /// flags in the `deployment_schemas` table act as a semaphore that limits
    /// the number of subgraphs that can undergo a migration at the same time.
    migrating: bool,
    /// Track which parts of a migration have already been performed. The
    /// `Ready` state means no work to get to the next version has been done
    /// yet. A migration will first perform a transaction that purely does DDL;
    /// since that generally requires fairly strong locks but is fast, that
    /// is done in its own transaction. Once we have done the necessary DDL,
    /// the state goes to `Tables`. The final state of the migration is
    /// copying data, which can be very slow, but should not require intrusive
    /// locks. When the data is in place, the migration updates `version` to
    /// the new version we migrated to, and sets the state to `Ready`
    state: public::DeploymentSchemaState,
}

/// Storage using JSONB for entities. All entities are stored in one table
#[derive(Debug, Clone)]
pub(crate) struct JsonStorage {
    /// The name of the database schema
    schema: String,
    /// The subgraph id
    subgraph: SubgraphDeploymentId,
    table: EntityTable,
    id: EntityColumn<diesel::sql_types::Text>,
    entity: EntityColumn<diesel::sql_types::Text>,
    data: EntityColumn<diesel::sql_types::Jsonb>,
    event_source: EntityColumn<diesel::sql_types::Text>,
    // The query to count all entities
    count_query: String,
}

#[derive(Debug, Clone)]
pub(crate) enum Storage {
    Json(JsonStorage),
    Relational(Mapping),
}

/// Helper struct to support a custom query for entity history
#[derive(Debug, Queryable)]
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

/// A connection into the database to handle entities which caches the
/// mapping to actual database tables. Instances of this struct must not be
/// cached across transactions as there is no mechanism in place to notify
/// other index nodes that a subgraph has been migrated
pub(crate) struct Connection<'a> {
    pub conn: PooledConnection<ConnectionManager<PgConnection>>,
    cache: RefCell<HashMap<SubgraphDeploymentId, Storage>>,
    store: &'a Store,
}

impl<'a> Connection<'a> {
    pub(crate) fn new(
        conn: PooledConnection<ConnectionManager<PgConnection>>,
        store: &'a Store,
    ) -> Connection<'a> {
        Connection {
            conn,
            cache: RefCell::new(HashMap::new()),
            store,
        }
    }

    /// Return the storage for the subgraph
    fn storage(&self, subgraph: &SubgraphDeploymentId) -> Result<Storage, StoreError> {
        let mut cache = self.cache.borrow_mut();

        match cache.get(subgraph) {
            Some(storage) => Ok(storage.clone()),
            None => {
                let storage = Storage::new(&self.conn, subgraph, self.store)?;
                cache.insert(subgraph.clone(), storage.clone());
                Ok(storage)
            }
        }
    }

    /// Do any cleanup to bring the subgraph into a known good state
    pub(crate) fn start_subgraph(&self, subgraph: &SubgraphDeploymentId) -> Result<(), StoreError> {
        use public::deployment_schemas as dsl;

        // Clear the `migrating` lock on the subgraph; this flag must be
        // visible to other db users before the migration starts and is
        // therefore set in its own txn before migration actually starts.
        // If the migration does not finish, e.g., because the server is shut
        // down, `migrating` remains set to `true` even though no work for
        // the migration is being done.
        Ok(
            diesel::update(dsl::table.filter(dsl::subgraph.eq(subgraph.to_string())))
                .set(dsl::migrating.eq(false))
                .execute(&self.conn)
                .map(|_| ())?,
        )
    }

    pub(crate) fn find(
        &self,
        subgraph: &SubgraphDeploymentId,
        entity: &String,
        id: &String,
        block: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        match self.storage(subgraph)? {
            Storage::Json(json) => json.find(&self.conn, entity, id),
            Storage::Relational(mapping) => mapping.find(&self.conn, entity, id, block),
        }
    }

    pub(crate) fn query(
        &self,
        subgraph: &SubgraphDeploymentId,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, &str, &str)>,
        first: Option<u32>,
        skip: u32,
        block: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        match self.storage(subgraph)? {
            Storage::Json(json) => json.query(&self.conn, entity_types, filter, order, first, skip),
            Storage::Relational(mapping) => {
                mapping.query(&self.conn, entity_types, filter, order, first, skip, block)
            }
        }
    }

    pub(crate) fn conflicting_entity(
        &self,
        subgraph: &SubgraphDeploymentId,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        match self.storage(subgraph)? {
            Storage::Json(json) => json.conflicting_entity(&self.conn, entity_id, entities),
            Storage::Relational(mapping) => {
                mapping.conflicting_entity(&self.conn, entity_id, entities)
            }
        }
    }

    pub(crate) fn insert(
        &self,
        key: &EntityKey,
        entity: Entity,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        match self.storage(&key.subgraph_id)? {
            Storage::Json(json) => json.insert(&self.conn, &key, &entity, history_event),
            Storage::Relational(mapping) => {
                mapping.insert(&self.conn, key, entity, block_number(&history_event))
            }
        }
    }

    /// Update an existing entity. If `overwrite` is true, `entity` contains
    /// the entire entity, and the existing entity should be completely
    /// replaced by it. Otherwise, `entity` is only partial, and only the
    /// attributes explicitly mentioned in `entity` should be changed.
    pub(crate) fn update(
        &self,
        key: &EntityKey,
        entity: Entity,
        overwrite: bool,
        guard: Option<EntityFilter>,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        match self.storage(&key.subgraph_id)? {
            Storage::Json(json) => {
                json.update(&self.conn, key, entity, overwrite, guard, history_event)
            }
            Storage::Relational(mapping) => mapping.update(
                &self.conn,
                key,
                entity,
                overwrite,
                guard,
                block_number(&history_event),
            ),
        }
    }

    pub(crate) fn delete(
        &self,
        key: &EntityKey,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        match self.storage(&key.subgraph_id)? {
            Storage::Json(json) => json.delete(&self.conn, key, history_event),
            Storage::Relational(mapping) => {
                mapping.delete(&self.conn, key, block_number(&history_event))
            }
        }
    }

    pub(crate) fn build_attribute_index(
        &self,
        index: &AttributeIndexDefinition,
    ) -> Result<usize, StoreError> {
        match self.storage(&index.subgraph_id)? {
            Storage::Json(json) => json.build_attribute_index(&self.conn, index),
            Storage::Relational(_) => Ok(1),
        }
    }

    pub(crate) fn revert_block(
        &self,
        subgraph: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) -> Result<(StoreEvent, i32), StoreError> {
        // Revert the block in the subgraph itself
        let (event, count) = match self.storage(subgraph)? {
            Storage::Json(json) => json.revert_block(&self.conn, block_ptr.hash_hex())?,
            Storage::Relational(mapping) => {
                let block = block_ptr.number.try_into().unwrap();
                mapping.revert_block(&self.conn, block)?
            }
        };
        // Revert the meta data changes that correspond to this subgraph.
        // Only certain meta data changes need to be reverted, most
        // importantly creation of dynamic data sources. We ensure in the
        // rest of the code that we only record history for those meta data
        // changes that might need to be reverted
        match self.storage(&SUBGRAPHS_ID)? {
            Storage::Json(json) => {
                let (meta_event, _) =
                    json.revert_block_meta(&self.conn, subgraph, block_ptr.hash_hex())?;
                Ok((event.extend(meta_event), count))
            }
            Storage::Relational(_) => unreachable!(
                "Storing the subgraph of subgraphs in a relational schema is not supported"
            ),
        }
    }

    pub(crate) fn update_entity_count(
        &self,
        subgraph: &Option<SubgraphDeploymentId>,
        count: i32,
    ) -> Result<(), StoreError> {
        if count == 0 {
            return Ok(());
        }
        if let Some(subgraph) = subgraph {
            let storage = self.storage(&subgraph)?;
            storage.update_entity_count(&self.conn, subgraph, count)
        } else {
            Ok(())
        }
    }

    pub(crate) fn create_history_event(
        &self,
        subgraph: SubgraphDeploymentId,
        event_source: EventSource,
    ) -> Result<HistoryEvent, Error> {
        create_history_event(&self.conn, subgraph, event_source)
    }

    /// Check if the schema for `subgraph` needs to be migrated, and if so
    /// if now (indicated by the block pointer) is the right time to do so.
    /// We try to spread the actual database work associated with checking
    /// if a subgraph should be migrated out as much as possible. This
    /// function does not query the database, and the actual check for
    /// migrating should only be done if this function returns `true`
    pub(crate) fn should_migrate(
        &self,
        subgraph: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) -> Result<bool, StoreError> {
        // How often to check whether a subgraph needs to be migrated (in
        // blocks) Doing it every 20 blocks translates to roughly once every
        // 5 minutes
        const MIGRATION_CHECK_FREQ: u64 = 20;

        let storage = self.storage(subgraph)?;

        if storage.needs_migrating() {
            // We determine whether it is time for us to check if we should
            // migrate in a way that tries to splay the checks for different
            // subgraphs, using the hash of the subgraph id as a somewhat
            // arbitrary indicator. We really just want the checks to be
            // distributed across all possible values mod MIGRATION_CHECK_FREQ
            // so that we don't have a mad dash to migrate every
            // MIGRATION_CHECK_FREQ blocks, which would happen if we checked
            // for `block_ptr.number % MIGRATION_CHECK_FREQ == 0`
            let mut hasher = DefaultHasher::new();
            subgraph.hash(&mut hasher);
            let hash = hasher.finish();
            Ok(hash % MIGRATION_CHECK_FREQ == block_ptr.number % MIGRATION_CHECK_FREQ)
        } else {
            Ok(false)
        }
    }

    /// Check if the database schema for `subgraph` needs to be migrated, and
    /// if so, perform the migration. Return `true` if a migration was
    /// performed, and `false` otherwise. A return value of `false` does not
    /// indicate that no migration is necessary, just that we currently can
    /// not perform it, for example, because too many other subgraphs are
    /// migrating.
    ///
    /// Migrating requires performing multiple transactions, and the connection
    /// in `self` must therefore not have a transaction open already.
    pub(crate) fn migrate(
        self,
        logger: &Logger,
        subgraph: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) -> Result<bool, Error> {
        // How many simultaneous subgraph migrations we allow
        const MIGRATION_LIMIT: i32 = 2;

        if !self.should_migrate(subgraph, block_ptr)? {
            return Ok(false);
        }

        let do_migrate = self.conn.transaction(|| -> Result<bool, Error> {
            let lock =
                diesel::sql_query("lock table public.deployment_schemas in exclusive mode nowait")
                    .execute(&self.conn);
            if lock.is_err() {
                return Ok(false);
            }

            let query = "
                UPDATE public.deployment_schemas
                   SET migrating = true
                 WHERE subgraph=$1
                   AND (SELECT count(*) FROM public.deployment_schemas WHERE migrating) < $2";
            let query = diesel::sql_query(query)
                .bind::<Text, _>(subgraph.to_string())
                .bind::<Integer, _>(MIGRATION_LIMIT);
            Ok(query.execute(&self.conn)? > 0)
        })?;

        if do_migrate {
            use self::public::deployment_schemas as dsl;

            let result = loop {
                match self.migration_step(logger, subgraph) {
                    Err(e) => {
                        // An error in a migration should not lead to the
                        // subgraph being marked as failed
                        warn!(logger, "aborted migrating";
                                        "subgraph" => subgraph.to_string(),
                                        "error" => e.to_string(),
                        );
                        break Ok(false);
                    }
                    Ok(again) if !again => break Ok(true),
                    Ok(_) => continue,
                }
            };
            // Relinquish the migrating lock, no matter what happened in
            // the migration
            diesel::update(dsl::table.filter(dsl::subgraph.eq(subgraph.to_string())))
                .set(dsl::migrating.eq(false))
                .execute(&self.conn)?;
            result
        } else {
            Ok(false)
        }
    }

    /// Perform one migration step and return true if there are more steps
    /// left to do. Each step of the migration is performed in  a separate
    /// transaction so that any locks a step takes are freed up at the end
    fn migration_step(
        &self,
        logger: &Logger,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<bool, Error> {
        self.conn.transaction(|| -> Result<bool, Error> {
            let storage = self.storage(subgraph)?;
            let errmsg = format_err!(
                "subgraph {} has no entry in deployment_schemas and can not be migrated",
                subgraph.to_string()
            );
            let schema = find_schema(&self.conn, &subgraph)?.ok_or(errmsg)?;

            debug!(
                logger,
                "start migrating";
                "name" => &schema.name,
                "subgraph" => subgraph.to_string(),
                "state" => format!("{:?}", schema.state)
            );
            let start = Instant::now();
            let storage = storage.migrate(&self.conn, logger, &schema)?;
            let needs_migrating = storage.needs_migrating();
            self.cache.borrow_mut().insert(subgraph.clone(), storage);
            info!(
                logger,
                "finished migrating";
                "name" => &schema.name,
                "subgraph" => subgraph.to_string(),
                "state" => format!("{:?}", schema.state),
                "migration_time_ms" => start.elapsed().as_millis()
            );
            Ok(needs_migrating)
        })
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

fn entity_to_json(key: &EntityKey, entity: &Entity) -> Result<serde_json::Value, Error> {
    serde_json::to_value(entity).map_err(|e| {
        format_err!(
            "Failed to convert entity ({}, {}, {}) to JSON: {}",
            key.subgraph_id,
            key.entity_type,
            key.entity_id,
            e
        )
    })
}

fn entity_from_json(json: serde_json::Value, entity: &str) -> Result<Entity, StoreError> {
    let mut value = serde_json::from_value::<Entity>(json)?;
    value.set("__typename", entity);
    Ok(value)
}

impl JsonStorage {
    fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
        id: &String,
    ) -> Result<Option<Entity>, StoreError> {
        let entities = self.clone();
        entities
            .table
            .filter(entities.entity.eq(entity).and(entities.id.eq(id)))
            .select(entities.data)
            .first::<serde_json::Value>(conn)
            .optional()?
            .map(|json| entity_from_json(json, entity))
            .transpose()
    }

    /// order is a tuple (attribute, value_type, cast, direction)
    fn query(
        &self,
        conn: &PgConnection,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, &str, &str)>,
        first: Option<u32>,
        skip: u32,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let entities = self.clone();
        let mut query = entities
            .table
            .filter((&self.entity).eq(any(entity_types)))
            .select((&self.data, &self.entity))
            .into_boxed::<Pg>();

        if let Some(filter) = filter {
            let filter = build_filter(filter).map_err(|e| {
                QueryExecutionError::FilterNotSupportedError(format!("{}", e.value), e.filter)
            })?;
            query = query.filter(filter);
        }

        if let Some((attribute, value_type, cast, direction)) = order {
            query = match value_type {
                ValueType::String => query.order(
                    sql::<Text>("left(data ->")
                        .bind::<Text, _>(attribute)
                        .sql("->> 'data', ")
                        .sql(&STRING_PREFIX_SIZE.to_string())
                        .sql(") ")
                        .sql(direction)
                        .sql(" NULLS LAST"),
                ),
                _ => query.order(
                    sql::<Text>("(data ->")
                        .bind::<Text, _>(attribute)
                        .sql("->> 'data')")
                        .sql(cast)
                        .sql(" ")
                        .sql(direction)
                        .sql(" NULLS LAST"),
                ),
            };
        }
        query = query.then_order_by(entities.id.asc());

        if let Some(first) = first {
            query = query.limit(first as i64);
        }
        if skip > 0 {
            query = query.offset(skip as i64);
        }

        let query_debug_info = debug_query(&query).to_string();

        let values = query
            .load::<(serde_json::Value, String)>(conn)
            .map_err(|e| {
                QueryExecutionError::ResolveEntitiesError(format!(
                    "{}, query = {:?}",
                    e, query_debug_info
                ))
            })?;
        values
            .into_iter()
            .map(|(value, entity_type)| {
                entity_from_json(value, &entity_type).map_err(QueryExecutionError::from)
            })
            .collect()
    }

    fn insert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: &Entity,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        let data = entity_to_json(key, entity)?;
        let event_source = HistoryEvent::to_event_source_string(&history_event);

        self.add_entity_history_record(conn, history_event, &key, OperationType::Insert)?;

        Ok(diesel::sql_query(format!(
            "insert into {}.entities(entity, id, data, event_source)
                       values($1, $2, $3, $4)",
            self.schema
        ))
        .bind::<Text, _>(&key.entity_type)
        .bind::<Text, _>(&key.entity_id)
        .bind::<Jsonb, _>(&data)
        .bind::<Text, _>(&event_source)
        .execute(conn)?)
    }

    fn upsert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        data: &serde_json::Value,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        let event_source = HistoryEvent::to_event_source_string(&history_event);

        let query = format!(
            "insert into {}.entities(entity, id, data, event_source)
                       values($1, $2, $3, $4)
                     on conflict(entity, id)
                       do update set data = $3, event_source = $4",
            self.schema
        );
        let query = diesel::sql_query(query)
            .bind::<Text, _>(&key.entity_type)
            .bind::<Text, _>(&key.entity_id)
            .bind::<Jsonb, _>(data)
            .bind::<Text, _>(event_source);
        Ok(query.execute(conn)?)
    }

    fn update(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: Entity,
        overwrite: bool,
        guard: Option<EntityFilter>,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        let data = entity_to_json(key, &entity)?;

        self.add_entity_history_record(conn, history_event, &key, OperationType::Update)?;

        if !guard.is_none() && key.subgraph_id != *SUBGRAPHS_ID {
            // We can only make adding additional conditions to the update
            // operation work for a query that is fully generated with
            // Diesel's DSL. Trying to combine the result of build_filter
            // with a direct query is too cumbersome because of various type
            // system gyrations. For now, we also only need update guards for
            // the subgraph of subgraphs
            panic!("update guards are only possible for the 'subgraphs' subgraph");
        }

        let event_source = HistoryEvent::to_event_source_string(&history_event);

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

            if overwrite {
                Ok(diesel::update(target)
                    .set((
                        entities::data.eq(&data),
                        entities::event_source.eq(&event_source),
                    ))
                    .filter(filter)
                    .execute(conn)?)
            } else {
                Ok(diesel::update(target)
                    .set((
                        entities::data.eq(entities::data.merge(&data)),
                        entities::event_source.eq(&event_source),
                    ))
                    .filter(filter)
                    .execute(conn)?)
            }
        } else {
            // If there is no guard (which has to include all 'normal' subgraphs),
            // we need to use a direct query since diesel::update does not like
            // dynamic tables.
            let query = if overwrite {
                format!(
                    "update {}.entities
                       set data = $3, event_source = $4
                       where entity = $1 and id = $2",
                    self.schema
                )
            } else {
                format!(
                    "update {}.entities
                       set data = data || $3, event_source = $4
                       where entity = $1 and id = $2",
                    self.schema
                )
            };
            let query = diesel::sql_query(query)
                .bind::<Text, _>(&key.entity_type)
                .bind::<Text, _>(&key.entity_id)
                .bind::<Jsonb, _>(data)
                .bind::<Text, _>(&event_source);
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

    fn delete(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        history_event: Option<&HistoryEvent>,
    ) -> Result<usize, StoreError> {
        diesel::select(set_config(
            "vars.current_event_source",
            HistoryEvent::to_event_source_string(&history_event),
            true,
        ))
        .execute(conn)
        .map_err(|e| format_err!("Failed to set event source for remove operation: {}", e))
        .map(|_| ())?;

        self.add_entity_history_record(conn, history_event, &key, OperationType::Delete)?;

        let query = format!(
            "delete from {}.entities
                      where entity = $1
                        and id = $2",
            self.schema
        );
        let query = diesel::sql_query(query)
            .bind::<Text, _>(&key.entity_type)
            .bind::<Text, _>(&key.entity_id);
        Ok(query.execute(conn)?)
    }

    fn conflicting_entity(
        &self,
        conn: &PgConnection,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        let ents = self.clone();
        Ok(ents
            .table
            .select(ents.entity.clone())
            .filter(ents.entity.eq(any(entities)))
            .filter(ents.id.eq(entity_id))
            .first(conn)
            .optional()?)
    }

    /// This takes a history event, a reversion flag, an entity key to create
    /// the history record for and an operation type (e.g. `OperationType::Insert`).
    /// It then creates an entry in the subgraph's `entity_history` table.
    ///
    /// Special casing is applied for the `subgraphs.entities` table, as its
    /// history records include the subgraph ID in the `subgraph` column.
    fn add_entity_history_record(
        &self,
        conn: &PgConnection,
        history_event: Option<&HistoryEvent>,
        key: &EntityKey,
        operation: OperationType,
    ) -> Result<(), Error> {
        // We only need to do work for the subgraph of subgraphs. All other
        // entities tables have triggers that will populate a history record
        // whenever we make a change to an entity
        if key.subgraph_id != *SUBGRAPHS_ID {
            return Ok(());
        }
        let history_event = match history_event {
            None => return Ok(()),
            Some(event) => event,
        };

        let schema = self.schema.as_str();

        if schema == SUBGRAPHS_ID.to_string() {
            diesel::sql_query(format!(
                "insert into {}.entity_history(
                   event_id,
                   subgraph, entity, entity_id,
                   data_before, op_id
                 )
                 select
                   $1 as event_id,
                   $2 as subgraph,
                   $3 as entity,
                   $4 as entity_id,
                   (select data
                      from {}.entities
                     where entity = $3
                       and id = $4) as data_before,
                   $5 as op_id",
                schema, schema,
            ))
            .bind::<Integer, _>(history_event.id)
            .bind::<Text, _>(&*history_event.subgraph)
            .bind::<Text, _>(&key.entity_type)
            .bind::<Text, _>(&key.entity_id)
            .bind::<Integer, i32>(operation.into())
            .execute(conn)?;
        } else {
            diesel::sql_query(format!(
                "insert into {}.entity_history(
                   event_id,
                   entity, entity_id,
                   data_before, op_id
                 )
                 select
                   $1 as event_id,
                   $2 as entity,
                   $3 as entity_id,
                   (select data from {}.entities where entity = $2 and id = $3) as data_before,
                   $4 as op_id",
                schema, schema
            ))
            .bind::<Integer, _>(history_event.id)
            .bind::<Text, _>(&key.entity_type)
            .bind::<Text, _>(&key.entity_id)
            .bind::<Integer, i32>(operation.into())
            .execute(conn)?;
        }

        Ok(())
    }

    fn revert_block_meta(
        &self,
        conn: &PgConnection,
        subgraph_id: &SubgraphDeploymentId,
        block_ptr: String,
    ) -> Result<(StoreEvent, i32), StoreError> {
        use self::subgraphs::entity_history::dsl as h;
        use self::subgraphs::event_meta_data as m;
        // Collect entity history events in the subgraph of subgraphs that
        // match the subgraph for which we're reverting the block
        let entries: Vec<RawHistory> = h::entity_history
            .inner_join(m::table)
            .select((h::id, h::entity, h::entity_id, h::data_before, h::op_id))
            .filter(h::subgraph.eq(&**subgraph_id))
            .filter(m::source.eq(&block_ptr))
            .order(h::event_id.desc())
            .load(conn)?;

        // Apply revert operations
        self.revert_entity_history_records(conn, entries)
            .map(|(changes, count)| (StoreEvent::new(changes), count))
    }

    /// Revert the block with the given `block_ptr` which must be the hash
    /// of the block to revert. The returned `StoreEvent` reflects the changes
    /// that were made during reversion
    fn revert_block(
        &self,
        conn: &PgConnection,
        block_ptr: String,
    ) -> Result<(StoreEvent, i32), StoreError> {
        // We can't use Diesel's JoinOnDsl here because DynamicTable
        // does not implement AppearsInFromClause, so we have to run
        // a raw SQL query
        let query = format!(
            "select h.id, h.entity, h.entity_id, h.data_before, h.op_id
                       from {}.entity_history h, event_meta_data m
                      where m.id = h.event_id
                        and m.source = $1
                      order by h.event_id desc",
            self.schema
        );

        let query = diesel::sql_query(query).bind::<Text, _>(&block_ptr);

        // Collect entity history events for the subgraph for which we're
        // reverting the block
        let entries: Vec<RawHistory> = query.get_results(conn)?;

        // Apply revert operations
        self.revert_entity_history_records(conn, entries)
            .map(|(changes, count)| (StoreEvent::new(changes), count))
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
            | ValueType::Int
            | ValueType::String => (String::from("btree"), String::from(""), "->>"),
            ValueType::List => (String::from("gin"), String::from("jsonb_path_ops"), "->"),
        };
        // Cast between the type we store in JSONB for the field and the type
        // as which comparisons should be made. For example, we store BigInt
        // as a string in JSONB, but the comparison needs to be made as
        // a number
        let type_cast = match index.field_value_type {
            ValueType::BigInt | ValueType::BigDecimal => "::numeric",
            ValueType::Boolean => "::bool",
            _ => "",
        };
        // It is not possible to use bind variables in this code,
        // and we have to interpolate everything into the query directly.
        // We also have to use conn.batch_execute to issue the `create index`
        // commands; using `sql_query(..).execute(conn)` will make the database
        // accept the commands, and log them as if they were successful, but
        // without any effect on the schema
        //
        // Note that this code depends on `index.entity_number` and
        // `index.attribute_number` to be stable, i.e., that we always get the
        // same numbers for the same `(entity, attribute)` combination. If it is
        // not stable, we will create duplicate indexes

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
        let query = match index.field_value_type {
            ValueType::String => format!(
                "create index if not exists {name}
                         on {subgraph}.entities
                      using btree(left(data->'{attribute_name}'->>'data', {prefix_size}))
                      where entity='{entity_name}'",
                name = name,
                subgraph = self.schema,
                attribute_name = &index.attribute_name,
                entity_name = &index.entity_name,
                prefix_size = STRING_PREFIX_SIZE,
            ),
            _ => format!(
                "create index if not exists {name}
                         on {subgraph}.entities
                      using {index_type} (
                              ((data->'{attribute_name}'{jsonb_operator}'data'){type_cast})
                              {index_operator}
                            )
                      where entity='{entity_name}'",
                name = name,
                subgraph = self.schema,
                index_type = index_type,
                attribute_name = &index.attribute_name,
                jsonb_operator = jsonb_operator,
                type_cast = type_cast,
                index_operator = index_operator,
                entity_name = &index.entity_name
            ),
        };
        conn.batch_execute(&*query)?;
        Ok(1)
    }

    fn revert_entity_history_records(
        &self,
        conn: &PgConnection,
        records: Vec<RawHistory>,
    ) -> Result<(Vec<EntityChange>, i32), StoreError> {
        let subgraph_id = self.subgraph.clone();

        let mut changes = vec![];
        let mut count = 0;

        for history in records.into_iter() {
            // Perform the actual reversion
            let key = EntityKey {
                subgraph_id: self.subgraph.clone(),
                entity_type: history.entity.clone(),
                entity_id: history.entity_id.clone(),
            };
            match history.op {
                0 => {
                    // Reverse an insert
                    self.delete(conn, &key, None)?;
                    count -= 1;
                }
                1 | 2 => {
                    // Reverse an update or delete
                    if let Some(data) = history.data {
                        self.upsert(conn, &key, &data, None)?;
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

        Ok((changes, count))
    }
}

impl Storage {
    /// The version for newly created subgraph schemas. Changing this most
    /// likely also requires changing `create_schema`
    #[allow(dead_code)]
    const DEFAULT_VERSION: public::DeploymentSchemaVersion = public::DeploymentSchemaVersion::Split;

    /// Look up the schema for `subgraph` and return its entity storage.
    /// Returns an error if `subgraph` does not have an entry in
    /// `deployment_schemas`, which can only happen if `create_schema` was not
    /// called for that `subgraph`
    fn new(
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
        store: &Store,
    ) -> Result<Self, StoreError> {
        use public::DeploymentSchemaVersion as V;

        let schema = find_schema(conn, subgraph)?
            .ok_or_else(|| StoreError::Unknown(format_err!("unknown subgraph {}", subgraph)))?;
        let storage = match schema.version {
            V::Split => {
                let table =
                    diesel_dynamic_schema::schema(schema.name.clone()).table("entities".to_owned());
                let id = table.column::<Text, _>("id".to_string());
                let entity = table.column::<Text, _>("entity".to_string());
                let data = table.column::<Jsonb, _>("data".to_string());
                let event_source = table.column::<Text, _>("event_source".to_string());
                let count_query = format!("select count(*) from \"{}\".entities", schema.name);

                Storage::Json(JsonStorage {
                    schema: schema.name,
                    subgraph: subgraph.clone(),
                    table,
                    id,
                    entity,
                    data,
                    event_source,
                    count_query,
                })
            }
            V::Relational => {
                let subgraph_schema = store.raw_subgraph_schema(subgraph)?;
                let mapping = Mapping::new(
                    &subgraph_schema.document,
                    IdType::String,
                    subgraph.clone(),
                    schema.name,
                )?;
                Storage::Relational(mapping)
            }
        };
        Ok(storage)
    }

    /// Adjust the `entityCount` property of the `SubgraphDeployment` for
    /// `subgraph` by `count`. This needs to be performed after the changes
    /// underlying `count` have been written to the store.
    pub(crate) fn update_entity_count(
        &self,
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
        count: i32,
    ) -> Result<(), StoreError> {
        let count_query = match self {
            Storage::Json(json) => json.count_query.as_str(),
            Storage::Relational(mapping) => mapping.count_query.as_str(),
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

    fn migrate(
        self,
        _conn: &PgConnection,
        _logger: &Logger,
        _schema: &Schema,
    ) -> Result<Self, Error> {
        Ok(self)
    }

    fn needs_migrating(&self) -> bool {
        false
    }
}

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
pub fn delete_all_entities_for_test_use_only(conn: &PgConnection) -> Result<usize, StoreError> {
    // Delete public entities and related data
    let mut rows = diesel::delete(public::event_meta_data::table).execute(conn)?;
    // Delete all subgraph schemas
    for subgraph in public::deployment_schemas::table
        .select(public::deployment_schemas::subgraph)
        .filter(public::deployment_schemas::subgraph.ne("subgraphs"))
        .get_results::<String>(conn)?
    {
        let subgraph = SubgraphDeploymentId::new(subgraph.clone())
            .map_err(|_| StoreError::Unknown(format_err!("illegal subgraph {}", subgraph)))?;
        drop_schema(conn, &subgraph)?;
    }
    // Delete subgraphs entities
    rows = rows + diesel::delete(subgraphs::entities::table).execute(conn)?;
    Ok(rows)
}

/// Create the database schema for a new subgraph, including all tables etc.
///
/// It is an error if `deployment_schemas` already has an entry for this
/// `subgraph_id`
pub(crate) fn create_schema(
    conn: &PgConnection,
    schema: &SubgraphSchema,
) -> Result<(), StoreError> {
    // Check if there already is an entry for this subgraph. If so, do
    // nothing
    let count = deployment_schemas::table
        .filter(deployment_schemas::subgraph.eq(schema.id.to_string()))
        .count()
        .first::<i64>(conn)?;
    if count > 0 {
        return Ok(());
    }

    // We temporarily use an environment variable to determine whether
    // to create a relational or split schema. Utimately, we will default
    // to always creating a relational schema
    let scheme = match std::env::var_os("RELATIONAL_SCHEMA") {
        Some(_) => self::public::DeploymentSchemaVersion::Relational,
        None => self::public::DeploymentSchemaVersion::Split,
    };

    // Create a schema for the deployment.
    let schemas: Vec<String> = diesel::insert_into(deployment_schemas::table)
        .values((
            deployment_schemas::subgraph.eq(schema.id.to_string()),
            deployment_schemas::version.eq(scheme),
        ))
        .returning(deployment_schemas::name)
        .get_results(conn)?;
    let schema_name = schemas
        .first()
        .ok_or_else(|| format_err!("failed to read schema name for {} back", &schema.id))?;

    let query = format!("create schema {}", schema_name);
    conn.batch_execute(&*query)?;

    match std::env::var_os("RELATIONAL_SCHEMA") {
        Some(_) => Mapping::create_relational_schema(
            conn,
            &schema_name,
            schema.id.clone(),
            &schema.document,
        )
        .map(|_| ()),
        None => create_split_schema(conn, &schema_name),
    }
}

pub fn create_split_schema(conn: &PgConnection, schema_name: &str) -> Result<(), StoreError> {
    // The order of columns in the primary key matters a lot, since
    // we want the pk index to also support queries that do not have an id,
    // just an entity (like counting the number of entities of a certain type)
    let query = format!(
        "create table {}.entities
         (
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
        "create table {}.entity_history
         (
           id           serial primary key,
           event_id     integer,
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

/// Drop the schema for `subgraph`. This deletes all data for the subgraph,
/// and can not be reversed. It does not remove any of the metadata in
/// `subgraphs.entities` associated with the subgraph
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
        "insert into event_meta_data (db_transaction_id, db_transaction_time, source)
           values (txid_current(), statement_timestamp(), $1)
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

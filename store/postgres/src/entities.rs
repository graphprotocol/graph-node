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
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::sql_types::{Integer, Text};
use diesel::Connection as _;
use diesel::ExpressionMethods;
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use maybe_owned::MaybeOwned;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::ops::Deref as _;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use graph::data::schema::Schema as SubgraphSchema;
use graph::data::subgraph::schema::{POI_OBJECT, POI_TABLE, SUBGRAPHS_ID};
use graph::prelude::{
    debug, format_err, info, serde_json, warn, BlockNumber, Entity, EntityCollection, EntityFilter,
    EntityKey, EntityOrder, EntityRange, Error, EthereumBlockPointer, Logger, QueryExecutionError,
    StoreError, StoreEvent, SubgraphDeploymentId, BLOCK_NUMBER_MAX,
};

use crate::block_range::block_number;
use crate::metadata;
use crate::notification_listener::JsonNotification;
use crate::relational::{Catalog, Layout};

#[cfg(debug_assertions)]
lazy_static::lazy_static! {
/// Tests set this to true so that `send_store_event` will store a copy
/// of each event sent in `EVENT_TAP`
pub static ref EVENT_TAP_ENABLED: Mutex<bool> = Mutex::new(false);
    pub static ref EVENT_TAP: Mutex<Vec<StoreEvent>> = Mutex::new(Vec::new());
}

/// The size of string prefixes that we index. This is chosen so that we
/// will index strings that people will do string comparisons like
/// `=` or `!=` on; if text longer than this is stored in a String attribute
/// it is highly unlikely that they will be used for exact string operations.
/// This also makes sure that we do not put strings into a BTree index that's
/// bigger than Postgres' limit on such strings which is about 2k
pub const STRING_PREFIX_SIZE: usize = 256;

/// Marker trait for tables that store entities
pub(crate) trait EntitySource {}

// Tables in the public schema that are shared across subgraphs. We put them
// in this module to make sure that nobody else gets access to them. All
// access to these tables must go through functions in this module.
mod public {
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
    #[derive(DbEnum, Debug, Clone, Copy)]
    pub enum DeploymentSchemaVersion {
        Split,
        Relational,
    }

    /// A deployment has an internal lifecycle that is controlled by this
    /// enum. A subgraph that is ready to be used for indexing is in state
    /// `Ready`. The state `Init` is used to indicate that the subgraph has
    /// remaining initialization work to do, in particular, that it needs to
    /// copy data if it is grafted onto another subgraph. The `Tables` state
    /// is used during schema migrations as described below. Both of these
    /// states move the subgraph to `Ready` upon successful completion of the
    /// work associated with them.
    ///
    /// When a subgraph is migrated, the migration is broken into two steps:
    /// in the first step, the schema changes that the migration needs are
    /// put into place; in the second step data is moved from the old storage
    /// scheme to the new one. These two steps happen in separate database
    /// transactions, since the first step if fast but takes fairly strong
    /// locks that can block other database work. The second step, moving data,
    /// only requires relatively weak locks that do not block write activity
    /// in other subgraphs.
    #[derive(DbEnum, Debug, Clone)]
    pub enum DeploymentSchemaState {
        Ready,
        Tables,
        Init,
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
            /// See comment on DeploymentSchemaState
            state -> crate::entities::public::DeploymentSchemaStateMapping,
        }
    }
}

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

/// A cache for storage objects as constructing them takes a bit of
/// computation. The cache lives as an attribute on the Store, but is managed
/// solely from this module
pub(crate) type StorageCache = Mutex<HashMap<SubgraphDeploymentId, Arc<Layout>>>;

pub(crate) fn make_storage_cache() -> StorageCache {
    Mutex::new(HashMap::new())
}

/// A connection into the database to handle entities. The connection is
/// specific to one subgraph, and can only handle entities from that subgraph
/// or from the metadata subgraph. Attempts to access other subgraphs will
/// generally result in a panic.
///
/// Instances of this struct must not be cached across transactions as there
/// is no mechanism in place to notify other index nodes that a subgraph has
/// been migrated
#[derive(Constructor)]
pub(crate) struct Connection<'a> {
    pub conn: MaybeOwned<'a, PooledConnection<ConnectionManager<PgConnection>>>,
    /// The storage of the subgraph we are dealing with; entities
    /// go into this
    storage: Arc<Layout>,
    /// The layout of the subgraph of subgraphs where we keep subgraph
    /// metadata
    metadata: Arc<Layout>,
}

impl Connection<'_> {
    /// Return the storage for `key`, which must refer either to the subgraph
    /// for this connection, or the metadata subgraph.
    ///
    /// # Panics
    ///
    /// If `key` does not reference the connection's subgraph or the metadata
    /// subgraph
    fn storage_for(&self, key: &EntityKey) -> &Layout {
        if key.subgraph_id == *SUBGRAPHS_ID {
            self.metadata.as_ref()
        } else if &key.subgraph_id == &self.storage.subgraph {
            self.storage.as_ref()
        } else {
            panic!(
                "A connection can only be used with one subgraph and \
                 the metadata subgraph.\nThe connection for {} is also \
                 used with {}",
                self.storage.subgraph, key.subgraph_id
            );
        }
    }

    /// Do any cleanup to bring the subgraph into a known good state
    pub(crate) fn start_subgraph(&self, logger: &Logger) -> Result<(), StoreError> {
        use public::deployment_schemas as dsl;
        use public::DeploymentSchemaState as State;

        let state = dsl::table
            .select(dsl::state)
            .filter(dsl::subgraph.eq(self.storage.subgraph.as_str()))
            .first::<State>(self.conn.as_ref())?;

        match state {
            State::Init => {
                let graft = metadata::deployment_graft(&self.conn, &self.storage.subgraph)?;
                let layout = &self.storage;
                let start = Instant::now();
                if let Some((base, block)) = graft {
                    let base = &Connection::layout(&self.conn, &base)?;
                    layout.copy_from(logger, &self.conn, &base, block, &self.metadata)?;
                }
                diesel::update(dsl::table)
                    .set(dsl::state.eq(State::Ready))
                    .filter(dsl::subgraph.eq(self.storage.subgraph.as_str()))
                    .execute(self.conn.as_ref())?;
                info!(logger, "Subgraph successfully initialized";
                              "time_ms" => start.elapsed().as_millis());
            }
            State::Tables => unimplemented!("continue the migration"),
            State::Ready => { // Nothing to do
            }
        }

        // Clear the `migrating` lock on the subgraph; this flag must be
        // visible to other db users before the migration starts and is
        // therefore set in its own txn before migration actually starts.
        // If the migration does not finish, e.g., because the server is shut
        // down, `migrating` remains set to `true` even though no work for
        // the migration is being done.
        Ok(
            diesel::update(dsl::table.filter(dsl::subgraph.eq(self.storage.subgraph.to_string())))
                .set(dsl::migrating.eq(false))
                .execute(self.conn.deref())
                .map(|_| ())?,
        )
    }

    pub(crate) fn find(
        &self,
        entity: &String,
        id: &String,
        block: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        self.storage.find(&self.conn, entity, id, block)
    }

    /// Returns a sequence of `(type, entity)`.
    /// If the entity isn't present that means it wasn't found.
    pub(crate) fn find_many(
        &self,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
        block: BlockNumber,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        self.storage.find_many(&self.conn, ids_for_type, block)
    }

    pub(crate) fn query<T: crate::relational_queries::FromEntityData>(
        &self,
        logger: &Logger,
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: EntityOrder,
        range: EntityRange,
        block: BlockNumber,
        query_id: Option<String>,
    ) -> Result<Vec<T>, QueryExecutionError> {
        self.storage.query(
            logger, &self.conn, collection, filter, order, range, block, query_id,
        )
    }

    pub(crate) fn conflicting_entity(
        &self,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        self.storage
            .conflicting_entity(&self.conn, entity_id, entities)
    }

    pub(crate) fn insert(
        &self,
        key: &EntityKey,
        entity: Entity,
        ptr: Option<&EthereumBlockPointer>,
    ) -> Result<(), StoreError> {
        let layout = &self.storage_for(key);
        match ptr {
            Some(ptr) => layout.insert(&self.conn, key, entity, block_number(ptr)),
            None => layout.insert_unversioned(&self.conn, key, entity),
        }
    }

    /// Overwrite an entity with a new version. The `history_event` indicates
    /// at which block the new version becomes valid if it is given. If it is
    /// `None`, the entity is treated as unversioned
    pub(crate) fn update(
        &self,
        key: &EntityKey,
        entity: Entity,
        ptr: Option<&EthereumBlockPointer>,
    ) -> Result<(), StoreError> {
        let layout = &self.storage_for(key);
        match ptr {
            Some(ptr) => layout.update(&self.conn, key, entity, block_number(&ptr)),
            None => layout
                .overwrite_unversioned(&self.conn, key, entity)
                .map(|_| ()),
        }
    }

    /// Update a metadata entity. The `entity` should only contain the fields
    /// that should be changed.
    pub(crate) fn update_metadata(
        &self,
        key: &EntityKey,
        entity: &Entity,
    ) -> Result<usize, StoreError> {
        self.metadata.update_unversioned(&self.conn, key, entity)
    }

    pub(crate) fn find_metadata(
        &self,
        entity: &String,
        id: &String,
    ) -> Result<Option<Entity>, StoreError> {
        self.metadata.find(&self.conn, entity, id, BLOCK_NUMBER_MAX)
    }

    pub(crate) fn delete(
        &self,
        key: &EntityKey,
        ptr: Option<&EthereumBlockPointer>,
    ) -> Result<usize, StoreError> {
        let layout = &self.storage_for(key);
        match ptr {
            Some(ptr) => layout.delete(&self.conn, key, block_number(&ptr)),
            None => layout.delete_unversioned(&self.conn, key),
        }
    }

    pub(crate) fn revert_block(
        &self,
        block_ptr: &EthereumBlockPointer,
    ) -> Result<(StoreEvent, i32), StoreError> {
        // At 1 block per 15 seconds, the maximum i32
        // value affords just over 1020 years of blocks.
        let block = block_ptr
            .number
            .try_into()
            .expect("block numbers fit into an i32");

        // Revert the block in the subgraph itself
        let (event, count) = self.storage.revert_block(&self.conn, block)?;
        // Revert the meta data changes that correspond to this subgraph.
        // Only certain meta data changes need to be reverted, most
        // importantly creation of dynamic data sources. We ensure in the
        // rest of the code that we only record history for those meta data
        // changes that might need to be reverted
        let meta_event =
            self.metadata
                .revert_metadata(&self.conn, &self.storage.subgraph, block)?;
        Ok((event.extend(meta_event), count))
    }

    pub(crate) fn update_entity_count(&self, count: i32) -> Result<(), StoreError> {
        if count == 0 {
            return Ok(());
        }

        let count_query = self.storage.count_query.as_str();

        // The big complication in this query is how to determine what the
        // new entityCount should be. We want to make sure that if the entityCount
        // is NULL or the special value `-1`, it gets recomputed. Using `-1` here
        // makes it possible to manually set the `entityCount` to that value
        // to force a recount; setting it to `NULL` is not desirable since
        // `entityCount` on the GraphQL level is not nullable, and so setting
        // `entityCount` to `NULL` could cause errors at that layer; temporarily
        // returning `-1` is more palatable. To be exact, recounts have to be
        // done here, from the subgraph writer.
        //
        // The first argument of `coalesce` will be `NULL` if the entity count
        // is `NULL` or `-1`, forcing `coalesce` to evaluate its second
        // argument, the query to count entities. In all other cases,
        // `coalesce` does not evaluate its second argument
        let query = format!(
            "
            update subgraphs.subgraph_deployment
               set entity_count =
                     coalesce((nullif(entity_count, -1)) + $1,
                              ({count_query}))
             where id = $2
            ",
            count_query = count_query
        );
        let conn: &PgConnection = &self.conn;
        Ok(diesel::sql_query(query)
            .bind::<Integer, _>(count)
            .bind::<Text, _>(self.storage.subgraph.as_str())
            .execute(conn)
            .map(|_| ())?)
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

        if false {
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
        block_ptr: &EthereumBlockPointer,
    ) -> Result<bool, Error> {
        // How many simultaneous subgraph migrations we allow
        const MIGRATION_LIMIT: i32 = 2;
        let subgraph = &self.storage.subgraph;

        if !self.should_migrate(subgraph, block_ptr)? {
            return Ok(false);
        }

        let do_migrate = self.conn.transaction(|| -> Result<bool, Error> {
            let lock =
                diesel::sql_query("lock table public.deployment_schemas in exclusive mode nowait")
                    .execute(self.conn.deref());
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
            Ok(query.execute(self.conn.deref())? > 0)
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
                .execute(self.conn.deref())?;
            result
        } else {
            Ok(false)
        }
    }

    /// Perform one migration step and return true if there are more steps
    /// left to do. Each step of the migration is performed in  a separate
    /// transaction so that any locks a step takes are freed up at the end
    // We do not currently use this, but getting the framework right was
    // painful enough that we should preserve the general setup of
    // per-subgraph migrations
    #[allow(unreachable_code, unused_variables)]
    fn migration_step(
        &self,
        logger: &Logger,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<bool, Error> {
        unreachable!("The curent code base does not require any subgraph migrations");
        self.conn.transaction(|| -> Result<bool, Error> {
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
            // Do the actual migration, and return an updated storage
            // object, something like
            //
            // let storage = storage.migrate(&self.conn, logger, &schema)?;
            // let needs_migrating = storage.needs_migrating();
            // self.cache.borrow_mut().insert(subgraph.clone(), Arc::new(storage));
            //
            info!(
                logger,
                "finished migrating";
                "name" => &schema.name,
                "subgraph" => subgraph.to_string(),
                "state" => format!("{:?}", schema.state),
                "migration_time_ms" => start.elapsed().as_millis()
            );
            Ok(false)
        })
    }

    pub(crate) fn send_store_event(&self, event: &StoreEvent) -> Result<(), StoreError> {
        let v = serde_json::to_value(event)?;
        #[cfg(debug_assertions)]
        {
            if *EVENT_TAP_ENABLED.lock().unwrap() {
                EVENT_TAP.lock().unwrap().push(event.clone());
            }
        }
        JsonNotification::send("store_events", &v, &self.conn)
    }

    pub(crate) fn transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        self.conn.transaction(f)
    }

    /// Create the database schema for a new subgraph, including all tables etc.
    ///
    /// It is an error if `deployment_schemas` already has an entry for this
    /// `subgraph_id`. Note that `self` must be a connection for the subgraph
    /// of subgraphs
    pub(crate) fn create_schema(&self, schema: &SubgraphSchema) -> Result<(), StoreError> {
        use self::public::DeploymentSchemaState as s;
        use self::public::DeploymentSchemaVersion as v;

        assert_eq!(
            &*SUBGRAPHS_ID, &self.storage.subgraph,
            "create_schema can only be called on a Connection for the metadata subgraph"
        );

        // Check if there already is an entry for this subgraph. If so, do
        // nothing
        let count = deployment_schemas::table
            .filter(deployment_schemas::subgraph.eq(schema.id.to_string()))
            .count()
            .first::<i64>(self.conn.deref())?;
        if count > 0 {
            return Ok(());
        }

        // Create a schema for the deployment.
        let schemas: Vec<String> = diesel::insert_into(deployment_schemas::table)
            .values((
                deployment_schemas::subgraph.eq(schema.id.to_string()),
                deployment_schemas::version.eq(v::Relational),
                deployment_schemas::state.eq(s::Init),
            ))
            .returning(deployment_schemas::name)
            .get_results(self.conn.deref())?;
        let schema_name = schemas
            .first()
            .ok_or_else(|| format_err!("failed to read schema name for {} back", &schema.id))?;

        let query = format!("create schema {}", schema_name);
        self.conn.batch_execute(&*query)?;

        let layout = Layout::create_relational_schema(&self.conn, schema, schema_name.to_owned())?;
        // See if we are grafting and check that the graft is permissible
        if let Some((base, _)) = metadata::deployment_graft(&self.conn, &schema.id)? {
            let base = &Connection::layout(&self.conn, &base)?;
            let errors = layout.can_copy_from(&base);
            if !errors.is_empty() {
                return Err(StoreError::Unknown(format_err!(
                    "The subgraph `{}` cannot be used as the graft base \
                                        for `{}` because the schemas are incompatible:\n    - {}",
                    &base.subgraph,
                    &layout.subgraph,
                    errors.join("\n    - ")
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn supports_proof_of_indexing(&self) -> bool {
        self.storage.tables.contains_key(POI_OBJECT)
    }

    /// Look up the schema for `subgraph` and return its entity storage.
    /// Returns an error if `subgraph` does not have an entry in
    /// `deployment_schemas`, which can only happen if `create_schema` was not
    /// called for that `subgraph`
    pub(crate) fn layout(
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Layout, StoreError> {
        use public::DeploymentSchemaVersion as V;

        let schema = find_schema(conn, subgraph)?
            .ok_or_else(|| StoreError::Unknown(format_err!("unknown subgraph {}", subgraph)))?;
        let layout = match schema.version {
            V::Split => {
                return Err(StoreError::ConstraintViolation(format!(
                    "the subgraph {} uses JSONB storage which is not supported any longer",
                    subgraph.as_str()
                )))
            }
            V::Relational => {
                let subgraph_schema = metadata::subgraph_schema(conn, subgraph.to_owned())?;
                let has_poi = supports_proof_of_indexing(conn, subgraph, &schema.name)?;
                let catalog = Catalog::new(conn, schema.name)?;
                Layout::new(&subgraph_schema, catalog, has_poi)?
            }
        };
        Ok(layout)
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

fn supports_proof_of_indexing(
    conn: &diesel::pg::PgConnection,
    subgraph_id: &SubgraphDeploymentId,
    schema: &str,
) -> Result<bool, StoreError> {
    if subgraph_id == &*SUBGRAPHS_ID {
        return Ok(false);
    }
    #[derive(Debug, QueryableByName)]
    struct Table {
        #[sql_type = "Text"]
        pub table_name: String,
    }
    let query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2";
    let result: Vec<Table> = diesel::sql_query(query)
        .bind::<Text, _>(schema)
        .bind::<Text, _>(POI_TABLE)
        .load(conn)?;
    Ok(result.len() > 0)
}

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
#[cfg(debug_assertions)]
pub fn delete_all_entities_for_test_use_only(
    store: &crate::NetworkStore,
    conn: &PgConnection,
) -> Result<(), StoreError> {
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
    // Generated by running 'layout -g delete subgraphs.graphql'
    let query = "
        delete from subgraphs.ethereum_block_handler_filter_entity;
        delete from subgraphs.ethereum_contract_source;
        delete from subgraphs.dynamic_ethereum_contract_data_source;
        delete from subgraphs.ethereum_contract_abi;
        delete from subgraphs.subgraph;
        delete from subgraphs.subgraph_deployment;
        delete from subgraphs.ethereum_block_handler_entity;
        delete from subgraphs.subgraph_deployment_assignment;
        delete from subgraphs.ethereum_contract_mapping;
        delete from subgraphs.subgraph_version;
        delete from subgraphs.subgraph_manifest;
        delete from subgraphs.ethereum_call_handler_entity;
        delete from subgraphs.ethereum_contract_data_source;
        delete from subgraphs.ethereum_contract_data_source_template;
        delete from subgraphs.ethereum_contract_data_source_template_source;
        delete from subgraphs.ethereum_contract_event_handler;
    ";
    conn.batch_execute(query)?;
    store.clear_storage_cache();
    Ok(())
}

/// Drop the schema for `subgraph`. This deletes all data for the subgraph,
/// and can not be reversed. It does not remove any of the metadata in
/// `subgraphs.entities` associated with the subgraph
#[cfg(debug_assertions)]
fn drop_schema(
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

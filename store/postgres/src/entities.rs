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
use diesel::RunQueryDsl;
use maybe_owned::MaybeOwned;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use graph::data::schema::Schema as SubgraphSchema;
use graph::data::subgraph::schema::{POI_OBJECT, POI_TABLE, SUBGRAPHS_ID};
use graph::prelude::{
    format_err, info, BlockNumber, Entity, EntityCollection, EntityFilter, EntityKey, EntityOrder,
    EntityRange, EthereumBlockPointer, Logger, QueryExecutionError, StoreError, StoreEvent,
    SubgraphDeploymentId, BLOCK_NUMBER_MAX,
};

use crate::block_range::block_number;
use crate::deployment;
use crate::primary::Site;
use crate::relational::{Catalog, Layout};

/// The size of string prefixes that we index. This is chosen so that we
/// will index strings that people will do string comparisons like
/// `=` or `!=` on; if text longer than this is stored in a String attribute
/// it is highly unlikely that they will be used for exact string operations.
/// This also makes sure that we do not put strings into a BTree index that's
/// bigger than Postgres' limit on such strings which is about 2k
pub const STRING_PREFIX_SIZE: usize = 256;

/// Marker trait for tables that store entities
pub(crate) trait EntitySource {}

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
/// Instances of this struct must not be cached across transactions as it
/// contains a database connection
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
    pub(crate) fn start_subgraph(
        &self,
        logger: &Logger,
        graft_base: Option<(Site, EthereumBlockPointer)>,
    ) -> Result<(), StoreError> {
        if let Some((base, block)) = graft_base {
            let layout = &self.storage;
            let start = Instant::now();
            let base = &Connection::layout(&self.conn, &base.namespace, &base.deployment)?;
            layout.copy_from(logger, &self.conn, &base, block, &self.metadata)?;
            // Set the block ptr to the graft point to signal that we successfully
            // performed the graft
            deployment::forward_block_ptr(&self.conn, &self.storage.subgraph, block.clone())?;
            info!(logger, "Subgraph successfully initialized";
            "time_ms" => start.elapsed().as_millis());
        }
        Ok(())
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
    pub(crate) fn create_schema(
        &self,
        db_schema: String,
        schema: &SubgraphSchema,
        graft_site: Option<Site>,
    ) -> Result<(), StoreError> {
        let query = format!("create schema {}", db_schema);
        self.conn.batch_execute(&*query)?;

        let layout = Layout::create_relational_schema(&self.conn, schema, db_schema.to_owned())?;
        // See if we are grafting and check that the graft is permissible
        if let Some(graft_site) = graft_site {
            let base =
                &Connection::layout(&self.conn, &graft_site.namespace, &graft_site.deployment)?;
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
        schema: &str,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Layout, StoreError> {
        let subgraph_schema = deployment::schema(conn, subgraph.to_owned())?;
        let has_poi = supports_proof_of_indexing(conn, subgraph, schema)?;
        let catalog = Catalog::new(conn, schema.to_string())?;
        let layout = Layout::new(&subgraph_schema, catalog, has_poi)?;

        Ok(layout)
    }
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

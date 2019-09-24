use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::{insert_into, select, update};
use futures::sync::mpsc::{channel, Sender};
use lru_time_cache::LruCache;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

use graph::components::store::Store as StoreTrait;
use graph::data::subgraph::schema::*;
use graph::prelude::serde_json;
use graph::prelude::{ChainHeadUpdateListener as _, *};
use graph_graphql::prelude::api_schema;
use tokio::timer::Interval;
use web3::types::H256;

use crate::block_range::BLOCK_NUMBER_MAX;
use crate::chain_head_listener::ChainHeadUpdateListener;
use crate::entities as e;
use crate::functions::{attempt_chain_head_update, lookup_ancestor_block};
use crate::history_event::HistoryEvent;
use crate::store_events::StoreEventListener;

embed_migrations!("./migrations");

/// Run all schema migrations.
///
/// When multiple `graph-node` processes start up at the same time, we ensure
/// that they do not run migrations in parallel by using `blocking_conn` to
/// serialize them. The `conn` is used to run the actual migration.
fn initiate_schema(logger: &Logger, conn: &PgConnection, blocking_conn: &PgConnection) {
    // Collect migration logging output
    let mut output = vec![];

    // Make sure the locking table exists so we have something
    // to lock. We intentionally ignore errors here, because they are most
    // likely caused by us losing a race to create the table against another
    // graph-node. If this truly is an error, we will trip over it when
    // we try to lock the table and report it to the user
    if let Err(e) = blocking_conn.batch_execute(
        "create table if not exists \
         __graph_node_global_lock(id int)",
    ) {
        debug!(
            logger,
            "Creating lock table failed, this is most likely harmless";
            "error" => format!("{:?}", e)
        );
    }

    // blocking_conn holds the lock on the migrations table for the duration
    // of the migration on conn. Since all nodes execute this code, only one
    // of them can run this code at the same time. We need to use two
    // connections for this because diesel will run each migration in its
    // own txn, which makes it impossible to hold a lock across all of them
    // on that connection
    info!(
        logger,
        "Waiting for other graph-node instances to finish migrating"
    );
    let result = blocking_conn.transaction(|| {
        diesel::sql_query("lock table __graph_node_global_lock in exclusive mode")
            .execute(blocking_conn)?;
        info!(logger, "Running migrations");
        embedded_migrations::run_with_output(conn, &mut output)
    });
    info!(logger, "Migrations finished");

    match result {
        Ok(_) => info!(logger, "Completed pending Postgres schema migrations"),
        Err(e) => panic!(
            "Error setting up Postgres database: \
             You may need to drop and recreate your database to work with the \
             latest version of graph-node. Error information: {:?}",
            e
        ),
    };
    // If there was any migration output, log it now
    if !output.is_empty() {
        debug!(
            logger, "Postgres migration output";
            "output" => String::from_utf8(output)
                .unwrap_or_else(|_| String::from("<unreadable>"))
        );
        // We take getting output as a signal that a migration was actually
        // run, which is not easy to tell from the Diesel API, and reset the
        // query statistics since a schema change makes them not all that
        // useful. An error here is not serious and can be ignored.
        let res = conn.batch_execute("select pg_stat_statements_reset()");
        if let Err(e) = res {
            trace!(logger, "Failed to reset query statistics ({})", e);
        }
    }
}

/// Configuration for the Diesel/Postgres store.
pub struct StoreConfig {
    pub postgres_url: String,
    pub network_name: String,
    pub start_block: u64,
}

#[derive(Clone)]
struct SchemaPair {
    raw: Arc<Schema>,
    api: Arc<Schema>,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    logger: Logger,
    subscriptions: Arc<RwLock<HashMap<String, Sender<StoreEvent>>>>,
    // listen to StoreEvents generated when applying entity operations
    listener: StoreEventListener,
    chain_head_update_listener: ChainHeadUpdateListener,
    network_name: String,
    genesis_block_ptr: EthereumBlockPointer,
    conn: Pool<ConnectionManager<PgConnection>>,
    schema_cache: Mutex<LruCache<SubgraphDeploymentId, SchemaPair>>,
    /// A cache for the storage metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) storage_cache: e::StorageCache,
}

impl Store {
    pub fn new(
        config: StoreConfig,
        logger: &Logger,
        net_identifiers: EthereumNetworkIdentifier,
        pool: Pool<ConnectionManager<PgConnection>>,
    ) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Create the entities table (if necessary)
        initiate_schema(&logger, &pool.get().unwrap(), &pool.get().unwrap());

        // Listen to entity changes in Postgres
        let mut listener = StoreEventListener::new(&logger, config.postgres_url.clone());
        let store_events = listener
            .take_event_stream()
            .expect("Failed to listen to entity change events in Postgres");

        // Create the store
        let mut store = Store {
            logger: logger.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            listener,
            chain_head_update_listener: ChainHeadUpdateListener::new(
                &logger,
                config.postgres_url,
                config.network_name.clone(),
            ),
            network_name: config.network_name.clone(),
            genesis_block_ptr: (net_identifiers.genesis_block_hash, config.start_block).into(),
            conn: pool,
            schema_cache: Mutex::new(LruCache::with_capacity(100)),
            storage_cache: e::make_storage_cache(),
        };

        // Add network to store and check network identifiers
        store.add_network_if_missing(net_identifiers).unwrap();

        // Deal with store subscriptions
        store.handle_store_events(store_events);
        store.periodically_clean_up_stale_subscriptions();

        // We're ready for processing entity changes
        store.listener.start();

        // Return the store
        store
    }

    fn add_network_if_missing(
        &self,
        new_net_identifiers: EthereumNetworkIdentifier,
    ) -> Result<(), Error> {
        use crate::db_schema::ethereum_networks::dsl::*;

        let new_genesis_block_hash = new_net_identifiers.genesis_block_hash;
        let new_net_version = new_net_identifiers.net_version;

        let network_identifiers_opt = ethereum_networks
            .select((net_version, genesis_block_hash))
            .filter(name.eq(&self.network_name))
            .first::<(Option<String>, Option<String>)>(&*self.get_conn()?)
            .optional()?;

        match network_identifiers_opt {
            // Network is missing in database
            None => {
                insert_into(ethereum_networks)
                    .values((
                        name.eq(&self.network_name),
                        head_block_hash.eq::<Option<String>>(None),
                        head_block_number.eq::<Option<i64>>(None),
                        net_version.eq::<Option<String>>(Some(new_net_version.to_owned())),
                        genesis_block_hash
                            .eq::<Option<String>>(Some(format!("{:x}", new_genesis_block_hash))),
                    ))
                    .on_conflict(name)
                    .do_nothing()
                    .execute(&*self.get_conn()?)?;
            }

            // Network is in database and has identifiers
            Some((Some(last_net_version), Some(last_genesis_block_hash))) => {
                if last_net_version != new_net_version {
                    panic!(
                        "Ethereum node provided net_version {}, \
                         but we expected {}. Did you change networks \
                         without changing the network name?",
                        new_net_version, last_net_version
                    );
                }

                if last_genesis_block_hash.parse().ok() != Some(new_genesis_block_hash) {
                    panic!(
                        "Ethereum node provided genesis block hash {}, \
                         but we expected {}. Did you change networks \
                         without changing the network name?",
                        new_genesis_block_hash, last_genesis_block_hash
                    );
                }
            }

            // Network is in database but is missing identifiers
            Some(_) => {
                update(ethereum_networks)
                    .set((
                        net_version.eq::<Option<String>>(Some(new_net_version.to_owned())),
                        genesis_block_hash
                            .eq::<Option<String>>(Some(format!("{:x}", new_genesis_block_hash))),
                    ))
                    .filter(name.eq(&self.network_name))
                    .execute(&*self.get_conn()?)?;
            }
        }

        Ok(())
    }

    /// Receive store events from Postgres and send them to all active
    /// subscriptions. Detect stale subscriptions in the process and
    /// close them.
    fn handle_store_events(
        &self,
        store_events: Box<dyn Stream<Item = StoreEvent, Error = ()> + Send>,
    ) {
        let logger = self.logger.clone();
        let subscriptions = self.subscriptions.clone();

        tokio::spawn(store_events.for_each(move |event| {
            let senders = subscriptions.read().unwrap().clone();
            let logger = logger.clone();
            let subscriptions = subscriptions.clone();

            // Write change to all matching subscription streams; remove subscriptions
            // whose receiving end has been dropped
            stream::iter_ok::<_, ()>(senders).for_each(move |(id, sender)| {
                let logger = logger.clone();
                let subscriptions = subscriptions.clone();

                sender.send(event.clone()).then(move |result| {
                    match result {
                        Err(_send_error) => {
                            // Receiver was dropped
                            debug!(logger, "Unsubscribe"; "id" => &id);
                            subscriptions.write().unwrap().remove(&id);
                            Ok(())
                        }
                        Ok(_sender) => Ok(()),
                    }
                })
            })
        }));
    }

    fn periodically_clean_up_stale_subscriptions(&self) {
        let logger = self.logger.clone();
        let subscriptions = self.subscriptions.clone();

        // Clean up stale subscriptions every 5s
        tokio::spawn(
            Interval::new(Instant::now(), Duration::from_secs(5))
                .for_each(move |_| {
                    let mut subscriptions = subscriptions.write().unwrap();

                    // Obtain IDs of subscriptions whose receiving end has gone
                    let stale_ids = subscriptions
                        .iter_mut()
                        .filter_map(|(id, sender)| match sender.poll_ready() {
                            Err(_) => Some(id.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();

                    // Remove all stale subscriptions
                    for id in stale_ids {
                        debug!(logger, "Unsubscribe"; "id" => &id);
                        subscriptions.remove(&id);
                    }

                    Ok(())
                })
                .map_err(|_| unreachable!()),
        );
    }

    /// Gets an entity from Postgres.
    fn get_entity(
        &self,
        conn: &e::Connection,
        op_subgraph: &SubgraphDeploymentId,
        op_entity: &String,
        op_id: &String,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        // We should really have callers pass in a block number; but until
        // that is fully plumbed in, we just use the biggest possible block
        // number so that we will always return the latest version,
        // i.e., the one with an infinite upper bound
        conn.find(op_entity, op_id, BLOCK_NUMBER_MAX).map_err(|e| {
            QueryExecutionError::ResolveEntityError(
                op_subgraph.clone(),
                op_entity.clone(),
                op_id.clone(),
                format!("Invalid entity {}", e),
            )
        })
    }

    fn execute_query(
        &self,
        conn: &e::Connection,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        // Add order by filters to query
        let order = match query.order_by {
            Some((attribute, value_type)) => {
                let direction = query
                    .order_direction
                    .map(|direction| match direction {
                        EntityOrder::Ascending => "ASC",
                        EntityOrder::Descending => "DESC",
                    })
                    .unwrap_or("ASC");
                Some((attribute, value_type, direction))
            }
            None => None,
        };

        // Process results; deserialize JSON data
        conn.query(
            query.entity_types,
            query.filter,
            order,
            query.range.first,
            query.range.skip,
            BLOCK_NUMBER_MAX,
        )
    }

    fn check_interface_entity_uniqueness(
        &self,
        conn: &e::Connection,
        key: &EntityKey,
    ) -> Result<(), StoreError> {
        // Collect all types that share an interface implementation with this
        // entity type, and make sure there are no conflicting IDs.
        //
        // To understand why this is necessary, suppose that `Dog` and `Cat` are
        // types and both implement an interface `Pet`, and both have instances
        // with `id: "Fred"`. If a type `PetOwner` has a field `pets: [Pet]`
        // then with the value `pets: ["Fred"]`, there's no way to disambiguate
        // if that's Fred the Dog, Fred the Cat or both.
        //
        // This assumes that there are no concurrent writes to a subgraph.
        let schema = self.subgraph_schema(&key.subgraph_id)?;
        let types_for_interface = schema.types_for_interface();
        let types_with_shared_interface = Vec::from_iter(
            schema
                .interfaces_for_type(&key.entity_type)
                .into_iter()
                .flatten()
                .map(|interface| &types_for_interface[&interface.name])
                .flatten()
                .map(|object_type| &object_type.name)
                .filter(|type_name| **type_name != key.entity_type),
        );

        if !types_with_shared_interface.is_empty() {
            if let Some(conflicting_entity) =
                conn.conflicting_entity(&key.entity_id, types_with_shared_interface)?
            {
                return Err(StoreError::ConflictingId(
                    key.entity_type.clone(),
                    key.entity_id.clone(),
                    conflicting_entity,
                ));
            }
        }
        Ok(())
    }

    /// Apply a metadata operation in Postgres.
    fn apply_metadata_operation(
        &self,
        conn: &e::Connection,
        operation: MetadataOperation,
    ) -> Result<i32, StoreError> {
        match operation {
            MetadataOperation::Set { entity, id, data } => {
                let key = MetadataOperation::entity_key(entity, id);

                self.check_interface_entity_uniqueness(conn, &key)?;

                // Load the entity if exists
                let entity = self
                    .get_entity(conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
                    .map_err(Error::from)?;

                // Identify whether this is an insert or an update operation and
                // merge the changes into the entity.
                let result = match entity {
                    Some(mut entity) => {
                        entity.merge(data);
                        conn.update(&key, &entity, None).map(|_| 0)
                    }
                    None => {
                        // Merge with a new entity since that removes values that
                        // were set to Value::Null
                        let mut entity = Entity::new();
                        entity.merge(data);
                        conn.insert(&key, &entity, None).map(|_| 1)
                    }
                };

                result.map_err(|e| {
                    format_err!(
                        "Failed to set entity ({}, {}, {}): {}",
                        key.subgraph_id,
                        key.entity_type,
                        key.entity_id,
                        e
                    )
                    .into()
                })
            }
            MetadataOperation::Update {
                entity,
                id,
                data,
                guard,
            } => {
                let key = MetadataOperation::entity_key(entity, id);

                self.check_interface_entity_uniqueness(conn, &key)?;

                // Update the entity in Postgres
                match conn.update_metadata(&key, &data, guard)? {
                    0 => Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids: vec![key.entity_id.clone()],
                        actual_entity_ids: vec![],
                        description: "update did not change any rows".to_owned(),
                    }
                    .into()),
                    1 => Ok(0),
                    res => Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids: vec![key.entity_id.clone()],
                        actual_entity_ids: vec![],
                        description: format!("update changed {} rows instead of just one", res),
                    }
                    .into()),
                }
            }
            MetadataOperation::Remove { entity, id } => {
                let key = MetadataOperation::entity_key(entity, id);
                conn.delete(&key, None)
                    // This conversion is ok since n will only be 0 or 1
                    .map(|n| -(n as i32))
                    .map_err(|e| {
                        format_err!(
                            "Failed to remove entity ({}, {}, {}): {}",
                            key.subgraph_id,
                            key.entity_type,
                            key.entity_id,
                            e
                        )
                        .into()
                    })
            }
            MetadataOperation::AbortUnless {
                description,
                query,
                entity_ids: mut expected_entity_ids,
            } => {
                // Execute query
                let actual_entities = self.execute_query(conn, query.clone()).map_err(|e| {
                    format_err!(
                        "AbortUnless ({}): query execution error: {:?}, {}",
                        description,
                        query,
                        e
                    )
                })?;

                // Extract IDs from entities
                let mut actual_entity_ids: Vec<String> = actual_entities
                    .into_iter()
                    .map(|entity| entity.id())
                    .collect::<Result<_, _>>()?;

                // Sort entity IDs lexicographically if and only if no sort order is specified.
                // When no sort order is specified, the entity ordering is arbitrary and should not be a
                // factor in deciding whether or not to abort.
                if query.order_by.is_none() {
                    expected_entity_ids.sort();
                    actual_entity_ids.sort();
                }

                // Abort if actual IDs do not match expected
                if actual_entity_ids != expected_entity_ids {
                    return Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids,
                        actual_entity_ids,
                        description,
                    }
                    .into());
                }

                // Safe to continue
                Ok(0)
            }
        }
    }

    fn apply_entity_modifications(
        &self,
        conn: &e::Connection,
        mods: Vec<EntityModification>,
        history_event: Option<&HistoryEvent>,
    ) -> Result<(), StoreError> {
        let mut count = 0;

        for modification in mods {
            use EntityModification::*;

            let do_count = !modification.entity_key().subgraph_id.is_meta();
            let n = match modification {
                Overwrite { key, data } => {
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    conn.update(&key, &data, history_event).map(|_| 0)
                }
                Insert { key, data } => {
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    conn.insert(&key, &data, history_event).map(|_| 1)
                }
                Remove { key } => conn
                    .delete(&key, history_event)
                    // This conversion is ok since n will only be 0 or 1
                    .map(|n| -(n as i32))
                    .map_err(|e| {
                        format_err!(
                            "Failed to remove entity ({}, {}, {}): {}",
                            key.subgraph_id,
                            key.entity_type,
                            key.entity_id,
                            e
                        )
                        .into()
                    }),
            }?;
            if do_count {
                count += n;
            }
        }
        conn.update_entity_count(count)?;
        Ok(())
    }

    fn apply_metadata_operations_with_conn(
        &self,
        econn: &e::Connection,
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        // Emit a store event for the changes we are about to make
        let event: StoreEvent = operations.clone().into();
        econn.send_store_event(&event)?;

        // Actually apply the operations
        for operation in operations.into_iter() {
            self.apply_metadata_operation(econn, operation)?;
        }
        Ok(())
    }

    /// Build a partial Postgres index on a Subgraph-Entity-Attribute
    fn build_entity_attribute_index_with_conn(
        &self,
        conn: &e::Connection,
        index: AttributeIndexDefinition,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        conn.build_attribute_index(&index)
            .map_err(|e| SubgraphAssignmentProviderError::Unknown(e.into()))
            .and_then(move |row_count| match row_count {
                1 => Ok(()),
                _ => Err(SubgraphAssignmentProviderError::BuildIndexesError(
                    index.subgraph_id.to_string(),
                    index.entity_name.clone(),
                    index.attribute_name.clone(),
                )),
            })
    }

    /// Build a set of indexes on the entities table
    fn build_entity_attribute_indexes_with_conn(
        &self,
        conn: &e::Connection,
        indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        for index in indexes.into_iter() {
            self.build_entity_attribute_index_with_conn(conn, index)?;
        }
        Ok(())
    }

    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        let start_time = Instant::now();
        let conn = self.conn.get();
        let wait = start_time.elapsed();
        if wait > Duration::from_millis(10) {
            warn!(self.logger, "Possible contention in DB connection pool";
                               "wait_ms" => wait.as_millis())
        }
        conn.map_err(Error::from)
    }

    fn get_entity_conn(&self, subgraph: &SubgraphDeploymentId) -> Result<e::Connection, Error> {
        let conn = self.get_conn()?;
        let storage = self.storage(&conn, subgraph)?;
        let metadata = self.storage(&conn, &*SUBGRAPHS_ID)?;
        Ok(e::Connection::new(conn, storage, metadata))
    }

    /// Return the storage for the subgraph. Since constructing a `Storage`
    /// object takes a bit of computation, we cache storage objects that do
    /// not have a pending migration in the Store, i.e., for the lifetime of
    /// the Store. Storage objects with a pending migration can not be
    /// cached for longer than a transaction since they might change
    /// without us knowing
    fn storage(
        &self,
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Arc<e::Storage>, StoreError> {
        if let Some(storage) = self.storage_cache.lock().unwrap().get(subgraph) {
            return Ok(storage.clone());
        }

        let storage = Arc::new(e::Storage::new(conn, subgraph, self)?);
        if storage.is_cacheable() {
            self.storage_cache
                .lock()
                .unwrap()
                .insert(subgraph.clone(), storage.clone());
        }
        Ok(storage.clone())
    }

    fn cached_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<SchemaPair, Error> {
        if let Some(pair) = self.schema_cache.lock().unwrap().get(&subgraph_id) {
            trace!(self.logger, "schema cache hit"; "id" => subgraph_id.to_string());
            return Ok(pair.clone());
        }
        trace!(self.logger, "schema cache miss"; "id" => subgraph_id.to_string());

        let raw_schema = if *subgraph_id == *SUBGRAPHS_ID {
            // The subgraph of subgraphs schema is built-in.
            include_str!("subgraphs.graphql").to_owned()
        } else {
            let manifest_entity = self
                .get(EntityKey {
                    subgraph_id: SUBGRAPHS_ID.clone(),
                    entity_type: SubgraphManifestEntity::TYPENAME.to_owned(),
                    entity_id: SubgraphManifestEntity::id(&subgraph_id),
                })?
                .ok_or_else(|| format_err!("Subgraph entity not found {}", subgraph_id))?;

            match manifest_entity.get("schema") {
                Some(Value::String(raw)) => raw.clone(),
                _ => {
                    return Err(format_err!(
                        "Schema not present or has wrong type, subgraph: {}",
                        subgraph_id
                    ));
                }
            }
        };

        // Parse the schema and add @subgraphId directives
        let raw_schema = Schema::parse(&raw_schema, subgraph_id.clone())?;
        let mut schema = raw_schema.clone();

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        schema.document = api_schema(&schema.document)?;
        schema.add_subgraph_id_directives(subgraph_id.clone());

        let pair = SchemaPair {
            raw: Arc::new(raw_schema),
            api: Arc::new(schema),
        };

        // Insert the schema into the cache.
        let mut cache = self.schema_cache.lock().unwrap();
        cache.insert(subgraph_id.clone(), pair);

        Ok(cache.get(&subgraph_id).unwrap().clone())
    }

    pub(crate) fn raw_subgraph_schema(
        &self,
        subgraph_id: &SubgraphDeploymentId,
    ) -> Result<Arc<Schema>, Error> {
        Ok(self.cached_schema(subgraph_id)?.raw)
    }
}

impl StoreTrait for Store {
    fn block_ptr(&self, subgraph_id: SubgraphDeploymentId) -> Result<EthereumBlockPointer, Error> {
        let subgraph_entity = self
            .get(SubgraphDeploymentEntity::key(subgraph_id.clone()))
            .map_err(|e| format_err!("error reading subgraph entity: {}", e))?
            .ok_or_else(|| {
                format_err!(
                    "could not read block ptr for non-existent subgraph {}",
                    subgraph_id
                )
            })?;

        let hash = subgraph_entity
            .get("latestEthereumBlockHash")
            .ok_or_else(|| format_err!("SubgraphDeployment is missing latestEthereumBlockHash"))?
            .to_owned()
            .as_string()
            .ok_or_else(|| {
                format_err!("SubgraphDeployment has wrong type in latestEthereumBlockHash")
            })?
            .parse::<H256>()
            .map_err(|e| format_err!("latestEthereumBlockHash: {}", e))?;

        let number = subgraph_entity
            .get("latestEthereumBlockNumber")
            .ok_or_else(|| format_err!("SubgraphDeployment is missing latestEthereumBlockNumber"))?
            .to_owned()
            .as_bigint()
            .ok_or_else(|| {
                format_err!("SubgraphDeployment has wrong type in latestEthereumBlockNumber")
            })?
            .to_u64();

        Ok(EthereumBlockPointer { hash, number })
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&key.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.get_entity(&conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&query.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.execute_query(&conn, query)
    }

    fn find_one(&self, mut query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        query.range = EntityRange::first(1);

        let conn = self
            .get_entity_conn(&query.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        let mut results = self.execute_query(&conn, query)?;
        match results.len() {
            0 | 1 => Ok(results.pop()),
            n => panic!("find_one query found {} results", n),
        }
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        use crate::db_schema::ens_names as dsl;

        let conn = self
            .get_conn()
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        dsl::table
            .select(dsl::name)
            .find(hash)
            .get_result::<String>(&conn)
            .optional()
            .map_err(|e| {
                QueryExecutionError::StoreError(format_err!(
                    "error looking up ens_name for hash {}: {}",
                    hash,
                    e
                ))
            })
    }

    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<bool, StoreError> {
        let ops = SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
            &subgraph_id,
            block_ptr_from,
            block_ptr_to,
        );
        let conn = self.get_entity_conn(&subgraph_id).map_err(Error::from)?;
        conn.transaction(|| self.apply_metadata_operations_with_conn(&conn, ops))?;

        conn.should_migrate(&subgraph_id, &block_ptr_to)
    }

    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
    ) -> Result<bool, StoreError> {
        // Sanity check on block numbers
        if block_ptr_from.number != block_ptr_to.number - 1 {
            panic!("transact_block_operations must transact a single block only");
        }

        // All operations should apply only to entities in this subgraph or
        // the subgraph of subgraphs
        if mods
            .iter()
            .map(|modification| modification.entity_key())
            .any(|key| key.subgraph_id != subgraph_id && key.subgraph_id != *SUBGRAPHS_ID)
        {
            panic!(
                "transact_block_operations must affect only entities \
                 in the subgraph or in the subgraph of subgraphs"
            );
        }

        let econn = self.get_entity_conn(&subgraph_id)?;

        econn.transaction(|| {
            // Ensure the history event exists in the database
            let history_event = econn.create_history_event(block_ptr_to, &mods)?;

            let should_migrate = econn.should_migrate(&subgraph_id, &block_ptr_to)?;

            // Emit a store event for the changes we are about to make
            let event: StoreEvent = mods.iter().collect();
            econn.send_store_event(&event)?;

            // Make the changes
            self.apply_entity_modifications(&econn, mods, Some(&history_event))?;

            // Update the subgraph block pointer, without an event source; this way
            // no entity history is recorded for the block pointer update itself
            let block_ptr_ops = SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
                &subgraph_id,
                block_ptr_from,
                block_ptr_to,
            );
            self.apply_metadata_operations_with_conn(&econn, block_ptr_ops)?;
            Ok(should_migrate)
        })
    }

    /// Apply a series of entity operations. Return `true` if the subgraph
    /// mentioned in `history_event` should have its schema migrated
    fn apply_metadata_operations(
        &self,
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID)?;
        econn.transaction(|| {
            self.apply_metadata_operations_with_conn(&econn, operations)
                .map(|_| ())
        })
    }

    fn build_entity_attribute_indexes(
        &self,
        subgraph: &SubgraphDeploymentId,
        indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        let econn = self.get_entity_conn(subgraph)?;
        econn.transaction(|| self.build_entity_attribute_indexes_with_conn(&econn, indexes))
    }

    fn revert_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        // Sanity check on block numbers
        if block_ptr_from.number != block_ptr_to.number + 1 {
            panic!("revert_block_operations must revert a single block only");
        }

        let econn = self.get_entity_conn(&subgraph_id)?;
        econn.transaction(|| {
            let ops = SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
                &subgraph_id,
                block_ptr_from,
                block_ptr_to,
            );
            self.apply_metadata_operations_with_conn(&econn, ops)?;

            let (event, count) = econn.revert_block(&block_ptr_from)?;
            econn.update_entity_count(count)?;
            econn.send_store_event(&event)
        })
    }

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        let subscriptions = self.subscriptions.clone();

        // Generate a new (unique) UUID; we're looping just to be sure we avoid collisions
        let mut id = Uuid::new_v4().to_string();
        while subscriptions.read().unwrap().contains_key(&id) {
            id = Uuid::new_v4().to_string();
        }

        debug!(self.logger, "Subscribe";
               "id" => &id,
               "entities" => format!("{:?}", entities));

        // Prepare the new subscription by creating a channel and a subscription object
        let (sender, receiver) = channel(100);

        // Add the new subscription
        let mut subscriptions = subscriptions.write().unwrap();
        subscriptions.insert(id, sender);

        // Return the subscription ID and entity change stream
        StoreEventStream::new(Box::new(receiver)).filter_by_entities(entities)
    }

    fn create_subgraph_deployment(
        &self,
        subgraph_logger: &Logger,
        schema: &Schema,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        // Various timing parameters, all in seconds
        const INITIAL_DELAY: u64 = 2;
        const MAX_DELAY: u64 = 64;
        const LOCK_TIMEOUT: u64 = 2;

        let econn = self.get_entity_conn(&*SUBGRAPHS_ID)?;
        let mut delay = Duration::from_secs(INITIAL_DELAY);

        // Creating a subgraph creates a table that references
        // `event_meta_data`.  To validate that reference, Postgres takes a
        // `share update exclusive` lock; for this lock, Postgres has to
        // wait for all write activity that started before the lock was
        // requested to finish, and it also has to hold all write activity
        // that starts after the lock request. Usually, this is not a
        // problem as the lock is only held for a very short amount of
        // time.
        //
        // If there is other activity, like an autovacuum, happening
        // already when the lock is requested though, we have to wait until
        // that activity finishes, which in the case of an autovacuum can
        // be an hour or longer. The autovacuum by itself is not a problem,
        // as it still allows writes to happen, but once the lock request
        // from this code gets into the lock queue, write activity also has
        // to wait for the autovacuum to finish, effectively blocking all
        // subgraph indexing until the autovacuum has finished.
        //
        // To avoid this, we set a lock timeout of 2s, which should be long
        // enough to get the lock under normal conditions, but not so long
        // that it materially impedes indexing in the above situation. If
        // we can not get the lock within 2s, the subgraph creation fails,
        // and we sleep an increasing amount of time (up to about a minute)
        // and then retry the subgraph creation.
        loop {
            let start = Instant::now();
            let result = econn.transaction(|| -> Result<(), StoreError> {
                self.apply_metadata_operations_with_conn(&econn, ops.clone())?;
                econn.create_schema(schema, LOCK_TIMEOUT)
            });
            if let Err(StoreError::Unknown(_)) = &result {
                // There is no robust way to actually find out that we timed
                // out on the lock from the error message; diesel shields us
                // from these details too much. Rather than grep the error
                // message, which would be very fragile, we assume that if a
                // failure occurred after more than LOCK_TIMEOUT seconds that
                // it was because we timed out on the lock and try again.
                if start.elapsed() >= Duration::from_secs(LOCK_TIMEOUT) {
                    debug!(
                        subgraph_logger,
                        "Could not acquire lock for subgraph creation, trying again in {}s",
                        delay.as_secs()
                    );
                    std::thread::sleep(delay);
                    if delay.as_secs() < MAX_DELAY {
                        delay *= 2;
                    }
                    continue;
                }
            }
            break result;
        }
    }

    fn start_subgraph_deployment(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(subgraph_id)?;

        econn.transaction(|| {
            self.apply_metadata_operations_with_conn(&econn, ops)?;
            econn.start_subgraph()
        })
    }

    fn migrate_subgraph_deployment(
        &self,
        logger: &Logger,
        subgraph_id: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) {
        let econn = match self.get_entity_conn(subgraph_id) {
            Ok(econn) => econn,
            Err(e) => {
                warn!(logger, "failed to get connection to start migrating";
                                "subgraph" => subgraph_id.to_string(),
                                "error" => e.to_string(),
                );
                return;
            }
        };

        if let Err(e) = econn.migrate(logger, block_ptr) {
            // An error in a migration should not lead to the
            // subgraph being marked as failed
            warn!(logger, "aborted migrating";
                            "subgraph" => subgraph_id.to_string(),
                            "error" => e.to_string(),
            );
        }
    }
}

impl SubgraphDeploymentStore for Store {
    fn subgraph_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.cached_schema(subgraph_id)?.api)
    }
}

impl ChainStore for Store {
    type ChainHeadUpdateListener = ChainHeadUpdateListener;

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        Ok(self.genesis_block_ptr)
    }

    fn upsert_blocks<'a, B, E>(
        &self,
        blocks: B,
    ) -> Box<dyn Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a,
    {
        use crate::db_schema::ethereum_blocks::dsl::*;

        let conn = self.conn.clone();
        let net_name = self.network_name.clone();
        Box::new(blocks.for_each(move |block| {
            let json_blob = serde_json::to_value(&block).expect("Failed to serialize block");
            let values = (
                hash.eq(format!("{:x}", block.block.hash.unwrap())),
                number.eq(block.block.number.unwrap().as_u64() as i64),
                parent_hash.eq(format!("{:x}", block.block.parent_hash)),
                network_name.eq(&net_name),
                data.eq(json_blob),
            );

            // Insert blocks.
            // If the table already contains a block with the same hash,
            // then overwrite that block (on conflict do update).
            // That case is a no-op because blocks are immutable
            // (unless the Ethereum node returned corrupt data).
            insert_into(ethereum_blocks)
                .values(values.clone())
                .on_conflict(hash)
                .do_update()
                .set(values)
                .execute(&*conn.get().map_err(Error::from)?)
                .map_err(Error::from)
                .map_err(E::from)
                .map(|_| ())
        }))
    }

    fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error> {
        // Call attempt_head_update SQL function
        select(attempt_chain_head_update(
            &self.network_name,
            ancestor_count as i64,
        ))
        .load(&*self.get_conn()?)
        .map_err(Error::from)
        // We got a single return value, but it's returned generically as a set of rows
        .map(|mut rows: Vec<_>| {
            assert_eq!(rows.len(), 1);
            rows.pop().unwrap()
        })
        // Parse block hashes into H256 type
        .map(|hashes: Vec<String>| {
            hashes
                .into_iter()
                .map(|h| h.parse())
                .collect::<Result<Vec<H256>, _>>()
        })
        .and_then(|r| r.map_err(Error::from))
    }

    fn chain_head_updates(&self) -> ChainHeadUpdateStream {
        self.chain_head_update_listener.subscribe()
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        use crate::db_schema::ethereum_networks::dsl::*;

        ethereum_networks
            .select((head_block_hash, head_block_number))
            .filter(name.eq(&self.network_name))
            .load::<(Option<String>, Option<i64>)>(&*self.get_conn()?)
            .map(|rows| {
                rows.first()
                    .map(|(hash_opt, number_opt)| match (hash_opt, number_opt) {
                        (Some(hash), Some(number)) => Some((hash.parse().unwrap(), *number).into()),
                        (None, None) => None,
                        _ => unreachable!(),
                    })
                    .and_then(|opt| opt)
            })
            .map_err(Error::from)
    }

    fn block(&self, block_hash: H256) -> Result<Option<EthereumBlock>, Error> {
        use crate::db_schema::ethereum_blocks::dsl::*;

        ethereum_blocks
            .select(data)
            .filter(network_name.eq(&self.network_name))
            .filter(hash.eq(format!("{:x}", block_hash)))
            .load::<serde_json::Value>(&*self.get_conn()?)
            .map_err(Error::from)
            .map(|json_blocks| match json_blocks.len() {
                0 => Ok(None),
                1 => Ok(Some(serde_json::from_value(json_blocks[0].clone())?)),
                _ => unreachable!(),
            })
            .and_then(|x| x)
    }

    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        if block_ptr.number < offset {
            bail!("block offset points to before genesis block");
        }

        select(lookup_ancestor_block(block_ptr.hash_hex(), offset as i64))
            .first::<Option<serde_json::Value>>(&*self.get_conn()?)
            .map(|val_opt| {
                val_opt.map(|val| {
                    serde_json::from_value::<EthereumBlock>(val)
                        .expect("Failed to deserialize block from database")
                })
            })
            .map_err(Error::from)
    }
}

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
#[cfg(debug_assertions)]
pub use crate::entities::delete_all_entities_for_test_use_only;

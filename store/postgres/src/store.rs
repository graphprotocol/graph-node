use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::{insert_into, select, update};
use futures::sync::mpsc::{channel, Sender};
use lru_time_cache::LruCache;
use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

use graph::components::store::Store as StoreTrait;
use graph::data::subgraph::schema::EthereumContractDataSourceEntity;
use graph::data::subgraph::schema::{
    SubgraphDeploymentEntity, SubgraphManifestEntity, TypedEntity as _, SUBGRAPHS_ID,
};
use graph::prelude::{
    bail, bigdecimal::ToPrimitive, debug, ethabi, format_err, futures03, info, o, serde_json,
    stream, tiny_keccak, tokio, trace, warn, web3, AttributeIndexDefinition, BigDecimal, BigInt,
    BlockNumber, ChainHeadUpdateListener as _, ChainHeadUpdateStream, ChainStore, Entity,
    EntityKey, EntityModification, EntityOrder, EntityQuery, EntityRange, Error, EthereumBlock,
    EthereumBlockPointer, EthereumCallCache, EthereumNetworkIdentifier, EventProducer as _, Future,
    Future01CompatExt, LightEthereumBlock, Logger, MetadataOperation, MetricsRegistry,
    QueryExecutionError, Schema, Sink as _, StopwatchMetrics, StoreError, StoreEvent,
    StoreEventStream, StoreEventStreamBox, Stream, SubgraphAssignmentProviderError,
    SubgraphDeploymentId, SubgraphDeploymentStore, SubgraphEntityPair, TransactionAbortError,
    Value, BLOCK_NUMBER_MAX,
};

use graph_chain_ethereum::BlockIngestorMetrics;
use graph_graphql::prelude::api_schema;
use web3::types::H256;

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
}

/// Commonly needed information about a subgraph that we cache in
/// `Store.subgraph_cache`. Only immutable subgraph data can be cached this
/// way as the cache lives for the lifetime of the `Store` object
#[derive(Clone)]
struct SubgraphInfo {
    /// The schema as supplied by the user
    input: Arc<Schema>,
    /// The schema we derive from `input` with `graphql::schema::api::api_schema`
    api: Arc<Schema>,
    /// The name of the network from which the subgraph is syncing
    network: Option<String>,
    /// The block number at which this subgraph was grafted onto
    /// another one. We do not allow reverting past this block
    graft_block: Option<BlockNumber>,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    logger: Logger,
    subscriptions: Arc<RwLock<HashMap<String, Sender<StoreEvent>>>>,

    /// listen to StoreEvents generated when applying entity operations
    listener: StoreEventListener,
    chain_head_update_listener: ChainHeadUpdateListener,
    network_name: String,
    genesis_block_ptr: EthereumBlockPointer,
    conn: Pool<ConnectionManager<PgConnection>>,

    /// A cache of commonly needed data about a subgraph.
    subgraph_cache: Mutex<LruCache<SubgraphDeploymentId, SubgraphInfo>>,

    /// A cache for the storage metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) storage_cache: e::StorageCache,

    registry: Arc<dyn MetricsRegistry>,
}

impl Store {
    pub fn new(
        config: StoreConfig,
        logger: &Logger,
        net_identifiers: EthereumNetworkIdentifier,
        pool: Pool<ConnectionManager<PgConnection>>,
        registry: Arc<dyn MetricsRegistry>,
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
        let block_ingestor_metrics = Arc::new(BlockIngestorMetrics::new(registry.clone()));

        // Create the store
        let mut store = Store {
            logger: logger.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            listener,
            chain_head_update_listener: ChainHeadUpdateListener::new(
                &logger,
                block_ingestor_metrics,
                config.postgres_url,
                config.network_name.clone(),
            ),
            network_name: config.network_name.clone(),
            genesis_block_ptr: (net_identifiers.genesis_block_hash, 0 as u64).into(),
            conn: pool,
            subgraph_cache: Mutex::new(LruCache::with_capacity(100)),
            storage_cache: e::make_storage_cache(),
            registry,
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

        graph::spawn(
            store_events
                .for_each(move |event| {
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
                })
                .compat(),
        );
    }

    fn periodically_clean_up_stale_subscriptions(&self) {
        use futures03::stream::StreamExt;

        let logger = self.logger.clone();
        let subscriptions = self.subscriptions.clone();

        // Clean up stale subscriptions every 5s
        graph::spawn(
            tokio::time::interval(Duration::from_secs(5)).for_each(move |_| {
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

                futures03::future::ready(())
            }),
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
                let direction = query.order_direction.unwrap_or(EntityOrder::Ascending);
                Some((attribute, value_type, direction))
            }
            None => None,
        };

        // Process results; deserialize JSON data
        let logger = query.logger.unwrap_or(self.logger.clone());
        conn.query(
            &logger,
            query.collection,
            query.filter,
            order,
            query.range,
            query.block,
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
        let schema = self.api_schema(&key.subgraph_id)?;
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
                        entity.merge_remove_null_fields(data);
                        conn.update(&key, entity, None).map(|_| 0)
                    }
                    None => {
                        // Merge with a new entity since that removes values that
                        // were set to Value::Null
                        let mut entity = Entity::new();
                        entity.merge_remove_null_fields(data);
                        conn.insert(&key, entity, None).map(|_| 1)
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
            MetadataOperation::Update { entity, id, data } => {
                let key = MetadataOperation::entity_key(entity, id);

                self.check_interface_entity_uniqueness(conn, &key)?;

                // Update the entity in Postgres
                match conn.update_metadata(&key, &data)? {
                    0 => Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids: vec![key.entity_id.clone()],
                        actual_entity_ids: vec![],
                        description: format!("update for entity {:?} with data {:?} did not change any rows", key, data),
                    }
                    .into()),
                    1 => Ok(0),
                    res => Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids: vec![key.entity_id.clone()],
                        actual_entity_ids: vec![],
                        description: format!(
                            "update for entity {:?} with data {:?} changed {} rows instead of just one",
                            key, data, res
                        ),
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
        stopwatch: StopwatchMetrics,
    ) -> Result<(), StoreError> {
        let mut count = 0;

        for modification in mods {
            use EntityModification::*;

            let do_count = !modification.entity_key().subgraph_id.is_meta();
            let n = match modification {
                Overwrite { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_update");
                    conn.update(&key, data, history_event).map(|_| 0)
                }
                Insert { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_insert");
                    conn.insert(&key, data, history_event).map(|_| 1)
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
    ) -> Result<StoreEvent, StoreError> {
        // Emit a store event for the changes we are about to make
        let event: StoreEvent = operations.clone().into();

        // Actually apply the operations
        for operation in operations.into_iter() {
            self.apply_metadata_operation(econn, operation)?;
        }
        Ok(event)
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
        let start = Instant::now();
        let conn = self.get_conn()?;
        self.registry
            .global_counter(format!("{}_get_entity_conn_secs", subgraph))?
            .inc_by(start.elapsed().as_secs_f64());
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

    fn subgraph_info(&self, subgraph_id: &SubgraphDeploymentId) -> Result<SubgraphInfo, Error> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&subgraph_id) {
            return Ok(info.clone());
        }
        trace!(self.logger, "schema cache miss"; "id" => subgraph_id.to_string());

        let (input_schema, network) = if *subgraph_id == *SUBGRAPHS_ID {
            // The subgraph of subgraphs schema is built-in. Use an impossible
            // network name so that we will never find blocks for this subgraph
            (
                include_str!("subgraphs.graphql").to_owned(),
                Some("subgraphs".to_owned()),
            )
        } else {
            let manifest_entity = self
                .get(EntityKey {
                    subgraph_id: SUBGRAPHS_ID.clone(),
                    entity_type: SubgraphManifestEntity::TYPENAME.to_owned(),
                    entity_id: SubgraphManifestEntity::id(&subgraph_id),
                })?
                .ok_or_else(|| format_err!("Subgraph entity not found {}", subgraph_id))?;

            let input_schema = match manifest_entity.get("schema") {
                Some(Value::String(raw)) => raw.clone(),
                _ => {
                    return Err(format_err!(
                        "Schema not present or has wrong type, subgraph: {}",
                        subgraph_id
                    ));
                }
            };

            fn bad_data_source(code: i32, subgraph_id: &SubgraphDeploymentId) -> Error {
                return format_err!(
                    "Data source for subgraph is malformed, code: {}, subgraph: {}",
                    code,
                    subgraph_id
                );
            }

            let network = match manifest_entity.get("dataSources") {
                Some(Value::List(dss)) => match dss.first() {
                    Some(Value::String(dsid)) => {
                        let data_source = self
                            .get(EntityKey {
                                subgraph_id: SUBGRAPHS_ID.clone(),
                                entity_type: EthereumContractDataSourceEntity::TYPENAME.to_owned(),
                                entity_id: dsid.to_owned(),
                            })?
                            .ok_or_else(|| bad_data_source(1, subgraph_id))?;
                        match data_source.get("network") {
                            Some(Value::String(network)) => Ok(Some(network.to_owned())),
                            None => Ok(None),
                            _ => Err(bad_data_source(2, subgraph_id)),
                        }
                    }
                    // The NetworkIndexer puts us here as it does not create
                    // data sources for the subgraphs it creates, and the
                    // manifest contains an empty list of data sources
                    None => Ok(None),
                    _ => Err(bad_data_source(3, subgraph_id)),
                },
                _ => Err(bad_data_source(4, subgraph_id)),
            }?;
            (input_schema, network)
        };

        use crate::metadata::subgraph_deployment as sd;
        let conn = self.get_conn()?;
        let graft_block = if subgraph_id.is_meta() {
            // There is no SubgraphDeployment for the metadata subgraph
            None
        } else {
            sd::table
                .select(sd::graft_block_number)
                .filter(sd::id.eq(subgraph_id.as_str()))
                .first::<Option<BigDecimal>>(&conn)?
                .map(|block| block.to_i32())
                .flatten()
        };

        // Parse the schema and add @subgraphId directives
        let input_schema = Schema::parse(&input_schema, subgraph_id.clone())?;
        let mut schema = input_schema.clone();

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        schema.document = api_schema(&schema.document)?;
        schema.add_subgraph_id_directives(subgraph_id.clone());

        let info = SubgraphInfo {
            input: Arc::new(input_schema),
            api: Arc::new(schema),
            network: network,
            graft_block,
        };

        // Insert the schema into the cache.
        let mut cache = self.subgraph_cache.lock().unwrap();
        cache.insert(subgraph_id.clone(), info);

        Ok(cache.get(&subgraph_id).unwrap().clone())
    }

    fn block_ptr_with_conn(
        &self,
        subgraph_id: SubgraphDeploymentId,
        conn: &e::Connection,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        let key = SubgraphDeploymentEntity::key(subgraph_id.clone());
        let subgraph_entity = conn
            .find_metadata(&key.entity_type, &key.entity_id)
            .map_err(|e| format_err!("error reading subgraph entity: {}", e))?
            .ok_or_else(|| {
                format_err!(
                    "could not read block ptr for non-existent subgraph {}",
                    subgraph_id
                )
            })?;

        let hash: Option<H256> = match subgraph_entity.get("latestEthereumBlockHash") {
            None => None,
            Some(value) => value.clone().try_into()?,
        };

        let number: Option<BigInt> = match subgraph_entity.get("latestEthereumBlockNumber") {
            None => None,
            Some(value) => value.clone().try_into()?,
        };

        match (hash, number) {
            (Some(hash), Some(number)) => Ok(Some(EthereumBlockPointer {
                hash,
                number: number.to_u64(),
            })),
            (None, None) => Ok(None),
            _ => Err(format_err!(
                "Ethereum block pointer has invalid `latestEthereumBlockHash` \
                 or `latestEthereumBlockNumber`"
            )),
        }
    }
}

impl StoreTrait for Store {
    fn block_ptr(
        &self,
        subgraph_id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        self.block_ptr_with_conn(
            subgraph_id,
            &self
                .get_entity_conn(&*SUBGRAPHS_ID)
                .map_err(|e| QueryExecutionError::StoreError(e.into()))?,
        )
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&key.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.get_entity(&conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
    }

    fn get_many(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }
        let conn = self
            .get_entity_conn(subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        conn.find_many(ids_for_type, BLOCK_NUMBER_MAX)
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

    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
    ) -> Result<bool, StoreError> {
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

        let (event, metadata_event, should_migrate) =
            econn.transaction(|| -> Result<_, StoreError> {
                let block_ptr_from = self.block_ptr_with_conn(subgraph_id.clone(), &econn)?;
                if let Some(ref block_ptr_from) = block_ptr_from {
                    assert!(block_ptr_from.number < block_ptr_to.number);
                }

                // Ensure the history event exists in the database
                let history_event = econn.create_history_event(block_ptr_to, &mods)?;

                let should_migrate = econn.should_migrate(&subgraph_id, &block_ptr_to)?;

                // Emit a store event for the changes we are about to make. We
                // wait with sending it until we have done all our other work
                // so that we do not hold a lock on the notification queue
                // for longer than we have to
                let event: StoreEvent = mods.iter().collect();

                // Make the changes
                let section = stopwatch.start_section("apply_entity_modifications");
                self.apply_entity_modifications(&econn, mods, Some(&history_event), stopwatch)?;
                section.end();

                // Update the subgraph block pointer, without an event source; this way
                // no entity history is recorded for the block pointer update itself
                let block_ptr_ops =
                    SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
                        &subgraph_id,
                        block_ptr_to,
                    );
                let metadata_event =
                    self.apply_metadata_operations_with_conn(&econn, block_ptr_ops)?;
                Ok((event, metadata_event, should_migrate))
            })?;

        // Send the events separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| {
            econn.send_store_event(&metadata_event)?;
            econn.send_store_event(&event)
        })?;

        Ok(should_migrate)
    }

    /// Apply a series of entity operations. Return `true` if the subgraph
    /// mentioned in `history_event` should have its schema migrated
    fn apply_metadata_operations(
        &self,
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID)?;
        let event =
            econn.transaction(|| self.apply_metadata_operations_with_conn(&econn, operations))?;

        // Send the event separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| econn.send_store_event(&event))
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
        // Don't revert past a graft point
        let info = self.subgraph_info(&subgraph_id)?;
        if let Some(graft_block) = info.graft_block {
            if graft_block as u64 > block_ptr_to.number {
                return Err(format_err!(
                    "Can not revert subgraph `{}` to block {} \
                  as it was grafted at block {} and we do not allow reverting \
                  past a graft point",
                    subgraph_id,
                    block_ptr_to.number,
                    graft_block
                )
                .into());
            }
        }

        let econn = self.get_entity_conn(&subgraph_id)?;
        let (event, metadata_event) = econn.transaction(|| -> Result<_, StoreError> {
            assert_eq!(
                Some(block_ptr_from),
                self.block_ptr_with_conn(subgraph_id.clone(), &econn)?
            );
            let ops = SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
                &subgraph_id,
                block_ptr_to,
            );
            let metadata_event = self.apply_metadata_operations_with_conn(&econn, ops)?;

            let (event, count) = econn.revert_block(&block_ptr_from)?;
            econn.update_entity_count(count)?;
            Ok((event, metadata_event))
        })?;

        // Send the events separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| {
            econn.send_store_event(&metadata_event)?;
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
        schema: &Schema,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID)?;
        econn.transaction(|| -> Result<(), StoreError> {
            let event = self.apply_metadata_operations_with_conn(&econn, ops.clone())?;
            econn.create_schema(schema)?;
            econn.send_store_event(&event)
        })
    }

    fn start_subgraph_deployment(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(subgraph_id)?;

        if !econn.uses_relational_schema() {
            warn!(&self.logger, "This subgraph uses JSONB storage, which is \
              deprecated and support for it will be removed in a future release. \
              Please redeploy the subgraph to address this warning";
              "subgraph" => subgraph_id.to_string());
        }

        econn.transaction(|| {
            let event = self.apply_metadata_operations_with_conn(&econn, ops)?;
            econn.start_subgraph()?;
            econn.send_store_event(&event)
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

    fn block_number(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError> {
        use crate::db_schema::ethereum_blocks::dsl;

        // We should also really check that the block with the given hash is
        // on the chain starting at the subgraph's current head. That check is
        // very expensive though with the data structures we have currently
        // available. Ideally, we'd have the last REORG_THRESHOLD blocks in
        // memory so that we can check against them, and then mark in the
        // database the blocks on the main chain that we consider final
        let block: Option<(i64, String)> = dsl::ethereum_blocks
            .select((dsl::number, dsl::network_name))
            .filter(dsl::hash.eq(format!("{:x}", hash)))
            .first(&*self.get_conn()?)
            .optional()?;
        let subgraph_network = self.network_name(subgraph_id)?;
        block
            .map(|(number, network_name)| {
                if subgraph_network.is_none() || Some(&network_name) == subgraph_network.as_ref() {
                    BlockNumber::try_from(number)
                        .map_err(|e| StoreError::QueryExecutionError(e.to_string()))
                } else {
                    Err(StoreError::QueryExecutionError(format!(
                        "subgraph {} belongs to network {} but block {:x} belongs to network {}",
                        subgraph_id,
                        subgraph_network.unwrap_or("(none)".to_owned()),
                        hash,
                        network_name
                    )))
                }
            })
            .transpose()
    }
}

impl SubgraphDeploymentStore for Store {
    fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.input)
    }

    fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.api)
    }

    fn uses_relational_schema(&self, subgraph: &SubgraphDeploymentId) -> Result<bool, Error> {
        self.get_entity_conn(subgraph)
            .map(|econn| econn.uses_relational_schema())
    }

    fn network_name(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Option<String>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.network)
    }
}

impl ChainStore for Store {
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        Ok(self.genesis_block_ptr)
    }

    fn upsert_blocks<B, E>(
        &self,
        blocks: B,
    ) -> Box<dyn Future<Item = (), Error = E> + Send + 'static>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'static,
        E: From<Error> + Send + 'static,
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
            //
            // If the table already contains a block with the same hash, then overwrite that block
            // if it may be adding transaction receipts.
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

    fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), Error> {
        use crate::db_schema::ethereum_blocks::dsl::*;

        let conn = self.conn.clone();
        let net_name = self.network_name.clone();
        for block in blocks {
            let block_hash = format!("{:x}", block.hash.unwrap());
            let p_hash = format!("{:x}", block.parent_hash);
            let block_number = block.number.unwrap().as_u64();
            let json_blob = serde_json::to_value(&EthereumBlock {
                block,
                transaction_receipts: Vec::new(),
            })
            .expect("Failed to serialize block");
            let values = (
                hash.eq(block_hash),
                number.eq(block_number as i64),
                parent_hash.eq(p_hash),
                network_name.eq(&net_name),
                data.eq(json_blob),
            );

            // Insert blocks. On conflict do nothing, we don't want to erase transaction receipts.
            insert_into(ethereum_blocks)
                .values(values.clone())
                .on_conflict(hash)
                .do_nothing()
                .execute(&*conn.get()?)?;
        }
        Ok(())
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

    fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error> {
        use crate::db_schema::ethereum_blocks::dsl::*;
        use diesel::dsl::{any, sql};
        use diesel::sql_types::Jsonb;

        ethereum_blocks
            .select(sql::<Jsonb>("data -> 'block'"))
            .filter(network_name.eq(&self.network_name))
            .filter(hash.eq(any(Vec::from_iter(
                hashes.into_iter().map(|h| format!("{:x}", h)),
            ))))
            .load::<serde_json::Value>(&*self.get_conn()?)?
            .into_iter()
            .map(|block| serde_json::from_value(block).map_err(Into::into))
            .collect()
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

    fn cleanup_cached_blocks(&self, ancestor_count: u64) -> Result<(BlockNumber, usize), Error> {
        use crate::db_schema::ethereum_blocks::dsl;
        use diesel::sql_types::{Integer, Text};

        #[derive(QueryableByName)]
        struct MinBlock {
            #[sql_type = "Integer"]
            block: i32,
        };

        // Remove all blocks from the cache that are behind the slowest
        // subgraph's head block, but retain the genesis block. We stay
        // behind the slowest subgraph so that we do not interfere with its
        // syncing activity.
        // We also stay `ancestor_count` many blocks behind the head of the
        // chain since the block ingestor consults these blocks frequently
        //
        // Only consider active subgraphs that have not failed
        let conn = self.get_conn()?;
        let query = "
            select least(a.block,
                        (select head_block_number::int - $1
                           from ethereum_networks
                          where name = $2)) as block
              from (
                select min((d.data->'latestEthereumBlockNumber'->>'data')::int) as block
                  from subgraphs.entities d,
                       subgraphs.entities a,
                       subgraphs.entities ds
                 where d.entity = 'SubgraphDeployment'
                   and a.entity = 'SubgraphDeploymentAssignment'
                   and ds.entity = 'EthereumContractDataSource'
                   and left(ds.id, 46) = d.id
                   and a.id = d.id
                   and not (d.data->'failed'->>'data')::bool
                   and ds.data->'network'->>'data' = $2) a;";
        let ancestor_count = i32::try_from(ancestor_count)
            .expect("ancestor_count fits into a signed 32 bit integer");
        diesel::sql_query(query)
            .bind::<Integer, _>(ancestor_count)
            .bind::<Text, _>(&self.network_name)
            .load::<MinBlock>(&conn)?
            .first()
            .map(|MinBlock { block }| {
                diesel::delete(dsl::ethereum_blocks)
                    .filter(dsl::network_name.eq(&self.network_name))
                    .filter(dsl::number.lt(*block as i64))
                    .filter(dsl::number.gt(0))
                    .execute(&conn)
                    .map(|rows| (*block, rows))
            })
            .unwrap_or(Ok((0, 0)))
            .map_err(|e| e.into())
    }

    fn block_hashes_by_block_number(&self, number: u64) -> Result<Vec<H256>, Error> {
        use crate::db_schema::ethereum_blocks::dsl;

        let conn = self.get_conn()?;
        dsl::ethereum_blocks
            .select(dsl::hash)
            .filter(dsl::network_name.eq(&self.network_name))
            .filter(dsl::number.eq(number as i64))
            .get_results::<String>(&conn)?
            .into_iter()
            .map(|h| h.parse())
            .collect::<Result<Vec<H256>, _>>()
            .map_err(Error::from)
    }

    fn confirm_block_hash(&self, number: u64, hash: &H256) -> Result<usize, Error> {
        use crate::db_schema::ethereum_blocks::dsl;

        let conn = self.get_conn()?;
        diesel::delete(dsl::ethereum_blocks)
            .filter(dsl::network_name.eq(&self.network_name))
            .filter(dsl::number.eq(number as i64))
            .filter(dsl::hash.ne(&format!("{:x}", hash)))
            .execute(&conn)
            .map_err(Error::from)
    }
}

impl EthereumCallCache for Store {
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, Error> {
        use crate::db_schema::{eth_call_cache, eth_call_meta};
        use diesel::dsl::sql;

        let id = contract_call_id(contract_address, encoded_call, block);
        let conn = &*self.get_conn()?;
        conn.transaction(|| {
            if let Some((return_value, update_accessed_at)) = eth_call_cache::table
                .find(id.as_ref())
                .inner_join(eth_call_meta::table)
                .select((
                    eth_call_cache::return_value,
                    sql("CURRENT_DATE > eth_call_meta.accessed_at"),
                ))
                .get_result(conn)
                .optional()?
            {
                if update_accessed_at {
                    update(eth_call_meta::table.find(contract_address.as_ref()))
                        .set(eth_call_meta::accessed_at.eq(sql("CURRENT_DATE")))
                        .execute(conn)?;
                }
                Ok(Some(return_value))
            } else {
                Ok(None)
            }
        })
    }

    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
        return_value: &[u8],
    ) -> Result<(), Error> {
        use crate::db_schema::{eth_call_cache, eth_call_meta};
        use diesel::dsl::sql;

        let id = contract_call_id(contract_address, encoded_call, block);
        let conn = &*self.get_conn()?;
        conn.transaction(|| {
            insert_into(eth_call_cache::table)
                .values((
                    eth_call_cache::id.eq(id.as_ref()),
                    eth_call_cache::contract_address.eq(contract_address.as_ref()),
                    eth_call_cache::block_number.eq(block.number as i32),
                    eth_call_cache::return_value.eq(return_value),
                ))
                .on_conflict_do_nothing()
                .execute(conn)?;

            let accessed_at = eth_call_meta::accessed_at.eq(sql("CURRENT_DATE"));
            insert_into(eth_call_meta::table)
                .values((
                    eth_call_meta::contract_address.eq(contract_address.as_ref()),
                    accessed_at.clone(),
                ))
                .on_conflict(eth_call_meta::contract_address)
                .do_update()
                .set(accessed_at)
                .execute(conn)
                .map(|_| ())
                .map_err(Error::from)
        })
    }
}

/// The id is the hashed contract_address + encoded_call + block hash. This uniquely identifies the
/// call. Use 128 bits of output to save some bytes in the DB.
fn contract_call_id(
    contract_address: ethabi::Address,
    encoded_call: &[u8],
    block: EthereumBlockPointer,
) -> [u8; 16] {
    let mut id = [0; 16];
    let mut hash = tiny_keccak::Keccak::new_shake128();
    hash.update(contract_address.as_ref());
    hash.update(encoded_call);
    hash.update(block.hash.as_ref());
    hash.finalize(&mut id);
    id
}

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
#[cfg(debug_assertions)]
pub use crate::entities::delete_all_entities_for_test_use_only;

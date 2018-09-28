use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel::{debug_query, delete, insert_into, select, update};
use failure::*;
use filter::store_filter;
use futures::sync::mpsc::{channel, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

use graph::components::store::Store as StoreTrait;
use graph::prelude::*;
use graph::serde_json;
use graph::web3::types::H256;
use graph::{tokio, tokio::timer::Interval};

use chain_head_listener::ChainHeadUpdateListener;
use entity_changes::EntityChangeListener;
use functions::{attempt_chain_head_update, lookup_ancestor_block, revert_block, set_config};

embed_migrations!("./migrations");

/// Internal representation of a Store subscription.
struct Subscription {
    pub entities: Vec<SubgraphEntityPair>,
    pub sender: Sender<EntityChange>,
}

/// Run all initial schema migrations.
///
/// Creates the "entities" table if it doesn't already exist.
fn initiate_schema(logger: &slog::Logger, conn: &PgConnection) {
    // Collect migration logging output
    let mut output = vec![];

    match embedded_migrations::run_with_output(conn, &mut output) {
        Ok(_) => info!(logger, "Completed pending Postgres schema migrations"),
        Err(e) => panic!("Error with Postgres schema setup: {:?}", e),
    }

    // If there was any migration output, log it now
    if !output.is_empty() {
        debug!(logger, "Postgres migration output";
               "output" => String::from_utf8(output).unwrap_or(String::from("<unreadable>")));
    }
}

/// Configuration for the Diesel/Postgres store.
pub struct StoreConfig {
    pub url: String,
    pub network_name: String,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    logger: slog::Logger,
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    change_listener: EntityChangeListener,
    url: String,
    network_name: String,
    genesis_block_ptr: EthereumBlockPointer,
    pub conn: Arc<Mutex<PgConnection>>,
}

impl Store {
    pub fn new(
        config: StoreConfig,
        logger: &slog::Logger,
        net_identifiers: EthereumNetworkIdentifiers,
    ) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Connect to Postgres
        let conn =
            PgConnection::establish(config.url.as_str()).expect("failed to connect to Postgres");

        info!(logger, "Connected to Postgres"; "url" => &config.url);

        // Create the entities table (if necessary)
        initiate_schema(&logger, &conn);

        // Listen to entity changes in Postgres
        let mut change_listener = EntityChangeListener::new(config.url.clone());
        let entity_changes = change_listener
            .take_event_stream()
            .expect("Failed to listen to entity change events in Postgres");

        // Create the store
        let mut store = Store {
            logger: logger.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            change_listener,
            url: config.url.clone(),
            network_name: config.network_name.clone(),
            genesis_block_ptr: (net_identifiers.genesis_block_hash, 0u64).into(),
            conn: Arc::new(Mutex::new(conn)),
        };

        // Add network to store and check network identifiers
        store.add_network_if_missing(net_identifiers).unwrap();

        // Deal with store subscriptions
        store.handle_entity_changes(entity_changes);
        store.periodically_clean_up_stale_subscriptions();

        // We're ready for processing entity changes
        store.change_listener.start();

        // Return the store
        store
    }

    fn add_network_if_missing(
        &self,
        new_net_identifiers: EthereumNetworkIdentifiers,
    ) -> Result<(), Error> {
        use db_schema::ethereum_networks::dsl::*;

        let new_genesis_block_hash = new_net_identifiers.genesis_block_hash;
        let new_net_version = new_net_identifiers.net_version;

        let network_identifiers_opt = ethereum_networks
            .select((net_version, genesis_block_hash))
            .filter(name.eq(&self.network_name))
            .first::<(Option<String>, Option<String>)>(&*self.conn.lock().unwrap())
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
                    )).on_conflict(name)
                    .do_nothing()
                    .execute(&*self.conn.lock().unwrap())?;
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
                    )).filter(name.eq(&self.network_name))
                    .execute(&*self.conn.lock().unwrap())?;
            }
        }

        Ok(())
    }

    /// Handles entity changes emitted by Postgres.
    fn handle_entity_changes(
        &self,
        entity_changes: Box<Stream<Item = EntityChange, Error = ()> + Send>,
    ) {
        let logger = self.logger.clone();
        let subscriptions = self.subscriptions.clone();

        tokio::spawn(entity_changes.for_each(move |change| {
            debug!(logger, "Entity change";
                           "subgraph" => &change.subgraph,
                           "entity" => &change.entity,
                           "id" => &change.id);

            // Obtain IDs and senders of subscriptions matching the entity change
            let matches = subscriptions
                .read()
                .unwrap()
                .iter()
                .filter(|(_, subscription)| {
                    subscription
                        .entities
                        .contains(&(change.subgraph.clone(), change.entity.clone()))
                }).map(|(id, subscription)| (id.clone(), subscription.sender.clone()))
                .collect::<Vec<_>>();

            let subscriptions = subscriptions.clone();
            let logger = logger.clone();

            // Write change to all matching subscription streams; remove subscriptions
            // whose receiving end has been dropped
            stream::iter_ok::<_, ()>(matches).for_each(move |(id, sender)| {
                let logger = logger.clone();
                let subscriptions = subscriptions.clone();
                sender
                    .send(change.clone())
                    .map_err(move |_| {
                        debug!(logger, "Unsubscribe"; "id" => &id);
                        subscriptions.write().unwrap().remove(&id);
                    }).and_then(|_| Ok(()))
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
                        .filter_map(
                            |(id, subscription)| match subscription.sender.poll_ready() {
                                Err(_) => Some(id.clone()),
                                _ => None,
                            },
                        ).collect::<Vec<_>>();

                    // Remove all stale subscriptions
                    for id in stale_ids {
                        debug!(logger, "Unsubscribe"; "id" => &id);
                        subscriptions.remove(&id);
                    }

                    Ok(())
                }).map_err(|_| unreachable!()),
        );
    }

    /// Handles block reorganizations.
    /// Revert all store events related to the given block
    pub fn revert_events(&self, block_hash: String, subgraph_id: String) {
        select(revert_block(block_hash, subgraph_id))
            .execute(&*self.conn.lock().unwrap())
            .unwrap();
    }

    /// Gets an entity from Postgres, returns an entity with just an ID if none is found.
    fn get_entity(
        &self,
        conn: &PgConnection,
        op_subgraph: &String,
        op_entity: &String,
        op_id: &String,
    ) -> Result<Entity, Error> {
        use db_schema::entities::dsl::*;

        match entities
            .find((op_id, op_subgraph, op_entity))
            .select(data)
            .first::<serde_json::Value>(conn)
        {
            Ok(json) => serde_json::from_value::<Entity>(json).map_err(|e| {
                format_err!(
                    "Encountered invalid entity ({}, {}, {}) in the store: {}",
                    op_subgraph,
                    op_entity,
                    op_id,
                    e
                )
            }),
            Err(_) => Ok(Entity::from(vec![("id", Value::from(op_id.clone()))])),
        }
    }

    /// Applies a set operation in Postgres.
    fn apply_set_operation(
        &self,
        conn: &PgConnection,
        operation: EntityOperation,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<usize, Error> {
        use db_schema::entities::dsl::*;

        let (op_subgraph, op_entity, op_id) = operation.entity_info();

        // Load the entity if exists
        let existing_entity = self.get_entity(conn, op_subgraph, op_entity, op_id)?;

        // Apply the operation
        let updated_entity = operation.apply(Some(existing_entity));
        let updated_json: serde_json::Value =
            serde_json::to_value(&updated_entity).map_err(|e| {
                format_err!(
                    "Entity ({}, {}, {}) invalid after operation: {}",
                    op_subgraph,
                    op_entity,
                    op_id,
                    e
                )
            })?;

        // Either add or update the entity in Postgres
        insert_into(entities)
            .values((
                id.eq(op_id),
                entity.eq(op_entity),
                subgraph.eq(op_subgraph),
                data.eq(&updated_json),
                event_source.eq(block_ptr_to.hash.to_string()),
            )).on_conflict((id, entity, subgraph))
            .do_update()
            .set((
                id.eq(op_id),
                entity.eq(op_entity),
                subgraph.eq(op_subgraph),
                data.eq(&updated_json),
                event_source.eq(block_ptr_to.hash.to_string()),
            )).execute(conn)
            .map_err(|e| {
                format_err!(
                    "Failed to set entity ({}, {}, {}): {}",
                    op_subgraph,
                    op_entity,
                    op_id,
                    e
                )
            })
    }

    /// Applies a remove operation by deleting the entity from Postgres.
    fn apply_remove_operation(
        &self,
        conn: &PgConnection,
        operation: EntityOperation,
    ) -> Result<usize, Error> {
        use db_schema::entities::dsl::*;

        let (op_subgraph, op_entity, op_id) = operation.entity_info();

        delete(
            entities
                .filter(subgraph.eq(op_subgraph))
                .filter(entity.eq(op_entity))
                .filter(id.eq(op_id)),
        ).execute(conn)
        .map_err(|e| {
            format_err!(
                "Failed to remove entity ({}, {}, {}) {}",
                op_subgraph,
                op_entity,
                op_id,
                e
            )
        })
    }

    /// Apply an entity operation in Postgres.
    fn apply_entity_operation(
        &self,
        conn: &PgConnection,
        operation: EntityOperation,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<usize, Error> {
        match operation {
            EntityOperation::Set { .. } => self.apply_set_operation(conn, operation, block_ptr_to),
            EntityOperation::Remove { .. } => self.apply_remove_operation(conn, operation),
        }
    }

    /// Apply a series of entity operations in Postgres.
    fn apply_entity_operations(
        &self,
        conn: &PgConnection,
        operations: Vec<EntityOperation>,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), Error> {
        for operation in operations.into_iter() {
            self.apply_entity_operation(conn, operation, block_ptr_to)?;
        }
        Ok(())
    }

    /// Update the block pointer of the subgraph with the given ID.
    fn update_subgraph_block_pointer(
        &self,
        conn: &PgConnection,
        subgraph_id: SubgraphId,
        from: EthereumBlockPointer,
        to: EthereumBlockPointer,
    ) -> Result<(), Error> {
        use db_schema::subgraphs::dsl::*;

        update(subgraphs)
            .set((
                latest_block_hash.eq(to.hash_hex()),
                latest_block_number.eq(to.number as i64),
            )).filter(id.eq(&subgraph_id))
            .filter(latest_block_hash.eq(from.hash_hex()))
            .filter(latest_block_number.eq(from.number as i64))
            .execute(conn)
            .map_err(Error::from)
            .and_then(move |row_count| match row_count {
                0 => Err(format_err!(
                    "failed to update subgraph block pointer from {:?} to {:?}",
                    from,
                    to
                )),
                1 => Ok(()),
                _ => unreachable!(),
            })
    }
}

impl StoreTrait for Store {
    fn add_subgraph_if_missing(
        &self,
        subgraph_id: SubgraphId,
        block_ptr: EthereumBlockPointer,
    ) -> Result<(), Error> {
        use db_schema::subgraphs::dsl::*;

        insert_into(subgraphs)
            .values((
                id.eq(&subgraph_id),
                network_name.eq(&self.network_name),
                latest_block_hash.eq(block_ptr.hash_hex()),
                latest_block_number.eq(block_ptr.number as i64),
            )).on_conflict(id)
            .do_nothing()
            .execute(&*self.conn.lock().unwrap())
            .map_err(Error::from)
            .map(|_| ())
    }

    fn block_ptr(&self, subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        use db_schema::subgraphs::dsl::*;

        subgraphs
            .select((latest_block_hash, latest_block_number))
            .filter(id.eq(&subgraph_id))
            .first::<(String, i64)>(&*self.conn.lock().unwrap())
            .map(|(hash, number)| {
                (
                    hash.parse()
                        .expect("subgraph block ptr hash in database should be a valid H256"),
                    number,
                )
                    .into()
            }).map_err(Error::from)
    }

    fn get(&self, key: StoreKey) -> Result<Entity, Error> {
        use db_schema::entities::dsl::*;

        // Use primary key fields to get the entity; deserialize the result JSON
        entities
            .find((key.id, key.subgraph, key.entity))
            .select(data)
            .first::<serde_json::Value>(&*self.conn.lock().unwrap())
            .map_err(|e| format_err!("Failed to get entity from store: {}", e))
            .and_then(|value| {
                serde_json::from_value::<Entity>(value)
                    .map_err(|e| format_err!("Broken entity found in store: {}", e))
            })
    }

    fn find(&self, query: StoreQuery) -> Result<Vec<Entity>, ()> {
        use db_schema::entities::dsl::*;

        // Create base boxed query; this will be added to based on the
        // query parameters provided
        let mut diesel_query = entities
            .filter(entity.eq(query.entity))
            .filter(subgraph.eq(query.subgraph))
            .select(data)
            .into_boxed::<Pg>();

        // Add specified filter to query
        if let Some(filter) = query.filter {
            diesel_query = store_filter(diesel_query, filter).map_err(|e| {
                error!(self.logger, "value does not support this filter";
                                    "value" => format!("{:?}", e.value),
                                    "filter" => e.filter)
            })?;
        }

        // Add order by filters to query
        if let Some(order_attribute) = query.order_by {
            let direction = query
                .order_direction
                .map(|direction| match direction {
                    StoreOrder::Ascending => String::from("ASC"),
                    StoreOrder::Descending => String::from("DESC"),
                }).unwrap_or(String::from("ASC"));

            diesel_query = diesel_query.order(
                sql::<Text>("data ->> ")
                    .bind::<Text, _>(order_attribute)
                    .sql(&format!(" {} ", direction)),
            )
        }

        // Add range filter to query
        if let Some(range) = query.range {
            diesel_query = diesel_query
                .limit(range.first as i64)
                .offset(range.skip as i64);
        }

        // Process results; deserialize JSON data
        diesel_query
            .load::<serde_json::Value>(&*self.conn.lock().unwrap())
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| {
                        serde_json::from_value::<Entity>(value)
                            .expect("Error to deserialize entity")
                    }).collect()
            }).map_err(|_| ())
    }

    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        self.update_subgraph_block_pointer(&*conn, subgraph_id, block_ptr_from, block_ptr_to)
    }

    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
        operations: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        // Fold the operations of each entity into a single one
        let operations = EntityOperation::fold(&operations);

        let conn = self.conn.lock().unwrap();

        conn.transaction::<(), _, _>(|| {
            self.apply_entity_operations(&*conn, operations, block_ptr_to)?;
            self.update_subgraph_block_pointer(&*conn, subgraph_id, block_ptr_from, block_ptr_to)
        })
    }

    fn revert_block_operations(
        &self,
        _subgraph_id: SubgraphId,
        _block_ptr_from: EthereumBlockPointer,
        _block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
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
        let subscription = Subscription { entities, sender };

        // Add the new subscription
        let mut subscriptions = subscriptions.write().unwrap();
        subscriptions.insert(id, subscription);

        // Return the subscription ID and entity change stream
        Box::new(receiver)
    }
}

impl ChainStore for Store {
    type ChainHeadUpdateListener = ChainHeadUpdateListener;

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        Ok(self.genesis_block_ptr)
    }

    fn upsert_blocks<'a, B: Stream<Item = EthereumBlock, Error = Error> + Send + 'a>(
        &self,
        blocks: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a> {
        use db_schema::ethereum_blocks::dsl::*;

        let conn = self.conn.clone();
        let net_name = self.network_name.clone();
        Box::new(blocks.for_each(move |block| {
            let json_blob = serde_json::to_value(&block).expect("Failed to serialize block");
            let values = (
                hash.eq(format!("{:#x}", block.block.hash.unwrap())),
                number.eq(block.block.number.unwrap().as_u64() as i64),
                parent_hash.eq(format!("{:#x}", block.block.parent_hash)),
                network_name.eq(&net_name),
                data.eq(json_blob),
            );

            insert_into(ethereum_blocks)
                .values(values.clone())
                .on_conflict(hash)
                .do_update()
                .set(values)
                .execute(&*conn.lock().unwrap())
                .map_err(Error::from)
                .map(|_| ())
        }))
    }

    fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error> {
        // Call attempt_head_update SQL function
        select(attempt_chain_head_update(&self.network_name, ancestor_count as i64))
            .load(&*self.conn.lock().unwrap())
            .map_err(Error::from)

            // We got a single return value, but it's returned generically as a set of rows
            .map(|mut rows: Vec<_>| {
                assert_eq!(rows.len(), 1);
                rows.pop().unwrap()
            })

            // Parse block hashes into H256 type
            .map(|hashes: Vec<String>| {
                hashes.into_iter()
                    .map(|h| h.parse())
                    .collect::<Result<Vec<H256>, _>>()
            })
            .and_then(|r| r.map_err(Error::from))
    }

    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener {
        Self::ChainHeadUpdateListener::new(self.url.clone(), self.network_name.clone())
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        use db_schema::ethereum_networks::dsl::*;

        ethereum_networks
            .select((head_block_hash, head_block_number))
            .filter(name.eq(&self.network_name))
            .load::<(Option<String>, Option<i64>)>(&*self.conn.lock().unwrap())
            .map(|rows| {
                rows.first()
                    .map(|(hash_opt, number_opt)| match (hash_opt, number_opt) {
                        (Some(hash), Some(number)) => Some((hash.parse().unwrap(), *number).into()),
                        (None, None) => None,
                        _ => unreachable!(),
                    }).and_then(|opt| opt)
            }).map_err(Error::from)
    }

    fn block(&self, block_hash: H256) -> Result<Option<EthereumBlock>, Error> {
        use db_schema::ethereum_blocks::dsl::*;

        ethereum_blocks
            .select(data)
            .filter(network_name.eq(&self.network_name))
            .filter(hash.eq(format!("{:x}", block_hash)))
            .load::<serde_json::Value>(&*self.conn.lock().unwrap())
            .map(|json_blocks| match json_blocks.len() {
                0 => None,
                1 => Some(
                    serde_json::from_value::<EthereumBlock>(json_blocks[0].clone())
                        .expect("Failed to deserialize block"),
                ),
                _ => unreachable!(),
            }).map_err(Error::from)
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
            .first::<Option<serde_json::Value>>(&*self.conn.lock().unwrap())
            .map(|val_opt| {
                val_opt.map(|val| {
                    serde_json::from_value::<EthereumBlock>(val)
                        .expect("Failed to deserialize block from database")
                })
            }).map_err(Error::from)
    }
}

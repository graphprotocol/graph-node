use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel::{debug_query, delete, insert_into, result, select, update};
use filter::store_filter;
use futures::sync::mpsc::{channel, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use uuid::Uuid;
use web3::types::Block;
use web3::types::H256;
use web3::types::Transaction;

use graph::components::store::{Store as StoreTrait, *};
use graph::prelude::*;
use graph::serde_json;
use graph::{tokio, tokio::timer::Interval};

use entity_changes::EntityChangeListener;
use functions::{attempt_head_update, revert_block, set_config};
use head_block_updates::HeadBlockUpdateListener;

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
#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub url: String,
    pub network_name: String,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    config: StoreConfig,
    logger: slog::Logger,
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    change_listener: EntityChangeListener,
    pub conn: Arc<Mutex<PgConnection>>,
}

impl Store {
    pub fn new(config: StoreConfig, logger: &slog::Logger) -> Result<Self, Error> {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Connect to Postgres
        let conn =
            PgConnection::establish(config.url.as_str()).expect("failed to connect to Postgres");

        info!(logger, "Connected to Postgres"; "url" => &config.url);

        // Create the entities table (if necessary)
        initiate_schema(&logger, &conn);

        // Create ethereum_networks entry (if necessary)
        use db_schema::ethereum_networks::dsl::*;
        insert_into(ethereum_networks)
            .values((
                name.eq(&config.network_name),
                head_block_hash.eq::<Option<String>>(None),
                head_block_number.eq::<Option<i64>>(None),
            ))
            .on_conflict(name)
            .do_nothing()
            .execute(&conn)?;

        // Listen to entity changes in Postgres
        let mut change_listener = EntityChangeListener::new(config.url.clone());
        let entity_changes = change_listener
            .take_event_stream()
            .expect("Failed to listen to entity change events in Postgres");

        // Create the store
        let mut store = Store {
            config,
            logger: logger.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            change_listener,
            conn: Arc::new(Mutex::new(conn)),
        };

        // Deal with store subscriptions
        store.handle_entity_changes(entity_changes);
        store.periodically_clean_up_stale_subscriptions();

        // We're ready for processing entity changes
        store.change_listener.start();

        // Return the store
        Ok(store)
    }

    /// Handles entity changes emitted by Postgres.
    fn handle_entity_changes(
        &mut self,
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
                })
                .map(|(id, subscription)| (id.clone(), subscription.sender.clone()))
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
                    })
                    .and_then(|_| Ok(()))
            })
        }));
    }

    fn periodically_clean_up_stale_subscriptions(&mut self) {
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
                        )
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

    /// Do not use.
    // TODO remove this, only here for compatibility with existing tests
    pub fn revert_events(&self, block_hash: String) {
        select(revert_block(block_hash, ""))
            .execute(&*self.conn.lock().unwrap())
            .unwrap();
    }

    /// Do not use.
    // TODO remove this, only here for compatibility with existing tests
    pub fn set(
        &mut self,
        key: StoreKey,
        entity: Entity,
        event_source: EventSource,
    ) -> Result<(), Error> {
        let subgraph_id = SubgraphId(key.subgraph.clone());
        self.deprecated_set(key, entity, event_source, self.block_ptr(subgraph_id)?)
    }

    /// Do not use.
    // TODO remove this, only here for compatibility with existing tests
    pub fn delete(&mut self, key: StoreKey, event_source: EventSource) -> Result<(), Error> {
        self.deprecated_delete(key, event_source)
    }

    // TODO replace with commit_transaction
    fn deprecated_set(
        &self,
        key: StoreKey,
        input_entity: Entity,
        input_event_source: EventSource,
        subgraph_block_ptr: EthereumBlockPointer,
    ) -> Result<(), Error> {
        debug!(self.logger, "set"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        // Update the existing entity, if necessary
        let updated_entity = match self.get(key.clone(), subgraph_block_ptr) {
            Ok(mut existing_entity) => {
                existing_entity.merge(input_entity);
                existing_entity
            }
            Err(_) => input_entity,
        };

        // Convert Entity hashmap to serde_json::Value for insert
        let entity_json: serde_json::Value =
            serde_json::to_value(&updated_entity).expect("Failed to serialize entity");

        // Insert entity, perform an update in case of a primary key conflict
        insert_into(entities)
            .values((
                id.eq(&key.id),
                entity.eq(&key.entity),
                subgraph.eq(&key.subgraph),
                data.eq(&entity_json),
                event_source.eq(&input_event_source.to_string()),
            ))
            .on_conflict((id, entity, subgraph))
            .do_update()
            .set((
                id.eq(&key.id),
                entity.eq(&key.entity),
                subgraph.eq(&key.subgraph),
                data.eq(&entity_json),
                event_source.eq(&input_event_source.to_string()),
            ))
            .execute(&*self.conn.lock().unwrap())
            .map(|_| ())
            .map_err(Error::from)
    }

    // TODO replace with commit_transaction
    fn deprecated_delete(
        &self,
        key: StoreKey,
        input_event_source: EventSource,
    ) -> Result<(), Error> {
        debug!(self.logger, "delete"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        let conn = self.conn.lock().unwrap();
        conn.transaction::<usize, result::Error, _>(|| {
            // Set session variable to store the source of the event
            select(set_config(
                "vars.current_event_source",
                input_event_source.to_string(),
                false,
            )).execute(&*conn)?;

            // Delete from DB where rows match the subgraph ID, entity name and ID
            delete(
                entities
                    .filter(subgraph.eq(&key.subgraph))
                    .filter(entity.eq(&key.entity))
                    .filter(id.eq(&key.id)),
            ).execute(&*conn)
        }).map(|_| ())
            .map_err(Error::from)
    }
}

impl BasicStore for Store {
    fn add_subgraph_if_missing(&self, subgraph_id: SubgraphId) -> Result<(), Error> {
        use db_schema::subgraphs::dsl::*;

        insert_into(subgraphs)
            .values((
                id.eq(&subgraph_id.0),
                network_name.eq(&self.config.network_name),
                // TODO genesis block hash... put this somewhere better?
                latest_block_hash
                    .eq::<&str>("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
                latest_block_number.eq::<i64>(0),
            ))
            .on_conflict(id)
            .do_nothing()
            .execute(&*self.conn.lock().unwrap())
            .map_err(Error::from)
            .map(|_| ())
    }

    fn block_ptr(&self, subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        use db_schema::subgraphs::dsl::*;

        subgraphs
            .select((latest_block_hash, latest_block_number))
            .filter(id.eq(subgraph_id.0))
            .first::<(String, i64)>(&*self.conn.lock().unwrap())
            .map(|(hash, number)| {
                (
                    hash.parse()
                        .expect("subgraph block ptr hash must be a valid H256"),
                    number,
                ).into()
            })
            .map_err(Error::from)
    }

    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphId,
        from: EthereumBlockPointer,
        to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        use db_schema::subgraphs::dsl::*;

        update(subgraphs)
            .set((
                latest_block_hash.eq(format!("{:x}", to.hash)),
                latest_block_number.eq(to.number as i64),
            ))
            .filter(id.eq(subgraph_id.0))
            .filter(latest_block_hash.eq(format!("{:x}", from.hash)))
            .filter(latest_block_number.eq(from.number as i64))
            .execute(&*self.conn.lock().unwrap())
            .map_err(Error::from)
            .map_err(StoreError::Database)
            .and_then(|row_count| match row_count {
                0 => Err(StoreError::VersionConflict),
                1 => Ok(()),
                _ => unreachable!(),
            })
    }

    fn revert_block(
        &self,
        subgraph_id: SubgraphId,
        block: Block<Transaction>,
    ) -> Result<(), StoreError> {
        // TODO make this atomic
        select(revert_block(
            block.hash.unwrap().to_string(),
            subgraph_id.0.clone(),
        )).execute(&*self.conn.lock().unwrap())
            .map(|_| ())
            .map_err(Error::from)
            .map_err(StoreError::Database)?;
        self.set_block_ptr_with_no_changes(
            subgraph_id,
            block.clone().into(),
            EthereumBlockPointer::to_parent(&block),
        )
    }

    fn get(&self, key: StoreKey, _block_ptr: EthereumBlockPointer) -> Result<Entity, StoreError> {
        debug!(self.logger, "get"; "key" => format!("{:?}", key));

        use db_schema::entities;
        use db_schema::entities::dsl::*;
        use db_schema::subgraphs;
        use db_schema::subgraphs::dsl::*;

        // Use primary key fields to get the entity; deserialize the result JSON
        entities
            .inner_join(subgraphs)
            .select((data, latest_block_hash, latest_block_number))
            .filter(entities::id.eq(key.id))
            .filter(entities::subgraph.eq(&key.subgraph))
            .filter(entities::entity.eq(key.entity))
            .filter(subgraphs::id.eq(&key.subgraph))
            .first::<(serde_json::Value, String, i64)>(&*self.conn.lock().unwrap())
            .map(|(value, _block_hash, _block_number)| {
                // TODO reenable this
                /*
                assert_eq!(
                    EthereumBlockPointer::from((block_hash.parse().unwrap(), block_number)),
                    block_ptr
                );
                */
                serde_json::from_value::<Entity>(value).expect("Failed to deserialize entity")
            })
            .map_err(Error::from)
            .map_err(StoreError::Database)
    }

    fn find(
        &self,
        query: StoreQuery,
        _block_ptr: EthereumBlockPointer,
    ) -> Result<Vec<Entity>, StoreError> {
        use db_schema::entities;
        use db_schema::entities::dsl::*;

        // TODO check block_ptr

        // Create base boxed query; this will be added to based on the
        // query parameters provided
        let mut diesel_query = entities
            .filter(entities::entity.eq(query.entity))
            .filter(entities::subgraph.eq(query.subgraph))
            .select(data)
            .into_boxed::<Pg>();

        // Add specified filter to query
        if let Some(filter) = query.filter {
            diesel_query = store_filter(diesel_query, filter).map_err(|e| {
                error!(self.logger, "value does not support this filter";
                                    "value" => format!("{:?}", e.value),
                                    "filter" => e.filter);
                StoreError::Database(format_err!("value does not support this filter"))
            })?;
        }

        // Add order by filters to query
        if let Some(order_attribute) = query.order_by {
            let direction = query
                .order_direction
                .map(|direction| match direction {
                    StoreOrder::Ascending => String::from("ASC"),
                    StoreOrder::Descending => String::from("DESC"),
                })
                .unwrap_or(String::from("ASC"));

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

        debug!(self.logger, "find";
                "sql" => format!("{:?}", debug_query::<Pg, _>(&diesel_query)));

        // Process results; deserialize JSON data
        diesel_query
            .load::<serde_json::Value>(&*self.conn.lock().unwrap())
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| {
                        serde_json::from_value::<Entity>(value)
                            .expect("Error to deserialize entity")
                    })
                    .collect()
            })
            .map_err(Error::from)
            .map_err(StoreError::Database)
    }

    fn commit_transaction(
        &self,
        _subgraph_id: SubgraphId,
        tx_ops: Vec<StoreOp>,
        block: Block<Transaction>,
    ) -> Result<(), StoreError> {
        let event_source = EventSource::EthereumBlock(block.hash.unwrap());

        // TODO this is not atomic
        let parent_block_ptr = EthereumBlockPointer::to_parent(&block);
        tx_ops
            .into_iter()
            .map(|op| match op {
                StoreOp::Set(key, entity) => {
                    self.deprecated_set(key, entity, event_source.clone(), parent_block_ptr)
                }
                StoreOp::Delete(key) => self.deprecated_delete(key, event_source.clone()),
            })
            .collect::<Result<Vec<_>, Error>>()
            .map(|_| ())
            .map_err(StoreError::Database)
        // TODO update block ptr
    }
}

impl BlockStore for Store {
    fn upsert_blocks<'a, B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a>(
        &self,
        blocks: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a> {
        use db_schema::ethereum_blocks::dsl::*;

        let conn = self.conn.clone();
        let net_name = self.config.network_name.clone();
        Box::new(blocks.for_each(move |block| {
            let block_json = serde_json::to_value(&block).expect("Failed to serialize block");
            let values = (
                hash.eq(format!("{:#x}", block.hash.unwrap())),
                number.eq(block.number.unwrap().as_u64() as i64),
                parent_hash.eq(format!("{:#x}", block.parent_hash)),
                network_name.eq(&net_name),
                block_data.eq(block_json),
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

    fn attempt_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error> {
        // Call attempt_head_update SQL function
        select(attempt_head_update(&self.config.network_name, ancestor_count as i64))
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

    fn head_block_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        use db_schema::ethereum_networks::dsl::*;

        ethereum_networks
            .select((head_block_hash, head_block_number))
            .filter(name.eq(&self.config.network_name))
            .load::<(Option<String>, Option<i64>)>(&*self.conn.lock().unwrap())
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

    fn head_block_updates(&self) -> Box<Stream<Item = HeadBlockUpdateEvent, Error = Error> + Send> {
        let mut listener =
            HeadBlockUpdateListener::new(self.config.url.clone(), self.config.network_name.clone());
        let updates = Box::new(
            listener
                .take_event_stream()
                .expect("Failed to listen to head block update events in Postgres")
                .map_err(|_| format_err!("error in head block update stream")),
        );
        listener.start();
        Box::leak(Box::new(listener)); // TODO need a better idea
        updates
    }

    fn block(&self, block_hash: H256) -> Result<Option<Block<Transaction>>, Error> {
        use db_schema::ethereum_blocks::dsl::*;

        ethereum_blocks
            .select(block_data)
            .filter(network_name.eq(&self.config.network_name))
            .filter(hash.eq(format!("{:x}", block_hash)))
            .load::<serde_json::Value>(&*self.conn.lock().unwrap())
            .map(|json_blocks| match json_blocks.len() {
                0 => None,
                1 => Some(
                    serde_json::from_value::<Block<Transaction>>(json_blocks[0].clone())
                        .expect("Failed to deserialize block"),
                ),
                _ => unreachable!(),
            })
            .map_err(Error::from)
    }

    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        mut offset: u64,
    ) -> Result<Option<Block<Transaction>>, Error> {
        if block_ptr.number < offset {
            bail!("block offset points to before genesis block");
        }

        // TODO do this in one query? not necessary but nice for perf

        let mut block_hash = block_ptr.hash;

        while offset > 0 {
            // Try to load block. If missing, return Ok(None).
            let block = match self.block(block_hash)? {
                None => return Ok(None),
                Some(b) => b,
            };

            block_hash = block.parent_hash;
            offset -= 1;
        }

        self.block(block_hash)
    }
}

impl StoreTrait for Store {
    fn subscribe(&mut self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
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

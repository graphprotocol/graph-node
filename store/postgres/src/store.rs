use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel::{delete, insert_into, select, update};
use filter::store_filter;
use futures::sync::mpsc::{channel, Sender};
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
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
use notification_listener::{NotificationListener, SafeChannelName};

embed_migrations!("./migrations");

/// Internal representation of a Store subscription.
struct Subscription {
    pub entities: Vec<SubgraphEntityPair>,
    pub sender: Sender<EntityChange>,
}

/// Run all initial schema migrations.
///
/// Creates the "entities" table if it doesn't already exist.
fn initiate_schema(logger: &Logger, conn: &PgConnection) {
    // Collect migration logging output
    let mut output = vec![];

    match embedded_migrations::run_with_output(conn, &mut output) {
        Ok(_) => info!(logger, "Completed pending Postgres schema migrations"),
        Err(e) => panic!("Error with Postgres schema setup: {:?}", e),
    }

    // If there was any migration output, log it now
    if !output.is_empty() {
        debug!(logger, "Postgres migration output";
               "output" => String::from_utf8(output).unwrap_or_else(|_| String::from("<unreadable>")));
    }
}

/// Configuration for the Diesel/Postgres store.
pub struct StoreConfig {
    pub postgres_url: String,
    pub network_name: String,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    logger: Logger,
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    change_listener: EntityChangeListener,
    postgres_url: String,
    network_name: String,
    genesis_block_ptr: EthereumBlockPointer,
    pub conn: Arc<Mutex<PgConnection>>,
}

impl Store {
    pub fn new(
        config: StoreConfig,
        logger: &Logger,
        net_identifiers: EthereumNetworkIdentifier,
    ) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Connect to Postgres
        let conn = PgConnection::establish(config.postgres_url.as_str())
            .expect("failed to connect to Postgres");

        info!(logger, "Connected to Postgres"; "url" => &config.postgres_url);

        // Create the entities table (if necessary)
        initiate_schema(&logger, &conn);

        // Listen to entity changes in Postgres
        let mut change_listener = EntityChangeListener::new(config.postgres_url.clone());
        let entity_changes = change_listener
            .take_event_stream()
            .expect("Failed to listen to entity change events in Postgres");

        // Create the store
        let mut store = Store {
            logger: logger.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            change_listener,
            postgres_url: config.postgres_url.clone(),
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
        new_net_identifiers: EthereumNetworkIdentifier,
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
                    ))
                    .on_conflict(name)
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
                    ))
                    .filter(name.eq(&self.network_name))
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
            trace!(logger, "Received entity change event";
                           "subgraph_id" => change.subgraph_id.to_string(),
                           "entity_type" => &change.entity_type,
                           "entity_id" => &change.entity_id);

            // Obtain IDs and senders of subscriptions matching the entity change
            let matches = subscriptions
                .read()
                .unwrap()
                .iter()
                .filter(|(_, subscription)| {
                    subscription
                        .entities
                        .contains(&(change.subgraph_id.clone(), change.entity_type.clone()))
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

    /// Gets an entity from Postgres, returns an entity with just an ID if none is found.
    fn get_entity(
        &self,
        conn: &PgConnection,
        op_subgraph: &SubgraphId,
        op_entity: &String,
        op_id: &String,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        use db_schema::entities::dsl::*;

        match entities
            .find((op_id, op_subgraph.to_string(), op_entity))
            .select(data)
            .first::<serde_json::Value>(conn)
            .optional()
            .map_err(|e| {
                QueryExecutionError::ResolveEntityError(
                    op_subgraph.clone(),
                    op_entity.clone(),
                    op_id.clone(),
                    format!("{}", e),
                )
            })? {
            Some(json) => serde_json::from_value::<Entity>(json)
                .map(Some)
                .map_err(|e| {
                    QueryExecutionError::ResolveEntityError(
                        op_subgraph.clone(),
                        op_entity.clone(),
                        op_id.clone(),
                        format!("Invalid entity: {}", e),
                    )
                }),
            None => Ok(None),
        }
    }

    /// Applies a set operation in Postgres.
    fn apply_set_operation(
        &self,
        conn: &PgConnection,
        operation: EntityOperation,
        op_event_source: EventSource,
    ) -> Result<(), Error> {
        assert!(operation.is_set());

        use db_schema::entities::dsl::*;

        let EntityKey {
            subgraph_id: op_subgraph_id,
            entity_type: op_entity_type,
            entity_id: op_entity_id,
        } = operation.entity_key();

        // Load the entity if exists
        let existing_entity =
            self.get_entity(conn, op_subgraph_id, op_entity_type, op_entity_id)?;

        // Apply the operation
        let updated_entity = operation.apply(existing_entity);
        let updated_json: serde_json::Value =
            serde_json::to_value(&updated_entity).map_err(|e| {
                format_err!(
                    "Failed to set entity ({}, {}, {}) as setting it would break it: {}",
                    op_subgraph_id,
                    op_entity_type,
                    op_entity_id,
                    e
                )
            })?;

        // Either add or update the entity in Postgres
        insert_into(entities)
            .values((
                id.eq(op_entity_id),
                entity.eq(op_entity_type),
                subgraph.eq(op_subgraph_id.to_string()),
                data.eq(&updated_json),
                event_source.eq(op_event_source.to_string()),
            ))
            .on_conflict((id, entity, subgraph))
            .do_update()
            .set((
                id.eq(op_entity_id),
                entity.eq(op_entity_type),
                subgraph.eq(op_subgraph_id.to_string()),
                data.eq(&updated_json),
                event_source.eq(op_event_source.to_string()),
            ))
            .execute(conn)
            .map(|_| ())
            .map_err(|e| {
                format_err!(
                    "Failed to set entity ({}, {}, {}): {}",
                    op_subgraph_id,
                    op_entity_type,
                    op_entity_id,
                    e
                )
            })
    }

    /// Applies a remove operation by deleting the entity from Postgres.
    fn apply_remove_operation(
        &self,
        conn: &PgConnection,
        operation: EntityOperation,
        op_event_source: EventSource,
    ) -> Result<(), Error> {
        use db_schema::entities::dsl::*;

        let EntityKey {
            subgraph_id: op_subgraph_id,
            entity_type: op_entity_type,
            entity_id: op_entity_id,
        } = operation.entity_key();

        select(set_config(
            "vars.current_event_source",
            op_event_source.to_string(),
            true,
        ))
        .execute(conn)
        .map_err(|e| format_err!("Failed to save event source for remove operation: {}", e))
        .map(|_| ())?;

        delete(
            entities
                .filter(subgraph.eq(op_subgraph_id.to_string()))
                .filter(entity.eq(op_entity_type))
                .filter(id.eq(op_entity_id)),
        )
        .execute(conn)
        .map(|_| ())
        .map_err(|e| {
            format_err!(
                "Failed to remove entity ({}, {}, {}): {}",
                op_subgraph_id,
                op_entity_type,
                op_entity_id,
                e
            )
        })
    }

    /// Apply an entity operation in Postgres.
    fn apply_entity_operation(
        &self,
        conn: &PgConnection,
        operation: EntityOperation,
        event_source: EventSource,
    ) -> Result<(), Error> {
        match operation {
            EntityOperation::Set { .. } => self.apply_set_operation(conn, operation, event_source),
            EntityOperation::Remove { .. } => {
                self.apply_remove_operation(conn, operation, event_source)
            }
        }
    }

    /// Apply a series of entity operations in Postgres.
    fn apply_entity_operations_with_conn(
        &self,
        conn: &PgConnection,
        operations: Vec<EntityOperation>,
        event_source: EventSource,
    ) -> Result<(), Error> {
        for operation in operations.into_iter() {
            self.apply_entity_operation(conn, operation, event_source)?;
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
            ))
            .filter(id.eq(subgraph_id.to_string()))
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
                id.eq(subgraph_id.to_string()),
                network_name.eq(&self.network_name),
                latest_block_hash.eq(block_ptr.hash_hex()),
                latest_block_number.eq(block_ptr.number as i64),
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
            .filter(id.eq(subgraph_id.to_string()))
            .first::<(String, i64)>(&*self.conn.lock().unwrap())
            .map(|(hash, number)| {
                (
                    hash.parse()
                        .expect("subgraph block ptr hash in database should be a valid H256"),
                    number,
                )
                    .into()
            })
            .map_err(Error::from)
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let conn = self.conn.lock().unwrap();
        self.get_entity(&*conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        use db_schema::entities::dsl::*;

        // Create base boxed query; this will be added to based on the
        // query parameters provided
        let mut diesel_query = entities
            .filter(entity.eq(query.entity_type))
            .filter(subgraph.eq(query.subgraph_id.to_string()))
            .select(data)
            .into_boxed::<Pg>();

        // Add specified filter to query
        if let Some(filter) = query.filter {
            diesel_query = store_filter(diesel_query, filter).map_err(|e| {
                QueryExecutionError::FilterNotSupportedError(format!("{}", e.value), e.filter)
            })?;
        }

        // Add order by filters to query
        if let Some((order_attribute, value_type)) = query.order_by {
            let direction = query
                .order_direction
                .map(|direction| match direction {
                    EntityOrder::Ascending => "ASC",
                    EntityOrder::Descending => "DESC",
                })
                .unwrap_or("ASC");
            let cast_type = match value_type {
                ValueType::BigInt => "::numeric",
                ValueType::Boolean => "::boolean",
                ValueType::Bytes => "",
                ValueType::Float => "::float",
                ValueType::ID => "",
                ValueType::Int => "::bigint",
                ValueType::String => "",
            };
            diesel_query = diesel_query.order(
                sql::<Text>("(data ->")
                    .bind::<Text, _>(order_attribute)
                    .sql("->> 'data')")
                    .sql(cast_type)
                    .sql(" ")
                    .sql(direction)
                    .sql(" NULLS LAST"),
            );
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
                        serde_json::from_value::<Entity>(value).expect("Error parsing entity JSON")
                    })
                    .collect()
            })
            .map_err(|e| QueryExecutionError::ResolveEntitiesError(e.to_string()))
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
        // Sanity check on block numbers
        if block_ptr_from.number != block_ptr_to.number - 1 {
            panic!("transact_block_operations must transact a single block only");
        }

        // All operations should apply only to entities in this subgraph
        for op in &operations {
            if op.entity_key().subgraph_id != subgraph_id {
                panic!("transact_block_operations must affect only entities in the subgraph");
            }
        }

        // Fold the operations of each entity into a single one
        let operations = EntityOperation::fold(&operations);

        let conn = self.conn.lock().unwrap();

        conn.transaction(|| {
            let event_source = EventSource::EthereumBlock(block_ptr_to);
            self.apply_entity_operations_with_conn(&*conn, operations, event_source)?;
            self.update_subgraph_block_pointer(&*conn, subgraph_id, block_ptr_from, block_ptr_to)
        })
    }

    fn apply_entity_operations(
        &self,
        operations: Vec<EntityOperation>,
        event_source: EventSource,
    ) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.transaction(|| self.apply_entity_operations_with_conn(&conn, operations, event_source))
    }

    fn revert_block_operations(
        &self,
        subgraph_id: SubgraphId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), Error> {
        // Sanity check on block numbers
        if block_ptr_from.number != block_ptr_to.number + 1 {
            panic!("revert_block_operations must revert a single block only");
        }

        select(revert_block(
            &block_ptr_from.hash_hex(),
            block_ptr_from.number as i64,
            &block_ptr_to.hash_hex(),
            subgraph_id.to_string(),
        ))
        .execute(&*self.conn.lock().unwrap())
        .map_err(|e| format_err!("Error reverting block: {}", e))
        .map(|_| ())
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

    fn count_entities(&self, subgraph_id: SubgraphId) -> Result<u64, Error> {
        use db_schema::entities::dsl::*;

        let count: i64 = entities
            .filter(subgraph.eq(subgraph_id.to_string()))
            .count()
            .get_result(&*self.conn.lock().unwrap())?;
        Ok(count as u64)
    }
}

impl SubgraphDeploymentStore for Store {
    fn read_by_node_id(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<(SubgraphDeploymentName, SubgraphId)>, Error> {
        use db_schema::subgraph_deployments;

        subgraph_deployments::table
            .select((
                subgraph_deployments::deployment_name,
                subgraph_deployments::subgraph_id,
            ))
            .filter(subgraph_deployments::node_id.eq(node_id.to_string()))
            .load::<(String, String)>(&*self.conn.lock().unwrap())
            .map_err(Error::from)
            .map(|rows| {
                rows.into_iter()
                    .map(|(name, subgraph_id)| {
                        let name = SubgraphDeploymentName::new(name)
                            .expect("invalid subgraph name found in database");
                        let subgraph_id = SubgraphId::new(subgraph_id)
                            .expect("invalid subgraph ID found in database");
                        (name, subgraph_id)
                    })
                    .collect()
            })
    }

    fn write(
        &self,
        name: SubgraphDeploymentName,
        subgraph_id: SubgraphId,
        node_id: NodeId,
    ) -> Result<(), Error> {
        use db_schema::subgraph_deployments;

        insert_into(subgraph_deployments::table)
            .values((
                subgraph_deployments::deployment_name.eq(name.to_string()),
                subgraph_deployments::subgraph_id.eq(subgraph_id.to_string()),
                subgraph_deployments::node_id.eq(node_id.to_string()),
            ))
            .on_conflict(subgraph_deployments::deployment_name)
            .do_update()
            .set((
                subgraph_deployments::subgraph_id.eq(subgraph_id.to_string()),
                subgraph_deployments::node_id.eq(node_id.to_string()),
            ))
            .execute(&*self.conn.lock().unwrap())
            .map_err(Error::from)
            .map(|_| ())
    }

    fn read(&self, name: SubgraphDeploymentName) -> Result<Option<(SubgraphId, NodeId)>, Error> {
        use db_schema::subgraph_deployments;

        subgraph_deployments::table
            .select((
                subgraph_deployments::subgraph_id,
                subgraph_deployments::node_id,
            ))
            .filter(subgraph_deployments::deployment_name.eq(name.to_string()))
            .first::<(String, String)>(&*self.conn.lock().unwrap())
            .optional()
            .map_err(Error::from)
            .map(|row_opt| {
                row_opt.map(|(subgraph_id, node_id)| {
                    let subgraph_id = SubgraphId::new(subgraph_id)
                        .expect("invalid subgraph ID found in database");
                    let node_id = NodeId::new(node_id).expect("invalid node ID found in database");
                    (subgraph_id, node_id)
                })
            })
    }

    fn remove(&self, name: SubgraphDeploymentName) -> Result<bool, Error> {
        use db_schema::subgraph_deployments;

        delete(subgraph_deployments::table.find(name.to_string()))
            .execute(&*self.conn.lock().unwrap())
            .map(|row_count| match row_count {
                0 => false,
                1 => true,
                _ => unreachable!(),
            })
            .map_err(Error::from)
    }

    fn deployment_events(
        &self,
        node_id: NodeId,
    ) -> Box<Stream<Item = DeploymentEvent, Error = Error> + Send> {
        // Create postgres listener
        // This way of choosing a channel name is only safe because NodeId is restricted to
        // alphanumeric and underscores, and because we forcibly prefix the node ID (which prevents
        // the channel name from being a keyword).
        let raw_channel_name = format!("subgraph_deployments_{}", node_id);
        let channel_name = SafeChannelName::i_promise_this_is_safe(raw_channel_name);
        let mut listener = NotificationListener::new(self.postgres_url.clone(), channel_name);

        // Start receiving notifications
        listener.start();

        Box::new(
            listener
                .take_event_stream()
                .unwrap()
                .map(move |notification| {
                    // Move listener into closure so that listener lives as long as stream does
                    let _ = listener;

                    // Parse notification as JSON
                    let value: serde_json::Value = serde_json::from_str(&notification.payload)
                        .expect("invalid JSON deployment event received from database");

                    // Create DeploymentEvent from JSON
                    let update: DeploymentEvent = serde_json::from_value(value.clone())
                        .unwrap_or_else(|_| {
                            panic!(
                                "invalid deployment event received from database: {:?}",
                                value
                            )
                        });

                    update
                })
                .map_err(|()| format_err!("deployment event notification listener failed")),
        )
    }
}

impl ChainStore for Store {
    type ChainHeadUpdateListener = ChainHeadUpdateListener;

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        Ok(self.genesis_block_ptr)
    }

    fn upsert_blocks<'a, B, E>(&self, blocks: B) -> Box<Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a,
    {
        use db_schema::ethereum_blocks::dsl::*;

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
                .execute(&*conn.lock().unwrap())
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
        .load(&*self.conn.lock().unwrap())
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

    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener {
        Self::ChainHeadUpdateListener::new(self.postgres_url.clone(), self.network_name.clone())
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
                    })
                    .and_then(|opt| opt)
            })
            .map_err(Error::from)
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
            })
            .map_err(Error::from)
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
            })
            .map_err(Error::from)
    }
}
